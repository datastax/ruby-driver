# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require 'spec_helper'

module Cassandra
  class Cluster
    describe(ControlConnection) do
      let :control_connection do
        ControlConnection.new(logger, io_reactor, cluster_registry, cluster_schema, cluster_metadata, load_balancing_policy, reconnection_policy, address_resolution_policy, driver.connector, connection_options, schema_fetcher)
      end

      let :io_reactor do
        FakeIoReactor.new
      end

      let :cluster_registry do
        driver.cluster_registry
      end

      let :cluster_schema do
        driver.cluster_schema
      end

      let(:cluster_metadata) { driver.cluster_metadata }

      let :logger do
        driver.logger
      end

      let :driver do
        Driver.new(protocol_version: nil, io_reactor: io_reactor)
      end

      let :load_balancing_policy do
        driver.load_balancing_policy
      end

      let :connection_options do
        driver.connection_options
      end

      let :reconnection_policy do
        driver.reconnection_policy
      end

      let(:address_resolution_policy) { driver.address_resolution_policy }

      let(:schema_fetcher) { driver.schema_fetcher }

      def connections
        io_reactor.connections
      end

      def last_connection
        connections.last
      end

      def requests
        last_connection.requests
      end

      def last_request
        requests.last
      end

      def handle_request(&handler)
        @request_handler = handler
      end

      let :local_info do
        {
          'data_center' => 'dc1',
          'host_id' => nil,
        }
      end

      let :local_metadata do
        [
          ['system', 'local', 'data_center', :text],
          ['system', 'local', 'host_id', :uuid],
        ]
      end

      let :peer_metadata do
        [
          ['system', 'peers', 'peer', :inet],
          ['system', 'peers', 'data_center', :varchar],
          ['system', 'peers', 'host_id', :uuid],
          ['system', 'peers', 'rpc_address', :inet],
        ]
      end

      let :data_centers do
        ::Hash.new('dc1')
      end

      let :racks do
        ::Hash.new('rack1')
      end

      let :tokens do
        Hash.new(['token1'])
      end

      let :release_versions do
        ::Hash.new('2.0.7-SNAPSHOT')
      end

      let :host_ids do
        ::Hash.new {|hash, ip| hash[ip] = uuid_generator.uuid}
      end

      let :additional_nodes do
        ::Array.new(5) { ::IPAddr.new("127.0.#{rand(255)}.#{rand(255)}") }
      end

      let :bind_all_rpc_addresses do
        false
      end

      let :min_peers do
        [2]
      end

      let :uuid_generator do
        Uuid::Generator.new
      end

      before do
        driver.connection_options.protocol_version = 7
        cluster_registry.add_listener(driver.load_balancing_policy)
        cluster_registry.add_listener(control_connection)
        cluster_registry.host_found('127.0.0.1')
        io_reactor.connection_options = connection_options
        io_reactor.on_connection do |connection|
          connection[:spec_rack]            = racks[connection.host]
          connection[:spec_data_center]     = data_centers[connection.host]
          connection[:spec_host_id]         = host_ids[connection.host]
          connection[:spec_release_version] = release_versions[connection.host]
          connection[:spec_tokens]          = tokens[connection.host]

          connection.handle_request do |request, timeout|
            additional_rpc_addresses = additional_nodes.dup
            if @request_handler
              response = @request_handler.call(request, connection, proc { connection.default_request_handler(request) }, timeout)
            end

            response ||= case request
            when Protocol::StartupRequest, Protocol::RegisterRequest
              Protocol::ReadyResponse.new
            when Protocol::QueryRequest
              response = case request.cql
              when /USE\s+"?(\S+)"?/
                Cassandra::Protocol::SetKeyspaceResultResponse.new(nil, nil, $1, nil)
              when /FROM system\.local/
                row = {
                  'rack'            => connection[:spec_rack],
                  'data_center'     => connection[:spec_data_center],
                  'host_id'         => connection[:spec_host_id],
                  'release_version' => connection[:spec_release_version],
                  'tokens'          => connection[:spec_tokens]
                }
                Protocol::RowsResultResponse.new(nil, nil, [row], local_metadata, nil, nil)
              when /FROM system\.peers WHERE peer = '?(\S+)'/
                ip   = $1
                rows = [
                  {
                    'rack'            => racks[ip],
                    'data_center'     => data_centers[ip],
                    'host_id'         => host_ids[ip],
                    'release_version' => release_versions[ip],
                    'tokens'          => tokens[ip]
                  }
                ]
                Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
              when /FROM system\.peers/
                rows = min_peers[0].times.map do |host_id|
                  ip = additional_rpc_addresses.shift
                  {
                    'peer'            => ip,
                    'rack'            => racks[ip],
                    'data_center'     => data_centers[ip],
                    'host_id'         => host_ids[ip],
                    'rpc_address'     => bind_all_rpc_addresses ? ::IPAddr.new('0.0.0.0') : ip,
                    'release_version' => release_versions[ip],
                    'tokens'          => tokens[ip]
                  }
                end
                Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
              end
            when Protocol::OptionsRequest
              Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.0.0], 'COMPRESSION' => %w[lz4 snappy])
            end

            response ||= connection.default_request_handler(request)

            response
          end
        end
      end

      describe "#connect_async" do
        it 'tries decreasing protocol versions until one succeeds' do
          counter = 0
          handle_request do |request|
            if counter < 3
              counter += 1
              Protocol::ErrorResponse.new(nil, nil, 0x0a, 'Bork version, dummy!')
            elsif counter == 3
              counter += 1
              Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.0.0], 'COMPRESSION' => %w[lz4 snappy])
            end
          end

          control_connection.connect_async.value

          expect(connection_options.protocol_version).to eq(4)
        end

        it 'logs when it tries the next protocol version' do
          logger.stub(:info)
          counter = 0
          handle_request do |request|
            if counter < 3
              counter += 1
              Protocol::ErrorResponse.new(nil, nil, 0x0a, 'Bork version, dummy!')
            elsif counter == 3
              counter += 1
              Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.0.0], 'COMPRESSION' => %w[lz4 snappy])
            end
          end

          control_connection.connect_async.value
          expect(logger).to have_received(:info).with("Host 127.0.0.1 doesn't support protocol version 7, downgrading")
        end

        it 'gives up when the protocol version is zero' do
          counter = 0
          handle_request do |request|
            counter += 1
            Protocol::ErrorResponse.new(nil, nil, 0x0a, 'Bork version, dummy!')
          end
          expect { control_connection.connect_async.value }.to raise_error(Cassandra::Errors::ProtocolError, 'Bork version, dummy!')
          counter.should == 7
        end

        it 'gives up when the protocol version is non-negotiable' do

          driver = Driver.new(protocol_version: 3, io_reactor: io_reactor)
          io_reactor.connection_options = driver.connection_options
          control_conn = ControlConnection.new(driver.logger,
                                               driver.io_reactor,
                                               driver.cluster_registry,
                                               driver.cluster_schema,
                                               driver.cluster_metadata,
                                               driver.load_balancing_policy,
                                               driver.reconnection_policy,
                                               driver.address_resolution_policy,
                                               driver.connector,
                                               driver.connection_options,
                                               driver.schema_fetcher)

          driver.cluster_registry.add_listener(driver.load_balancing_policy)
          driver.cluster_registry.add_listener(control_conn)
          driver.cluster_registry.host_found('127.0.0.1')

          counter = 0
          handle_request do |request|
            counter += 1
            Protocol::ErrorResponse.new(nil, nil, 0x0a, 'Bork version, dummy!')
          end
          expect { control_conn.connect_async.value }.to raise_error(Cassandra::Errors::ProtocolError, 'Bork version, dummy!')
          expect(counter).to eq(1)
        end

        it 'gives up when a non-protocol version related error is raised' do
          handle_request do |request|
            Protocol::ErrorResponse.new(nil, nil, 0x1001, 'Get off my lawn!')
          end
          expect { control_connection.connect_async.value }.to raise_error(Cassandra::Errors::NoHostsAvailable)
        end

        it 'fails authenticating when an auth provider has been specified but the protocol is negotiated to v1' do
          driver.protocol_version = 1
          driver.auth_provider    = double(:auth_provider)

          counter = 0
          handle_request do |request|
            case request
            when Protocol::OptionsRequest
              Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.0.0], 'COMPRESSION' => %w[lz4 snappy])
            when Protocol::StartupRequest
              Protocol::AuthenticateResponse.new('org.apache.cassandra.auth.PasswordAuthenticator')
            end
          end
          expect { control_connection.connect_async.value }.to raise_error(Errors::AuthenticationError)
        end

        it 'registers an event listener' do
          control_connection.connect_async.value
          last_connection.should have_event_listener
        end

        it 'populates cluster state' do
          control_connection.connect_async.value
          expect(cluster_registry).to have(3).hosts

          cluster_registry.hosts.each do |host|
            ip = host.ip
            host.rack.should == racks[ip]
            host.datacenter.should == data_centers[ip]
            host.release_version.should == release_versions[ip]
            host.tokens.should == tokens[ip]
          end
        end

        context 'with empty peers' do
          it 'skips empty peers' do
            additional_rpc_addresses = additional_nodes.dup

            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[0]).and_return(additional_rpc_addresses[0])
            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[1]).and_return(additional_rpc_addresses[1])

            # RUBY-255: We should never try to do address resolution of nil.
            expect(address_resolution_policy).to_not receive(:resolve).with(nil)

            # We should give up on a peer if 'peer' is empty. This is indicated by *not* doing
            # an address resolution of rpc-address.
            expect(address_resolution_policy).to_not receive(:resolve).with('127.1.2.3')

            handle_request do |request|
              case request
              when Protocol::QueryRequest
                case request.cql
                when /FROM system\.peers/
                  rows = min_peers[0].times.map do
                    ip = additional_rpc_addresses.shift
                    {
                      'peer'            => ip,
                      'rack'            => racks[ip],
                      'data_center'     => data_centers[ip],
                      'host_id'         => host_ids[ip],
                      'rpc_address'     => bind_all_rpc_addresses ? ::IPAddr.new('0.0.0.0') : ip,
                      'release_version' => release_versions[ip],
                      'tokens'          => tokens[ip]
                    }
                  end

                  rows << {
                      'peer'            => nil,
                      'rack'            => racks['127.1.2.3'],
                      'data_center'     => data_centers['127.1.2.3'],
                      'host_id'         => host_ids['127.1.2.3'],
                      'rpc_address'     => '127.1.2.3',
                      'release_version' => release_versions['127.1.2.3'],
                      'tokens'          => tokens['127.1.2.3']
                  }
                  Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                end
              end
            end

            control_connection.connect_async.value

            expect(cluster_registry).to have(3).hosts
          end
        end

        context 'with empty rack' do
          it 'skips empty rack' do
            additional_rpc_addresses = additional_nodes.dup

            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[0]).and_return(additional_rpc_addresses[0])
            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[1]).and_return(additional_rpc_addresses[1])

            # RUBY-255: We should never try to do address resolution of nil.
            expect(address_resolution_policy).to_not receive(:resolve).with(nil)

            # We should give up on a peer if 'rpc-address' is empty. This is indicated by *not* doing
            # an address resolution of peer.
            expect(address_resolution_policy).to_not receive(:resolve).with('127.1.2.3')

            handle_request do |request|
              case request
                when Protocol::QueryRequest
                  case request.cql
                    when /FROM system\.peers/
                      rows = min_peers[0].times.map do
                        ip = additional_rpc_addresses.shift
                        {
                            'peer'            => ip,
                            'rack'            => racks[ip],
                            'data_center'     => data_centers[ip],
                            'host_id'         => host_ids[ip],
                            'rpc_address'     => bind_all_rpc_addresses ? IPAddr.new('0.0.0.0') : ip,
                            'release_version' => release_versions[ip],
                            'tokens'          => tokens[ip]
                        }
                      end

                      rows << {
                          'peer'            => '127.1.2.3',
                          'rack'            => nil,
                          'data_center'     => data_centers['127.1.2.3'],
                          'host_id'         => host_ids['127.1.2.3'],
                          'rpc_address'     => '127.1.2.3',
                          'release_version' => release_versions['127.1.2.3'],
                          'tokens'          => tokens['127.1.2.3']
                      }
                      Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                  end
              end
            end

            control_connection.connect_async.value

            expect(cluster_registry).to have(3).hosts
          end
        end

        context 'with empty data_center' do
          it 'skips empty data_center' do
            additional_rpc_addresses = additional_nodes.dup

            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[0]).and_return(additional_rpc_addresses[0])
            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[1]).and_return(additional_rpc_addresses[1])

            # RUBY-255: We should never try to do address resolution of nil.
            expect(address_resolution_policy).to_not receive(:resolve).with(nil)

            # We should give up on a peer if 'rpc-address' is empty. This is indicated by *not* doing
            # an address resolution of peer.
            expect(address_resolution_policy).to_not receive(:resolve).with('127.1.2.3')

            handle_request do |request|
              case request
                when Protocol::QueryRequest
                  case request.cql
                    when /FROM system\.peers/
                      rows = min_peers[0].times.map do
                        ip = additional_rpc_addresses.shift
                        {
                            'peer'            => ip,
                            'rack'            => racks[ip],
                            'data_center'     => data_centers[ip],
                            'host_id'         => host_ids[ip],
                            'rpc_address'     => bind_all_rpc_addresses ? IPAddr.new('0.0.0.0') : ip,
                            'release_version' => release_versions[ip],
                            'tokens'          => tokens[ip]
                        }
                      end

                      rows << {
                          'peer'            => '127.1.2.3',
                          'rack'            => racks['127.1.2.3'],
                          'data_center'     => nil,
                          'host_id'         => host_ids['127.1.2.3'],
                          'rpc_address'     => '127.1.2.3',
                          'release_version' => release_versions['127.1.2.3'],
                          'tokens'          => tokens['127.1.2.3']
                      }
                      Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                  end
              end
            end

            control_connection.connect_async.value

            expect(cluster_registry).to have(3).hosts
          end
        end

        context 'with empty host_id' do
          it 'skips empty host_id' do
            additional_rpc_addresses = additional_nodes.dup

            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[0]).and_return(additional_rpc_addresses[0])
            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[1]).and_return(additional_rpc_addresses[1])

            # RUBY-255: We should never try to do address resolution of nil.
            expect(address_resolution_policy).to_not receive(:resolve).with(nil)

            # We should give up on a peer if 'rpc-address' is empty. This is indicated by *not* doing
            # an address resolution of peer.
            expect(address_resolution_policy).to_not receive(:resolve).with('127.1.2.3')

            handle_request do |request|
              case request
                when Protocol::QueryRequest
                  case request.cql
                    when /FROM system\.peers/
                      rows = min_peers[0].times.map do
                        ip = additional_rpc_addresses.shift
                        {
                            'peer'            => ip,
                            'rack'            => racks[ip],
                            'data_center'     => data_centers[ip],
                            'host_id'         => host_ids[ip],
                            'rpc_address'     => bind_all_rpc_addresses ? IPAddr.new('0.0.0.0') : ip,
                            'release_version' => release_versions[ip],
                            'tokens'          => tokens[ip]
                        }
                      end

                      rows << {
                          'peer'            => '127.1.2.3',
                          'rack'            => racks['127.1.2.3'],
                          'data_center'     => data_centers['127.1.2.3'],
                          'host_id'         => nil,
                          'rpc_address'     => '127.1.2.3',
                          'release_version' => release_versions['127.1.2.3'],
                          'tokens'          => tokens['127.1.2.3']
                      }
                      Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                  end
              end
            end

            control_connection.connect_async.value

            expect(cluster_registry).to have(3).hosts
          end
        end

        context 'with empty rpc_address' do
          it 'skips empty rpc_address' do
            additional_rpc_addresses = additional_nodes.dup

            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[0]).and_return(additional_rpc_addresses[0])
            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[1]).and_return(additional_rpc_addresses[1])

            # RUBY-255: We should never try to do address resolution of nil.
            expect(address_resolution_policy).to_not receive(:resolve).with(nil)

            # We should give up on a peer if 'rpc-address' is empty. This is indicated by *not* doing
            # an address resolution of peer.
            expect(address_resolution_policy).to_not receive(:resolve).with('127.1.2.3')

            handle_request do |request|
              case request
                when Protocol::QueryRequest
                  case request.cql
                    when /FROM system\.peers/
                      rows = min_peers[0].times.map do
                        ip = additional_rpc_addresses.shift
                        {
                            'peer'            => ip,
                            'rack'            => racks[ip],
                            'data_center'     => data_centers[ip],
                            'host_id'         => host_ids[ip],
                            'rpc_address'     => bind_all_rpc_addresses ? ::IPAddr.new('0.0.0.0') : ip,
                            'release_version' => release_versions[ip],
                            'tokens'          => tokens[ip]
                        }
                      end

                      rows << {
                          'peer'            => '127.1.2.3',
                          'rack'            => racks['127.1.2.3'],
                          'data_center'     => data_centers['127.1.2.3'],
                          'host_id'         => host_ids['127.1.2.3'],
                          'rpc_address'     => nil,
                          'release_version' => release_versions['127.1.2.3'],
                          'tokens'          => tokens['127.1.2.3']
                      }
                      Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                  end
              end
            end

            control_connection.connect_async.value

            expect(cluster_registry).to have(3).hosts
          end
        end

        context 'with empty tokens' do
          it 'skips empty tokens' do
            additional_rpc_addresses = additional_nodes.dup

            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[0]).and_return(additional_rpc_addresses[0])
            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[1]).and_return(additional_rpc_addresses[1])

            # RUBY-255: We should never try to do address resolution of nil.
            expect(address_resolution_policy).to_not receive(:resolve).with(nil)

            # We should give up on a peer if 'rpc-address' is empty. This is indicated by *not* doing
            # an address resolution of peer.
            expect(address_resolution_policy).to_not receive(:resolve).with('127.1.2.3')

            handle_request do |request|
              case request
                when Protocol::QueryRequest
                  case request.cql
                    when /FROM system\.peers/
                      rows = min_peers[0].times.map do
                        ip = additional_rpc_addresses.shift
                        {
                            'peer'            => ip,
                            'rack'            => racks[ip],
                            'data_center'     => data_centers[ip],
                            'host_id'         => host_ids[ip],
                            'rpc_address'     => bind_all_rpc_addresses ? ::IPAddr.new('0.0.0.0') : ip,
                            'release_version' => release_versions[ip],
                            'tokens'          => tokens[ip]
                        }
                      end

                      rows << {
                          'peer'            => '127.1.2.3',
                          'rack'            => racks['127.1.2.3'],
                          'data_center'     => data_centers['127.1.2.3'],
                          'host_id'         => host_ids['127.1.2.3'],
                          'rpc_address'     => '127.1.2.3',
                          'release_version' => release_versions['127.1.2.3'],
                          'tokens'          => nil
                      }
                      Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                  end
              end
            end

            control_connection.connect_async.value

            expect(cluster_registry).to have(3).hosts
          end
        end

        context 'with local node' do
          it 'skips matching peer' do
            additional_rpc_addresses = additional_nodes.dup

            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[0]).and_return(additional_rpc_addresses[0])
            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[1]).and_return(additional_rpc_addresses[1])

            # RUBY-255: We should never try to do address resolution of nil.
            expect(address_resolution_policy).to_not receive(:resolve).with(nil)

            # We should give up on a peer if its rpc_address is the local host. This is indicated by *not* doing
            # an address resolution of it.
            expect(address_resolution_policy).to_not receive(:resolve).with('127.0.0.9')
            expect(address_resolution_policy).to_not receive(:resolve).with('127.0.0.1')

            handle_request do |request|
              case request
                when Protocol::QueryRequest
                  case request.cql
                    when /FROM system\.peers/
                      rows = min_peers[0].times.map do
                        ip = additional_rpc_addresses.shift
                        {
                            'peer'            => ip,
                            'rack'            => racks[ip],
                            'data_center'     => data_centers[ip],
                            'host_id'         => host_ids[ip],
                            'rpc_address'     => bind_all_rpc_addresses ? IPAddr.new('0.0.0.0') : ip,
                            'release_version' => release_versions[ip],
                            'tokens'          => tokens[ip]
                        }
                      end

                      rows << {
                          'peer'            => connections.first.host,
                          'rack'            => racks['127.0.0.9'],
                          'data_center'     => data_centers['127.0.0.9'],
                          'host_id'         => host_ids['127.0.0.9'],
                          'rpc_address'     => bind_all_rpc_addresses ? IPAddr.new('0.0.0.0') : '127.0.0.9',
                          'release_version' => release_versions['127.0.0.9'],
                          'tokens'          => tokens['127.0.0.9']
                      }
                      Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                  end
              end
            end

            control_connection.connect_async.value

            expect(cluster_registry).to have(3).hosts
          end

          it 'skips matching rpc_address' do
            additional_rpc_addresses = additional_nodes.dup

            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[0]).and_return(additional_rpc_addresses[0])
            expect(address_resolution_policy).to receive(:resolve).with(additional_rpc_addresses[1]).and_return(additional_rpc_addresses[1])

            # RUBY-255: We should never try to do address resolution of nil.
            expect(address_resolution_policy).to_not receive(:resolve).with(nil)

            # We should give up on a peer if its rpc_address is the local host. This is indicated by *not* doing
            # an address resolution of it.
            expect(address_resolution_policy).to_not receive(:resolve).with('127.0.0.9')
            expect(address_resolution_policy).to_not receive(:resolve).with('127.0.0.1')

            handle_request do |request|
              case request
                when Protocol::QueryRequest
                  case request.cql
                    when /FROM system\.peers/
                      rows = min_peers[0].times.map do
                        ip = additional_rpc_addresses.shift
                        {
                            'peer'            => ip,
                            'rack'            => racks[ip],
                            'data_center'     => data_centers[ip],
                            'host_id'         => host_ids[ip],
                            'rpc_address'     => bind_all_rpc_addresses ? IPAddr.new('0.0.0.0') : ip,
                            'release_version' => release_versions[ip],
                            'tokens'          => tokens[ip]
                        }
                      end

                      rows << {
                          'peer'            => '127.0.0.9',
                          'rack'            => racks['127.0.0.9'],
                          'data_center'     => data_centers['127.0.0.9'],
                          'host_id'         => host_ids['127.0.0.9'],
                          'rpc_address'     => connections.first.host,
                          'release_version' => release_versions['127.0.0.9'],
                          'tokens'          => tokens['127.0.0.9']
                      }
                      Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                  end
              end
            end

            control_connection.connect_async.value

            expect(cluster_registry).to have(3).hosts
          end
        end

        context 'when the nodes have 0.0.0.0 as rpc_address' do
          let :bind_all_rpc_addresses do
            true
          end

          it 'falls back on using the peer column' do
            control_connection.connect_async.value
            cluster_registry.should have(3).hosts

            cluster_registry.hosts.each do |host|
              ip = host.ip
              host.rack.should == racks[ip]
              host.datacenter.should == data_centers[ip]
              host.release_version.should == release_versions[ip]
              host.tokens.should == tokens[ip]
            end
          end
        end

        context 'when connection closed' do
          let(:reconnect_interval)    { 5 }
          let(:reconnection_schedule) { double('reconnection schedule', :next => reconnect_interval) }

          before do
            reconnection_policy.stub(:schedule) { reconnection_schedule }
            control_connection.connect_async.value
          end

          it 'reconnects' do
            last_connection.close
            io_reactor.advance_time(reconnect_interval)
            last_connection.should be_connected
          end

          context 'and reconnected' do
            it 'has an event listener' do
              control_connection.connect_async.value
              last_connection.should have_event_listener
            end
          end

          context 'and all hosts are down' do
            before do
              cluster_registry.each_host.each do |host|
                io_reactor.node_down(host.ip.to_s)
              end

              connections.each do |connection|
                connection.close
              end
            end

            it 'keeps trying until some host comes up' do
              rand(10).times { io_reactor.advance_time(reconnect_interval) }

              last_connection.should_not be_connected

              io_reactor.node_up('127.0.0.1')
              io_reactor.advance_time(reconnect_interval)
              last_connection.should be_connected
            end
          end
        end

        context 'registered for events' do
          let :registry do
            double("registry stub")
          end

          before do
            control_connection.connect_async.value
          end

          context 'when a status change event is received' do
            let :event do
              Protocol::StatusChangeEventResponse.new(change, address, 9999)
            end

            context 'with UP' do
              let :change do
                'UP'
              end

              let :address do
                ::IPAddr.new('127.0.0.1')
              end

              it 'logs when it receives an UP event' do
                logger.stub(:debug)
                cluster_registry.stub(:host_up)
                connections.first.trigger_event(event)
                logger.should have_received(:debug).with(/Event received EVENT STATUS_CHANGE UP/)
              end

              context 'and host is known' do
                before do
                  cluster_registry.stub(:has_host?) { true }
                end

                let :address do
                  additional_nodes[0]
                end

                it 'notifies registry' do
                  ip = address.to_s
                  expect(cluster_registry).to receive(:host_up).once.with(address)
                  connections.first.trigger_event(event)
                end
              end

              context 'and host is unknown' do
                before do
                  cluster_registry.stub(:has_host?) { false }
                end

                let :address do
                  additional_nodes[3]
                end

                it 'refreshes metadata and notifies registry' do
                  ip = address.to_s
                  expect(cluster_registry).to receive(:host_found).once.with(address, {
                    'rack'            => racks[ip],
                    'data_center'     => data_centers[ip],
                    'host_id'         => host_ids[ip],
                    'release_version' => release_versions[ip],
                    'tokens'          => tokens[ip]
                  })

                  connections.first.trigger_event(event)
                end
              end
            end

            context 'with DOWN' do
              let :change do
                'DOWN'
              end

              let :address do
                '127.0.0.1'
              end

              it 'logs when it receives an DOWN event' do
                logger.stub(:debug)
                connections.first.trigger_event(event)
                logger.should have_received(:debug).with(/Event received EVENT STATUS_CHANGE DOWN/)
              end

              it 'does not notify registry' do
                expect(cluster_registry).to_not receive(:host_down)
                connections.first.trigger_event(event)
              end
            end
          end

          context 'when a topology change event is received' do
            let :event do
              Protocol::TopologyChangeEventResponse.new(change, address, 9999)
            end

            context 'with NEW_NODE' do
              let :change do
                'NEW_NODE'
              end

              let :address do
                '127.0.0.1'
              end

              it 'logs when it receives an NEW_NODE event' do
                logger.stub(:debug)
                connections.first.trigger_event(event)
                logger.should have_received(:debug).with(/Event received EVENT TOPOLOGY_CHANGE NEW_NODE/)
              end

              context 'and host is unknown' do
                let :address do
                  additional_nodes[3]
                end

                before do
                  cluster_registry.stub(:has_host?) { false }
                end

                it 'notifies registry' do
                  ip = address.to_s
                  expect(cluster_registry).to receive(:host_found).once.with(address, {
                    'rack'            => racks[ip],
                    'data_center'     => data_centers[ip],
                    'host_id'         => host_ids[ip],
                    'release_version' => release_versions[ip],
                    'tokens'          => tokens[ip]
                  })
                  connections.first.trigger_event(event)
                end
              end

              context 'and host is known' do
                let :address do
                  additional_nodes[0]
                end

                before do
                  cluster_registry.stub(:has_host?) { true }
                end

                it 'does nothing' do
                  expect(cluster_registry).to_not receive(:host_found)

                  connections.first.trigger_event(event)
                end
              end

              context 'and host not found in system tables' do
                let(:address)               { additional_nodes[3] }
                let(:reconnect_interval)    { 5 }
                let(:reconnection_schedule) { double('reconnection schedule', :next => reconnect_interval) }

                before do
                  attempts = 0
                  handle_request do |request|
                    case request
                    when Protocol::QueryRequest
                      case request.cql
                      when /FROM system\.peers WHERE peer = '?(\S+)'/
                        if attempts >= 1
                          ip   = $1
                          rows = [
                            {
                              'rack'            => racks[ip],
                              'data_center'     => data_centers[ip],
                              'host_id'         => host_ids[ip],
                              'release_version' => release_versions[ip],
                              'tokens'          => tokens[ip]
                            }
                          ]
                          Protocol::RowsResultResponse.new(nil, nil, rows, peer_metadata, nil, nil)
                        else
                          attempts += 1
                          Protocol::RowsResultResponse.new(nil, nil, [], peer_metadata, nil, nil)
                        end
                      end
                    end
                  end

                  reconnection_policy.stub(:schedule) { reconnection_schedule }
                  cluster_registry.stub(:has_host?) { false }
                end

                it 'tries again' do
                  connections.first.trigger_event(event)
                  ip = address.to_s
                  expect(cluster_registry).to receive(:host_found).once.with(address, {
                    'rack'            => racks[ip],
                    'data_center'     => data_centers[ip],
                    'host_id'         => host_ids[ip],
                    'release_version' => release_versions[ip],
                    'tokens'          => tokens[ip]
                  })
                  io_reactor.advance_time(reconnect_interval)
                end
              end
            end

            context 'with REMOVED_NODE' do
              let :change do
                'REMOVED_NODE'
              end

              let :address do
                '127.0.0.1'
              end

              it 'logs when it receives an REMOVED_NODE event' do
                logger.stub(:debug)
                connections.first.trigger_event(event)
                logger.should have_received(:debug).with(/Event received EVENT TOPOLOGY_CHANGE REMOVED_NODE/)
              end

              it 'notifies registry' do
                ip = address.to_s
                expect(cluster_registry).to receive(:host_lost).once.with(ip)
                connections.first.trigger_event(event)
              end
            end
          end
        end
      end

      describe "#close_async" do
        context 'when connected' do
          before do
            control_connection.connect_async.value
          end

          it 'closes reactor' do
            future = double('close future')

            expect(future).to receive(:on_value)
            expect(future).to receive(:on_failure)
            io_reactor.should_receive(:stop).once.and_return(future)
            control_connection.close_async
          end

          it 'calls close listeners' do
            called = false

            control_connection.on_close do
              called = true
            end

            expect(io_reactor).to receive(:stop).once.and_return(Ione::Future.resolved)
            control_connection.close_async.value

            expect(called).to be_truthy
          end
        end

        context 'not connected' do
          before do
            control_connection.connect_async.value
            control_connection.close_async.value
          end

          it 'returns a fulfilled future' do
            future = control_connection.close_async
            future.should be_resolved
            future.value.should be_nil
          end
        end

        context 'when reconnecting' do
          let(:reconnect_interval)    { 5 }
          let(:reconnection_schedule) { double('reconnection schedule', :next => reconnect_interval) }

          before do
            reconnection_policy.stub(:schedule) { reconnection_schedule }
            control_connection.connect_async.value

            cluster_registry.each_host do |host|
              io_reactor.node_down(host.ip.to_s)
            end

            last_connection.close
          end

          it 'stops reconnecting' do
            connections.select(&:connected?).should be_empty
            control_connection.close_async

            cluster_registry.each_host do |host|
              io_reactor.node_up(host.ip.to_s)
            end

            io_reactor.advance_time(reconnect_interval)
            io_reactor.advance_time(reconnect_interval)

            connections.select(&:connected?).should be_empty
          end
        end
      end

      describe 'managing full schema refresh' do
        let(:promise) { Ione::Promise.new }
        let(:future) { Ione::CompletableFuture.new }
        let(:schema_refresh_timer) { 'refresh_timer'}
        let(:schema_refresh_window) { 'refresh_window'}
        before do
          control_connection.stub(:refresh_maybe_retry) { future }
          io_reactor.stub(:cancel_timer)
          io_reactor.stub(:schedule_timer) { Ione::CompletableFuture.new }
        end

        describe "#refresh_schema_async_wrapper" do

          it 'should refresh schema if a refresh is not in progress' do
            control_connection.send(:refresh_schema_async_wrapper)
            expect(control_connection).to have_received(:refresh_maybe_retry).once
          end

          it 'should not refresh schema if a refresh is in progress' do
            control_connection.send(:refresh_schema_async_wrapper)
            control_connection.send(:refresh_schema_async_wrapper)
            expect(control_connection).to have_received(:refresh_maybe_retry).once
          end

          it 'should clear pending schema changes and schema timers when starting a new refresh' do
            # Put some junk in @schema_changes
            control_connection.instance_variable_set(:@schema_changes, ['foo'])
            control_connection.instance_variable_set(:@schema_refresh_window,
                                                     schema_refresh_window)
            control_connection.instance_variable_set(:@schema_refresh_timer,
                                                     schema_refresh_timer)
            control_connection.send(:refresh_schema_async_wrapper)

            expect(control_connection.instance_variable_get(:@schema_changes)).to be_empty
            expect(control_connection.instance_variable_get(:@schema_refresh_timer)).
                to be_nil
            expect(control_connection.instance_variable_get(:@schema_refresh_window)).
                to be_nil
            expect(io_reactor).to have_received(:cancel_timer).twice
          end

          it 'should not clear pending schema changes when a refresh is in progress' do
            control_connection.send(:refresh_schema_async_wrapper)

            # Put some junk in @schema_changes
            control_connection.instance_variable_set(:@schema_changes, ['foo'])

            control_connection.send(:refresh_schema_async_wrapper)

            expect(control_connection.instance_variable_get(:@schema_changes)).to eq(['foo'])
          end

          it 'should launch another refresh after current one completes when needed' do
            # A follow-on refresh is needed if we try to refresh twice: the first time
            # starts a refresh, the second returns the cached future. However, when the
            # future is completed, a new refresh triggers.

            future1 = control_connection.send(:refresh_schema_async_wrapper)
            future2 = control_connection.send(:refresh_schema_async_wrapper)
            expect(future1).to be(future2)

            # Upto this point, only one refresh should have fired.
            expect(control_connection).to have_received(:refresh_maybe_retry).once

            # Resolve the future and another refresh should fire.
            future1.resolve('foo')

            expect(control_connection).to have_received(:refresh_maybe_retry).twice
          end

          it 'should not launch a follow-on refresh if not needed' do
            future1 = control_connection.send(:refresh_schema_async_wrapper)

            # Upto this point, only one refresh should have fired.
            expect(control_connection).to have_received(:refresh_maybe_retry).once

            # Resolve the future and another refresh should not fire since no one asked
            # for a refresh after the first.
            future1.resolve('foo')

            expect(control_connection).to have_received(:refresh_maybe_retry).once
          end

          it 'should not restore refresh timers if not needed after full refresh completes' do
            future1 = control_connection.send(:refresh_schema_async_wrapper)

            # Upto this point, only one refresh should have fired.
            expect(control_connection).to have_received(:refresh_maybe_retry).once

            # Resolve the future and another refresh should not fire since no one asked
            # for a refresh after the first.
            future1.resolve('foo')

            expect(control_connection).to have_received(:refresh_maybe_retry).once
            expect(io_reactor).to_not have_received(:schedule_timer)
          end

          it 'should restore refresh timers after refresh completes if no follow on refresh' do
            future = control_connection.send(:refresh_schema_async_wrapper)

            # Upto this point, only one refresh should have fired.
            expect(control_connection).to have_received(:refresh_maybe_retry).once

            # Create a pending schema-change
            control_connection.instance_variable_set(:@schema_changes, ['foo'])

            # Resolve the future and another refresh should not fire since no one asked
            # for a refresh after the first.
            future.resolve('foo')

            expect(control_connection).to have_received(:refresh_maybe_retry).once
            expect(io_reactor).to have_received(:schedule_timer).twice
          end
        end

        describe '#handle_schema_change' do
          it 'should not enable refresh timers if full refresh is in progress' do
            control_connection.send(:refresh_schema_async_wrapper)

            control_connection.send(:handle_schema_change, 'foo')

            expect(io_reactor).to_not have_received(:schedule_timer)
          end

          it 'should enable refresh timers if a full refresh is not in progress' do
            control_connection.send(:handle_schema_change, 'foo')

            expect(io_reactor).to have_received(:schedule_timer).twice
          end
        end
      end
    end
  end
end
