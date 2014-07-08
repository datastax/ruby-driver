# encoding: utf-8

require 'spec_helper'

module Cql
  class Cluster
    describe(ControlConnection) do
      let :control_connection do
        described_class.new(io_reactor, request_runner, cluster_state, builder_settings)
      end

      let :io_reactor do
        FakeIoReactor.new
      end

      let :cluster_state do
        State.new({'127.0.0.1' => Cluster::Host.new('127.0.0.1')})
      end

      let :request_runner do
        Client::RequestRunner.new
      end

      let :logger do
        Client::NullLogger.new
      end

      let :builder_settings do
        Builder::Settings.new(Set.new, 9042, 7, 10, :one, logger, nil, nil, nil)
      end

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

      before do
        io_reactor.on_connection do |connection|
          connection.handle_request do |request, timeout|
            response = nil
            if @request_handler
              response = @request_handler.call(request, connection, proc { connection.default_request_handler(request) }, timeout)
            end
            unless response
              response = connection.default_request_handler(request)
            end
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
              Protocol::ErrorResponse.new(0x0a, 'Bork version, dummy!')
            elsif counter == 3
              counter += 1
              Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.0.0], 'COMPRESSION' => %w[lz4 snappy])
            else
              Protocol::RowsResultResponse.new([], [], nil, nil)
            end
          end

          control_connection.connect_async.get
          builder_settings.protocol_version.should == 4
        end

        it 'logs when it tries the next protocol version' do
          logger.stub(:warn)
          counter = 0
          handle_request do |request|
            if counter < 3
              counter += 1
              Protocol::ErrorResponse.new(0x0a, 'Bork version, dummy!')
            elsif counter == 3
              counter += 1
              Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.0.0], 'COMPRESSION' => %w[lz4 snappy])
            else
              Protocol::RowsResultResponse.new([], [], nil, nil)
            end
          end


          control_connection.connect_async.get
          logger.should have_received(:warn).with(/could not connect using protocol version 7 \(will try again with 6\): bork version, dummy!/i)
        end

        it 'gives up when the protocol version is zero' do
          counter = 0
          handle_request do |request|
            counter += 1
            Protocol::ErrorResponse.new(0x0a, 'Bork version, dummy!')
          end
          expect { control_connection.connect_async.get }.to raise_error(NoHostsAvailable)
          counter.should == 7
        end

        it 'gives up when a non-protocol version related error is raised' do
          handle_request do |request|
            Protocol::ErrorResponse.new(0x1001, 'Get off my lawn!')
          end
          expect { control_connection.connect_async.get }.to raise_error(NoHostsAvailable) do |e|
            e.errors.should have(1).error
            e.errors.values.first.message.should match(/Get off my lawn/)
          end
        end


        it 'fails authenticating when an auth provider has been specified but the protocol is negotiated to v1' do
          builder_settings.protocol_version = 2
          builder_settings.auth_provider    = double(:auth_provider)

          counter = 0
          handle_request do |request|
            if counter == 0
              counter += 1
              Protocol::ErrorResponse.new(0x0a, 'Bork version, dummy!')
            else
              case request
              when Protocol::OptionsRequest
                Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.0.0], 'COMPRESSION' => %w[lz4 snappy])
              when Protocol::StartupRequest
                Protocol::AuthenticateResponse.new('org.apache.cassandra.auth.PasswordAuthenticator')
              end
            end
          end
          expect { control_connection.connect_async.get }.to raise_error(NoHostsAvailable) do |e|
            e.errors.should have(1).error
            $stderr.p e.errors.values.first
            e.errors.values.first.should be_a(AuthenticationError)
          end

          counter.should == 1
        end
      end

      describe "#refresh_hosts_async" do
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
          Hash.new('dc1')
        end

        let :racks do
          Hash.new('rack1')
        end

        let :release_versions do
          Hash.new('2.0.7-SNAPSHOT')
        end

        let :additional_nodes do
          Array.new(5) { IPAddr.new("127.0.#{rand(255)}.#{rand(255)}") }
        end

        let :bind_all_rpc_addresses do
          false
        end

        let :min_peers do
          [2]
        end

        before do
          uuid_generator = TimeUuid::Generator.new
          additional_rpc_addresses = additional_nodes.dup
          io_reactor.on_connection do |connection|
            connection[:spec_rack]            = racks[connection.host]
            connection[:spec_data_center]     = data_centers[connection.host]
            connection[:spec_host_id]         = uuid_generator.next
            connection[:spec_release_version] = release_versions[connection.host]

            connection.handle_request do |request|
              case request
              when Protocol::StartupRequest
                Protocol::ReadyResponse.new
              when Protocol::QueryRequest
                case request.cql
                when /USE\s+"?(\S+)"?/
                  Cql::Protocol::SetKeyspaceResultResponse.new($1, nil)
                when /FROM system\.local/
                  row = {
                    'rack'            => connection[:spec_rack],
                    'data_center'     => connection[:spec_data_center],
                    'host_id'         => connection[:spec_host_id],
                    'release_version' => connection[:spec_release_version]
                  }
                  Protocol::RowsResultResponse.new([row], local_metadata, nil, nil)
                when /FROM system\.peers/
                  other_host_ids = connections.reject { |c| c[:spec_host_id] == connection[:spec_host_id] }.map { |c| c[:spec_host_id] }
                  until other_host_ids.size >= min_peers[0]
                    other_host_ids << uuid_generator.next
                  end
                  rows = other_host_ids.map do |host_id|
                    ip = additional_rpc_addresses.shift
                    {
                      'peer'            => ip,
                      'rack'            => racks[ip],
                      'data_center'     => data_centers[ip],
                      'host_id'         => host_id,
                      'rpc_address'     => bind_all_rpc_addresses ? IPAddr.new('0.0.0.0') : ip,
                      'release_version' => release_versions[ip]
                    }
                  end
                  Protocol::RowsResultResponse.new(rows, peer_metadata, nil, nil)
                end
              end
            end
          end

          control_connection.connect_async.get
        end

        it 'populates cluster state with other hosts' do
          control_connection.refresh_hosts_async.get
          cluster_state.hosts.should have(3).items

          cluster_state.hosts.each do |(ip, host)|
            host.ip.should == ip
            host.rack.should == racks[ip]
            host.datacenter.should == data_centers[ip]
            host.release_version.should == release_versions[ip]
          end
        end

        context 'when the nodes have 0.0.0.0 as rpc_address' do
          let :bind_all_rpc_addresses do
            true
          end

          it 'falls back on using the peer column' do
            control_connection.refresh_hosts_async.get
            cluster_state.hosts.should have(3).items

            cluster_state.hosts.each do |(ip, host)|
              host.ip.should == ip
              host.rack.should == racks[ip]
              host.datacenter.should == data_centers[ip]
              host.release_version.should == release_versions[ip]
            end
          end
        end
      end
    end
  end
end
