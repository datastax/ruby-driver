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
    describe(Client) do
      let(:hosts) { ['127.0.0.1', '127.0.0.2'] }
      let(:io_reactor) { FakeIoReactor.new }
      let(:reconnection_policy) { Reconnection::Policies::Exponential.new(0.5, 30, 2) }
      let(:load_balancing_policy) { FakeLoadBalancingPolicy.new(cluster_registry) }
      let(:cluster_registry) { FakeClusterRegistry.new(hosts) }
      let(:logger) { Cassandra::NullLogger.new }
      let(:driver_settings)  { {
                                 :io_reactor => io_reactor,
                                 :load_balancing_policy => load_balancing_policy,
                                 :cluster_registry => cluster_registry,
                                 :connections_per_local_node => 2,
                                 :connections_per_remote_node => 1,
                                 :reconnection_policy => reconnection_policy,
                                 :executor => Executors::SameThread.new,
                                 :logger => logger,
                                 :protocol_version => 2
                               } }

      let(:driver) { Driver.new(driver_settings) }
      let(:client) { Client.new(driver.logger, driver.cluster_registry, driver.cluster_schema, driver.io_reactor, driver.connector, driver.profile_manager, driver.reconnection_policy, driver.address_resolution_policy, driver.connection_options, driver.futures_factory, driver.timestamp_generator) }
      let(:execution_options) { driver.execution_options }

      before do
        io_reactor.connection_options = driver.connection_options
      end

      describe('defaults') do
        context 'for driver' do
          it 'should use local_one consistency' do
            expect(driver.consistency).to eq(:local_one)
          end
        end
      end

      describe('#connect') do
        context 'when all hosts are ignored' do
          before do
            load_balancing_policy.stub(:distance) { :ignore }
          end

          it 'fails' do
            expect { client.connect.value }.to raise_error(Errors::NoHostsAvailable)
          end
        end

        it 'creates connections to each host based on distance' do
          client.connect.value
          expect(io_reactor).to have(4).connections
        end

        it 'gets distance from profile-manager' do
          driver.profile_manager.stub(:distance) { :remote }
          client.connect.value
          expect(io_reactor).to have(2).connections
        end

        it 'can be called multiple times' do
          future = client.connect
          expect(future).to eq(client.connect)
        end

        it 'starts listening to cluster registry' do
          client.connect.value
          expect(cluster_registry).to have(1).listeners
          expect(cluster_registry.listeners).to include(client)
        end

        it 'succeeds even if some of the connections failed' do
          io_reactor.node_down(hosts.first)
          client.connect.value
          expect(io_reactor).to have(2).connections
        end

        it 'fails if all hosts are down' do
          hosts.each {|host| io_reactor.node_down(host)}
          expect do
            client.connect.value
          end.to raise_error(Cassandra::Errors::NoHostsAvailable)
        end

        context('when connections become idle') do
          let(:io_reactor)          { StubIoReactor.new }
          let(:port)                { '9042' }
          let(:reconnection_policy) { Reconnection::Policies::Constant.new(reconnect_interval) }
          let(:driver_settings)     { {
                                       :io_reactor => io_reactor,
                                       :connections_per_local_node => 2,
                                       :connections_per_remote_node => 1,
                                       :reconnection_policy => reconnection_policy,
                                       :executor => Executors::SameThread.new,
                                       :heartbeat_interval => heartbeat_interval,
                                       :idle_timeout => idle_timeout,
                                       :logger => logger,
                                       :port => port,
                                       :protocol_version => 2
                                    } }
          let(:heartbeat_interval)  { 30 }
          let(:idle_timeout)        { 60 }
          let(:reconnect_interval)  { 5  }
          let(:cluster_registry)    { driver.cluster_registry }

          it 'sets host to down when all its connections are idle' do
            cluster_registry.add_listener(driver.load_balancing_policy)
            cluster_registry.host_found(::IPAddr.new(hosts.first))
            io_reactor.enable_node(hosts.first)

            client.connect.value
            expect(io_reactor).to have(2).connections
            expect(cluster_registry.host(hosts.first)).to be_up

            io_reactor.block_node(hosts.first)
            io_reactor.advance_time(idle_timeout)

            expect(io_reactor).to have(0).connections
            expect(cluster_registry.host(hosts.first)).to be_down

            io_reactor.unblock_node(hosts.first)
            cluster_registry.host_up(::IPAddr.new(hosts.first))
            io_reactor.advance_time(reconnect_interval)
            sleep(2)

            expect(io_reactor).to have(2).connections
            expect(cluster_registry.host(hosts.first)).to be_up
          end

          it 'automatically replaces hanged connections' do
            cluster_registry.add_listener(driver.load_balancing_policy)
            cluster_registry.host_found(::IPAddr.new(hosts.first))
            io_reactor.enable_node(hosts.first)

            client.connect.value
            expect(io_reactor).to have(2).connections

            connection = io_reactor.connections.first
            connection.block

            io_reactor.advance_time(idle_timeout / 2)
            sleep(2)
            io_reactor.advance_time(idle_timeout / 2)

            expect(io_reactor).to have(2).connections
            expect(io_reactor.connections).to_not include(connection)
          end

          it 'fails when cannot fully connect to any hosts' do
            cluster_registry.add_listener(driver.load_balancing_policy)
            cluster_registry.host_found(::IPAddr.new(hosts.first))
            io_reactor.enable_node(hosts.first)
            io_reactor.set_max_connections(hosts.first, 1)

            expect(io_reactor).to have(0).connections
            expect { client.connect.value }.to raise_error(Cassandra::Errors::NoHostsAvailable)
            expect(io_reactor).to have(1).connections
            io_reactor.advance_time(0)
            expect(io_reactor).to have(0).connections
          end

          it 'succeeds when can fully connect to at least one host' do
            cluster_registry.add_listener(driver.load_balancing_policy)
            cluster_registry.host_found(::IPAddr.new(hosts.first))
            io_reactor.enable_node(hosts.first)
            cluster_registry.host_found(::IPAddr.new(hosts.last))
            io_reactor.enable_node(hosts.last)
            io_reactor.set_max_connections(hosts.first, 1)

            expect(io_reactor).to have(0).connections
            client.connect.value
            expect(io_reactor).to have(3).connections
            io_reactor.advance_time(0)
            expect(io_reactor).to have(3).connections

            io_reactor.unset_max_connections(hosts.first)
            io_reactor.advance_time(reconnect_interval)
            expect(io_reactor).to have(4).connections
          end
        end
      end

      describe('#close') do
        it 'closes all connections' do
          client.connect.value
          client.close.value
          expect(io_reactor).to have(4).connections
          expect(io_reactor.connections.select(&:connected?)).to be_empty
        end

        it 'stop listening to cluster registry' do
          client.connect.value
          client.close.value
          expect(cluster_registry.listeners).to be_empty
        end

        context 'with error' do
          it 'logs error' do
            logger.stub(:error)
            client.connect.value
            io_reactor.connections.first.stub(:close).and_return(Ione::Future.failed(StandardError.new('Hurgh blurgh')))
            client.close.value rescue nil
            logger.should have_received(:error).with(/Session failed to close \(StandardError: Hurgh blurgh\)/)
          end
        end

        context 'when connecting' do
          it 'schedules close after connect' do
            future   = Ione::CompletableFuture.new
            complete = false

            io_reactor.stub(:connect) { future }
            client.connect
            client.close.on_complete { complete = true }
            expect(complete).to be_falsey
            future.resolve(nil)
            expect(complete).to be_truthy
          end
        end
      end

      describe('#host_up') do
        before do
          client.connect.value
        end

        context 'when host is ignored by load balancing policy' do
          it 'ignores it' do
            load_balancing_policy.stub(:distance) { :ignore }
            expect do
              client.host_up(Cassandra::Host.new('1.1.1.1'))
            end.to_not change { io_reactor.connections.size }
          end
        end

        context 'when host is local' do
          it 'connects to it the right number of times' do
            load_balancing_policy.stub(:distance) { :local }
            expect do
              client.host_up(Cassandra::Host.new('1.1.1.1'))
            end.to change { io_reactor.connections.size }.from(4).to(6)
          end
        end

        context 'when host is remote' do
          it 'connects to it the right number of times' do
            load_balancing_policy.stub(:distance) { :remote }
            expect do
              client.host_up(Cassandra::Host.new('1.1.1.1'))
            end.to change { io_reactor.connections.size }.from(4).to(5)
          end
        end

        it 'gets distance from policy_manager' do
          driver.profile_manager.stub(:distance) { :remote }
          expect do
            client.host_up(Cassandra::Host.new('1.1.1.1'))
          end.to change { io_reactor.connections.size }.from(4).to(5)
        end

        context 'host not responding' do
          let(:address) { '1.1.1.1' }
          let(:host)    { Host.new(address) }

          before do
            client.connect.value
            io_reactor.node_down(address)
            load_balancing_policy.stub(:distance) { :local }
          end

          it 'keeps trying until host responds' do
            reconnect_interval = 5
            schedule = double('reconnection schedule', :next => reconnect_interval)
            expect(reconnection_policy).to receive(:schedule).once.and_return(schedule)

            expect do
              client.host_up(host)
            end.to_not change { io_reactor.connections.size }

            expect do
              5.times { io_reactor.advance_time(reconnect_interval) }
            end.to_not change { io_reactor.connections.size }

            expect do
              io_reactor.node_up(address)
              io_reactor.advance_time(reconnect_interval)
            end.to change { io_reactor.connections.size }.by(2)
          end

          it 'logs reconnection attempts' do
            logger.stub(:debug)
            logger.stub(:warn)

            io_reactor.node_down(address)
            cluster_registry.hosts.each { |host| io_reactor.node_down(host.ip.to_s) }

            client.host_up(host)

            logger.should have_received(:warn).with("Host 1.1.1.1 refused all connections").at_least(1).times
            logger.should have_received(:debug).with(/Reconnecting to (.*) in (.*) seconds/)
          end
        end
      end

      describe('#host_down') do
        context 'when connected to it' do
          before do
            client.connect.value
          end

          it 'does not clear prepared-statements cache when there are no connections' do
            h = Host.new('127.0.0.1')
            client.host_down(h)
            expect(client.instance_variable_get(:@prepared_statements).key?(h)).to be true
            expect(client.instance_variable_get(:@preparing_statements).key?(h)).to be true
          end

          it 'does clear prepared-statements cache if there are no connections' do
            h = Host.new('127.0.0.1')

            # This cheats a little, but it's much easier than concocting a realistic scenario using only public
            # interfaces.
            conns = client.instance_variable_get(:@connections)
            conns[h] = double("ConnectionPool")
            expect(conns[h]).to receive(:size).and_return(0)
            expect(conns[h]).to receive(:snapshot).and_return([])
            client.host_down(h)
            expect(client.instance_variable_get(:@prepared_statements).key?(h)).to be false
            expect(client.instance_variable_get(:@preparing_statements).key?(h)).to be false
          end
        end

        context 'when reconnecting to it' do
          let(:reconnect_interval) { 5 }

          before do
            client.connect.value
            load_balancing_policy.stub(:distance) { :local }
          end

          it 'stops reconnecting' do
            host = Host.new('1.1.1.1')

            io_reactor.node_down('1.1.1.1')

            reconnect_interval = 5
            schedule = double('reconnection schedule')
            reconnection_policy.stub(:schedule) { schedule }

            expect(schedule).to receive(:next).once.and_return(reconnect_interval)

            client.host_up(host)
            io_reactor.node_up('1.1.1.1')
            client.host_down(host)

            expect(schedule).to_not receive(:next)

            expect do
              io_reactor.advance_time(reconnect_interval)
            end.to_not change { io_reactor.connections.size }
          end
        end
      end

      describe('#query') do
        it 'sends a QueryRequest' do
          handled = false

          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  handled = true
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                else
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('SELECT * FROM songs'), execution_options).get

          expect(handled).to be_truthy
        end

        it 'switches keyspace of new connections automatically' do
          count = 0
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                when 'USE foo'
                  count += 1
                  Cassandra::Protocol::SetKeyspaceResultResponse.new(nil, nil, 'foo', false)
                else
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('USE foo'), execution_options).get
          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(cluster_registry.hosts.first)
          client.query(Statements::Simple.new('SELECT * FROM songs'), execution_options).get

          expect(count).to eq(2)
        end

        it 'correctly escapes keyspace name when automatically switching' do
          count = 0
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                when 'USE "FooBar"'
                  count += 1
                  Cassandra::Protocol::SetKeyspaceResultResponse.new(nil, nil, 'FooBar', false)
                else
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('USE "FooBar"'), execution_options).get
          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(cluster_registry.hosts.first)
          client.query(Statements::Simple.new('SELECT * FROM songs'), execution_options).get

          expect(count).to eq(2)
        end

        it 'follows the plan on failure' do
          count    = 0
          attempts = []
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  attempts << connection.host
                  if count == 0
                    count += 1
                    raise Cassandra::Errors::InternalError.new
                  else
                    Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                  end
                else
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('SELECT * FROM songs'), execution_options).get
          expect(attempts).to have(2).items
          expect(attempts).to eq(hosts)
        end

        it 'does not retry on client timeout error if statement is not idempotent' do
          attempts = []
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
                when Cassandra::Protocol::OptionsRequest
                  Cassandra::Protocol::SupportedResponse.new({})
                when Cassandra::Protocol::StartupRequest
                  Cassandra::Protocol::ReadyResponse.new
                when Cassandra::Protocol::QueryRequest
                  case request.cql
                    when 'SELECT * FROM songs'
                      attempts << connection.host
                        raise Cassandra::Errors::TimeoutError.new
                    else
                      Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                  end
              end
            end
          end
          client.connect.value
          future = client.query(Statements::Simple.new('SELECT * FROM songs'),
                                execution_options)
          got_excp = false
          future.on_failure do |ex|
            got_excp = true
            expect(ex).to be_a(Cassandra::Errors::TimeoutError)
          end
          expect(got_excp)
          expect(attempts).to have(1).items
          expect(attempts.first).to eq(hosts.first)
        end

        it 'retries on client timeout error if statement is idempotent' do
          count    = 0
          attempts = []
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
                when Cassandra::Protocol::OptionsRequest
                  Cassandra::Protocol::SupportedResponse.new({})
                when Cassandra::Protocol::StartupRequest
                  Cassandra::Protocol::ReadyResponse.new
                when Cassandra::Protocol::QueryRequest
                  case request.cql
                    when 'SELECT * FROM songs'
                      attempts << connection.host
                      if count == 0
                        count += 1
                        raise raise Cassandra::Errors::TimeoutError.new
                      else
                        Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                      end
                    else
                      Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                  end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('SELECT * FROM songs', nil, nil, true),
                       execution_options).get
          expect(attempts).to have(2).items
          expect(attempts).to eq(hosts)
        end

        it 'raises if all hosts failed' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  raise Cassandra::Errors::InternalError.new
                else
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          expect do
            client.query(Statements::Simple.new('SELECT * FROM songs'), execution_options).get
          end.to raise_error(Errors::NoHostsAvailable)
        end

        it 'raises immediately on Errors::ValidationError' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  Protocol::ErrorResponse.new(nil, nil, 0x2200, 'blargh')
                else
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                end
              end
            end
          end

          client.connect.value
          expect do
            client.query(Statements::Simple.new('SELECT * FROM songs'), execution_options).get
          end.to raise_error(Cassandra::Errors::InvalidError, 'blargh')
        end

        it 'waits for keyspace to be switched before running other requests' do
          keyspace_set = Cassandra::Protocol::SetKeyspaceResultResponse.new(nil, nil, 'foo', false)
          future       = Ione::CompletableFuture.new
          count        = 0
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                when 'USE foo'
                  if count == 0
                    count += 1
                    keyspace_set
                  else
                    throw(:halt, future)
                  end
                else
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('USE foo'), execution_options).get

          # make sure we get a different host in the load balancing plan
          cluster_registry.remove_host(cluster_registry.hosts.first)

          completed = 0
          5.times do
            f = client.query(Statements::Simple.new('SELECT * FROM songs'), execution_options)
            f.on_success do
              completed += 1
            end
          end

          expect(completed).to eq(0)
          future.resolve(keyspace_set)
          expect(completed).to eq(5)
        end
      end

      describe('#prepare') do
        it 'sends a PrepareRequest' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(nil, nil, 123, [], [], nil, nil)
              end
            end
          end
          client.connect.value
          statement = client.prepare('SELECT * FROM songs', execution_options).get
          expect(statement.cql).to eq('SELECT * FROM songs')
        end
      end

      describe('#execute') do
        it 'sends an ExecuteRequest' do
          sent = false
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(nil, nil, 123, [], [], nil, nil)
              when Cassandra::Protocol::ExecuteRequest
                sent = true
                Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
              end
            end
          end
          client.connect.value
          statement = client.prepare('SELECT * FROM songs', execution_options).get
          client.execute(statement.bind, execution_options).get
          expect(sent).to be_truthy
        end

        it 'does not re-prepare a statement on new connection if the new host already has the statement prepared' do
          count = 0
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                count += 1
                Protocol::PreparedResultResponse.new(nil, nil, '123', [], [], nil, nil)
              when Cassandra::Protocol::ExecuteRequest
                Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
              end
            end
          end
          client.connect.value
          statement = client.prepare('SELECT * FROM songs', execution_options).get

          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(cluster_registry.hosts.first)

          client.execute(statement.bind, execution_options).get

          # Expected sequence of events:
          # 1. prepare on host1
          # 2. execute on host2. Since execute succeeds, the implication is that host2 already had the statement
          #    prepared.
          expect(count).to eq(1)
        end

        it 're-prepares a statement on unprepared error' do
          count = 0
          error = true
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                count += 1
                Protocol::PreparedResultResponse.new(nil, nil, '123', [], [], nil, nil)
              when Cassandra::Protocol::ExecuteRequest
                if error
                  error = false
                  Cassandra::Protocol::UnpreparedErrorResponse.new(nil, nil, 0x2500, 'unprepared', "0xbad1d")
                else
                  Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          statement = client.prepare('SELECT * FROM songs', execution_options).get

          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(statement.execution_info.hosts.first)

          client.execute(statement.bind, execution_options).get


          # Expected sequence of events:
          # 1. prepare on host1
          # 2. execute on host2, yielding unprepared error.
          # 3. prepare on host2
          # 4. execute on host2.
          expect(count).to eq(2)
          expect(error).to be(false)
        end

        it 'follows the plan on failure' do
          count    = 0
          attempts = []
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(nil, nil, '123', [], [], nil, nil)
              when Cassandra::Protocol::ExecuteRequest
                attempts << connection.host
                if count == 0
                  count += 1
                  raise Cassandra::Errors::InternalError.new
                end
                Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
              end
            end
          end
          client.connect.value

          statement = client.prepare('SELECT * FROM songs', execution_options).get

          client.execute(statement.bind, execution_options).get

          expect(attempts).to have(2).items
          expect(attempts.sort!).to eq(hosts)
        end

        it 'raises immediately on query error' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(nil, nil, 123, [], [], nil, nil)
              when Cassandra::Protocol::ExecuteRequest
                Protocol::ErrorResponse.new(nil, nil, 0x2200, 'blargh')
              end
            end
          end

          client.connect.value

          statement = client.prepare('SELECT * FROM songs', execution_options).get

          expect do
            client.execute(statement.bind, execution_options).get
          end.to raise_error(Cassandra::Errors::InvalidError, 'blargh')
        end

        it 'returns a Void result when there is a write-timeout with downgrading-consistency policy' do
          driver_settings[:retry_policy] = Retry::Policies::DowngradingConsistency.new
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
                when Cassandra::Protocol::OptionsRequest
                  Cassandra::Protocol::SupportedResponse.new({})
                when Cassandra::Protocol::StartupRequest
                  Cassandra::Protocol::ReadyResponse.new
                when Cassandra::Protocol::PrepareRequest
                  Protocol::PreparedResultResponse.new(nil, nil, 123, [], [], nil, nil)
                when Cassandra::Protocol::ExecuteRequest
                  Protocol::WriteTimeoutErrorResponse.new(nil, nil, 123, 'write timeout', :one, 'x', 'y', 'simple')
              end
            end
          end

          client.connect.value

          statement = client.prepare('SELECT * FROM songs', execution_options).get

          expect(client.execute(statement.bind, execution_options).get)
              .to be_instance_of(Results::Void)
        end

        it 'raises if all hosts failed' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(nil, nil, 123, [], [], nil, nil)
              when Cassandra::Protocol::ExecuteRequest
                raise Cassandra::Errors::InternalError.new
              end
            end
          end

          client.connect.value

          statement = client.prepare('SELECT * FROM songs', execution_options).get

          expect do
            client.execute(statement.bind, execution_options).get
          end.to raise_error(Errors::NoHostsAvailable)
        end
      end

      describe('#batch') do
        it 'sends a BatchRequest' do
          sent = false
          batch = Statements::Batch::Logged.new(driver.execution_options)
          batch_request = double('batch request', :consistency => :one, :retries => 0)
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when batch_request
                sent = true
                Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
              end
            end
          end

          batch.add('INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)', arguments: [1, 2, 3, 4, 5])

          client.connect.value

          expect(Cassandra::Protocol::BatchRequest).to receive(:new).once.with(0, :one, false, nil, nil, nil).and_return(batch_request)
          allow(batch_request).to receive(:clear)
          expect(batch_request).to receive(:add_query).once.with('INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)', [1, 2, 3, 4, 5], [Cassandra::Types.bigint, Cassandra::Types.bigint, Cassandra::Types.bigint, Cassandra::Types.bigint, Cassandra::Types.bigint])
          expect(batch_request).to receive(:retries=).once.with(0)
          client.batch(batch, execution_options.override(consistency: :one)).get
          expect(sent).to be_truthy
        end

        it 'can include prepared statements' do
          sent = false
          batch = Statements::Batch::Logged.new(driver.execution_options)
          batch_request = double('batch request', :consistency => :one, :retries => 0)
          params_metadata = [
            ['simplex', 'songs', 'id', Cassandra::Types.uuid],
            ['simplex', 'songs', 'title', Cassandra::Types.varchar],
            ['simplex', 'songs', 'album', Cassandra::Types.varchar],
            ['simplex', 'songs', 'artist', Cassandra::Types.varchar],
            ['simplex', 'songs', 'tags', Cassandra::Types.set(Cassandra::Types.varchar)]
          ]
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(nil, nil, 123, params_metadata, [], nil, nil)
              when batch_request
                sent = true
                Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
              end
            end
          end

          client.connect.value

          statement = client.prepare('INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)',
                                     execution_options.override(consistency: :one)).get

          batch.add(statement, arguments: [Cassandra::Uuid.new(1), 'some title', 'some album', 'some artist', Set['cool', 'stuff']])

          expect(Cassandra::Protocol::BatchRequest).to receive(:new).once.with(0, :one, false, nil, nil, nil).and_return(batch_request)
          allow(batch_request).to receive(:clear)
          expect(batch_request).to receive(:add_prepared).once.with(123, [Cassandra::Uuid.new(1), 'some title', 'some album', 'some artist', Set['cool', 'stuff']], params_metadata.map(&:last))
          expect(batch_request).to receive(:retries=).once.with(0)
          client.batch(batch, execution_options.override(consistency: :one)).get
          expect(sent).to be_truthy
        end

        it 'does not automatically re-prepare statements' do
          sent = false
          count = 0
          batch = Statements::Batch::Logged.new(driver.execution_options)
          batch_request = double('batch request', :consistency => :one, :retries => 0)
          params_metadata = [
            ['simplex', 'songs', 'id', Cassandra::Types.uuid],
            ['simplex', 'songs', 'title', Cassandra::Types.varchar],
            ['simplex', 'songs', 'album', Cassandra::Types.varchar],
            ['simplex', 'songs', 'artist', Cassandra::Types.varchar],
            ['simplex', 'songs', 'tags', Cassandra::Types.set(Cassandra::Types.varchar)]
          ]
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                count += 1
                Protocol::PreparedResultResponse.new(nil, nil, 123, params_metadata, [], nil, nil)
              when batch_request
                sent = true
                Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
              end
            end
          end

          client.connect.value

          statement = client.prepare('INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)',
                                     execution_options.override(consistency: :one)).get

          batch.add(statement, arguments: [Cassandra::Uuid.new(1), 'some title', 'some album', 'some artist', Set['cool', 'stuff']])

          expect(Cassandra::Protocol::BatchRequest).to receive(:new).once.with(0, :one, false, nil, nil, nil).and_return(batch_request)
          allow(batch_request).to receive(:clear)
          expect(batch_request).to receive(:add_prepared).once.with(123, [Cassandra::Uuid.new(1), 'some title', 'some album', 'some artist', Set['cool', 'stuff']], params_metadata.map(&:last))
          expect(batch_request).to receive(:retries=).once.with(0)

          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(cluster_registry.hosts.first)

          client.batch(batch, execution_options.override(consistency: :one)).get
          expect(sent).to be_truthy

          # Expected sequence of events:
          # 1. prepare on host1
          # 2. run batch on host2 with prepared-statement id's. This succeeds, so no more work to do.
          expect(count).to eq(1)
        end

        it 'follows the plan on failure' do
          count    = 0
          attempts = []
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::BatchRequest
                attempts << connection.host
                if count == 0
                  count += 1
                  raise Cassandra::Errors::InternalError.new
                end
                Cassandra::Protocol::RowsResultResponse.new(nil, nil, [], [], nil, nil)
              end
            end
          end

          client.connect.value
          batch = Statements::Batch::Logged.new(driver.execution_options)

          client.batch(batch, execution_options).get

          expect(attempts).to have(2).items
          expect(attempts).to eq(hosts)
        end

        it 'raises if all hosts failed' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::OptionsRequest
                Cassandra::Protocol::SupportedResponse.new({})
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::BatchRequest
                raise Cassandra::Errors::InternalError.new
              end
            end
          end

          client.connect.value

          batch = Statements::Batch::Logged.new(driver.execution_options)

          expect do
            client.batch(batch, execution_options).get
          end.to raise_error(Errors::NoHostsAvailable)
        end
      end
    end
  end
end
