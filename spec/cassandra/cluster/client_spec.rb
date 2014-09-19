# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
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
      let(:logger) { Cassandra::Client::NullLogger.new }
      let(:driver_settings)  { {
                                 :io_reactor => io_reactor,
                                 :load_balancing_policy => load_balancing_policy,
                                 :cluster_registry => cluster_registry,
                                 :connections_per_local_node => 2,
                                 :connections_per_remote_node => 1,
                                 :reconnection_policy => reconnection_policy,
                                 :logger => logger
                               } }

      let(:driver) { Driver.new(driver_settings) }
      let(:client) { Client.new(driver.logger, driver.cluster_registry, driver.io_reactor, driver.connector, driver.load_balancing_policy, driver.reconnection_policy, driver.retry_policy, driver.connection_options, driver.futures_factory) }

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
            logger.should have_received(:error).with(/Session close failed: Hurgh blurgh/)
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

          it 'does not start a new reconnection loop when one is already in progress' do
            reconnect_interval = 4
            schedule = double('reconnection schedule', :next => reconnect_interval)
            reconnection_policy.stub(:schedule) { schedule }
            io_reactor.should_receive(:schedule_timer).once.with(reconnect_interval).and_call_original

            client.host_up(host)
            client.host_up(host)
            client.host_up(host)
            client.host_up(host)
          end

          it 'logs reconnection attempts' do
            logger.stub(:info)
            logger.stub(:warn)

            io_reactor.node_down(address)
            cluster_registry.hosts.each { |host| io_reactor.node_down(host.ip.to_s) }

            client.host_up(host)

            logger.should have_received(:warn).with(/Connection failed ip=(.*) error=(.*)/).at_least(1).times
            logger.should have_received(:info).with(/Session started reconnecting to ip=(.*) delay=(.*)/)
          end
        end
      end

      describe('#host_down') do
        context 'when connected to it' do
          before do
            client.connect.value
          end

          it 'closes connections to that host' do
            expect do
              client.host_down(Host.new('127.0.0.1'))
            end.to change { io_reactor.connections.select(&:connected?).size }.from(4).to(2)
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
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  handled = true
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                else
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('SELECT * FROM songs'), Execution::Options.new(:consistency => :one)).get

          expect(handled).to be_truthy
        end

        it 'switches keyspace of new connections automatically' do
          count = 0
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                when 'USE foo'
                  count += 1
                  Cassandra::Protocol::SetKeyspaceResultResponse.new('foo', false)
                else
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('USE foo'), Execution::Options.new(:consistency => :one)).get
          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(cluster_registry.hosts.first)
          client.query(Statements::Simple.new('SELECT * FROM songs'), Execution::Options.new(:consistency => :one)).get

          expect(count).to eq(2)
        end

        it 'follows the plan on failure' do
          count    = 0
          attempts = []
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  attempts << connection.host
                  if count == 0
                    count += 1
                    raise Cassandra::Errors::NotConnectedError.new
                  else
                    Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                  end
                else
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('SELECT * FROM songs'), Execution::Options.new(:consistency => :one)).get
          expect(attempts).to have(2).items
          expect(attempts).to eq(hosts)
        end

        it 'raises if all hosts failed' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  raise Cassandra::Errors::NotConnectedError.new
                else
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          expect do
            client.query(Statements::Simple.new('SELECT * FROM songs'), Execution::Options.new(:consistency => :one)).get
          end.to raise_error(Errors::NoHostsAvailable)
        end

        it 'raises immediately on Errors::QueryError' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  Protocol::ErrorResponse.new(0x00, 'blargh')
                else
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                end
              end
            end
          end

          client.connect.value
          expect do
            client.query(Statements::Simple.new('SELECT * FROM songs'), Execution::Options.new(:consistency => :one)).get
          end.to raise_error(Cassandra::Errors::QueryError, 'blargh')
        end

        it 'waits for keyspace to be switched before running other requests' do
          keyspace_set = Cassandra::Protocol::SetKeyspaceResultResponse.new('foo', false)
          future       = Ione::CompletableFuture.new
          count        = 0
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::QueryRequest
                case request.cql
                when 'SELECT * FROM songs'
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                when 'USE foo'
                  if count == 0
                    count += 1
                    keyspace_set
                  else
                    throw(:halt, future)
                  end
                else
                  Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
                end
              end
            end
          end
          client.connect.value
          client.query(Statements::Simple.new('USE foo'), Execution::Options.new(:consistency => :one)).get

          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(cluster_registry.hosts.first)

          completed = 0
          5.times do
            f = client.query(Statements::Simple.new('SELECT * FROM songs'), Execution::Options.new(:consistency => :one))
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
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(123, [], [], nil)
              end
            end
          end
          client.connect.value
          statement = client.prepare('SELECT * FROM songs', Execution::Options.new(:consistency => :one)).get
          expect(statement.cql).to eq('SELECT * FROM songs')
        end
      end

      describe('#execute') do
        it 'sends an ExecuteRequest' do
          sent = false
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(123, [], [], nil)
              when Cassandra::Protocol::ExecuteRequest
                sent = true
                Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
              end
            end
          end
          client.connect.value
          statement = client.prepare('SELECT * FROM songs', Execution::Options.new(:consistency => :one)).get
          client.execute(statement.bind, Execution::Options.new(:consistency => :one)).get
          expect(sent).to be_truthy
        end

        it 're-prepares a statement on new connection' do
          count = 0
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                count += 1
                Protocol::PreparedResultResponse.new('123', [], [], nil)
              when Cassandra::Protocol::ExecuteRequest
                sent = true
                Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
              end
            end
          end
          client.connect.value
          statement = client.prepare('SELECT * FROM songs', Execution::Options.new(:consistency => :one)).get

          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(cluster_registry.hosts.first)

          client.execute(statement.bind, Execution::Options.new(:consistency => :one)).get
          expect(count).to eq(2)
        end

        it 'follows the plan on failure' do
          count    = 0
          attempts = []
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(123, [], [], nil)
              when Cassandra::Protocol::ExecuteRequest
                attempts << connection.host
                if count == 0
                  count += 1
                  raise Cassandra::Errors::NotConnectedError.new
                end
                Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
              end
            end
          end
          client.connect.value
          statement = client.prepare('SELECT * FROM songs', Execution::Options.new(:consistency => :one)).get

          client.execute(statement.bind, Execution::Options.new(:consistency => :one)).get

          expect(attempts).to have(2).items
          expect(attempts).to eq(hosts)
        end

        it 'raises immediately on query error' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(123, [], [], nil)
              when Cassandra::Protocol::ExecuteRequest
                Protocol::ErrorResponse.new(0x00, 'blargh')
              end
            end
          end

          client.connect.value

          statement = client.prepare('SELECT * FROM songs', Execution::Options.new(:consistency => :one)).get

          expect do
            client.execute(statement.bind, Execution::Options.new(:consistency => :one)).get
          end.to raise_error(Cassandra::Errors::QueryError, 'blargh')
        end

        it 'raises if all hosts failed' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(123, [], [], nil)
              when Cassandra::Protocol::ExecuteRequest
                raise Cassandra::Errors::NotConnectedError.new
              end
            end
          end

          client.connect.value

          statement = client.prepare('SELECT * FROM songs', Execution::Options.new(:consistency => :one)).get

          expect do
            client.execute(statement.bind, Execution::Options.new(:consistency => :one)).get
          end.to raise_error(Errors::NoHostsAvailable)
        end
      end

      describe('#batch') do
        it 'sends a BatchRequest' do
          sent = false
          batch = Statements::Batch::Logged.new
          batch_request = double('batch request', :consistency => :one, :retries => 0)
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when batch_request
                sent = true
                Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
              end
            end
          end

          batch.add('INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)', 1, 2, 3, 4, 5)

          client.connect.value

          expect(Cassandra::Protocol::BatchRequest).to receive(:new).once.with(0, :one, false).and_return(batch_request)
          expect(batch_request).to receive(:add_query).once.with('INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)', [1, 2, 3, 4, 5])
          expect(batch_request).to receive(:retries=).once.with(0)
          client.batch(batch, Execution::Options.new(:consistency => :one, :trace => false)).get
          expect(sent).to be_truthy
        end

        it 'can include prepared statements' do
          sent = false
          batch = Statements::Batch::Logged.new
          batch_request = double('batch request', :consistency => :one, :retries => 0)
          params_metadata = double('params metadata', :size => 5)
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                Protocol::PreparedResultResponse.new(123, params_metadata, [], nil)
              when batch_request
                sent = true
                Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
              end
            end
          end

          client.connect.value

          statement = client.prepare('INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)', Execution::Options.new(:consistency => :one, :trace => false)).get

          batch.add(statement, 1, 2, 3, 4, 5)

          expect(Cassandra::Protocol::BatchRequest).to receive(:new).once.with(0, :one, false).and_return(batch_request)
          expect(batch_request).to receive(:add_prepared).once.with(123, params_metadata, [1,2,3,4,5])
          expect(batch_request).to receive(:retries=).once.with(0)
          client.batch(batch, Execution::Options.new(:consistency => :one, :trace => false)).get
          expect(sent).to be_truthy
        end

        it 'automatically re-prepares statements' do
          sent = false
          count = 0
          batch = Statements::Batch::Logged.new
          batch_request = double('batch request', :consistency => :one, :retries => 0)
          params_metadata = double('params metadata', :size => 5)
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::PrepareRequest
                count += 1
                Protocol::PreparedResultResponse.new(123, params_metadata, [], nil)
              when batch_request
                sent = true
                Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
              end
            end
          end

          client.connect.value

          statement = client.prepare('INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)', Execution::Options.new(:consistency => :one, :trace => false)).get

          batch.add(statement, 1, 2, 3, 4, 5)

          expect(Cassandra::Protocol::BatchRequest).to receive(:new).once.with(0, :one, false).and_return(batch_request)
          expect(batch_request).to receive(:add_prepared).once.with(123, params_metadata, [1,2,3,4,5])
          expect(batch_request).to receive(:retries=).once.with(0)

          # make sure we get a different host in the load balancing plan
          cluster_registry.hosts.delete(cluster_registry.hosts.first)

          client.batch(batch, Execution::Options.new(:consistency => :one, :trace => false)).get
          expect(sent).to be_truthy
          expect(count).to eq(2)
        end

        it 'follows the plan on failure' do
          count    = 0
          attempts = []
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::BatchRequest
                attempts << connection.host
                if count == 0
                  count += 1
                  raise Cassandra::Errors::NotConnectedError.new
                end
                Cassandra::Protocol::RowsResultResponse.new([], [], nil, nil)
              end
            end
          end

          client.connect.value
          batch = Statements::Batch::Logged.new

          client.batch(batch, Execution::Options.new(:consistency => :one)).get

          expect(attempts).to have(2).items
          expect(attempts).to eq(hosts)
        end

        it 'raises if all hosts failed' do
          io_reactor.on_connection do |connection|
            connection.handle_request do |request|
              case request
              when Cassandra::Protocol::StartupRequest
                Cassandra::Protocol::ReadyResponse.new
              when Cassandra::Protocol::BatchRequest
                raise Cassandra::Errors::NotConnectedError.new
              end
            end
          end

          client.connect.value

          batch = Statements::Batch::Logged.new

          expect do
            client.batch(batch, Execution::Options.new(:consistency => :one)).get
          end.to raise_error(Errors::NoHostsAvailable)
        end
      end
    end
  end
end
