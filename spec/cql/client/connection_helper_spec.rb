# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe ConnectionHelper do
      let :connection_helper do
        described_class.new(io_reactor, 9876, authenticator, 2, 1, 7, compressor, logger)
      end

      let :io_reactor do
        double(:io_reactor)
      end

      let :authenticator do
        nil
      end

      let :logger do
        NullLogger.new
      end

      let :compressor do
        nil
      end

      describe '#connect' do
        let :hosts do
          %w[host0 host1]
        end

        let :local_metadata do
          [
            ['system', 'local', 'data_center', :text],
            ['system', 'local', 'host_id', :uuid],
          ]
        end

        before do
          io_reactor.stub(:connect).and_return(Future.resolved)
        end

        it 'connects to the specified hosts' do
          connection_helper.connect(hosts, nil)
          io_reactor.should have_received(:connect).with('host0', 9876, 7)
          io_reactor.should have_received(:connect).with('host1', 9876, 7)
        end

        it 'logs a message when a node connects' do
          logger.stub(:info)
          io_reactor.stub(:connect).and_return(Future.resolved(FakeConnection.new('host', 9876, 7)))
          connection_helper.connect(hosts, nil)
          logger.should have_received(:info).with(/Connected to node/).exactly(hosts.size).times
        end

        it 'logs a message when connecting to a node' do
          logger.stub(:debug)
          io_reactor.stub(:connect).and_return(Future.resolved(FakeConnection.new('host', 9876, 7)))
          connection_helper.connect(hosts, nil)
          logger.should have_received(:debug).with(/Connecting to node/).exactly(hosts.size).times
        end

        it 'fails when all hosts fail to connect' do
          io_reactor.stub(:connect).and_return(Future.failed(StandardError.new('bork')))
          f = connection_helper.connect(hosts, nil)
          expect { f.value }.to raise_error('bork')
        end

        it 'logs a message when a node fails to connect' do
          logger.stub(:warn)
          io_reactor.stub(:connect).and_return(Future.failed(StandardError.new('bork')))
          connection_helper.connect(hosts, nil)
          logger.should have_received(:warn).with(/Failed connecting to node/).exactly(hosts.size).times
        end

        it 'fails with an AuthenticationError when the connections fail to connect because of authentication issues' do
          io_reactor.stub(:connect).and_return(Future.failed(QueryError.new(0x100, 'bork')))
          f = connection_helper.connect(hosts, nil)
          expect { f.value }.to raise_error(AuthenticationError)
        end

        it 'initializes the connections' do
          connection0 = FakeConnection.new('host0', 9876, 7)
          connection1 = FakeConnection.new('host1', 9876, 7)
          io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection0))
          io_reactor.stub(:connect).with('host1', 9876, 7).and_return(Future.resolved(connection1))
          connection_helper.connect(hosts, 'some_keyspace')
          [connection0, connection1].each do |c|
            c.requests[0].should be_a(Protocol::OptionsRequest)
            c.requests[1].should be_a(Protocol::StartupRequest)
            c.requests[2].cql.should match(/SELECT .* FROM system.local/)
            c.requests[3].cql.should == 'USE some_keyspace'
          end
        end

        it 'saves the supported CQL version and compression algorithms on the connection' do
          connection = FakeConnection.new('host0', 9876, 7)
          connection.handle_request do |request, timeout|
            if request.is_a?(Protocol::OptionsRequest)
              Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.1.1], 'COMPRESSION' => %w[lz4 snappy])
            else
              connection.default_request_handler(request, timeout)
            end
          end
          io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
          connection_helper.connect(hosts.take(1), 'some_keyspace')
          connection[:cql_version].should == %w[3.1.1]
          connection[:compression].should == %w[lz4 snappy]
        end

        it 'fails if authentication is required and no authenticator was given' do
          connection = FakeConnection.new('host0', 9876, 7)
          connection.handle_request do |request|
            if request.is_a?(Protocol::StartupRequest)
              Protocol::AuthenticateResponse.new('xyz')
            else
              connection.default_request_handler(request)
            end
          end
          io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
          f = connection_helper.connect(hosts, nil)
          expect { f.value }.to raise_error(AuthenticationError)
        end

        context 'with protocol v1' do
          it 'authenticates when authentication is required and an authenticator was given' do
            authenticator = PasswordAuthenticator.new('foo', 'bar')
            connection_helper = described_class.new(io_reactor, 9876, authenticator, 1, 1, 7, nil, logger)
            connection = FakeConnection.new('host0', 9876, 7)
            authentication_sent = false
            connection.handle_request do |request|
              if request.is_a?(Protocol::StartupRequest)
                Protocol::AuthenticateResponse.new('org.apache.cassandra.auth.PasswordAuthenticator')
              elsif request == Protocol::CredentialsRequest.new(username: 'foo', password: 'bar')
                authentication_sent = true
                Protocol::ReadyResponse.new
              else
                connection.default_request_handler(request)
              end
            end
            io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
            f = connection_helper.connect(hosts, nil)
            f.value
            authentication_sent.should be_true
          end
        end

        context 'with protocol v2' do
          it 'authenticates when authentication is required and an authenticator was given' do
            authenticator = PasswordAuthenticator.new('foo', 'bar')
            connection_helper = described_class.new(io_reactor, 9876, authenticator, 2, 1, 7, nil, logger)
            connection = FakeConnection.new('host0', 9876, 7)
            authentication_sent = false
            connection.handle_request do |request|
              if request.is_a?(Protocol::StartupRequest)
                Protocol::AuthenticateResponse.new('org.apache.cassandra.auth.PasswordAuthenticator')
              elsif request == Protocol::AuthResponseRequest.new("\x00foo\x00bar")
                authentication_sent = true
                Protocol::AuthSuccessResponse.new('welcome')
              else
                connection.default_request_handler(request)
              end
            end
            io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
            f = connection_helper.connect(hosts, nil)
            f.value
            authentication_sent.should be_true
          end
        end

        it 'decorates the connections with :host_id and :data_center' do
          connection = FakeConnection.new('host0', 9876, 7)
          connection.handle_request do |request|
            if request.is_a?(Protocol::QueryRequest) && request.cql =~ /SELECT .* FROM system\.local/
              row = {'data_center' => 'dc1', 'host_id' => Uuid.new('eac69196-1e28-11e3-8e2b-191b6d153d0c')}
              Protocol::RowsResultResponse.new([row], local_metadata, nil, nil)
            else
              connection.default_request_handler(request)
            end
          end
          io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
          connection_helper.connect(hosts, nil)
          connection[:host_id].should == Uuid.new('eac69196-1e28-11e3-8e2b-191b6d153d0c')
          connection[:data_center].should == 'dc1'
        end

        it 'registers a close handler that logs when connections closes unexpectedly' do
          logger.stub(:warn)
          connection = FakeConnection.new('host', 9876, 7)
          io_reactor.stub(:connect).and_return(Future.resolved(connection))
          connection_helper.connect(hosts.take(1), nil)
          connection.close(StandardError.new('bork'))
          logger.should have_received(:warn).with(/Connection to node .* closed: bork/)
        end

        it 'registers a close handler that logs when connections close' do
          logger.stub(:info)
          connection = FakeConnection.new('host', 9876, 7)
          io_reactor.stub(:connect).and_return(Future.resolved(connection))
          connection_helper.connect(hosts.take(1), nil)
          connection.close
          logger.should have_received(:info).with(/Connection to node .* closed/)
        end

        it 'initializes a peer discovery when connected to the specified hosts' do
          connection_helper.stub(:discover_peers)
          connection_helper.connect(hosts, nil)
          connection0 = FakeConnection.new('host0', 9876, 7)
          connection1 = FakeConnection.new('host1', 9876, 7)
          io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection0))
          io_reactor.stub(:connect).with('host1', 9876, 7).and_return(Future.resolved(connection1))
          connection_helper.connect(hosts, 'some_keyspace')
          connection_helper.should have_received(:discover_peers).with([connection0, connection1], 'some_keyspace')
        end

        it 'initializes a peer discovery with the successfull connections as seeds' do
          connection_helper.stub(:discover_peers)
          connection_helper.connect(hosts, nil)
          connection = FakeConnection.new('host0', 9876, 7)
          io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
          io_reactor.stub(:connect).with('host1', 9876, 7).and_return(Future.failed(StandardError.new('bork')))
          connection_helper.connect(hosts, 'some_keyspace')
          connection_helper.should have_received(:discover_peers).with([connection], 'some_keyspace')
        end

        it 'connects to each node a configurable number of times' do
          connection_helper = described_class.new(io_reactor, 9876, nil, 2, connections_per_node = 3, 7, nil, logger)
          connection_helper.connect(hosts, nil)
          io_reactor.should have_received(:connect).with('host0', 9876, 7).exactly(3).times
          io_reactor.should have_received(:connect).with('host1', 9876, 7).exactly(3).times
        end

        context 'when a compressor is specified' do
          let :compressor do
            double(:compressor, algorithm: 'snappy')
          end

          let :connection do
            FakeConnection.new('host0', 9876, 7)
          end

          it 'enables compression by sending the algorithm with the STARTUP request' do
            connection.handle_request do |request, timeout|
              if request.is_a?(Protocol::OptionsRequest)
                Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.1.1], 'COMPRESSION' => %w[lz4 snappy])
              else
                connection.default_request_handler(request, timeout)
              end
            end
            io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
            connection_helper.connect(hosts.take(1), 'some_keyspace')
            connection.requests[1].options['COMPRESSION'].should == 'snappy'
          end

          it 'does not enable compression when the algorithm is not supported' do
            connection.handle_request do |request, timeout|
              if request.is_a?(Protocol::OptionsRequest)
                Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.1.1], 'COMPRESSION' => %w[lz4])
              else
                connection.default_request_handler(request, timeout)
              end
            end
            io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
            connection_helper.connect(hosts.take(1), 'some_keyspace')
            connection.requests[1].options.should_not have_key('COMPRESSION')
          end

          it 'logs a warning when compression was disabled because the algorithm was not supported' do
            logger.stub(:warn)
            connection.handle_request do |request, timeout|
              if request.is_a?(Protocol::OptionsRequest)
                Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.1.1], 'COMPRESSION' => %w[lz4])
              else
                connection.default_request_handler(request, timeout)
              end
            end
            io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
            connection_helper.connect(hosts.take(1), 'some_keyspace')
            logger.should have_received(:warn).with(/not supported/)
          end

          it 'logs the name of the compression algorithm when connecting' do
            logger.stub(:debug)
            connection.handle_request do |request, timeout|
              if request.is_a?(Protocol::OptionsRequest)
                Protocol::SupportedResponse.new('CQL_VERSION' => %w[3.1.1], 'COMPRESSION' => %w[lz4 snappy])
              else
                connection.default_request_handler(request, timeout)
              end
            end
            io_reactor.stub(:connect).with('host0', 9876, 7).and_return(Future.resolved(connection))
            connection_helper.connect(hosts.take(1), 'some_keyspace')
            logger.should have_received(:debug).with(/using "snappy" compression/i)
          end
        end
      end

      describe '#discover_peers' do
        let :seed_connections do
          [
            FakeConnection.new('host0', 9042, 5),
            FakeConnection.new('host1', 9042, 5),
            FakeConnection.new('host2', 9042, 5),
          ]
        end

        let :seed_connection_rows do
          [
            {'peer' => IPAddr.new('2.0.0.0'), 'rpc_address' => IPAddr.new('1.0.0.0'), 'data_center' => 'dc1', 'host_id' => Uuid.new('eac69196-1e28-11e3-8e2b-191b6d153d0c')},
            {'peer' => IPAddr.new('2.0.0.1'), 'rpc_address' => IPAddr.new('1.0.0.1'), 'data_center' => 'dc1', 'host_id' => Uuid.new('fa5f9562-1e28-11e3-bf05-3d3a155d0608')},
            {'peer' => IPAddr.new('2.0.0.2'), 'rpc_address' => IPAddr.new('1.0.0.2'), 'data_center' => 'dc1', 'host_id' => Uuid.new('018b8f1c-1e29-11e3-b14f-532d016437ce')},
          ]
        end

        let :extra_connection_rows do
          [
            {'peer' => IPAddr.new('2.0.0.3'), 'rpc_address' => IPAddr.new('1.0.0.3'), 'data_center' => 'dc1', 'host_id' => Uuid.new('7a3ccace-1e2a-11e3-a447-43312b1c66e4')},
            {'peer' => IPAddr.new('2.0.0.4'), 'rpc_address' => IPAddr.new('1.0.0.4'), 'data_center' => 'dc1', 'host_id' => Uuid.new('7bbd4e32-1e2a-11e3-b21d-69d7c02cece8')},
            {'peer' => IPAddr.new('2.0.0.5'), 'rpc_address' => IPAddr.new('1.0.0.5'), 'data_center' => 'dc1', 'host_id' => Uuid.new('7d7e76f6-1e2a-11e3-bfa0-4fb416ef4064')},
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

        before do
          seed_connections.each_with_index do |c, i|
            c[:host_id] = seed_connection_rows[i]['host_id']
            c[:data_center] = seed_connection_rows[i]['data_center']
          end
        end

        def peer_request_response
          seed_connections.each do |c|
            c.handle_request do |request|
              if request.cql =~ /SELECT .* FROM system\.peers/
                Protocol::RowsResultResponse.new(yield, peer_metadata, nil, nil)
              end
            end
          end
        end

        it 'returns immediately if there are no seed connections' do
          f = connection_helper.discover_peers([], nil)
          f.value
        end

        it 'logs a message when it begins' do
          logger.stub(:debug)
          connection_helper.discover_peers(seed_connections, nil)
          logger.should have_received(:debug).with(/Looking for additional nodes/)
        end

        it 'asks a random connection for its peers' do
          connection_helper.discover_peers(seed_connections, nil)
          connection = seed_connections.find { |c| c.requests.any? }
          connection.requests.first.cql.should match(/SELECT .* FROM system\.peers/)
        end

        it 'returns an empty list when it only finds nodes it\'s already connected to' do
          peer_request_response { seed_connection_rows }
          f = connection_helper.discover_peers(seed_connections, nil)
          f.value.should be_empty
        end

        it 'logs a message when it finds no new nodes' do
          logger.stub(:debug)
          peer_request_response { seed_connection_rows }
          connection_helper.discover_peers(seed_connections, nil)
          logger.should have_received(:debug).with(/No additional nodes found/)
        end

        it 'returns an empty list when it only finds nodes data centers other than those of the seed connections' do
          seed_connections[1][:data_center] = 'dc2'
          seed_connection_rows[1]['data_center'] = 'dc2'
          extra_connection_rows[0]['data_center'] = 'dc3'
          peer_request_response { seed_connection_rows + extra_connection_rows.take(1) }
          f = connection_helper.discover_peers(seed_connections, nil)
          f.value.should be_empty
        end

        it 'connects to the nodes it finds that it is not already connected to' do
          connection = FakeConnection.new('host3', 9876, 7)
          io_reactor.stub(:connect).with('1.0.0.3', 9876, 7).and_return(Future.resolved(connection))
          peer_request_response { seed_connection_rows + extra_connection_rows.take(1) }
          f = connection_helper.discover_peers(seed_connections, nil)
          f.value
        end

        it 'logs the number of new nodes found' do
          logger.stub(:debug)
          connection = FakeConnection.new('host3', 9876, 7)
          io_reactor.stub(:connect).with('1.0.0.3', 9876, 7).and_return(Future.resolved(connection))
          peer_request_response { seed_connection_rows + extra_connection_rows.take(1) }
          connection_helper.discover_peers(seed_connections, nil)
          logger.should have_received(:debug).with(/1 additional nodes found/)
        end

        it 'returns the new connections' do
          connection = FakeConnection.new('host3', 9876, 7)
          io_reactor.stub(:connect).with('1.0.0.3', 9876, 7).and_return(Future.resolved(connection))
          peer_request_response { seed_connection_rows + extra_connection_rows.take(1) }
          f = connection_helper.discover_peers(seed_connections, nil)
          f.value.should == [connection]
        end

        it 'initializes the new connections' do
          connection = FakeConnection.new('host3', 9876, 7)
          io_reactor.stub(:connect).with('1.0.0.3', 9876, 7).and_return(Future.resolved(connection))
          peer_request_response { seed_connection_rows + extra_connection_rows.take(1) }
          f = connection_helper.discover_peers(seed_connections, 'some_keyspace')
          f.value
          connection.requests[0].should be_a(Protocol::OptionsRequest)
          connection.requests[1].should be_a(Protocol::StartupRequest)
          connection.requests[2].cql.should match(/SELECT .* FROM system.local/)
          connection.requests[3].cql.should == 'USE some_keyspace'
        end

        it 'connects only to node in the same data centers as the seed nodes' do
          seed_connections[1][:data_center] = 'dc2'
          seed_connection_rows[1]['data_center'] = 'dc2'
          extra_connection_rows[0]['data_center'] = 'dc3'
          extra_connection_rows[1]['data_center'] = 'dc2'
          extra_connection_rows[2]['data_center'] = 'dc1'
          connection4 = FakeConnection.new('host4', 9876, 7)
          connection5 = FakeConnection.new('host5', 9876, 7)
          io_reactor.stub(:connect).with('1.0.0.4', 9876, 7).and_return(Future.resolved(connection4))
          io_reactor.stub(:connect).with('1.0.0.5', 9876, 7).and_return(Future.resolved(connection5))
          peer_request_response { seed_connection_rows + extra_connection_rows.take(3) }
          f = connection_helper.discover_peers(seed_connections, nil)
          f.value.should == [connection4, connection5]
        end

        it 'uses the peer address instead of the RPC address when latter is 0.0.0.0' do
          extra_connection_rows[0]['rpc_address'] = IPAddr.new('0.0.0.0')
          connection = FakeConnection.new('host3', 9876, 7)
          io_reactor.stub(:connect).with(extra_connection_rows[0]['peer'].to_s, 9876, 7).and_return(Future.resolved(connection))
          peer_request_response { seed_connection_rows + extra_connection_rows.take(1) }
          f = connection_helper.discover_peers(seed_connections, nil)
          f.value
        end

        it 'connects to each node a configurable number of times' do
          connection_helper = described_class.new(io_reactor, 9876, nil, 2, connections_per_node = 3, 7, nil, logger)
          connection = FakeConnection.new('host3', 9876, 7)
          io_reactor.stub(:connect).with('1.0.0.3', 9876, 7).and_return(Future.resolved(connection))
          peer_request_response { seed_connection_rows + extra_connection_rows.take(1) }
          connection_helper.discover_peers(seed_connections, nil).value
          io_reactor.should have_received(:connect).with('1.0.0.3', 9876, 7).exactly(3).times
        end
      end
    end
  end
end