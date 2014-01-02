# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe AsynchronousPreparedStatement do
      let :connection_manager do
        ConnectionManager.new
      end

      let :logger do
        NullLogger.new
      end

      let :raw_metadata do
        [
          ['my_keyspace', 'my_table', 'my_column', :int],
          ['my_keyspace', 'my_table', 'my_other_column', :text],
        ]
      end

      let :rows do
        [
          {'my_column' => 11, 'my_other_column' => 'hello'},
          {'my_column' => 22, 'my_other_column' => 'foo'},
          {'my_column' => 33, 'my_other_column' => 'bar'},
        ]
      end

      let :cql do
        'SELECT * FROM my_table'
      end

      let :connections do
        [
          FakeConnection.new('h0.example.com', 1234, 42),
          FakeConnection.new('h1.example.com', 1234, 42),
          FakeConnection.new('h2.example.com', 1234, 42),
        ]
      end

      def handle_request(connection, request, timeout)
        case request
        when Protocol::PrepareRequest
          statement_id = Array.new(16) { [rand(255)].pack('c') }.join('')
          connection[:last_prepared_statement_id] = statement_id
          Protocol::PreparedResultResponse.new(statement_id, raw_metadata, nil, nil)
        when Protocol::ExecuteRequest
          Protocol::RowsResultResponse.new(rows, raw_metadata, nil, nil)
        else
          raise %(Unexpected request: #{request})
        end
      end

      before do
        connections.each do |c|
          c.handle_request { |r, t| handle_request(c, r, t) }
        end
        connection_manager.add_connections(connections)
      end

      describe '.prepare' do
        it 'prepares a statement on all connections' do
          f = described_class.prepare(cql, ExecuteOptionsDecoder.new(:one), connection_manager, logger)
          f.value
          connections.each do |c|
            c.requests.should include(Protocol::PrepareRequest.new(cql))
          end
        end

        it 'returns a prepared statement object' do
          f = described_class.prepare(cql, ExecuteOptionsDecoder.new(:two), connection_manager, logger)
          f.value.should be_a(PreparedStatement)
        end

        it 'returns a failed future when something goes wrong in the preparation' do
          connections.each(&:close)
          f = described_class.prepare(cql, ExecuteOptionsDecoder.new(:three), connection_manager, logger)
          expect { f.value }.to raise_error(NotConnectedError)
        end

        it 'returns a failed future if the preparation results in an error' do
          connections.each do |connection|
            connection.stub(:send_request).and_return(Future.resolved(Protocol::ErrorResponse.new(99, 'bork')))
          end
          f = described_class.prepare(cql, :quorum, connection_manager, logger)
          expect { f.value }.to raise_error('bork')
        end
      end

      describe '#metadata' do
        let :statement do
          described_class.prepare(cql, ExecuteOptionsDecoder.new(:all), connection_manager, logger).value
        end

        it 'returns the interpreted metadata' do
          statement.metadata.should be_a(ResultMetadata)
          statement.metadata['my_column'].should be_a(ColumnMetadata)
        end
      end

      describe '#execute' do
        let :statement do
          described_class.prepare(cql, ExecuteOptionsDecoder.new(:local_quorum), connection_manager, logger).value
        end

        it 'executes itself on one of the connections' do
          statement.execute(11, 'hello')
          requests = connections.flat_map(&:requests).select { |r| r.is_a?(Protocol::ExecuteRequest) }
          requests.should have(1).item
          requests.first.metadata.should == raw_metadata
          requests.first.values.should == [11, 'hello']
        end

        it 'uses the right statement ID for the connection' do
          statement.execute(11, 'hello')
          connection, request = connections.map { |c| [c, c.requests.find { |r| r.is_a?(Protocol::ExecuteRequest) }] }.find { |c, r| r }
          request.id.should == connection[:last_prepared_statement_id]
        end

        it 'sends the default consistency level' do
          statement.execute(11, 'hello')
          request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
          request.consistency.should == :local_quorum
        end

        it 'sends the consistency given as last argument' do
          statement.execute(11, 'hello', :two)
          request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
          request.consistency.should == :two
        end

        it 'sends the consistency given as an option' do
          statement.execute(11, 'hello', consistency: :two)
          request = connections.flat_map(&:requests).find { |r| r.is_a?(Protocol::ExecuteRequest) }
          request.consistency.should == :two
        end

        it 'uses the specified timeout' do
          sent_timeout = nil
          connections.each do |c|
            c.handle_request do |r, t|
              sent_timeout = t
              handle_request(c, r, t)
            end
          end
          statement.execute(11, 'hello', timeout: 3)
          sent_timeout.should == 3
        end

        context 'when it receives a new connection from the connection manager' do
          let :new_connection do
            FakeConnection.new('h3.example.com', 1234, 5)
          end

          before do
            statement
            new_connection.handle_request { |r, t| handle_request(new_connection, r, t) }
            connections.each(&:close)
            connection_manager.add_connections([new_connection])
          end

          it 'prepares itself on the connection' do
            statement.execute(11, 'hello')
            new_connection.requests.should include(Protocol::PrepareRequest.new(cql))
            execute_request = new_connection.requests.find { |r| r.is_a?(Protocol::ExecuteRequest) }
            execute_request.metadata.should == raw_metadata
            execute_request.values.should == [11, 'hello']
          end

          it 'logs a message with the host ID, host address and its own ID' do
            new_connection[:host_id] = Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
            logger.stub(:debug)
            statement.execute(11, 'hello')
            logger.should have_received(:debug).with(/Statement [0-9a-f]{32} prepared on node a4a70900-24e1-11df-8924-001ff3591711 \(h3.example.com:1234\)/).once
          end
        end

        it 'returns a future that resolves to the result' do
          f = statement.execute(11, 'hello')
          query_result = f.value
          query_result.metadata['my_other_column'].should == ColumnMetadata.new('my_keyspace', 'my_table', 'my_other_column', :text)
          query_result.first.should == rows.first
        end

        it 'returns a failed future when the number of arguments is wrong' do
          f1 = statement.execute(11, :one)
          f2 = statement.execute(11, 'foo', 22, :one)
          expect { f1.value }.to raise_error(NoMethodError)
          expect { f2.value }.to raise_error(ArgumentError)
        end

        it 'sets the trace flag' do
          tracing = false
          connections.each do |c|
            c.handle_request do |r, t|
              if r.is_a?(Protocol::ExecuteRequest)
                tracing = r.trace
              end
              handle_request(c, r, t)
            end
          end
          statement.execute(11, 'hello', trace: true).value
          tracing.should be_true
        end
      end
    end
  end
end
