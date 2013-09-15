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

      def handle_request(connection, request)
        case request
        when Protocol::PrepareRequest
          statement_id = [rand(2**31)].pack('c*')
          connection[:last_prepared_statement_id] = statement_id
          Protocol::PreparedResultResponse.new(statement_id, raw_metadata)
        when Protocol::ExecuteRequest
          Protocol::RowsResultResponse.new(rows, raw_metadata)
        else
          raise %(Unexpected request: #{request})
        end
      end

      before do
        connections.each do |c|
          c.handle_request { |r| handle_request(c, r) }
        end
        connection_manager.add_connections(connections)
      end

      describe '.prepare' do
        it 'prepares a statement on all connections' do
          f = described_class.prepare(cql, :one, connection_manager, logger)
          f.value
          connections.each do |c|
            c.requests.should include(Protocol::PrepareRequest.new(cql))
          end
        end

        it 'returns a prepared statement object' do
          f = described_class.prepare(cql, :two, connection_manager, logger)
          f.value.should be_a(PreparedStatement)
        end

        it 'returns a failed future when something goes wrong in the preparation' do
          connections.each(&:close)
          f = described_class.prepare(cql, :three, connection_manager, logger)
          expect { f.value }.to raise_error(NotConnectedError)
        end
      end

      describe '#metadata' do
        let :statement do
          described_class.prepare(cql, :all, connection_manager, logger).value
        end

        it 'returns the interpreted metadata' do
          statement.metadata.should be_a(ResultMetadata)
          statement.metadata['my_column'].should be_a(ColumnMetadata)
        end
      end

      describe '#execute' do
        let :statement do
          described_class.prepare(cql, :local_quorum, connection_manager, logger).value
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

        context 'when it receives a new connection from the connection manager' do
          let :new_connection do
            FakeConnection.new('h3.example.com', 1234, 5)
          end

          before do
            statement
            new_connection.handle_request { |r| handle_request(new_connection, r) }
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

          it 'logs a message' do
            logger.stub(:debug)
            statement.execute(11, 'hello')
            logger.should have_received(:debug).with(/Statement prepared/).once
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
          expect { f1.value }.to raise_error
          expect { f2.value }.to raise_error
        end
      end
    end
  end
end
