# encoding: utf-8

require 'spec_helper'


module Cql
  describe Cluster do
    class FakeIoReactor
      attr_reader :connections

      def initialize
        @running = false
        @connections = []
      end

      def next_response=(response)
        @next_response = response
      end

      def start
        @running = true
        @connections.each do |connection|
          connection[:future].complete!
        end
        CompletedFuture.new
      end

      def stop
        @running = false
        CompletedFuture.new
      end

      def running?
        @running
      end

      def add_connection(host, port)
        future = Future.new
        @connections << {:host => host, :port => port, :future => future, :requests => []}
        future.complete! if @running
        future
      end

      def queue_request(request)
        @connections.last[:requests] << request
        CompletedFuture.new(@next_response).tap { @next_response = nil }
      end
    end

    let :connection_options do
      {:host => 'example.com', :port => 12321, :io_reactor => io_reactor}
    end

    let :io_reactor do
      FakeIoReactor.new
    end

    let :cluster do
      described_class.new(connection_options)
    end

    def connections
      io_reactor.connections
    end

    def connection
      connections.first
    end

    def requests
      connection[:requests]
    end

    def last_request
      requests.last
    end

    describe '#start!' do
      it 'connects' do
        cluster.start!
        connections.should have(1).item
      end

      it 'connects only once' do
        cluster.start!
        cluster.start!
        connections.should have(1).item
      end

      it 'returns itself' do
        cluster.start!.should equal(cluster)
      end

      it 'forwards the host and port' do
        cluster.start!
        connection[:host].should == 'example.com'
        connection[:port].should == 12321
      end

      it 'sends a startup request' do
        cluster.start!
        last_request.should be_a(Protocol::StartupRequest)
      end

      it 'is not in a keyspace' do
        cluster.start!
        cluster.keyspace.should be_nil
      end

      it 'changes to the keyspace given as an option' do
        c = described_class.new(connection_options.merge(:keyspace => 'hello_world'))
        c.start!
        last_request.should == Protocol::QueryRequest.new('USE hello_world', :one)
      end

      it 'validates the keyspace name before sending the USE command' do
        c = described_class.new(connection_options.merge(:keyspace => 'system; DROP KEYSPACE system'))
        expect { c.start! }.to raise_error(InvalidKeyspaceNameError)
        requests.should_not include(Protocol::QueryRequest.new('USE system; DROP KEYSPACE system', :one))
      end
    end

    describe '#shutdown!' do
      it 'closes the connection' do
        cluster.start!
        cluster.shutdown!
        io_reactor.should_not be_running
      end

      it 'accepts multiple calls to #shutdown!' do
        cluster.start!
        cluster.shutdown!
        cluster.shutdown!
      end

      it 'returns itself' do
        cluster.start!.shutdown!.should equal(cluster)
      end
    end

    describe '#use' do
      before do
        cluster.start!
        io_reactor.next_response = Protocol::SetKeyspaceResultResponse.new('system')
      end

      it 'executes a USE query' do
        cluster.use('system')
        last_request.should == Protocol::QueryRequest.new('USE system', :one)
      end

      it 'knows which keyspace it changed to' do
        cluster.use('system')
        cluster.keyspace.should == 'system'
      end

      it 'raises an error if the keyspace name is not valid' do
        expect { cluster.use('system; DROP KEYSPACE system') }.to raise_error(InvalidKeyspaceNameError)
      end
    end

    describe '#execute' do
      before do
        cluster.start!
      end

      it 'asks the connection to execute the query' do
        cluster.execute('UPDATE stuff SET thing = 1 WHERE id = 3')
        last_request.should == Protocol::QueryRequest.new('UPDATE stuff SET thing = 1 WHERE id = 3', :quorum)
      end

      it 'uses the specified consistency' do
        cluster.execute('UPDATE stuff SET thing = 1 WHERE id = 3', :three)
        last_request.should == Protocol::QueryRequest.new('UPDATE stuff SET thing = 1 WHERE id = 3', :three)
      end

      context 'with a void CQL query' do
        it 'returns nil' do
          io_reactor.next_response = Protocol::VoidResultResponse.new
          result = cluster.execute('UPDATE stuff SET thing = 1 WHERE id = 3')
          result.should be_nil
        end
      end

      context 'with a USE query' do
        it 'returns nil' do
          io_reactor.next_response = Protocol::SetKeyspaceResultResponse.new('system')
          result = cluster.execute('USE system')
          result.should be_nil
        end

        it 'knows which keyspace it changed to' do
          io_reactor.next_response = Protocol::SetKeyspaceResultResponse.new('system')
          cluster.execute('USE system')
          cluster.keyspace.should == 'system'
        end
      end

      context 'with an SELECT query' do
        let :rows do
          [['xyz', 'abc'], ['abc', 'xyz'], ['123', 'xyz']]
        end

        let :metadata do
          [['thingies', 'things', 'thing', :text], ['thingies', 'things', 'item', :text]]
        end

        let :result do
          io_reactor.next_response = Protocol::RowsResultResponse.new(rows, metadata)
          cluster.execute('SELECT * FROM things')
        end

        it 'returns an Enumerable of rows' do
          row_count = 0
          result.each do |row|
            row_count += 1
          end
          row_count.should == 3
        end

        context 'with metadata that' do
          it 'has keyspace, table and type information' do
            result.metadata['item'].keyspace.should == 'thingies'
            result.metadata['item'].table.should == 'things'
            result.metadata['item'].column_name.should == 'item'
            result.metadata['item'].type.should == :text
          end

          it 'is an Enumerable' do
            result.metadata.map(&:type).should == [:text, :text]
          end

          it 'is splattable' do
            ks, table, col, type = result.metadata['thing']
            ks.should == 'thingies'
            table.should == 'things'
            col.should == 'thing'
            type.should == :text
          end
        end
      end

      context 'when the response is an error' do
        it 'raises an error' do
          io_reactor.next_response = Protocol::ErrorResponse.new(0xabcd, 'Blurgh')
          expect { cluster.execute('SELECT * FROM things') }.to raise_error(QueryError, 'Blurgh')
        end
      end
    end

    describe '#prepare' do
      before do
        cluster.start!
      end

      it 'sends a prepare request' do
        cluster.prepare('SELECT * FROM system.peers')
        last_request.should == Protocol::PrepareRequest.new('SELECT * FROM system.peers')
      end

      it 'returns a prepared statement' do
        io_reactor.next_response = Protocol::PreparedResultResponse.new('A' * 32, [['stuff', 'things', 'item', :varchar]])
        statement = cluster.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement.should_not be_nil
      end

      it 'executes a prepared statement' do
        id = 'A' * 32
        metadata = [['stuff', 'things', 'item', :varchar]]
        io_reactor.next_response = Protocol::PreparedResultResponse.new(id, metadata)
        statement = cluster.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement.execute('foo')
        last_request.should == Protocol::ExecuteRequest.new(id, metadata, ['foo'], :quorum)
      end
    end

    context 'when not connected' do
      it 'complains when #use is called before #start!' do
        expect { cluster.use('system') }.to raise_error(NotConnectedError)
      end

      it 'complains when #use is called after #shutdown!' do
        cluster.start!
        cluster.shutdown!
        expect { cluster.use('system') }.to raise_error(NotConnectedError)
      end

      it 'complains when #execute is called before #start!' do
        expect { cluster.execute('DELETE FROM stuff WHERE id = 3') }.to raise_error(NotConnectedError)
      end

      it 'complains when #execute is called after #shutdown!' do
        cluster.start!
        cluster.shutdown!
        expect { cluster.execute('DELETE FROM stuff WHERE id = 3') }.to raise_error(NotConnectedError)
      end

      it 'complains when #prepare is called before #start!' do
        expect { cluster.prepare('DELETE FROM stuff WHERE id = 3') }.to raise_error(NotConnectedError)
      end

      it 'complains when #prepare is called after #shutdown!' do
        cluster.start!
        cluster.shutdown!
        expect { cluster.prepare('DELETE FROM stuff WHERE id = 3') }.to raise_error(NotConnectedError)
      end

      it 'complains when #execute of a prepared statement is called after #shutdown!' do
        cluster.start!
        io_reactor.next_response = Protocol::PreparedResultResponse.new('A' * 32, [])
        statement = cluster.prepare('DELETE FROM stuff WHERE id = 3')
        cluster.shutdown!
        expect { statement.execute }.to raise_error(NotConnectedError)
      end
    end
  end
end