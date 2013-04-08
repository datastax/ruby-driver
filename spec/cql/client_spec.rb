# encoding: utf-8

require 'spec_helper'


module Cql
  describe Client do
    let :connection_options do
      {:host => 'example.com', :port => 12321, :io_reactor => io_reactor}
    end

    let :io_reactor do
      FakeIoReactor.new
    end

    let :client do
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

    describe '.connect' do
      it 'connects and returns the client' do
        client = described_class.connect(connection_options)
        client.should be_connected
      end
    end

    describe '#connect' do
      it 'connects' do
        client.connect
        connections.should have(1).item
      end

      it 'connects only once' do
        client.connect
        client.connect
        connections.should have(1).item
      end

      it 'connects to all hosts' do
        client.close
        io_reactor.stop.get
        io_reactor.start.get

        c = described_class.new(connection_options.merge(host: 'h1.example.com,h2.example.com,h3.example.com'))
        c.connect
        connections.should have(3).items
      end

      it 'returns itself' do
        client.connect.should equal(client)
      end

      it 'forwards the host and port' do
        client.connect
        connection[:host].should == 'example.com'
        connection[:port].should == 12321
      end

      it 'sends a startup request' do
        client.connect
        last_request.should be_a(Protocol::StartupRequest)
      end

      it 'sends a startup request to each connection' do
        client.close
        io_reactor.stop.get
        io_reactor.start.get

        c = described_class.new(connection_options.merge(host: 'h1.example.com,h2.example.com,h3.example.com'))
        c.connect
        connections.each do |cc|
          cc[:requests].last.should be_a(Protocol::StartupRequest)
        end
      end

      it 'is not in a keyspace' do
        client.connect
        client.keyspace.should be_nil
      end

      it 'changes to the keyspace given as an option' do
        c = described_class.new(connection_options.merge(:keyspace => 'hello_world'))
        c.connect
        last_request.should == Protocol::QueryRequest.new('USE hello_world', :one)
      end

      it 'validates the keyspace name before sending the USE command' do
        c = described_class.new(connection_options.merge(:keyspace => 'system; DROP KEYSPACE system'))
        expect { c.connect }.to raise_error(Client::InvalidKeyspaceNameError)
        requests.should_not include(Protocol::QueryRequest.new('USE system; DROP KEYSPACE system', :one))
      end

      it 're-raises any errors raised' do
        io_reactor.stub(:add_connection).and_raise(ArgumentError)
        expect { client.connect }.to raise_error(ArgumentError)
      end

      it 'is not connected if an error is raised' do
        io_reactor.stub(:add_connection).and_raise(ArgumentError)
        client.connect rescue nil
        client.should_not be_connected
      end

      it 'is connected after #connect returns' do
        client.connect
        client.should be_connected
      end
    end

    describe '#close' do
      it 'closes the connection' do
        client.connect
        client.close
        io_reactor.should_not be_running
      end

      it 'does nothing when called before #connect' do
        client.close
      end

      it 'accepts multiple calls to #close' do
        client.connect
        client.close
        client.close
      end

      it 'returns itself' do
        client.connect.close.should equal(client)
      end
    end

    describe '#use' do
      before do
        client.connect
      end

      it 'executes a USE query' do
        io_reactor.queue_response(Protocol::SetKeyspaceResultResponse.new('system'))
        client.use('system')
        last_request.should == Protocol::QueryRequest.new('USE system', :one)
      end

      it 'executes a USE query for each connection' do
        client.close
        io_reactor.stop.get
        io_reactor.start.get

        c = described_class.new(connection_options.merge(host: 'h1.example.com,h2.example.com,h3.example.com'))
        c.connect

        c.use('system')
        last_requests = connections.select { |c| c[:host] =~ /^h\d\.example\.com$/ }.sort_by { |c| c[:host] }.map { |c| c[:requests].last }
        last_requests.should == [
          Protocol::QueryRequest.new('USE system', :one),
          Protocol::QueryRequest.new('USE system', :one),
          Protocol::QueryRequest.new('USE system', :one)
        ]
      end

      it 'knows which keyspace it changed to' do
        io_reactor.queue_response(Protocol::SetKeyspaceResultResponse.new('system'))
        client.use('system')
        client.keyspace.should == 'system'
      end

      it 'raises an error if the keyspace name is not valid' do
        expect { client.use('system; DROP KEYSPACE system') }.to raise_error(Client::InvalidKeyspaceNameError)
      end
    end

    describe '#execute' do
      before do
        client.connect
      end

      it 'asks the connection to execute the query' do
        client.execute('UPDATE stuff SET thing = 1 WHERE id = 3')
        last_request.should == Protocol::QueryRequest.new('UPDATE stuff SET thing = 1 WHERE id = 3', :quorum)
      end

      it 'uses the specified consistency' do
        client.execute('UPDATE stuff SET thing = 1 WHERE id = 3', :three)
        last_request.should == Protocol::QueryRequest.new('UPDATE stuff SET thing = 1 WHERE id = 3', :three)
      end

      context 'with a void CQL query' do
        it 'returns nil' do
          io_reactor.queue_response(Protocol::VoidResultResponse.new)
          result = client.execute('UPDATE stuff SET thing = 1 WHERE id = 3')
          result.should be_nil
        end
      end

      context 'with a USE query' do
        it 'returns nil' do
          io_reactor.queue_response(Protocol::SetKeyspaceResultResponse.new('system'))
          result = client.execute('USE system')
          result.should be_nil
        end

        it 'knows which keyspace it changed to' do
          io_reactor.queue_response(Protocol::SetKeyspaceResultResponse.new('system'))
          client.execute('USE system')
          client.keyspace.should == 'system'
        end

        it 'detects that one connection changed to a keyspace and changes the others too' do
          client.close
          io_reactor.stop.get
          io_reactor.start.get

          c = described_class.new(connection_options.merge(host: 'h1.example.com,h2.example.com,h3.example.com'))
          c.connect

          io_reactor.queue_response(Protocol::SetKeyspaceResultResponse.new('system'), connections.find { |c| c[:host] == 'h1.example.com' }[:host])
          io_reactor.queue_response(Protocol::SetKeyspaceResultResponse.new('system'), connections.find { |c| c[:host] == 'h2.example.com' }[:host])
          io_reactor.queue_response(Protocol::SetKeyspaceResultResponse.new('system'), connections.find { |c| c[:host] == 'h3.example.com' }[:host])

          c.execute('USE system', :one)
          c.keyspace.should == 'system'

          last_requests = connections.select { |c| c[:host] =~ /^h\d\.example\.com$/ }.sort_by { |c| c[:host] }.map { |c| c[:requests].last }
          last_requests.should == [
            Protocol::QueryRequest.new('USE system', :one),
            Protocol::QueryRequest.new('USE system', :one),
            Protocol::QueryRequest.new('USE system', :one)
          ]
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
          io_reactor.queue_response(Protocol::RowsResultResponse.new(rows, metadata))
          client.execute('SELECT * FROM things')
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
          io_reactor.queue_response(Protocol::ErrorResponse.new(0xabcd, 'Blurgh'))
          expect { client.execute('SELECT * FROM things') }.to raise_error(QueryError, 'Blurgh')
        end
      end
    end

    describe '#prepare' do
      let :id do
        'A' * 32
      end

      let :metadata do
        [['stuff', 'things', 'item', :varchar]]
      end

      before do
        client.connect
      end

      it 'sends a prepare request' do
        client.prepare('SELECT * FROM system.peers')
        last_request.should == Protocol::PrepareRequest.new('SELECT * FROM system.peers')
      end

      it 'returns a prepared statement' do
        io_reactor.queue_response(Protocol::PreparedResultResponse.new('A' * 32, [['stuff', 'things', 'item', :varchar]]))
        statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement.should_not be_nil
      end

      it 'executes a prepared statement' do
        io_reactor.queue_response(Protocol::PreparedResultResponse.new(id, metadata))
        statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement.execute('foo')
        last_request.should == Protocol::ExecuteRequest.new(id, metadata, ['foo'], :quorum)
      end

      it 'returns a prepared statement that knows the metadata' do
        io_reactor.queue_response(Protocol::PreparedResultResponse.new(id, metadata))
        statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement.metadata['item'].type == :varchar
      end

      it 'executes a prepared statement with a specific consistency level' do
        io_reactor.queue_response(Protocol::PreparedResultResponse.new(id, metadata))
        statement = client.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement.execute('thing', :local_quorum)
        last_request.should == Protocol::ExecuteRequest.new(id, metadata, ['thing'], :local_quorum)
      end

      it 'executes a prepared statement using the right connection' do
        client.close
        io_reactor.stop.get
        io_reactor.start.get

        c = described_class.new(connection_options.merge(host: 'h1.example.com,h2.example.com,h3.example.com'))
        c.connect

        io_reactor.queue_response(Protocol::PreparedResultResponse.new('A' * 32, metadata))
        io_reactor.queue_response(Protocol::PreparedResultResponse.new('B' * 32, metadata))
        io_reactor.queue_response(Protocol::PreparedResultResponse.new('C' * 32, metadata))

        statement1 = c.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement1_connection = io_reactor.last_used_connection
        statement2 = c.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement2_connection = io_reactor.last_used_connection
        statement3 = c.prepare('SELECT * FROM stuff.things WHERE item = ?')
        statement3_connection = io_reactor.last_used_connection

        io_reactor.queue_response(Protocol::RowsResultResponse.new([{'thing' => 'foo1'}], metadata), statement1_connection[:host])
        io_reactor.queue_response(Protocol::RowsResultResponse.new([{'thing' => 'foo2'}], metadata), statement2_connection[:host])
        io_reactor.queue_response(Protocol::RowsResultResponse.new([{'thing' => 'foo3'}], metadata), statement3_connection[:host])

        statement1.execute('foo').first.should == {'thing' => 'foo1'}
        statement2.execute('foo').first.should == {'thing' => 'foo2'}
        statement3.execute('foo').first.should == {'thing' => 'foo3'}
      end
    end

    context 'when not connected' do
      it 'is not connected before #connect has been called' do
        client.should_not be_connected
      end

      it 'is not connected after #close has been called' do
        client.connect
        client.close
        client.should_not be_connected
      end

      it 'complains when #use is called before #connect' do
        expect { client.use('system') }.to raise_error(Client::NotConnectedError)
      end

      it 'complains when #use is called after #close' do
        client.connect
        client.close
        expect { client.use('system') }.to raise_error(Client::NotConnectedError)
      end

      it 'complains when #execute is called before #connect' do
        expect { client.execute('DELETE FROM stuff WHERE id = 3') }.to raise_error(Client::NotConnectedError)
      end

      it 'complains when #execute is called after #close' do
        client.connect
        client.close
        expect { client.execute('DELETE FROM stuff WHERE id = 3') }.to raise_error(Client::NotConnectedError)
      end

      it 'complains when #prepare is called before #connect' do
        expect { client.prepare('DELETE FROM stuff WHERE id = 3') }.to raise_error(Client::NotConnectedError)
      end

      it 'complains when #prepare is called after #close' do
        client.connect
        client.close
        expect { client.prepare('DELETE FROM stuff WHERE id = 3') }.to raise_error(Client::NotConnectedError)
      end

      it 'complains when #execute of a prepared statement is called after #close' do
        client.connect
        io_reactor.queue_response(Protocol::PreparedResultResponse.new('A' * 32, []))
        statement = client.prepare('DELETE FROM stuff WHERE id = 3')
        client.close
        expect { statement.execute }.to raise_error(Client::NotConnectedError)
      end
    end
  end
end