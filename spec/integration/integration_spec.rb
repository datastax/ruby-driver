# encoding: utf-8

require 'spec_helper'


describe 'Startup' do
  let :connection do
    Cql::Connection.new(host: ENV['CASSANDRA_HOST']).open
  end

  let :keyspace_name do
    "cql_rb_#{rand(1000)}"
  end

  after do
    connection.close
  end

  def execute_request(request)
    connection.execute!(request)
  end

  def query(cql, consistency=:any)
    response = execute_request(Cql::Protocol::QueryRequest.new(cql, consistency))
    raise response.to_s if response.is_a?(Cql::Protocol::ErrorResponse)
    response
  end

  def create_keyspace!
    query("CREATE KEYSPACE #{keyspace_name} WITH REPLICATION = {'CLASS': 'SimpleStrategy', 'replication_factor': 1}")
  end

  def use_keyspace!
    query("USE #{keyspace_name}")
  end

  def drop_keyspace!
    query("DROP KEYSPACE #{keyspace_name}")
  end

  def create_table!
    query('CREATE TABLE users (user_name VARCHAR, password VARCHAR, email VARCHAR, PRIMARY KEY (user_name))')
  end

  def in_keyspace
    create_keyspace!
    use_keyspace!
    begin
      yield
    ensure
      begin
        drop_keyspace!
      rescue  Errno::EPIPE => e
        # ignore since we're shutting down
      end
    end
  end

  def in_keyspace_with_table
    in_keyspace do
      create_table!
      yield
    end
  end

  context 'when setting up' do
    it 'sends OPTIONS and receives SUPPORTED' do
      response = execute_request(Cql::Protocol::OptionsRequest.new)
      response.options.should include('CQL_VERSION' => ['3.0.0'])
    end

    it 'sends STARTUP and receives READY' do
      response = execute_request(Cql::Protocol::StartupRequest.new)
      response.should be_a(Cql::Protocol::ReadyResponse)
    end

    it 'sends a bad STARTUP and receives ERROR' do
      response = execute_request(Cql::Protocol::StartupRequest.new('9.9.9'))
      response.code.should == 10
      response.message.should include('not supported')
    end
  end

  context 'when set up' do
    before do
      response = execute_request(Cql::Protocol::StartupRequest.new)
      response
    end

    context 'with events' do
      it 'sends a REGISTER request and receives READY' do
        response = execute_request(Cql::Protocol::RegisterRequest.new('TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE'))
        response.should be_a(Cql::Protocol::ReadyResponse)
      end

      it 'passes events to listeners' do
        semaphore = Queue.new
        event = nil
        execute_request(Cql::Protocol::RegisterRequest.new('SCHEMA_CHANGE'))
        connection.on_event do |event_response|
          event = event_response
          semaphore << :ping
        end
        begin
          create_keyspace!
          semaphore.pop
          event.change.should == 'CREATED'
          event.keyspace.should == keyspace_name
        ensure
          drop_keyspace!
        end
      end
    end

    context 'when running queries' do
      context 'with QUERY requests' do
        it 'sends a USE command' do
          response = query('USE system', :one)
          response.keyspace.should == 'system'
        end

        it 'sends a bad CQL string and receives ERROR' do
          response = execute_request(Cql::Protocol::QueryRequest.new('HELLO WORLD', :any))
          response.should be_a(Cql::Protocol::ErrorResponse)
        end

        it 'sends a CREATE KEYSPACE command' do
          response = query("CREATE KEYSPACE #{keyspace_name} WITH REPLICATION = {'CLASS': 'SimpleStrategy', 'replication_factor': 1}")
          begin
            response.change.should == 'CREATED'
            response.keyspace.should == keyspace_name
          ensure
            drop_keyspace!
          end
        end

        it 'sends a DROP KEYSPACE command' do
          create_keyspace!
          use_keyspace!
          response = query("DROP KEYSPACE #{keyspace_name}")
          response.change.should == 'DROPPED'
          response.keyspace.should == keyspace_name
        end

        it 'sends an ALTER KEYSPACE command' do
          create_keyspace!
          begin
            response = query("ALTER KEYSPACE #{keyspace_name} WITH DURABLE_WRITES = false")
            response.change.should == 'UPDATED'
            response.keyspace.should == keyspace_name
          ensure
            drop_keyspace!
          end
        end

        it 'sends a CREATE TABLE command' do
          in_keyspace do
            response = query('CREATE TABLE users (user_name VARCHAR, password VARCHAR, email VARCHAR, PRIMARY KEY (user_name))')
            response.change.should == 'CREATED'
            response.keyspace.should == keyspace_name
            response.table.should == 'users'
          end
        end

        it 'sends a DROP TABLE command' do
          in_keyspace_with_table do
            response = query('DROP TABLE users')
            response.change.should == 'DROPPED'
            response.keyspace.should == keyspace_name
            response.table.should == 'users'
          end
        end

        it 'sends an ALTER TABLE command' do
          in_keyspace_with_table do
            response = query('ALTER TABLE users ADD age INT')
            response.change.should == 'UPDATED'
            response.keyspace.should == keyspace_name
            response.table.should == 'users'
          end
        end

        it 'sends an INSERT command' do
          in_keyspace_with_table do
            query(%<INSERT INTO users (user_name, email) VALUES ('phil', 'phil@heck.com')>)
          end
        end

        it 'sends an UPDATE command' do
          in_keyspace_with_table do
            query(%<INSERT INTO users (user_name, email) VALUES ('phil', 'phil@heck.com')>)
            query(%<UPDATE users SET email = 'sue@heck.com' WHERE user_name = 'phil'>)
          end
        end

        it 'sends a DELETE command' do
          in_keyspace_with_table do
            query(%<DELETE email FROM users WHERE user_name = 'sue'>)
          end
        end

        it 'sends a TRUNCATE command' do
          pending 'this times out in C* with "Truncate timed out - received only 0 responses" (but it does that in cqlsh too, so not sure what is going on)'
          in_keyspace_with_table do
            query(%<TRUNCATE users>)
          end
        end

        it 'sends a BATCH command' do
          pending 'this times out'
          in_keyspace_with_table do
            query(<<-EOQ, :one)
              BEGIN BATCH
                INSERT INTO users (user_name, email) VALUES ('phil', 'phil@heck.com')
                INSERT INTO users (user_name, email) VALUES ('sue', 'sue@inter.net')
              APPLY BATCH
            EOQ
          end
        end

        it 'sends a SELECT command' do
          in_keyspace_with_table do
            query(%<INSERT INTO users (user_name, email) VALUES ('phil', 'phil@heck.com')>)
            query(%<INSERT INTO users (user_name, email) VALUES ('sue', 'sue@inter.net')>)
            response = query(%<SELECT * FROM users>, :quorum)
            response.rows.should == [
              {'user_name' => 'phil', 'email' => 'phil@heck.com', 'password' => nil},
              {'user_name' => 'sue',  'email' => 'sue@inter.net', 'password' => nil}
            ]
          end
        end
      end

      context 'with PREPARE requests' do
        it 'sends a PREPARE request and receives RESULT' do
          in_keyspace_with_table do
            response = execute_request(Cql::Protocol::PrepareRequest.new('SELECT * FROM users WHERE user_name = ?'))
            response.id.should_not be_nil
            response.metadata.should_not be_nil
          end
        end

        it 'sends an EXECUTE request and receives RESULT' do
          in_keyspace do
            create_table_cql = %<CREATE TABLE stuff (id1 UUID, id2 VARINT, id3 TIMESTAMP, value1 DOUBLE, value2 TIMEUUID, value3 BLOB, PRIMARY KEY (id1, id2, id3))>
            insert_cql = %<INSERT INTO stuff (id1, id2, id3, value1, value2, value3) VALUES (?, ?, ?, ?, ?, ?)>
            create_response = execute_request(Cql::Protocol::QueryRequest.new(create_table_cql, :one))
            create_response.should_not be_a(Cql::Protocol::ErrorResponse)
            prepare_response = execute_request(Cql::Protocol::PrepareRequest.new(insert_cql))
            prepare_response.should_not be_a(Cql::Protocol::ErrorResponse)
            execute_response = execute_request(Cql::Protocol::ExecuteRequest.new(prepare_response.id, prepare_response.metadata, [Cql::Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6'), -12312312312, Time.now, 345345.234234, Cql::Uuid.new('a4a70900-24e1-11df-8924-001ff3591711'), "\xab\xcd\xef".force_encoding(::Encoding::BINARY)], :one))
            execute_response.should_not be_a(Cql::Protocol::ErrorResponse)
          end
        end
      end

      context 'with pipelining' do
        it 'handles multiple concurrent requests' do
          in_keyspace_with_table do
            semaphore = Queue.new

            10.times do
              connection.execute(Cql::Protocol::QueryRequest.new('SELECT * FROM users', :quorum)) do |response|
                semaphore << :ping
              end
            end

            connection.execute(Cql::Protocol::QueryRequest.new(%<INSERT INTO users (user_name, email) VALUES ('sam', 'sam@ham.com')>, :one)) do |response|
              semaphore << :ping
            end

            11.times { semaphore.pop }
          end
        end

        it 'handles lots of concurrent requests' do
          in_keyspace_with_table do
            semaphore = Queue.new

            2000.times do
              connection.execute(Cql::Protocol::QueryRequest.new('SELECT * FROM users', :quorum)) do |response|
                semaphore << :ping
              end
            end

            2000.times { semaphore.pop }
          end
        end
      end
    end
  end

  context 'in special circumstances' do
    it 'raises an exception when it cannot connect to Cassandra' do
      expect { Cql::Connection.new(host: 'example.com', timeout: 0.1).open.execute(Cql::Protocol::OptionsRequest.new) }.to raise_error(Cql::ConnectionError)
      expect { Cql::Connection.new(host: 'blackhole', timeout: 0.1).open.execute(Cql::Protocol::OptionsRequest.new) }.to raise_error(Cql::ConnectionError)
    end

    it 'does nothing the second time #open is called' do
      connection = Cql::Connection.new
      connection.open
      connection.execute!(Cql::Protocol::StartupRequest.new)
      connection.open
      response = connection.execute!(Cql::Protocol::QueryRequest.new('USE system', :any))
      response.should_not be_a(Cql::Protocol::ErrorResponse)
    end
  end
end