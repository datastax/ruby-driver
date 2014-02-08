# encoding: utf-8

require 'spec_helper'


describe 'Protocol parsing and communication' do
  let :protocol_version do
    2
  end

  let! :io_reactor do
    ir = Cql::Io::IoReactor.new(lambda { |*args| Cql::Protocol::CqlProtocolHandler.new(*args, protocol_version) })
    ir.start
    connections << ir.connect(ENV['CASSANDRA_HOST'], 9042, 5).value
    ir
  end

  let :connections do
    []
  end

  let :keyspace_name do
    "cql_rb_#{rand(1000)}"
  end

  after do
    if io_reactor.running?
      drop_keyspace! rescue nil
      io_reactor.stop.value rescue nil
    end
  end

  def authentication_enabled?
    ir = Cql::Io::IoReactor.new(lambda { |*args| Cql::Protocol::CqlProtocolHandler.new(*args, protocol_version) })
    ir.start
    connected = ir.connect(ENV['CASSANDRA_HOST'], 9042, 5)
    started = connected.flat_map do |connection|
      connection.send_request(Cql::Protocol::StartupRequest.new)
    end
    response = started.value
    required = response.is_a?(Cql::Protocol::AuthenticateResponse)
    ir.stop.value
    required
  end


  def raw_execute_request(request)
    connection = connections.first
    connection.send_request(request).value
  end

  def execute_request(request, auto_authenticate=true)
    response = raw_execute_request(request)
    if response.is_a?(Cql::Protocol::AuthenticateResponse) && auto_authenticate
      unless response.authentication_class == 'org.apache.cassandra.auth.PasswordAuthenticator'
        raise "Cassandra required an unsupported authenticator: #{response.authentication_class}"
      end
      if protocol_version == 1
        response = execute_request(Cql::Protocol::CredentialsRequest.new('username' => 'cassandra', 'password' => 'cassandra'))
      else
        response = execute_request(Cql::Protocol::AuthResponseRequest.new("\x00cassandra\x00cassandra"))
      end
    end
    response
  end

  def query(cql, consistency=:one)
    response = execute_request(Cql::Protocol::QueryRequest.new(cql, nil, nil, consistency))
    raise response.to_s if response.is_a?(Cql::Protocol::ErrorResponse)
    response
  end

  def create_keyspace!
    query("CREATE KEYSPACE #{keyspace_name} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
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

  def create_counters_table!
    query('CREATE TABLE counters (id VARCHAR, c1 COUNTER, c2 COUNTER, PRIMARY KEY (id))')
  end

  def in_keyspace
    create_keyspace!
    use_keyspace!
    begin
      yield
    ensure
      begin
        drop_keyspace!
      rescue Cql::NotConnectedError, Errno::EPIPE => e
        # ignore since we're shutting down, and these errors are likely caused
        # by the code under test, re-raising them would mask the real errors
      end
    end
  end

  def in_keyspace_with_table
    in_keyspace do
      create_table!
      yield
    end
  end

  def in_keyspace_with_counters_table
    in_keyspace do
      create_counters_table!
      yield
    end
  end

  context 'when setting up' do
    it 'sends OPTIONS and receives SUPPORTED' do
      response = execute_request(Cql::Protocol::OptionsRequest.new)
      response.options.should have_key('CQL_VERSION')
    end

    it 'sends a bad STARTUP and receives ERROR' do
      response = execute_request(Cql::Protocol::StartupRequest.new('9.9.9'))
      response.code.should == 10
      response.message.should include('not supported')
    end

    context 'when authentication is not required' do
      it 'sends STARTUP and receives READY' do
        pending('authentication required', if: authentication_enabled?) do
          response = execute_request(Cql::Protocol::StartupRequest.new, false)
          response.should be_a(Cql::Protocol::ReadyResponse)
        end
      end
    end

    context 'when authentication is required' do
      context 'and the protocol version is 1' do
        let :protocol_version do
          1
        end

        it 'sends STARTUP and receives AUTHENTICATE' do
          pending('authentication not configured', unless: authentication_enabled?) do
            response = raw_execute_request(Cql::Protocol::StartupRequest.new)
            response.should be_a(Cql::Protocol::AuthenticateResponse)
          end
        end

        it 'ignores the AUTHENTICATE response and receives ERROR' do
          pending('authentication not configured', unless: authentication_enabled?) do
            raw_execute_request(Cql::Protocol::StartupRequest.new)
            response = raw_execute_request(Cql::Protocol::RegisterRequest.new('TOPOLOGY_CHANGE'))
            response.code.should == 10
          end
        end

        it 'sends STARTUP followed by CREDENTIALS and receives READY' do
          raw_execute_request(Cql::Protocol::StartupRequest.new)
          response = raw_execute_request(Cql::Protocol::CredentialsRequest.new('username' => 'cassandra', 'password' => 'cassandra'))
          response.should be_a(Cql::Protocol::ReadyResponse)
        end

        it 'sends bad username and password in CREDENTIALS and receives ERROR' do
          pending('authentication not configured', unless: authentication_enabled?) do
            raw_execute_request(Cql::Protocol::StartupRequest.new)
            response = raw_execute_request(Cql::Protocol::CredentialsRequest.new('username' => 'foo', 'password' => 'bar'))
            response.code.should == 0x100
          end
        end
      end

      context 'and the protocol version is 2' do
        it 'sends STARTUP and receives AUTHENTICATE' do
          pending('authentication not configured', unless: authentication_enabled?) do
            response = raw_execute_request(Cql::Protocol::StartupRequest.new)
            response.should be_a(Cql::Protocol::AuthenticateResponse)
          end
        end

        it 'ignores the AUTHENTICATE response and receives ERROR' do
          pending('authentication not configured', unless: authentication_enabled?) do
            raw_execute_request(Cql::Protocol::StartupRequest.new)
            response = raw_execute_request(Cql::Protocol::RegisterRequest.new('TOPOLOGY_CHANGE'))
            response.code.should == 10
          end
        end

        it 'sends STARTUP followed by AUTH_RESPONSE and receives AUTH_SUCCESS' do
          pending('authentication not configured', unless: authentication_enabled?) do
            raw_execute_request(Cql::Protocol::StartupRequest.new)
            response = raw_execute_request(Cql::Protocol::AuthResponseRequest.new("\x00cassandra\x00cassandra"))
            response.should be_a(Cql::Protocol::AuthSuccessResponse)
          end
        end

        it 'sends bad username and password in AUTH_RESPONSE and receives ERROR' do
          pending('authentication not configured', unless: authentication_enabled?) do
            raw_execute_request(Cql::Protocol::StartupRequest.new)
            response = raw_execute_request(Cql::Protocol::AuthResponseRequest.new("\x00cassandra\x00ardnassac"))
            response.code.should == 0x100
          end
        end
      end
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
        connections.first.on_event do |event_response|
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
          response = execute_request(Cql::Protocol::QueryRequest.new('HELLO WORLD', nil, nil, :any))
          response.should be_a(Cql::Protocol::ErrorResponse)
        end

        it 'sends a CREATE KEYSPACE command' do
          response = query("CREATE KEYSPACE #{keyspace_name} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
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
            response = query(%<INSERT INTO users (user_name, email) VALUES ('phil', 'phil@heck.com')>)
            response.should be_void
          end
        end

        it 'sends an UPDATE command' do
          in_keyspace_with_table do
            query(%<INSERT INTO users (user_name, email) VALUES ('phil', 'phil@heck.com')>)
            response = query(%<UPDATE users SET email = 'sue@heck.com' WHERE user_name = 'phil'>)
            response.should be_void
          end
        end

        it 'increments a counter' do
          in_keyspace_with_counters_table do
            response = query(%<UPDATE counters SET c1 = c1 + 1, c2 = c2 - 2 WHERE id = 'stuff'>)
            response.should be_void
          end
        end

        it 'sends a DELETE command' do
          in_keyspace_with_table do
            response = query(%<DELETE email FROM users WHERE user_name = 'sue'>)
            response.should be_void
          end
        end

        it 'sends a TRUNCATE command' do
          in_keyspace_with_table do
            response = query(%<TRUNCATE users>)
            response.should be_void
          end
        end

        it 'sends a BATCH command' do
          in_keyspace_with_table do
            response = query(<<-EOQ)
              BEGIN BATCH
                INSERT INTO users (user_name, email) VALUES ('phil', 'phil@heck.com')
                INSERT INTO users (user_name, email) VALUES ('sue', 'sue@inter.net')
              APPLY BATCH
            EOQ
            response.should be_void
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

        it 'sends an INSERT command with bound values' do
          in_keyspace_with_table do
            cql = %<INSERT INTO users (user_name, email) VALUES (?, ?)>
            response = execute_request(Cql::Protocol::QueryRequest.new(cql, ['sue', 'sue@inter.net'], nil, :one))
            response.should be_void
          end
        end

        it 'sends an UPDATE command with bound values' do
          in_keyspace_with_table do
            cql = %<UPDATE users SET email = ? WHERE user_name = ?>
            response = execute_request(Cql::Protocol::QueryRequest.new(cql, ['sue@inter.net', 'sue'], nil, :one))
            response.should be_void
          end
        end

        it 'sends a SELECT command with bound values' do
          in_keyspace_with_table do
            query(%<INSERT INTO users (user_name, email) VALUES ('phil', 'phil@heck.com')>)
            query(%<INSERT INTO users (user_name, email) VALUES ('sue', 'sue@inter.net')>)
            cql = %<SELECT * FROM users WHERE user_name = ?>
            response = execute_request(Cql::Protocol::QueryRequest.new(cql, ['sue'], nil, :one))
            response.rows.should == [
              {'user_name' => 'sue',  'email' => 'sue@inter.net', 'password' => nil}
            ]
          end
        end

        it 'guesses the types of bound values' do
          in_keyspace do
            query('CREATE TABLE types (a BIGINT PRIMARY KEY, b DOUBLE, c ASCII, d BOOLEAN, e TIMESTAMP, f UUID, g DECIMAL, h BLOB)')
            cql = %<UPDATE types SET b = ?, c = ?, d = ?, e = ?, f = ?, g = ?, h = ? WHERE a = ?>
            values = [123.456, 'foo', true, Time.now, Cql::TimeUuid::Generator.new.next, BigDecimal.new('0.01'), 'hello', 3]
            response = execute_request(Cql::Protocol::QueryRequest.new(cql, values, nil, :one))
            response.should be_void
          end
        end

        it 'uses the provided type hints to encode bound values' do
          in_keyspace do
            query('CREATE TABLE types (a INT PRIMARY KEY, b FLOAT)')
            cql = %<UPDATE types SET b = ? WHERE a = ?>
            values = [123.456, 3]
            hints = [:float, :int]
            response = execute_request(Cql::Protocol::QueryRequest.new(cql, values, hints, :one))
            response.should be_void
          end
        end

        it 'sends a SELECT command with a page size and receives a ROWS RESULT with a paging state' do
          in_keyspace_with_table do
            10.times do
              query(%<INSERT INTO users (user_name, email) VALUES ('#{rand(234234).to_s(36)}', 'someone@somewhere.sx')>)
            end
            response = execute_request(Cql::Protocol::QueryRequest.new('SELECT * FROM users', nil, nil, :one, nil, 6))
            response.paging_state.should_not be_nil
            response.rows.size.should == 6
            response = execute_request(Cql::Protocol::QueryRequest.new('SELECT * FROM users', nil, nil, :one, nil, 6, response.paging_state))
            response.paging_state.should be_nil
            response.rows.size.should == 4
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
            create_response = execute_request(Cql::Protocol::QueryRequest.new(create_table_cql, nil, nil, :one))
            create_response.should_not be_a(Cql::Protocol::ErrorResponse)
            prepare_response = execute_request(Cql::Protocol::PrepareRequest.new(insert_cql))
            prepare_response.should_not be_a(Cql::Protocol::ErrorResponse)
            values = [Cql::Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6'), -12312312312, Time.now, 345345.234234, Cql::Uuid.new('a4a70900-24e1-11df-8924-001ff3591711'), "\xab\xcd\xef".force_encoding(::Encoding::BINARY)]
            execute_response = execute_request(Cql::Protocol::ExecuteRequest.new(prepare_response.id, prepare_response.metadata, values, nil, :one, nil, nil, nil, true))
            execute_response.should_not be_a(Cql::Protocol::ErrorResponse)
          end
        end

        it 'sends an EXECUTE with a page size and receives a RESULT with a paging state' do
          in_keyspace_with_table do
            10.times do
              query(%<INSERT INTO users (user_name, email) VALUES ('#{rand(234234).to_s(36)}', 'someone@somewhere.sx')>)
            end
            response = execute_request(Cql::Protocol::PrepareRequest.new('SELECT * FROM users'))
            statement_id = response.id
            metadata = response.metadata
            response = execute_request(Cql::Protocol::ExecuteRequest.new(statement_id, metadata, [], true, :one, nil, 6, nil))
            response.paging_state.should_not be_nil
            response.rows.size.should == 6
            response = execute_request(Cql::Protocol::ExecuteRequest.new(statement_id, metadata, [], true, :one, nil, 6, response.paging_state))
            response.paging_state.should be_nil
            response.rows.size.should == 4
          end
        end
      end

      context 'with BATCH requests' do
        it 'sends a BATCH request and receives RESULT' do
          in_keyspace_with_table do
            cql1 = %<INSERT INTO users (user_name, email) VALUES (?, ?)>
            cql2 = %<UPDATE users SET email = ? WHERE user_name = 'sue'>
            cql3 = %<INSERT INTO users (user_name, email) VALUES ('tim', 'tim@pim.com')>
            prepared_response1 = execute_request(Cql::Protocol::PrepareRequest.new(cql1))
            prepared_response2 = execute_request(Cql::Protocol::PrepareRequest.new(cql3))
            request = Cql::Protocol::BatchRequest.new(0, :quorum)
            request.add_query(cql1, ['sue', 'sue@inter.net'])
            request.add_query(cql2, ['eve'])
            request.add_query(cql3)
            request.add_prepared(prepared_response1.id, prepared_response1.metadata, ['phil', 'phil@heck.com'])
            request.add_prepared(prepared_response2.id, prepared_response2.metadata, [])
            response = execute_request(request)
            response.should be_void
          end
        end
      end

      context 'with tracing' do
        it 'sends a QUERY request with the tracing flag and receives a RESULT with a trace ID' do
          in_keyspace_with_table do
            response = execute_request(Cql::Protocol::QueryRequest.new('SELECT * FROM users', nil, nil, :quorum, nil, nil, nil, true))
            response.trace_id.should_not be_nil
          end
        end

        it 'sends an PREPARE request with the tracing flag and receives a RESULT with a trace ID' do
          in_keyspace_with_table do
            response = execute_request(Cql::Protocol::PrepareRequest.new('SELECT * FROM users', true))
            response.trace_id.should_not be_nil
          end
        end

        it 'sends an EXECUTE request with the tracing flag and receives a RESULT with a trace ID' do
          in_keyspace_with_table do
            prepare_response = execute_request(Cql::Protocol::PrepareRequest.new('SELECT * FROM users'))
            execute_response = execute_request(Cql::Protocol::ExecuteRequest.new(prepare_response.id, prepare_response.metadata, [], true, :one, nil, nil, nil, true))
            execute_response.trace_id.should_not be_nil
          end
        end
      end

      context 'with compression' do
        let :compressor do
          Cql::Compression::SnappyCompressor.new(0)
        end

        it 'sends a compressed request and receives a compressed response' do
          compressor.stub(:compress).and_call_original
          compressor.stub(:decompress).and_call_original
          io_reactor = Cql::Io::IoReactor.new(lambda { |*args| Cql::Protocol::CqlProtocolHandler.new(*args, protocol_version, compressor) })
          io_reactor.start.value
          begin
            connection = io_reactor.connect(ENV['CASSANDRA_HOST'], 9042, 0.1).value
            connection.send_request(Cql::Protocol::StartupRequest.new(nil, 'snappy')).value
            connection.send_request(Cql::Protocol::PrepareRequest.new('SELECT * FROM system.peers')).value
            compressor.should have_received(:compress).at_least(1).times
            compressor.should have_received(:decompress).at_least(1).times
          ensure
            io_reactor.stop.value
          end
        end
      end

      context 'with pipelining' do
        let :connection do
          connections.first
        end

        it 'handles multiple concurrent requests' do
          in_keyspace_with_table do
            futures = 10.times.map do
              connection.send_request(Cql::Protocol::QueryRequest.new('SELECT * FROM users', nil, nil, :quorum))
            end

            futures << connection.send_request(Cql::Protocol::QueryRequest.new(%<INSERT INTO users (user_name, email) VALUES ('sam', 'sam@ham.com')>, nil, nil, :one))
            
            Cql::Future.all(*futures).value
          end
        end

        it 'handles lots of concurrent requests' do
          in_keyspace_with_table do
            threads = Array.new(10) do
              Thread.new do
                futures = 200.times.map do
                  connection.send_request(Cql::Protocol::QueryRequest.new('SELECT * FROM users', nil, nil, :quorum))
                end
                Cql::Future.all(*futures).value
              end
            end
            threads.each(&:join)
          end
        end
      end
    end
  end

  context 'in special circumstances' do
    it 'raises an exception when it cannot connect to Cassandra' do
      io_reactor = Cql::Io::IoReactor.new(lambda { |*args| Cql::Protocol::CqlProtocolHandler.new(*args, protocol_version) })
      io_reactor.start.value
      expect { io_reactor.connect('example.com', 9042, 0.1).value }.to raise_error(Cql::Io::ConnectionError)
      expect { io_reactor.connect('blackhole', 9042, 0.1).value }.to raise_error(Cql::Io::ConnectionError)
      io_reactor.stop.value
    end

    it 'does nothing the second time #start is called' do
      io_reactor = Cql::Io::IoReactor.new(lambda { |*args| Cql::Protocol::CqlProtocolHandler.new(*args, 2) })
      io_reactor.start.value
      connection = io_reactor.connect(ENV['CASSANDRA_HOST'], 9042, 0.1).value
      response = connection.send_request(Cql::Protocol::StartupRequest.new).value
      if response.is_a?(Cql::Protocol::AuthenticateResponse)
        response = connection.send_request(Cql::Protocol::AuthResponseRequest.new("\x00cassandra\x00cassandra")).value
        response.should be_a(Cql::Protocol::AuthSuccessResponse)
      end
      io_reactor.start.value
      response = connection.send_request(Cql::Protocol::QueryRequest.new('USE system', nil, nil, :any)).value
      response.should_not be_a(Cql::Protocol::ErrorResponse)
    end
  end
end