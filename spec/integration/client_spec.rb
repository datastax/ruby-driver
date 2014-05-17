# encoding: utf-8

require 'spec_helper'


describe 'A CQL client' do
  let :connection_options do
    {
      :host => ENV['CASSANDRA_HOST'],
      :credentials => {:username => 'cassandra', :password => 'cassandra'},
    }
  end

  let :client do
    Cql::Client.connect(connection_options)
  end

  after do
    client.close rescue nil
  end

  def create_keyspace_and_table
    begin
      client.execute(%(DROP KEYSPACE cql_rb_client_spec))
    rescue Cql::QueryError => e
      raise e unless e.code == 0x2300
    end
    client.execute(%(CREATE KEYSPACE cql_rb_client_spec WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}))
    client.use('cql_rb_client_spec')
    client.execute(%(CREATE TABLE users (user_id VARCHAR PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)))
    client.execute(%(CREATE TABLE counters (id VARCHAR PRIMARY KEY, count COUNTER)))
  end

  context 'with common operations' do
    it 'executes a query and returns the result' do
      result = client.execute('SELECT * FROM system.schema_keyspaces')
      result.should_not be_empty
    end

    it 'knows which keyspace it\'s in' do
      client.use('system')
      client.keyspace.should == 'system'
      client.use('system_traces')
      client.keyspace.should == 'system_traces'
    end

    it 'is not in a keyspace initially' do
      client.keyspace.should be_nil
    end

    it 'can be initialized with a keyspace' do
      c = Cql::Client.connect(connection_options.merge(:keyspace => 'system'))
      c.connect
      begin
        c.keyspace.should == 'system'
        expect { c.execute('SELECT * FROM schema_keyspaces') }.to_not raise_error
      ensure
        c.close
      end
    end
  end

  context 'when using prepared statements' do
    before do
      create_keyspace_and_table
      client.execute(%(UPDATE users SET first = 'Sue', last = 'Smith', age = 34 WHERE user_id = 'sue'))
    end

    let :statement do
      client.prepare('SELECT * FROM users WHERE user_id = ?')
    end

    it 'prepares a statement' do
      statement.should_not be_nil
    end

    it 'executes a prepared statement' do
      result = statement.execute('sue')
      result.should have(1).item
      result = statement.execute('sue', :one)
      result.should have(1).item
    end

    it 'executes a prepared statement with no bound values' do
      statement = client.prepare('SELECT * FROM users')
      result = statement.execute(:one)
      result.should_not be_empty
    end

    it 'executes a batch' do
      statement = client.prepare('UPDATE users SET first = ?, last = ?, age = ? WHERE user_id = ?')
      statement.batch do |batch|
        batch.add('Sam', 'Miller', 23, 'sam')
        batch.add('Kim', 'Jones', 62, 'kim')
      end
      result = client.execute(%(SELECT * FROM users WHERE user_id = 'kim'))
      result.first['last'].should == 'Jones'
    end

    it 'executes a counter batch' do
      statement = client.prepare('UPDATE counters SET count = count + ? WHERE id = ?')
      batch = statement.batch(:counter, consistency: :quorum)
      batch.add(5, 'foo')
      batch.add(3, 'bar')
      batch.add(6, 'foo')
      batch.execute
      result = client.execute('SELECT * FROM counters')
      counters = result.each_with_object({}) { |row, acc| acc[row['id']] = row['count'] }
      counters.should eql('foo' => 11, 'bar' => 3)
    end
  end

  context 'with multiple connections' do
    let :multi_client do
      opts = connection_options.dup
      opts[:host] = ([opts[:host]] * 10).join(',')
      opts[:connections_per_node] = 3
      Cql::Client.connect(opts)
    end

    before do
      client.close
    end

    after do
      multi_client.close rescue nil
    end

    it 'handles keyspace changes with #use' do
      multi_client.use('system')
      100.times do
        result = multi_client.execute(%<SELECT * FROM schema_keyspaces WHERE keyspace_name = 'system'>)
        result.should have(1).item
      end
    end

    it 'handles keyspace changes with #execute' do
      multi_client.execute('USE system')
      100.times do
        result = multi_client.execute(%<SELECT * FROM schema_keyspaces WHERE keyspace_name = 'system'>)
        result.should have(1).item
      end
    end

    it 'executes a prepared statement' do
      multi_client.use('system')
      statement = multi_client.prepare('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?')
      100.times do
        result = statement.execute('system')
        result.should have(1).item
      end
    end
  end

  context 'with authentication' do
    let :client do
      double(:client, connect: nil, close: nil)
    end

    context 'and protocol v1' do
      let :authentication_enabled do
        begin
          Cql::Client.connect(connection_options.merge(credentials: nil, protocol_version: 1))
          false
        rescue Cql::AuthenticationError
          true
        end
      end

      let :credentials do
        {:username => 'cassandra', :password => 'cassandra'}
      end

      it 'authenticates using the credentials given in the :credentials option' do
        client = Cql::Client.connect(connection_options.merge(credentials: credentials, protocol_version: 1))
        client.execute('SELECT * FROM system.schema_keyspaces')
      end

      it 'raises an error when no credentials have been given' do
        pending('authentication not configured', unless: authentication_enabled) do
          expect { Cql::Client.connect(connection_options.merge(credentials: nil, protocol_version: 1)) }.to raise_error(Cql::AuthenticationError)
        end
      end

      it 'raises an error when the credentials are bad' do
        pending('authentication not configured', unless: authentication_enabled) do
          expect {
            Cql::Client.connect(connection_options.merge(credentials: {:username => 'foo', :password => 'bar'}, protocol_version: 1))
          }.to raise_error(Cql::AuthenticationError)
        end
      end

      it 'raises an error when only an auth provider has been given' do
        pending('authentication not configured', unless: authentication_enabled) do
          auth_provider = Cql::Auth::PlainTextAuthProvider.new('cassandra', 'cassandra')
          expect { Cql::Client.connect(connection_options.merge(credentials: nil, auth_provider: auth_provider, protocol_version: 1)) }.to raise_error(Cql::AuthenticationError)
        end
      end
    end

    context 'and protocol v2' do
      let :authentication_enabled do
        begin
          Cql::Client.connect(connection_options.merge(auth_provider: nil, credentials: nil))
          false
        rescue Cql::AuthenticationError
          true
        end
      end

      it 'uses the auth provider given in the :auth_provider option' do
        auth_provider = Cql::Auth::PlainTextAuthProvider.new('cassandra', 'cassandra')
        client = Cql::Client.connect(connection_options.merge(auth_provider: auth_provider, credentials: nil))
        client.execute('SELECT * FROM system.schema_keyspaces')
      end

      it 'falls back on creating a PlainTextAuthProvider using the credentials given in the :credentials option' do
        client = Cql::Client.connect(connection_options.merge(auth_provider: nil, credentials: {:username => 'cassandra', :password => 'cassandra'}))
        client.execute('SELECT * FROM system.schema_keyspaces')
      end

      it 'raises an error when no auth provider or credentials have been given' do
        pending('authentication not configured', unless: authentication_enabled) do
          expect { Cql::Client.connect(connection_options.merge(auth_provider: nil, credentials: nil)) }.to raise_error(Cql::AuthenticationError)
        end
      end

      it 'raises an error when the credentials are bad' do
        pending('authentication not configured', unless: authentication_enabled) do
          expect {
            auth_provider = Cql::Auth::PlainTextAuthProvider.new('foo', 'bar')
            Cql::Client.connect(connection_options.merge(auth_provider: auth_provider, credentials: nil))
          }.to raise_error(Cql::AuthenticationError)
        end
      end
    end
  end

  context 'with tracing' do
    before do
      create_keyspace_and_table
    end

    it 'requests tracing and returns the trace ID, for row returning operations' do
      result = client.execute(%(SELECT * FROM users), trace: true)
      result.trace_id.should_not be_nil
    end

    it 'requests tracing and returns the trace ID, for void returning operations' do
      result = client.execute(%(INSERT INTO users (user_id, first, last, age) VALUES ('1', 'Sue', 'Smith', 25)), trace: true)
      result.trace_id.should_not be_nil
    end
  end

  context 'with compression' do
    begin
      require 'cql/compression/snappy_compressor'

      let :client do
        Cql::Client.connect(connection_options.merge(compressor: compressor))
      end

      let :compressor do
        Cql::Compression::SnappyCompressor.new(0)
      end

      it 'compresses requests and decompresses responses' do
        compressor.stub(:compress).and_call_original
        compressor.stub(:decompress).and_call_original
        client.execute('SELECT * FROM system.schema_keyspaces')
        compressor.should have_received(:compress).at_least(1).times
        compressor.should have_received(:decompress).at_least(1).times
      end
    rescue LoadError
      it 'compresses requests and decompresses responses' do
        pending 'No compressor available for the current platform'
      end
    end
  end

  context 'with on-the-fly bound variables' do
    before do
      create_keyspace_and_table
    end

    it 'executes a query and sends the values separately' do
      result = client.execute(%<INSERT INTO users (user_id, first, last) VALUES (?, ?, ?)>, 'sue', 'Sue', 'Smith')
      result.should be_empty
    end

    it 'encodes the values using the provided type hints' do
      result = client.execute(%<INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)>, 'sue', 'Sue', 'Smith', 35, type_hints: [nil, nil, nil, :int])
      result.should be_empty
    end
  end

  context 'when batching operations' do
    before do
      create_keyspace_and_table
    end

    it 'sends the operations as a single request' do
      batch = client.batch
      batch.add(%(UPDATE users SET last = 'Smith' WHERE user_id = 'smith'))
      batch.add(%(UPDATE users SET last = 'Jones' WHERE user_id = 'jones'))
      batch.execute
      result = client.execute(%(SELECT * FROM users WHERE user_id = 'jones'))
      result.first.should include('last' => 'Jones')
    end

    it 'accepts prepared statements' do
      prepared_statement = client.prepare(%(UPDATE users SET last = ? WHERE user_id = ?))
      batch = client.batch
      batch.add(prepared_statement, 'Smith', 'smith')
      batch.add(prepared_statement, 'Jones', 'jones')
      batch.execute
      result = client.execute(%(SELECT * FROM users WHERE user_id = 'jones'))
      result.first.should include('last' => 'Jones')
    end

    it 'accepts a mix of prepared, regular and statements with on-the-fly bound variables' do
      prepared_statement = client.prepare(%(UPDATE users SET last = ? WHERE user_id = ?))
      batch = client.batch
      batch.add(prepared_statement, 'Smith', 'smith')
      batch.add(%(UPDATE users SET last = 'Jones' WHERE user_id = 'jones'))
      batch.add(%(UPDATE users SET last = ?, age = ? WHERE user_id = ?), 'Taylor', 53, 'taylor', type_hints: [nil, :int, nil])
      batch.execute
      result = client.execute(%(SELECT * FROM users WHERE user_id = 'jones'))
      result.first.should include('last' => 'Jones')
      result = client.execute(%(SELECT * FROM users WHERE user_id = 'taylor'))
      result.first.should include('last' => 'Taylor')
    end

    it 'yields the batch to a block and executes it afterwards' do
      client.batch do |batch|
        batch.add(%(UPDATE users SET last = 'Jones' WHERE user_id = 'jones'))
      end
      result = client.execute(%(SELECT * FROM users WHERE user_id = 'jones'))
      result.first.should include('last' => 'Jones')
    end

    it 'can be used for counter increments' do
      client.batch(:counter) do |batch|
        batch.add(%(UPDATE counters SET count = count + 1 WHERE id = 'foo'))
        batch.add(%(UPDATE counters SET count = count + 2 WHERE id = 'bar'))
        batch.add(%(UPDATE counters SET count = count + 3 WHERE id = 'baz'))
      end
      result = client.execute('SELECT * FROM counters')
      counters = result.each_with_object({}) { |row, acc| acc[row['id']] = row['count'] }
      counters.should eql('foo' => 1, 'bar' => 2, 'baz' => 3)
    end

    it 'can be unlogged' do
      client.batch(:unlogged) do |batch|
        batch.add(%(UPDATE users SET last = 'Jones' WHERE user_id = 'jones'))
      end
      result = client.execute(%(SELECT * FROM users WHERE user_id = 'jones'))
      result.first.should include('last' => 'Jones')
    end

    it 'can be traced' do
      batch = client.batch(:unlogged)
      batch.add(%(UPDATE users SET last = 'Jones' WHERE user_id = 'jones'))
      result = batch.execute(trace: true)
      result.trace_id.should_not be_nil
      result = client.batch(:unlogged, trace: true) do |batch|
        batch.add(%(UPDATE users SET last = 'Jones' WHERE user_id = 'jones'))
      end
      result.trace_id.should_not be_nil
    end
  end

  context 'when paging large result sets' do
    let :row_count do
      200
    end

    before do
      create_keyspace_and_table
      statement = client.prepare('UPDATE counters SET count = count + ? WHERE id = ?')
      ids = Set.new
      ids << rand(234234).to_s(36) until ids.size == row_count
      ids = ids.to_a
      client.batch(:counter) do |batch|
        row_count.times do |i|
          batch.add(statement, rand(234), ids[i])
        end
      end
    end

    it 'returns the first page, and a way to retrieve the next when using #execute' do
      page_size = row_count/2 + 10
      result_page = client.execute('SELECT * FROM counters', page_size: page_size)
      result_page.count.should == page_size
      result_page.should_not be_last_page
      result_page = result_page.next_page
      result_page.count.should == row_count - page_size
      result_page.should be_last_page
    end

    it 'returns the first page, and a way to retrieve the next when using a prepared statement' do
      page_size = row_count/2 + 10
      statement = client.prepare('SELECT * FROM counters')
      result_page = statement.execute(page_size: page_size)
      result_page.count.should == page_size
      result_page.should_not be_last_page
      result_page = result_page.next_page
      result_page.count.should == row_count - page_size
      result_page.should be_last_page
    end

    it 'returns nil from #next_page when the last page has been returned' do
      page_size = row_count/5 + 1
      statement = client.prepare('SELECT * FROM counters')
      result_page = statement.execute(page_size: page_size)
      page_count = 0
      while result_page
        page_count += 1
        result_page = result_page.next_page
      end
      page_count.should == 5
    end
  end

  context 'with error conditions' do
    it 'raises an error for CQL syntax errors' do
      expect { client.execute('BAD cql') }.to raise_error(Cql::CqlError)
    end

    it 'raises an error for bad consistency' do
      expect { client.execute('SELECT * FROM system.peers', :helloworld) }.to raise_error(ArgumentError)
    end

    it 'fails gracefully when connecting to the Thrift port' do
      opts = connection_options.merge(port: 9160)
      expect { Cql::Client.connect(opts) }.to raise_error(Cql::IoError)
    end

    it 'fails gracefully when connecting to something that does not run C*' do
      expect { Cql::Client.connect(host: 'google.com') }.to raise_error(Cql::Io::ConnectionTimeoutError)
    end
  end
end
