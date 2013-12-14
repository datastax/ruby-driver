# encoding: utf-8

require 'spec_helper'


describe 'A CQL client' do
  let :connection_options do
    {:host => ENV['CASSANDRA_HOST'], :credentials => {:username => 'cassandra', :password => 'cassandra'}}
  end

  let :client do
    Cql::Client.connect(connection_options)
  end

  after do
    client.close rescue nil
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
      client.use('system')
    end

    let :statement do
      client.prepare('SELECT * FROM schema_keyspaces WHERE keyspace_name = ?')
    end

    it 'prepares a statement' do
      statement.should_not be_nil
    end

    it 'executes a prepared statement' do
      result = statement.execute('system')
      result.should have(1).item
      result = statement.execute('system', :one)
      result.should have(1).item
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

    let :authentication_enabled do
      begin
        Cql::Client.connect(connection_options.merge(credentials: nil))
        false
      rescue Cql::AuthenticationError
        true
      end
    end

    it 'sends credentials given in :credentials' do
      client = Cql::Client.connect(connection_options.merge(credentials: {username: 'cassandra', password: 'cassandra'}))
      client.execute('SELECT * FROM system.schema_keyspaces')
    end

    it 'raises an error when no credentials have been given' do
      pending('authentication not configured', unless: authentication_enabled) do
        expect { Cql::Client.connect(connection_options.merge(credentials: nil)) }.to raise_error(Cql::AuthenticationError)
      end
    end

    it 'raises an error when the credentials are bad' do
      pending('authentication not configured', unless: authentication_enabled) do
        expect {
          Cql::Client.connect(connection_options.merge(credentials: {username: 'foo', password: 'bar'}))
        }.to raise_error(Cql::AuthenticationError)
      end
    end
  end

  context 'with tracing' do
    before do
      client.execute(%(DROP KEYSPACE cql_rb_client_spec)) rescue nil
      client.execute(%(CREATE KEYSPACE cql_rb_client_spec WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}))
      client.use('cql_rb_client_spec')
      client.execute(%(CREATE TABLE users (user_id VARCHAR PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)))
    end

    it 'requests tracing and returns the trace ID' do
      result = client.execute(%(SELECT * FROM users), trace: true)
      result.trace_id.should_not be_nil
    end

    it 'requests tracing and returns the trace ID, for void returning operations' do
      pending 'since void operations return nil, this is currently not possible' do
        result = client.execute(%(INSERT INTO users (user_id, first, last, age) VALUES ('1', 'Sue', 'Smith', 25)), trace: true)
        result.trace_id.should_not be_nil
      end
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
