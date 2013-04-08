# encoding: utf-8

require 'spec_helper'


describe 'A CQL client' do
  let :connection_options do
    {:host => ENV['CASSANDRA_HOST']}
  end

  let :client do
    Cql::Client.new(connection_options)
  end

  before do
    client.connect
  end

  after do
    client.close
  end

  it 'executes a query and returns the result' do
    result = client.execute('SELECT * FROM system.schema_keyspaces')
    result.should_not be_empty
  end

  it 'knows which keyspace it\'s in' do
    client.use('system')
    client.keyspace.should == 'system'
    client.use('system_auth')
    client.keyspace.should == 'system_auth'
  end

  it 'is not in a keyspace initially' do
    client.keyspace.should be_nil
  end

  it 'can be initialized with a keyspace' do
    c = Cql::Client.new(connection_options.merge(:keyspace => 'system'))
    c.connect
    begin
      c.keyspace.should == 'system'
      expect { c.execute('SELECT * FROM schema_keyspaces') }.to_not raise_error
    ensure
      c.close
    end
  end

  it 'prepares a statement' do
    statement = client.prepare('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?')
    statement.should_not be_nil
  end

  it 'executes a prepared statement' do
    statement = client.prepare('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?')
    result = statement.execute('system')
    result.should have(1).item
    result = statement.execute('system', :one)
    result.should have(1).item
  end

  context 'with multiple connections' do
    let :multi_client do
      opts = connection_options.dup
      opts[:host] = ([opts[:host]] * 10).join(',')
      Cql::Client.new(opts)
    end

    before do
      client.close
      multi_client.connect
    end

    after do
      multi_client.close
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
      statement = multi_client.prepare('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?')
      100.times do
        result = statement.execute('system')
        result.should have(1).item
      end
    end
  end

  context 'with error conditions' do
    it 'raises an error for CQL syntax errors' do
      expect { client.execute('BAD cql') }.to raise_error(Cql::CqlError)
    end

    it 'raises an error for bad consistency levels' do
      expect { client.execute('SELECT * FROM system.peers', :helloworld) }.to raise_error(Cql::CqlError)
    end

    it 'fails gracefully when connecting to the Thrift port' do
      client = Cql::Client.new(connection_options.merge(port: 9160))
      expect { client.connect }.to raise_error(Cql::IoError)
    end
  end
end
