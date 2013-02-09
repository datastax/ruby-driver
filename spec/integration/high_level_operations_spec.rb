# encoding: utf-8

require 'spec_helper'


describe 'A CQL client' do
  let :connection_options do
    {:host => ENV['CASSANDRA_HOST']}
  end

  let :cluster do
    Cql::Cluster.new(connection_options)
  end

  before do
    cluster.start!
  end

  after do
    cluster.shutdown!
  end

  it 'executes a query and returns the result' do
    result = cluster.execute('SELECT * FROM system.schema_keyspaces')
    result.should_not be_empty
  end

  it 'knows which keyspace it\'s in' do
    cluster.use('system')
    cluster.keyspace.should == 'system'
    cluster.use('system_auth')
    cluster.keyspace.should == 'system_auth'
  end

  it 'is not in a keyspace initially' do
    cluster.keyspace.should be_nil
  end

  it 'can be initialized with a keyspace' do
    c = Cql::Cluster.new(connection_options.merge(:keyspace => 'system'))
    c.start!
    begin
      c.keyspace.should == 'system'
      expect { c.execute('SELECT * FROM schema_keyspaces') }.to_not raise_error
    ensure
      c.shutdown!
    end
  end

  it 'prepares a statement' do
    statement = cluster.prepare('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?')
    statement.should_not be_nil
  end

  it 'executes a prepared statement' do
    statement = cluster.prepare('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?')
    result = statement.execute('system')
    result.should have(1).item
  end

  context 'with multiple connections' do
    let :multi_cluster do
      opts = connection_options.dup
      opts[:host] = ([opts[:host]] * 10).join(',')
      Cql::Cluster.new(opts)
    end

    before do
      cluster.shutdown!
      multi_cluster.start!
    end

    after do
      multi_cluster.shutdown!
    end

    it 'handles keyspace changes with #use' do
      multi_cluster.use('system')
      100.times do
        result = multi_cluster.execute(%<SELECT * FROM schema_keyspaces WHERE keyspace_name = 'system'>)
        result.should have(1).item
      end
    end

    it 'handles keyspace changes with #execute' do
      multi_cluster.execute('USE system')
      100.times do
        result = multi_cluster.execute(%<SELECT * FROM schema_keyspaces WHERE keyspace_name = 'system'>)
        result.should have(1).item
      end
    end

    it 'executes a prepared statement' do
      statement = multi_cluster.prepare('SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?')
      100.times do
        result = statement.execute('system')
        result.should have(1).item
      end
    end
  end
end
