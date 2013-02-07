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
    c.keyspace.should == 'system'
    expect { c.execute('SELECT * FROM schema_keyspaces') }.to_not raise_error
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
end
