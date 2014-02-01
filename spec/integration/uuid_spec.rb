# encoding: utf-8

require 'spec_helper'


describe 'Loading and storing UUIDs' do
  let :connection_options do
    {
      :host => ENV['CASSANDRA_HOST'],
      :authenticator => Cql::Client::PasswordAuthenticator.new('cassandra', 'cassandra'),
    }
  end

  let :client do
    Cql::Client.connect(connection_options)
  end

  let :keyspace_name do
    "cql_rb_#{rand(1000)}"
  end

  before do
    client.connect
    client.execute(%<CREATE KEYSPACE #{keyspace_name} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}>)
    client.use(keyspace_name)
  end

  after do
    client.execute(%<DROP KEYSPACE #{keyspace_name}>)
    client.close
  end

  context Cql::Uuid do
    let :store_statement do
      client.prepare(%<INSERT INTO ids (id, data) VALUES (?, ?)>)
    end

    before do
      client.execute(%<CREATE TABLE ids (id UUID PRIMARY KEY, data TEXT)>)
    end

    it 'can be used to store data in UUID cells' do
      store_statement.execute(Cql::Uuid.new('39bc6ab8-d0f5-11e2-b041-adb2253022a3'), 'hello world')
    end

    it 'will be used when loading data from UUID cells' do
      store_statement.execute(Cql::Uuid.new('39bc6ab8-d0f5-11e2-b041-adb2253022a3'), 'hello world')
      client.execute(%<SELECT * FROM ids>).first['id'].should == Cql::Uuid.new('39bc6ab8-d0f5-11e2-b041-adb2253022a3')
    end

    it 'works even when the UUID could be represented as fewer than 16 bytes' do
      store_statement.execute(Cql::Uuid.new('00853800-5400-11e2-90c5-3409d6a3565d'), 'hello world')
    end
  end

  context Cql::TimeUuid::Generator do
    let :store_statement do
      client.prepare(%<INSERT INTO timeline (id, time, value) VALUES (?, ?, ?)>)
    end

    let :generator do
      Cql::TimeUuid::Generator.new
    end

    before do
      client.execute(%<CREATE TABLE timeline (id ASCII, time TIMEUUID, value INT, PRIMARY KEY (id, time))>)
    end

    it 'can be used to generate values for TIMEUUID cells' do
      store_statement.execute('foo', generator.next, 1)
      store_statement.execute('foo', generator.next, 2)
      store_statement.execute('foo', generator.next, 3)
      result = client.execute(%<SELECT * FROM timeline WHERE id = 'foo'>)
      result.map { |row| row['value'] }.should == [1, 2, 3]
    end

    it 'will be used when loading data from TIMEUUID cells' do
      store_statement.execute('foo', generator.next, 1)
      result = client.execute(%<SELECT * FROM timeline WHERE id = 'foo'>)
      result.first['time'].should be_a(Cql::TimeUuid)
    end
  end
end
