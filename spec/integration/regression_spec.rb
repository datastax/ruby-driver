# encoding: utf-8

require 'spec_helper'


describe 'Regressions' do
  let :connection_options do
    {
      :host => ENV['CASSANDRA_HOST'],
      :authenticator => Cql::Client::PasswordAuthenticator.new('cassandra', 'cassandra'),
    }
  end

  let :client do
    Cql::Client.connect(connection_options)
  end

  before do
    client.execute('DROP KEYSPACE cql_rb_client_spec') rescue nil
    client.execute(%(CREATE KEYSPACE cql_rb_client_spec WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}))
    client.use('cql_rb_client_spec')
  end

  after do
    client.execute('DROP KEYSPACE cql_rb_client_spec') rescue nil
    client.close rescue nil
  end

  context 'with multibyte characters' do
    it 'executes queries with multibyte characters' do
      client.execute(%(CREATE TABLE users (user_id VARCHAR PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)))
      client.execute(%(INSERT INTO users (user_id, first, last, age) VALUES ('test', 'ümlaut', 'test', 1)))
    end

    it 'executes prepared statements with multibyte characters' do
      client.execute(%(CREATE TABLE users (user_id VARCHAR PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)))
      client.execute("INSERT INTO users (user_id, first, last, age) VALUES ('test', 'ümlaut', 'test', 1)")
      statement = client.prepare('INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)')
      statement.execute('test2', 'test2', 'test2', 2)
      statement.execute('test3', 'ümlaut', 'test3', 3)
    end
  end

  context 'with collections' do
    it 'prepares and executes a statement with an append to a set' do
      client.execute(%(CREATE TABLE users (name VARCHAR PRIMARY KEY, emails SET<VARCHAR>)))
      statement = client.prepare(%(UPDATE users SET emails = emails + ? WHERE name = 'eve'))
      statement.execute(['eve@gmail.com'])
    end

    it 'prepares and executes a statement with an append to a list' do
      client.execute(%(CREATE TABLE users (name VARCHAR PRIMARY KEY, emails LIST<VARCHAR>)))
      statement = client.prepare(%(UPDATE users SET emails = emails + ? WHERE name = 'eve'))
      statement.execute(['eve@gmail.com', 'eve@yahoo.com'])
    end

    it 'prepares and executes a statement with an append to a map' do
      client.execute(%(CREATE TABLE users (name VARCHAR PRIMARY KEY, emails MAP<VARCHAR, VARCHAR>)))
      statement = client.prepare(%(UPDATE users SET emails = emails + ? WHERE name = 'eve'))
      statement.execute({'home' => 'eve@yahoo.com'})
    end

    it 'prepares and executes a statement with a map assignment' do
      client.execute(%(CREATE TABLE users (name VARCHAR PRIMARY KEY, emails MAP<VARCHAR, VARCHAR>)))
      statement = client.prepare(%(UPDATE users SET emails['home'] = ? WHERE name = 'eve'))
      statement.execute('eve@gmail.com')
    end
  end

  context 'with null values' do
    before do
      client.execute(<<-CQL)
        CREATE TABLE lots_of_types (
          id               INT,
          ascii_column     ASCII,
          bigint_column    BIGINT,
          blob_column      BLOB,
          boolean_column   BOOLEAN,
          decimal_column   DECIMAL,
          double_column    DOUBLE,
          float_column     FLOAT,
          int_column       INT,
          text_column      TEXT,
          timestamp_column TIMESTAMP,
          uuid_column      UUID,
          varchar_column   VARCHAR,
          varint_column    VARINT,
          timeuuid_column  TIMEUUID,
          inet_column      INET,
          list_column      LIST<ASCII>,
          map_column       MAP<TEXT, BOOLEAN>,
          set_column       SET<BLOB>,
          PRIMARY KEY (id)
        )
      CQL
      client.execute(<<-CQL)
        CREATE TABLE counters (
          id ASCII,
          counter1 COUNTER,
          counter2 COUNTER,
          PRIMARY KEY (id)
        )
      CQL
    end

    it 'decodes null counters' do
      client.execute(%<UPDATE counters SET counter1 = counter1 + 1 WHERE id = 'foo'>)
      result = client.execute(%<SELECT counter1, counter2 FROM counters WHERE id = 'foo'>)
      result.first['counter1'].should == 1
      result.first['counter2'].should be_nil
    end

    it 'decodes null values' do
      client.execute(%<INSERT INTO lots_of_types (id) VALUES (3)>)
      result = client.execute(%<SELECT * FROM lots_of_types WHERE id = 3>)
      row = result.first
      row['ascii_column'].should be_nil
      row['bigint_column'].should be_nil
      row['blob_column'].should be_nil
      row['boolean_column'].should be_nil
      row['decimal_column'].should be_nil
      row['double_column'].should be_nil
      row['float_column'].should be_nil
      row['int_column'].should be_nil
      row['text_column'].should be_nil
      row['timestamp_column'].should be_nil
      row['uuid_column'].should be_nil
      row['varchar_column'].should be_nil
      row['varint_column'].should be_nil
      row['timeuuid_column'].should be_nil
      row['inet_column'].should be_nil
      row['list_column'].should be_nil
      row['map_column'].should be_nil
      row['set_column'].should be_nil
    end

    it 'encodes null values' do
      statement = client.prepare(<<-CQL)
        UPDATE lots_of_types
        SET
          ascii_column = ?,
          bigint_column = ?,
          blob_column = ?,
          boolean_column = ?,
          decimal_column = ?,
          double_column = ?,
          float_column = ?,
          int_column = ?,
          text_column = ?,
          timestamp_column = ?,
          uuid_column = ?,
          varchar_column = ?,
          varint_column = ?,
          timeuuid_column = ?,
          inet_column = ?,
          list_column = ?,
          map_column = ?,
          set_column = ?
        WHERE id = 1
      CQL
      statement.execute(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
    end
  end

  context 'with negative numbers' do
    it 'decodes negative counters' do
      client.execute(%<CREATE TABLE counters (id ASCII, counter1 COUNTER, PRIMARY KEY (id))>)
      client.execute(%<UPDATE counters SET counter1 = counter1 - 1 WHERE id = 'foo'>)
      result = client.execute(%<SELECT counter1 FROM counters WHERE id = 'foo'>)
      result.first['counter1'].should == -1
    end

    it 'decodes negative numbers' do
      client.execute(<<-CQL)
        CREATE TABLE lots_of_types (
          id               INT,
          bigint_column    BIGINT,
          decimal_column   DECIMAL,
          double_column    DOUBLE,
          float_column     FLOAT,
          int_column       INT,
          varint_column    VARINT,
          PRIMARY KEY (id)
        )
      CQL
      client.execute(%<INSERT INTO lots_of_types (id, bigint_column, decimal_column, double_column, float_column, int_column, varint_column) VALUES (0, -1, -1, -1, -1, -1, -1)>)
      client.execute(%<INSERT INTO lots_of_types (id, bigint_column, decimal_column, double_column, float_column, int_column, varint_column) VALUES (1, -9223372036854775808, -0.0012095473475870063, -2.2250738585072014e-308, -1.175494351e-38, -2147483648, -23454545674351234123365765786894351234567456)>)
      result = client.execute(%<SELECT * FROM lots_of_types WHERE id IN (0, 1)>)
      row0, row1 = result.to_a
      row0['bigint_column'].should == -1
      row0['decimal_column'].should == -1
      row0['double_column'].should == -1
      row0['float_column'].should == -1
      row0['int_column'].should == -1
      row0['varint_column'].should == -1
      row1['bigint_column'].should == -9223372036854775808
      row1['decimal_column'].should == BigDecimal.new('-0.0012095473475870063')
      row1['double_column'].should == be_within(1.0e-308).of(-2.2250738585072014e-308)
      row1['float_column'].should be_within(1.0e-38).of(-1.175494351e-38)
      row1['int_column'].should == -2147483648
      row1['varint_column'].should == -23454545674351234123365765786894351234567456
    end
  end

  context 'with quoted keyspace names' do
    it 'handles quoted keyspace names' do
      client.use('"system"')
      client.keyspace.should == 'system'
    end
  end

  context 'with prepared statements where the table does not exist' do
    it 'raises an error when a statement is prepared on a table that does not exist' do
      expect { client.prepare('SELECT * FROM table_that_does_not_exist') }.to raise_error(Cql::QueryError)
    end
  end
end
