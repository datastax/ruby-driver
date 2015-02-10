@cassandra-version-specific @cassandra-version-2.1
Feature: User-Defined Types

  Cassandra 2.1 introduced user-defined types (UDTs).

  Background:
    Given a running cassandra cluster with schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      USE simplex;
      CREATE TYPE address (street text, zipcode int);
      CREATE TYPE check_in (
        location frozen <address>,
        time timestamp,
        data frozen <tuple<int, text, float>>
      );
      CREATE TABLE users (id int PRIMARY KEY, location frozen<address>);
      """

  Scenario: Using User-Defined Types with prepared statements
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect('simplex')
      insert  = session.prepare('INSERT INTO users (id, location) VALUES (?, ?)')

      session.execute(insert, arguments: [0, Cassandra::UDT.new(street: '123 Main St.', zipcode: 78723)])
      session.execute('SELECT * FROM users').each do |row|
        location = row['location']
        puts "Location: #{location}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Location: { street: "123 Main St.", zipcode: 78723 }
      """

  Scenario: Using User-Defined Types with raw CQL
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect('simplex')

      session.execute("INSERT INTO users (id, location) VALUES (0, {street: '123 Main St.', zipcode: 78723})")
      session.execute('SELECT * FROM users').each do |row|
        location = row['location']
        puts "Location: #{location}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Location: { street: "123 Main St.", zipcode: 78723 }
      """

  Scenario: Inspecting User-Defined Types
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      cluster.keyspace('simplex').each_type do |type|
        puts type.to_cql
      end
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE TYPE simplex.address (
        street varchar,
        zipcode int
      );
      CREATE TYPE simplex.check_in (
        location frozen <address>,
        time timestamp,
        data frozen <tuple<int, varchar, float>>
      );
      """

  Scenario: Inserting a partially-complete User-Defined Type
    Given the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect('simplex')
      insert  = session.prepare('INSERT INTO users (id, location) VALUES (?, ?)')

      session.execute(insert, arguments: [0, Cassandra::UDT.new(zipcode: 78723)])
      session.execute('SELECT * FROM users').each do |row|
        location = row['location']
        puts "Location: #{location}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Location: { street: nil, zipcode: 78723 }
      """

  Scenario: Nesting a User-Defined Type
    Given the following example:
    """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect('simplex')

      session.execute("CREATE TABLE registration (id int PRIMARY KEY, info frozen<check_in>)")
      insert  = session.prepare('INSERT INTO registration (id, info) VALUES (?, ?)')

      location = Cassandra::UDT.new(street: '123 Main St.', zipcode: 78723)
      tuple = Cassandra::Tuple.new(42, 'math', 3.14)
      input = Cassandra::UDT.new(location: location, time: Time.at(1358013521, 123000), data: tuple)

      session.execute(insert, arguments: [0, input])
      session.execute('SELECT * FROM registration').each do |row|
        info = row['info']
        puts "Info: {street: #{info.location.street}, zipcode: #{info.location.zipcode}}, #{info.time.httpdate}, #{info.data}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Info: {street: 123 Main St., zipcode: 78723}, Sat, 12 Jan 2013 17:58:41 GMT, (42, math, 3.140000104904175)
      """