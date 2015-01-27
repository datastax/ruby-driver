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
      require 'ostruct'

      cluster = Cassandra.cluster
      session = cluster.connect('simplex')
      insert  = session.prepare('INSERT INTO users (id, location) VALUES (?, ?)')

      session.execute(insert, arguments: [0, OpenStruct.new(street: '123 Main St.', zipcode: 78723)])
      session.execute('SELECT * FROM users').each do |row|
        location = row['location']
        puts "Location: #{location.class.name}, #{location.street} #{location.zipcode}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Location: Cassandra::UserValue, 123 Main St. 78723
      """

  Scenario: Using User-Defined Types with raw CQL
    Given the following example:
      """ruby
      require 'cassandra'
      require 'ostruct'

      cluster = Cassandra.cluster
      session = cluster.connect('simplex')

      session.execute("INSERT INTO users (id, location) VALUES (0, {street: '123 Main St.', zipcode: 78723})")
      session.execute('SELECT * FROM users').each do |row|
        location = row['location']
        puts "Location: #{location.class.name}, #{location.street} #{location.zipcode}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Location: Cassandra::UserValue, 123 Main St. 78723
      """

  Scenario: User-Defined Types are not supported as positional arguments in simple statements
    Given the following example:
      """ruby
      require 'cassandra'
      require 'ostruct'

      cluster = Cassandra.cluster

      session = cluster.connect('simplex')

      begin
        session.execute('INSERT INTO users (id, location) VALUES (?, ?)', arguments: [0, OpenStruct.new(street: '123 Main St.', zipcode: 78723)])
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      ArgumentError: Unable to guess the type of the argument
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
