Feature: Schema change detection

  A state listener registered with Cluster object will be notified of schema changes.

  There are three types of changes -- `keyspace_created`, `keyspace_changed` and
  `keyspace_dropped`. All will be communicated to a state listener using its
  accordingly named methods with a [Keyspace](/api/keyspace) instance as an
  argument.

  Background:
    Given a running cassandra cluster
    And a file named "printing_listener.rb" with:
      """ruby
      class PrintingListener
        def initialize(io)
          @out = io
        end

        def keyspace_created(keyspace)
          @out.puts("Keyspace #{keyspace.name.inspect} created")
        end

        def keyspace_changed(keyspace)
          @out.puts("Keyspace #{keyspace.name.inspect} changed")
        end

        def keyspace_dropped(keyspace)
          @out.puts("Keyspace #{keyspace.name.inspect} dropped")
        end
      end
      """
    And the following example:
      """ruby
      require 'printing_listener'
      require 'cassandra'

      listener = PrintingListener.new($stderr)
      cluster  = Cassandra.connect

      cluster.register(listener)

      $stdout.puts("=== START ===")
      $stdout.flush
      $stdin.gets
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    And it is running interactively
    And I wait for its output to contain "START"

  Scenario: Listening for keyspace creation
    Given an empty schema
    When I execute the following cql:
      """
      CREATE KEYSPACE new_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
      """
    And I close the stdin stream
    Then its output should contain:
      """
      Keyspace "new_keyspace" created
      """

  Scenario: Listening for keyspace drop
    Given the following schema:
      """sql
      CREATE KEYSPACE new_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
      """
    When I execute the following cql:
      """sql
      DROP KEYSPACE new_keyspace
      """
    And I close the stdin stream
    Then its output should contain:
      """
      Keyspace "new_keyspace" dropped
      """

  Scenario: Listening for keyspace changes
    Given the following schema:
      """sql
      CREATE KEYSPACE new_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
      """
    When I execute the following cql:
      """sql
      CREATE TABLE new_keyspace.new_table (id timeuuid PRIMARY KEY)
      """
    And I close the stdin stream
    Then its output should contain:
      """
      Keyspace "new_keyspace" changed
      """
