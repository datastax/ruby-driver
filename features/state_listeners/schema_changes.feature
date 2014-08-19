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
    And the following example running in the background:
      """ruby
      require 'printing_listener'
      require 'cql'

      listener = PrintingListener.new($stderr)
      cluster  = Cql.cluster
                  .with_contact_points("127.0.0.1")
                  .build

      cluster.register(listener)

      at_exit { cluster.close }

      sleep
      """

  Scenario: Listening for keyspace creation
    Given no keyspace "new_keyspace"
    And the following example:
      """ruby
      require 'cql'

      session = Cql.cluster.build.connect

      session.execute("CREATE KEYSPACE new_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
      """
    When it is executed
    Then background output should contain:
      """
      Keyspace "new_keyspace" created
      """

  Scenario: Listening for keyspace drop
    Given a keyspace "new_keyspace"
    And the following example:
      """ruby
      require 'cql'

      session = Cql.cluster.build.connect

      session.execute("DROP KEYSPACE new_keyspace")
      """
    When it is executed
    Then background output should contain:
      """
      Keyspace "new_keyspace" dropped
      """

  Scenario: Listening for keyspace changes
    Given a keyspace "new_keyspace"
    And the following example:
      """ruby
      require 'cql'

      session = Cql.cluster.build.connect

      session.execute("CREATE TABLE new_keyspace.new_table (id timeuuid PRIMARY KEY)")
      """
    When it is executed
    Then background output should contain:
      """
      Keyspace "new_keyspace" changed
      """
