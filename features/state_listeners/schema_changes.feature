@todo
Feature: keyspace change detection

  Cluster object allows registering state listeners. It then gurantees that
  they will be notifies on keyspace changes.

  Background:
    Given a running cassandra cluster
    And a file named "printing_listener.rb" with:
      """ruby
      class PrintingListener
        def initialize(io)
          @out = io
        end

        def keyspace_created(keyspace, table)
          @out.puts("Schema created keyspace=#{keyspace.inspect} table=#{table.inspect}")
        end

        def keyspace_updated(keyspace, table)
          @out.puts("Schema updated keyspace=#{keyspace.inspect} table=#{table.inspect}")
        end

        def keyspace_dropped(keyspace, table)
          @out.puts("Schema dropped keyspace=#{keyspace.inspect} table=#{table.inspect}")
        end
      end
      """
    And the following example running in the background:
      """ruby
      require 'printing_listener'
      require 'cql'

      listener = PrintingListener.new($stderr)
      cluster  = Cql.connect

      cluster.register(listener)

      at_exit { cluster.close }

      sleep
      """

  Scenario: a new keyspace is created and then dropped
    When keyspace "new_keyspace" is created
    And keyspace "new_keyspace" is dropped
    Then background output should contain:
      """
      Schema created keyspace="new_keyspace" table=""
      Schema dropped keyspace="new_keyspace" table=""
      """
