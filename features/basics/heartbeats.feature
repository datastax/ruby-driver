Feature: Heartbeats

  The Ruby driver sends periodic hearbeats to the Cassandra server to check if the connection is alive.
  The heartbeat interval and the timeout is adjustable when creating a new `Cassandra::Cluster`. If a 
  heartbeat is not returned, the connection will be considered dead and is closed automatically.

  Background:
    Given a running cassandra cluster

  @netblock
  Scenario: Connection is idle
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster(:heartbeat_interval => 2, :idle_timeout => 5)
      session = cluster.connect("simplex")
      
      $stdout.puts("=== START ===")
      $stdout.flush
      until (input = $stdin.gets).nil? # block until closed
        query = input.chomp
        begin
          results = session.execute(query)
          puts results.inspect
          execution_info = results.execution_info
          $stdout.puts("Query #{query.inspect} fulfilled by #{execution_info.hosts}")
        rescue => e
          $stdout.puts("#{e.class.name}: #{e.message}")
        end
        $stdout.flush
      end
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    When it is running interactively
    And I wait for its output to contain "START"
    And node 1 is unreachable
    And I type "CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT)"
    And I close the stdin stream
    Then its output should contain:
      """
      Cassandra::Errors::NoHostsAvailable: All attempted hosts failed
      """
    And its output should contain:
      """
      127.0.0.1 (Cassandra::Errors::IOError: Terminated due to inactivity)
      """
    And its output should contain:
      """
      127.0.0.2 (Cassandra::Errors::IOError: Terminated due to inactivity)
      """
    And its output should contain:
      """
      127.0.0.3 (Cassandra::Errors::IOError: Terminated due to inactivity)
      """