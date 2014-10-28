Feature: Heartbeats

  The Ruby driver sends periodic hearbeats to the Cassandra server to check if the connection is alive.
  The heartbeat interval and the timeout is adjustable when creating a new `Cassandra::Cluster`. If a
  heartbeat is not returned, the connection will be considered dead and is closed automatically.

  Background:
    Given a running cassandra cluster with schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      """
    And a file named "printing_listener.rb" with:
      """ruby
      class PrintingListener
        def initialize(io)
          @out = io
        end

        def host_found(host)
          @out.puts("Host #{host.ip} is found")
        end

        def host_lost(host)
          @out.puts("Host #{host.ip} is lost")
        end

        def host_up(host)
          @out.puts("Host #{host.ip} is up")
        end

        def host_down(host)
          @out.puts("Host #{host.ip} is down")
        end
      end
      """
    And the following example:
      """ruby
      require 'printing_listener'
      require 'cassandra'

      policy  = Cassandra::LoadBalancing::Policies::RoundRobin.new
      policy  = Cassandra::LoadBalancing::Policies::WhiteList.new(['127.0.0.3'], policy)
      cluster = Cassandra.cluster(
        heartbeat_interval:    2,
        idle_timeout:          5,
        hosts:                 '127.0.0.3',
        load_balancing_policy: policy
      )
      session = cluster.connect("simplex")

      listener = PrintingListener.new($stderr)
      cluster.register(listener)

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
    And it is running interactively
    And I wait for its output to contain "START"
    And node 3 is unreachable

  @netblock
  Scenario: Executing a query when a host is unreachable
    When I type "CREATE TABLE simplex.users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT)"
    And I close the stdin stream
    Then its output should contain:
      """
      Cassandra::Errors::NoHostsAvailable: All attempted hosts failed: 127.0.0.3 (Cassandra::Errors::IOError: Terminated due to inactivity)
      """

  @netblock
  Scenario: Receiving notification that an unreachable host is down
    When I wait for 5 seconds
    And I close the stdin stream
    Then its output should contain:
      """
      Host 127.0.0.3 is down
      """
