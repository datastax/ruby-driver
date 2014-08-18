Feature: Automatically reconnect

  Ruby driver automatically reestablishes failed connections to Cassandra
  cluster. It will use a reconnection policy to determine retry intervals.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"
    And a file named "printing_listener.rb" with:
      """ruby
      class PrintingListener
        def initialize(io)
          @out = io
        end

        def host_found(host)
          @out.puts("Host #{host.ip} is found")
          @out.flush
        end

        def host_lost(host)
          @out.puts("Host #{host.ip} is lost")
          @out.flush
        end

        def host_up(host)
          @out.puts("Host #{host.ip} is up")
          @out.flush
        end

        def host_down(host)
          @out.puts("Host #{host.ip} is down")
          @out.flush
        end
      end
      """
    And the following example:
      """ruby
      require 'cql'
      require 'printing_listener'
      
      interval = 2 # reconnect every 2 seconds
      policy   = Cql::Reconnection::Policies::Constant.new(interval)
      cluster  = Cql.cluster
                  .add_listener(PrintingListener.new($stdout))
                  .with_reconnection_policy(policy)
                  .build
      session = cluster.connect
      
      $stdout.puts("=== START ===")
      $stdout.flush
      until (input = $stdin.gets).nil? # block until closed
        query = input.chomp
        begin
          execution_info = session.execute(query).execution_info
          $stdout.puts("Query #{query.inspect} fulfilled by #{execution_info.hosts.last.ip}")
        rescue => e
          $stdout.puts("Query #{query.inspect} failed with #{e.class.name}: #{e.message}")
        end
        $stdout.flush
      end
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    And it is running interactively
    And I wait for its output to contain "START"

  Scenario: Driver reconnects when all hosts are down
    When all nodes go down
    And I type "SELECT * FROM simplex.songs"
    And node 1 starts
    And I wait for 3 seconds
    And I type "SELECT * FROM simplex.songs"
    And I close the stdin stream
    Then its output should contain:
    """
    Host 127.0.0.1 is found
    Host 127.0.0.1 is up
    Host 127.0.0.3 is found
    Host 127.0.0.3 is up
    Host 127.0.0.2 is found
    Host 127.0.0.2 is up
    === START ===
    Host 127.0.0.1 is down
    Host 127.0.0.3 is down
    Host 127.0.0.2 is down
    Query "SELECT * FROM simplex.songs" failed with Cql::Errors::NoHostsAvailable: no hosts available, check #errors property for details
    Host 127.0.0.1 is up
    Query "SELECT * FROM simplex.songs" fulfilled by 127.0.0.1
    === STOP ===
    """
