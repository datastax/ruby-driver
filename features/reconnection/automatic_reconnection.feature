Feature: Automatic reconnection

  Ruby driver automatically reestablishes failed connections to Cassandra
  cluster. It will use a reconnection policy to determine retry intervals.

  Background:
    Given a running cassandra cluster with schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      USE simplex;
      CREATE TABLE songs (
        id uuid PRIMARY KEY,
        title text,
        album text,
        artist text,
        tags set<text>,
        data blob
      );
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         756716f7-2e54-4715-9f00-91dcbea6cf50,
         'La Petite Tonkinoise',
         'Bye Bye Blackbird',
         'Joséphine Baker',
         {'jazz', '2013'})
      ;
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         f6071e72-48ec-4fcb-bf3e-379c8a696488,
         'Die Mösch',
         'In Gold',
         'Willi Ostermann',
         {'kölsch', '1996', 'birds'}
      );
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,
         'Memo From Turner',
         'Performance',
         'Mick Jager',
         {'soundtrack', '1991'}
      );
      """
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
      require 'cassandra'
      require 'printing_listener'
      
      interval = 2 # reconnect every 2 seconds
      policy   = Cassandra::Reconnection::Policies::Constant.new(interval)
      cluster  = Cassandra.connect(
                   listeners: [PrintingListener.new($stdout)],
                   reconnection_policy: policy,
                   consistency: :one
                 )
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
    When node 1 stops
    When node 2 stops
    When node 3 stops
    And I type "SELECT * FROM simplex.songs"
    And node 1 starts
    And I wait for 3 seconds
    And I type "SELECT * FROM simplex.songs"
    And I close the stdin stream
    Then its output should contain:
    """
    === START ===
    Host 127.0.0.1 is down
    Host 127.0.0.2 is down
    Host 127.0.0.3 is down
    Query "SELECT * FROM simplex.songs" failed with Cassandra::Errors::NoHostsAvailable: All hosts down
    Host 127.0.0.1 is up
    Query "SELECT * FROM simplex.songs" fulfilled by 127.0.0.1
    === STOP ===
    """
