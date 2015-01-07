Feature: Default Load Balancing Policy

  By default, Ruby driver will use a combination of Token aware and Data Center
  aware round robin policies for load balancing.

  This combination proved to be the most performant of of all built-in load
  balancing policies.

  When the name of the local data center is not specified explicitly using
  [`Cassandra.cluster`](/api/#cluster-class_method), the first datacenter seen
  by the load balancing policy will be considered local. Therefore, care must
  be taken to only include addresses of the nodes in the same datacenter as the
  application using the Ruby Driver in the `:hosts` option to 
  `Cassandra.cluster`, or to provide `:datacenter` option explicitly.

  Background:
    Given a running cassandra cluster in 2 datacenters with 2 nodes in each
    And the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '1', 'dc2': '1'};
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

  Scenario: Default load balancing policy always routes to primary replicas when possible
    Given the following example:
      """ruby
      require 'cassandra'

      cluster   = Cassandra.cluster(hosts: ['127.0.0.1', '127.0.0.2'])
      session   = cluster.connect('simplex')
      statement = session.prepare("SELECT token(id) FROM songs WHERE id = ?")

      coordinator_ips = 4.times.map do
        info = session.execute(statement, arguments: [Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50')]).execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort.uniq
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.2
      """

  Scenario: Default load balancing policy always uses primary replicas from the local datacenter
    Given the following example:
      """ruby
      require 'cassandra'

      cluster   = Cassandra.cluster(hosts: ['127.0.0.3', '127.0.0.4'])
      session   = cluster.connect('simplex')
      statement = session.prepare("SELECT token(id) FROM songs WHERE id = ?")

      coordinator_ips = 4.times.map do
        info = session.execute(statement, arguments: [Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50')]).execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort.uniq
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.4
      """

  Scenario: Default load balancing allows specifying data center explicitly
    Given the following example:
      """ruby
      require 'cassandra'

      cluster   = Cassandra.cluster(
                    datacenter: 'dc1',
                    hosts: ['127.0.0.3', '127.0.0.4']
                  )
      session   = cluster.connect('simplex')
      statement = session.prepare("SELECT token(id) FROM songs WHERE id = ?")

      coordinator_ips = 4.times.map do
        info = session.execute(statement, arguments: [Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50')]).execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort.uniq
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.2
      """

  Scenario: Default load balancing policy prevents requests to remote datacenters
    Given the following example:
      """ruby
      require 'cassandra'

      cluster   = Cassandra.cluster(
                    hosts: ['127.0.0.1', '127.0.0.2']
                  )
      session   = cluster.connect('simplex')
      statement = "SELECT token(id) FROM songs"

      $stdout.puts("=== START ===")
      $stdout.flush

      $stdin.gets # ready, block on stdin

      begin
        execution_info = session.execute(statement).execution_info
        $stdout.puts("Statement #{statement.inspect} fulfilled by #{execution_info.hosts.last.ip}")
      rescue => e
        $stdout.puts "#{e.class.name}: #{e.message}"
      end
      $stdout.flush
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    And it is running interactively
    And I wait for its output to contain "START"
    When node 1 stops
    And node 2 stops
    And I close the stdin stream
    Then its output should contain:
      """
      Cassandra::Errors::NoHostsAvailable: All hosts down
      """
