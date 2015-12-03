Feature: Round Robin Policy

  The Round Robin load balancing policy dispatches requests evenly on cluster
  nodes.

  The effects of the policy can be seen by enabling requests tracing. The
  coordinator node that served every request is the last host in
  execution info.

  Background:
    Given a running cassandra cluster in 2 datacenters with 2 nodes in each
    And the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '2', 'dc2': '2'};
      CREATE TABLE simplex.songs (
        id uuid PRIMARY KEY,
        title text,
        album text,
        artist text,
        tags set<text>,
        data blob
      );
      INSERT INTO simplex.songs (id, title, album, artist, tags)
      VALUES (
         756716f7-2e54-4715-9f00-91dcbea6cf50,
         'La Petite Tonkinoise',
         'Bye Bye Blackbird',
         'Joséphine Baker',
         {'jazz', '2013'})
      ;
      INSERT INTO simplex.songs (id, title, album, artist, tags)
      VALUES (
         f6071e72-48ec-4fcb-bf3e-379c8a696488,
         'Die Mösch',
         'In Gold',
         'Willi Ostermann',
         {'kölsch', '1996', 'birds'}
      );
      INSERT INTO simplex.songs (id, title, album, artist, tags)
      VALUES (
         fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,
         'Memo From Turner',
         'Performance',
         'Mick Jager',
         {'soundtrack', '1991'}
      );
      """

  Scenario: Configuring Round Robin load balancing policy
    Given the following example:
      """ruby
      require 'cassandra'

      policy  = Cassandra::LoadBalancing::Policies::RoundRobin.new
      cluster = Cassandra.cluster(load_balancing_policy: policy)
      session = cluster.connect('simplex')

      coordinator_ips = 4.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.2
      127.0.0.3
      127.0.0.4
      """

  Scenario: Round Robin policy ignores datacenters
    Given the following example:
    """ruby
      require 'cassandra'

      policy  = Cassandra::LoadBalancing::Policies::RoundRobin.new
      cluster = Cassandra.cluster(load_balancing_policy: policy)
      session = cluster.connect('simplex')

      coordinator_ips = 4.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.2
      127.0.0.3
      127.0.0.4
      """
