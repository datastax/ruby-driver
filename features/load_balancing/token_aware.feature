Feature: Token-aware Load Balancing Policy

  Token-aware policy is used to reduce network hops whenever possible by
  sending requests directly to the node that owns the data. Token-aware policy
  acts as a filter, wrapping another load balancing policy.

  Token-aware policy uses schema metadata available in the cluster to determine
  the right partitioners and replication strategies for a given keyspace and
  locate replicas for a given statement.

  In case replica node(s) cannot be found or reached, this policy fallsback
  onto the wrapped policy plan.

  Background:
    Given a running cassandra cluster in 2 datacenters with 2 nodes in each
    And the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1};
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

  Scenario: Requests are routed to the primary replica
    Given the following example:
      """ruby
      require 'cassandra'

      policy    = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new
      policy    = Cassandra::LoadBalancing::Policies::TokenAware.new(policy)
      cluster   = Cassandra.cluster(load_balancing_policy: policy)
      session   = cluster.connect('simplex')
      statement = session.prepare("SELECT token(id) FROM songs WHERE id = ?")

      [
        Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
        Cassandra::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
        Cassandra::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25')
      ].each do |uuid|
        result  = session.execute(statement, arguments: [uuid])
        replica = result.execution_info.hosts.first
        total   = result.execution_info.hosts.size
        puts "uuid=#{uuid} token=#{result.first['token(id)']} replica=#{replica.ip} total=#{total}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      uuid=756716f7-2e54-4715-9f00-91dcbea6cf50 token=-4565826248849633211 replica=127.0.0.2 total=1
      uuid=f6071e72-48ec-4fcb-bf3e-379c8a696488 token=-1176857621403111796 replica=127.0.0.2 total=1
      uuid=fbdf82ed-0063-4796-9c7c-a3d4f47b4b25 token=2440231132048646025 replica=127.0.0.1 total=1
      """

  Scenario: Requests are routed according to wrapped policy plan when primary replica is down
    Given the following example:
      """ruby
      require 'cassandra'

      policy    = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new
      policy    = Cassandra::LoadBalancing::Policies::TokenAware.new(policy)
      cluster   = Cassandra.cluster(load_balancing_policy: policy)
      session   = cluster.connect('simplex')
      statement = session.prepare("SELECT token(id) FROM songs WHERE id = ?")

      [
        Cassandra::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
        Cassandra::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25')
      ].each do |uuid|
        result  = session.execute(statement, arguments: [uuid], consistency: :one)
        replica = result.execution_info.hosts.first
        total   = result.execution_info.hosts.size
        puts "uuid=#{uuid} token=#{result.first['token(id)']} replica=#{replica.ip} total=#{total}"
      end
      """
    And node 2 is stopped
    When it is executed
    Then its output should contain:
      """
      uuid=f6071e72-48ec-4fcb-bf3e-379c8a696488 token=-1176857621403111796 replica=127.0.0.1 total=1
      uuid=fbdf82ed-0063-4796-9c7c-a3d4f47b4b25 token=2440231132048646025 replica=127.0.0.1 total=1
      """
