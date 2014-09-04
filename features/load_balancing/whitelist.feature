Feature: White List Policy

  The White List load balancing policy wraps a subpolicy and ensures that only
  hosts from a provided white list are used. This policy can be used to limit
  effects of automatic peer discovery to executing queries only on a given set
  of hosts.

  Scenario: Prevent queries from running on non-whitelisted hosts
    Given a running cassandra cluster with schema:
      """sql
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
    And the following example:
      """ruby
      require 'cassandra'

      allowed_ips = ["127.0.0.1", "127.0.0.3"]
      round_robin = Cassandra::LoadBalancing::Policies::RoundRobin.new
      whitelist   = Cassandra::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
      cluster     = Cassandra.connect(load_balancing_policy: whitelist)
      session     = cluster.connect('simplex')

      coordinator_ips = 3.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort.uniq
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.3
      """
