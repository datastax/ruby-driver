Feature: Downgrading Consistency Retry Policy

  The Downgrading Consistency retry policy retries failed queries with a lower
  consistency level than the one initially requested.

  BEWARE: By doing so, it may break consistency guarantees. In other words, if
  you use this retry policy, there are cases where a read at QUORUM may not see
  a preceding write at QUORUM. Do not use this policy unless you have
  understood the cases where this can happen and are ok with that.

  Scenario: Downgrading Consistency policy is used explicitly
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
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.connect(retry_policy: Cassandra::Retry::Policies::DowngradingConsistency.new)
      session = cluster.connect('simplex')
      result  = session.execute('SELECT * FROM songs', consistency: :all)

      puts "actual consistency: #{result.execution_info.consistency}"
      """
    When node 3 stops
    And it is executed
    Then its output should contain:
      """
      actual consistency: quorum
      """
