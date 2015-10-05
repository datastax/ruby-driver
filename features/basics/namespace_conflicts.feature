Feature: Namespace conflicts

  A special require syntax can be used to avoid namespace conflicts in cases
  where top level `Cassandra` namespace if taken.

  Background:
    Given a running cassandra cluster with schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.songs (
        id uuid PRIMARY KEY,
        title text,
        album text,
        artist text,
        tags set<text>,
        data blob
      );
      """

  @cassandra-version-specific @cassandra-version-2.0
  Scenario: Require the gem using an alternative method
    Given the following example:
      """ruby
      require 'datastax/cassandra'

      cluster = DataStax::Cassandra.cluster
      session = cluster.connect("simplex")

      rows = session.execute("SELECT * FROM songs")

      puts "songs contain #{rows.size} rows"

      batch   = session.batch do |b|
                  b.add("INSERT INTO songs (id, title, album, artist, tags)
                         VALUES (
                            756716f7-2e54-4715-9f00-91dcbea6cf50,
                            'La Petite Tonkinoise',
                            'Bye Bye Blackbird',
                            'Joséphine Baker',
                            {'jazz', '2013'}
                         )")
                  b.add("INSERT INTO songs (id, title, album, artist, tags)
                         VALUES (
                            f6071e72-48ec-4fcb-bf3e-379c8a696488,
                            'Die Mösch',
                            'In Gold',
                            'Willi Ostermann',
                            {'kölsch', '1996', 'birds'}
                         )")
                  b.add("INSERT INTO songs (id, title, album, artist, tags)
                         VALUES (
                            fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,
                            'Memo From Turner',
                            'Performance',
                            'Mick Jager',
                            {'soundtrack', '1991'}
                         )")
                end

      puts "inserting rows in a batch"

      session.execute(batch, consistency: :all)
      rows = session.execute("SELECT * FROM songs")

      puts "songs contain #{rows.size} rows"
      """
    When it is executed
    Then its output should contain:
      """
      songs contain 0 rows
      inserting rows in a batch
      songs contain 3 rows
      """
