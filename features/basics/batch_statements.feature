Feature: Batch statements

  Session objects can be used to construct a logged batch statement and later
  execute it.

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
      CREATE TABLE cas_batch (k text, v int, PRIMARY KEY (k, v));
      """

  @cassandra-version-specific @cassandra-version-2.0
  Scenario: A batch of simple statements is executed
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
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

  @cassandra-version-specific @cassandra-version-2.0
  Scenario: A batch of simple statements with parameters is executed
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      rows = session.execute("SELECT * FROM songs")

      puts "songs contain #{rows.size} rows"

      batch   = session.batch do |b|
                  b.add("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)",
                        Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
                        'La Petite Tonkinoise',
                        'Bye Bye Blackbird',
                        'Joséphine Baker',
                        Set['jazz', '2013']
                  )
                  b.add("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)",
                        Cassandra::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
                        'Die Mösch',
                        'In Gold',
                        'Willi Ostermann',
                        Set['kölsch', '1996', 'birds']
                  )
                  b.add("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)",
                        Cassandra::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25'),
                        'Memo From Turner',
                        'Performance',
                        'Mick Jager',
                        Set['soundtrack', '1991']
                  )
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

  @cassandra-version-specific @cassandra-version-2.0
  Scenario: A prepared statement is executed in a batch
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      rows = session.execute("SELECT * FROM songs")

      puts "songs contain #{rows.size} rows"

      insert  = session.prepare("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)")
      batch   = session.batch do |b|
                  b.add(insert,
                        Cassandra::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
                        'La Petite Tonkinoise',
                        'Bye Bye Blackbird',
                        'Joséphine Baker',
                        Set['jazz', '2013']
                  )
                  b.add(insert,
                        Cassandra::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
                        'Die Mösch',
                        'In Gold',
                        'Willi Ostermann',
                        Set['kölsch', '1996', 'birds']
                  )
                  b.add(insert,
                        Cassandra::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25'),
                        'Memo From Turner',
                        'Performance',
                        'Mick Jager',
                        Set['soundtrack', '1991']
                  )
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

  @cassandra-version-specific @cassandra-version-2.0.9
  Scenario: A cas batch is never applied more than once
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      statement = session.prepare("INSERT INTO cas_batch (k, v) VALUES (?, ?) IF NOT EXISTS")
      batch = session.batch

      batch.add("INSERT INTO cas_batch (k, v) VALUES ('key1', 0)")
      batch.add(statement, "key1", 1)
      batch.add(statement, "key1", 2)

      results =  session.execute(batch)
      rows = results.first
      puts "batch applied? #{rows["[applied]"]}"

      results =  session.execute(batch)
      rows = results.first
      puts "batch applied? #{rows["[applied]"]}"

      """
    When it is executed
    Then its output should contain:
      """
      batch applied? true
      batch applied? false
      """

  @cassandra-version-specific @cassandra-version-1.2
  Scenario: Cassandra 1.2 doesn't support batch statements
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")
      batch   = session.batch

      batch.add("INSERT INTO cas_batch (k, v) VALUES ('key1', 0)")

      begin
        session.execute(batch)
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Cassandra::Errors::ClientError: Batch statements are not supported by the current version of Apache Cassandra
      """
