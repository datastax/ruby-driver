# encoding: utf-8

Feature: logged batch

  Session objects can be used to construct a logged batch statement and later
  execute it.

  Background:
    Given a cassandra cluster with schema "simplex" with an empty table "songs"

  Scenario: a batch of simple statements is executed
    Given the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster
                  .with_contact_points("127.0.0.1")
                  .build

      at_exit { cluster.close }

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

      session.execute(batch)
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

  Scenario: a batch of simple statements with parameters is executed
    Given the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster
                  .with_contact_points("127.0.0.1")
                  .build

      at_exit { cluster.close }

      session = cluster.connect("simplex")

      rows = session.execute("SELECT * FROM songs")

      puts "songs contain #{rows.size} rows"

      batch   = session.batch do |b|
                  b.add("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)",
                        Cql::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
                        'La Petite Tonkinoise',
                        'Bye Bye Blackbird',
                        'Joséphine Baker',
                        Set['jazz', '2013']
                  )
                  b.add("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)",
                        Cql::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
                        'Die Mösch',
                        'In Gold',
                        'Willi Ostermann',
                        Set['kölsch', '1996', 'birds']
                  )
                  b.add("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)",
                        Cql::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25'),
                        'Memo From Turner',
                        'Performance',
                        'Mick Jager',
                        Set['soundtrack', '1991']
                  )
                end

      puts "inserting rows in a batch"

      session.execute(batch)
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

  Scenario: a prepared statement is executed in a batch
    Given the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster
                  .with_contact_points("127.0.0.1")
                  .build

      at_exit { cluster.close }

      session = cluster.connect("simplex")

      rows = session.execute("SELECT * FROM songs")

      puts "songs contain #{rows.size} rows"

      insert  = session.prepare("INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)")
      batch   = session.batch do |b|
                  b.add(insert,
                        Cql::Uuid.new('756716f7-2e54-4715-9f00-91dcbea6cf50'),
                        'La Petite Tonkinoise',
                        'Bye Bye Blackbird',
                        'Joséphine Baker',
                        Set['jazz', '2013']
                  )
                  b.add(insert,
                        Cql::Uuid.new('f6071e72-48ec-4fcb-bf3e-379c8a696488'),
                        'Die Mösch',
                        'In Gold',
                        'Willi Ostermann',
                        Set['kölsch', '1996', 'birds']
                  )
                  b.add(insert,
                        Cql::Uuid.new('fbdf82ed-0063-4796-9c7c-a3d4f47b4b25'),
                        'Memo From Turner',
                        'Performance',
                        'Mick Jager',
                        Set['soundtrack', '1991']
                  )
                end

      puts "inserting rows in a batch"

      session.execute(batch)
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
