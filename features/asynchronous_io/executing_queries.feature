Feature: Executing queries asynchronously

  Session objects support asynchronous statement execution using `Cassandra::Session#execute_async` method.
  This method returns a `Cassandra::Future<Cassandra::Result>`.

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

  Scenario: Listening for future
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")
      future  = session.execute_async("SELECT * FROM songs")

      future.on_success do |rows|
        rows.each do |row|
          puts "#{row["artist"]}: #{row["title"]} / #{row["album"]}"
        end
      end

      puts "driver is fetching rows from cassandra"
      future.join # block until the future has been resolved
      """
    When it is executed
    Then its output should contain:
      """
      driver is fetching rows from cassandra
      Joséphine Baker: La Petite Tonkinoise / Bye Bye Blackbird
      Willi Ostermann: Die Mösch / In Gold
      Mick Jager: Memo From Turner / Performance
      """

  Scenario: Running queries in parallel
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")
      count   = 10

      puts "running #{count} queries in parallel"
      futures = count.times.map { session.execute_async("SELECT * FROM songs") }

      puts "resolving futures"

      futures.each do |future|
        rows = future.get
        puts "fetched #{rows.size} rows"
      end
      """
    When it is executed
    Then its output should contain:
      """
      running 10 queries in parallel
      resolving futures
      fetched 3 rows
      fetched 3 rows
      fetched 3 rows
      fetched 3 rows
      fetched 3 rows
      fetched 3 rows
      fetched 3 rows
      fetched 3 rows
      fetched 3 rows
      fetched 3 rows
      """
