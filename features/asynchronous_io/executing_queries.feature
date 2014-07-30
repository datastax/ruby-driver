Feature: Executing queries asynchronously

  Session objects support asynchronous statement execution using `Cql::Session#execute_async` method.
  This method returns a `Cql::Future[Cql::Result]`.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"

  Scenario: Getting execution result
    Given the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster.build
      session = cluster.connect("simplex")
      promise = session.execute_async("SELECT * FROM songs")

      puts "driver is fetching rows from cassandra"

      promise.get.each do |row|
        puts "#{row["artist"]}: #{row["title"]} / #{row["album"]}"
      end
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
      require 'cql'

      cluster = Cql.cluster.build
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
