Feature: Executing queries asynchronously

  Session objects support asynchronous statement execution using `Cql::Session#execute_async` method.
  This method returns a `Cql::Future<Cql::Result>`.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"

  Scenario: Listerning for future
    Given the following example:
      """ruby
      require 'cql'

      cluster = Cql.connect
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
      require 'cql'

      cluster = Cql.connect
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
