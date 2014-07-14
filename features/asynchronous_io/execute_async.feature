# encoding: utf-8

Feature: asynchronous query execution

  Session objects support non-blocking statement execution using `Session#execute_async` method.
  This method returns a `Future` object that can be resolved to get actual value.

  Background:
    Given a cassandra cluster with schema "simplex" with table "songs"

  Scenario: an asynchronous query returns a promise that is fulfilled later
    Given the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster
                  .with_contact_points("127.0.0.1", "127.0.0.2")
                  .build

      at_exit { cluster.close }

      session = cluster.connect("simplex")

      promise = session.execute_async("SELECT * FROM songs")

      $stdout.puts("driver is fetching rows from cassandra")

      promise.get.each do |row|
        $stdout.puts("#{row["artist"]}: #{row["title"]} / #{row["album"]}")
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
