Feature: prepared statements for reads

  Prepared statements are used to prepare a read query only once and execute it
  multiple times with different values. A bind variable marker "?" is used to
  represent a dynamic value in a statement.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "playlists"

  Scenario: a SELECT statement with parameterized LIMIT is prepared
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.connect

      at_exit { cluster.close }

      session = cluster.connect("simplex")
      select  = session.prepare("SELECT * FROM playlists LIMIT ?")
      limits  = [1, 2, 3]

      limits.each do |limit|
        rows = session.execute(select, limit)
        puts "selected #{rows.size} row(s)"
      end
      """
    When it is executed
    Then its output should contain:
      """
      selected 1 row(s)
      selected 2 row(s)
      selected 3 row(s)
      """
