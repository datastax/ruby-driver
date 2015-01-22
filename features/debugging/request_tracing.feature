Feature: Request tracing

  Execution information can be used to access request trace if tracing was enabled.

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

  Scenario: tracing is disabled by default
    Given the following example:
      """ruby
      require 'cassandra'

      cluster   = Cassandra.cluster
      session   = cluster.connect("simplex")
      execution = session.execute("SELECT * FROM songs").execution_info

      if execution.trace
        puts "failure"
      else
        puts "success"
      end
      """
    When it is executed
    Then its output should contain:
      """
      success
      """

  Scenario: tracing is enabled explicitly
    Given the following example:
      """ruby
      require 'cassandra'

      cluster   = Cassandra.cluster
      session   = cluster.connect("simplex")
      execution = session.execute("SELECT * FROM songs", trace: true).execution_info
      trace     = execution.trace

      puts "coordinator: #{trace.coordinator}"
      puts "started at: #{trace.started_at}"
      puts "total events: #{trace.events.size}"
      puts "request: #{trace.request}"
      """
    When it is executed
    Then its output should match:
      """
      coordinator: 127\.0\.0\.(1|2|3)
      """
    And its output should match:
      """
      started at: \d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2} (-|\+)\d{4}
      """
    And its output should match:
      """
      total events: \d+
      """
    And its output should contain:
      """
      request: Execute CQL3 query
      """
