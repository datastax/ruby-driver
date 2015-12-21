Feature: Custom payloads

  Starting with Cassandra v2.2+, every request can include custom payload.
  This payload is meant to be processed by custom query handlers on Cassandra.
  Default handlers simply ignore it.

  Background:
    Given a running cassandra cluster

  @cassandra-version-specific @cassandra-version-2.2
  Scenario: Sending custom payload
    Given the following example:
      """ruby
      require 'cassandra'
      cluster = Cassandra.cluster
      session = cluster.connect

      result = session.execute('SELECT unixTimestampOf(NOW()) as date FROM system.local',
                               payload: {'some_key' => 'some_value'})

      delta = Time.now.to_i - result.first['date'] / 1000
      if delta < 0 || delta > 1
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

  @cassandra-version-specific @cassandra-version-2.2
  Scenario: Mirroring a sent custom payload
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
      USE simplex;
      CREATE TABLE test (k int, v int, PRIMARY KEY (k, v));
      """
    And the following example:
      """ruby
      require 'cassandra'
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      result = session.execute('SELECT * FROM test', payload: {'first_key' => 'first_value'})
      puts result.execution_info.payload

      select = session.prepare('SELECT * FROM test WHERE k=?')
      result = session.execute(select, arguments: [0], payload: {'second_key' => 'second_value'})
      puts result.execution_info.payload

      batch = session.batch do |b|
        b.add("INSERT INTO test (k, v) VALUES (1, 1)")
      end
      result = session.execute(batch, payload: {'third_key' => 'third_value', 'fourth_key' => 'fourth_value'})
      puts result.execution_info.payload
      """
    When payload mirroring query handler is enabled
    And it is executed
    Then its output should contain:
      """
      {"first_key"=>"first_value"}
      {"second_key"=>"second_value"}
      {"fourth_key"=>"fourth_value", "third_key"=>"third_value"}
      """