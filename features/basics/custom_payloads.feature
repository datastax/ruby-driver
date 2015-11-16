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
