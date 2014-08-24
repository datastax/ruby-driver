Feature: Fallthrough Retry Policy

  The Fallthrough retry policy prevents the driver from retrying queries when they failed.

  This strategy should be used when the retry policy has to be implemented in business code.

  Scenario: Fallthrough policy is used explicitly
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.connect(retry_policy: Cassandra::Retry::Policies::Fallthrough.new)
      session = cluster.connect('simplex')

      begin
        session.execute('SELECT * FROM songs', consistency: :all)
        puts "failed"
      rescue Cassandra::Errors::QueryError => e
        puts "success"
      end
      """
    When node 3 stops
    And it is executed
    Then its output should contain:
      """
      success
      """
