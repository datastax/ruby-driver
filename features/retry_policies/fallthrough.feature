Feature: Fallthrough Retry Policy

  The Fallthrough retry policy prevents the driver from retrying queries when they failed.

  This strategy should be used when the retry policy has to be implemented in business code.

  Scenario: Fallthrough policy is used explicitly
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"
    And the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster
                   .with_retry_policy(Cql::Retry::Policies::Fallthrough.new)
                   .build

      session = cluster.connect('simplex')

      begin
        session.execute('SELECT * FROM songs', consistency: :all)
        puts "failed"
      rescue Cql::QueryError => e
        puts "success"
      end
      """
    When node 3 stops
    And it is executed
    Then its output should contain:
      """
      success
      """
