# encoding: utf-8

Feature: Fallthrough Retry Policy

  The Fallthrough retry policy prevents the driver from retrying queries when they failed.

  This strategy should be used when the retry policy has to be implemented in business code.

  Scenario: Fallthrough policy is used explicitly
    Given a running cassandra cluster with a schema "simplex" and a table "songs"
    And node 3 stops
    And the following example:
      """ruby
      require 'cql'

      client = Cql::Client.connect(
        default_consistency: :all,
        keyspace: "simplex",
        retry_policy: Cql::Retry::Policies::Fallthrough.new
      )

      begin
        client.execute('SELECT * FROM songs', consistency: :all)
        puts "failed"
      rescue Cql::QueryError => e
        puts "success"
      end
      """
    When it is executed
    Then its output should contain:
      """
      success
      """
