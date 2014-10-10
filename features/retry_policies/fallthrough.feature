Feature: Fallthrough Retry Policy

  The Fallthrough retry policy prevents the driver from retrying queries when they failed.

  This strategy should be used when the retry policy has to be implemented in business code.

  Scenario: Fallthrough policy is used explicitly
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
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.connect(retry_policy: Cassandra::Retry::Policies::Fallthrough.new)
      session = cluster.connect('simplex')

      begin
        session.execute('SELECT * FROM songs', consistency: :all)
        puts "failed"
      rescue Cassandra::Errors::UnavailableError => e
        puts "success"
      end
      """
    When node 3 stops
    And it is executed
    Then its output should contain:
      """
      success
      """
