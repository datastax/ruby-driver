@auth
Feature: Standard authentication

  A Cluster can be configured to use a given username/password for
  authentication to cassandra cluster.

  Background:
    Given a running cassandra cluster with authentication enabled
    And the following example:
      """ruby
      require 'cassandra'
      
      begin
        cluster = Cassandra.cluster(
                    username: ENV['USER_NAME'],
                    password: ENV['PASSWORD']
                  )
        puts "authentication successful"
      rescue Cassandra::Errors::AuthenticationError => e
        puts "#{e.class.name}: #{e.message}"
        puts "authentication failed"
      else
        cluster.close
      end
      """

  Scenario: Authenticating with correct credentials
    When it is executed with a valid username and password in the environment
    Then its output should contain:
      """
      authentication successful
      """

  Scenario: Authenticating with incorrect credentials
    When it is executed with an invalid username and password in the environment
    Then its output should contain:
      """
      authentication failed
      """
