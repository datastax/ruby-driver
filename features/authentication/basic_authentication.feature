# encoding: utf-8

Feature: basic authentication

  Cluster object can be configured to use a given username/password for
  authentication to cassandra cluster.

  Background:
    Given a running cassandra cluster with authentication enabled
    And the following example:
      """ruby
      require 'cql'
      
      begin
        cluster  = Cql.cluster                                 \
                    .with_contact_points(["127.0.0.1"])                     \
                    .with_credentials(ENV['USERNAME'], ENV['PASSWORD']) \
                    .build
        
        cluster.close
        puts "authentication successful"
      rescue => e
        puts "#{e.class.name}: #{e.message}"
        puts "authentication failed"
      end
      """

  Scenario: authentication is successful
    When it is executed with a valid username and password in the environment
    Then its output should match:
      """
      authentication successful
      """

  Scenario: bad credentials fail authentication
    When it is executed with an invalid username and password in the environment
    Then its output should match:
      """
      authentication failed
      """
