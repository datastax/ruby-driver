Feature: Execution information

  Every result contains useful execution information.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"

  Scenario: execution information is accessible from execution result
    Given the following example:
      """ruby
      require 'cql'

      cluster = Cql.connect

      at_exit { cluster.close }

      session   = cluster.connect("simplex")
      execution = session.execute("SELECT * FROM songs").execution_info

      puts "coordinator: #{execution.hosts.last.ip}"
      puts "keyspace: #{execution.keyspace}"
      puts "cql: #{execution.statement.cql}"
      puts "requested consistency: #{execution.options.consistency}"
      puts "actual consistency: #{execution.consistency}"
      puts "number of retries: #{execution.retries}"
      """
    When it is executed
    Then its output should match:
      """
      coordinator: 127\.0\.0\.(1|2|3)
      """
    And its output should contain:
      """
      keyspace: simplex
      cql: SELECT * FROM songs
      requested consistency: one
      actual consistency: one
      number of retries: 0
      """

  Scenario: execution information reflects retry decision
    Given a file named "retrying_at_a_given_consistency_policy.rb" with:
      """ruby
      class RetryingAtAGivenConsistencyPolicy
        include Cql::Retry::Policy

        def initialize(consistency_to_use)
          @consistency_to_use = consistency_to_use
        end

        def read_timeout(statement, consistency_level, required_responses,
                         received_responses, data_retrieved, retries)
          try_again(@consistency_to_use)
        end

        def write_timeout(statement, consistency_level, write_type,
                          acks_required, acks_received, retries)
          try_again(@consistency_to_use)
        end

        def unavailable(statement, consistency_level, replicas_required,
                        replicas_alive, retries)
          try_again(@consistency_to_use)
        end
      end
      """
    And the following example:
      """ruby
      require 'cql'
      require 'retrying_at_a_given_consistency_policy'

      cluster   = Cql.connect(retry_policy: RetryingAtAGivenConsistencyPolicy.new(:one))
      session   = cluster.connect("simplex")
      execution = session.execute("SELECT * FROM songs", :consistency => :all).execution_info

      puts "requested consistency: #{execution.options.consistency}"
      puts "actual consistency: #{execution.consistency}"
      puts "number of retries: #{execution.retries}"
      """
    When node 3 stops
    And it is executed
    Then its output should contain:
      """
      requested consistency: all
      actual consistency: one
      number of retries: 1
      """
