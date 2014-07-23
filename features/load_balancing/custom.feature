Feature: custom load balancing policies

  Upon creation of a cluster object, a custom load balancing policy can be
  provided that will be used for all driver operations.

  Load balancing policies must also be state listeners and will receive updates
  of node membership and availability changes.

  Scenario: a policy that ignores a certain keyspace
    Given a running cassandra cluster with a schema "simplex" and a table "songs"
    And a file named "ignoring_keyspace_policy.rb" with:
      """ruby
      class IgnoringKeyspacePolicy < Cql::LoadBalancing::Policies::RoundRobin
        def initialize(keyspace_to_ignore)
          @keyspace = keyspace_to_ignore
          super()
        end

        def plan(keyspace, statement, options)
          return [].to_enum if keyspace == @keyspace
          super
        end
      end
      """
    And the following example:
      """ruby
      require 'cql'
      require 'ignoring_keyspace_policy'
      
      cluster = Cql.cluster
                  .with_load_balancing_policy(IgnoringKeyspacePolicy.new('simplex'))
                  .build
      session = cluster.connect('simplex')
      
      begin
        session.execute("SELECT * FROM songs")
        puts "failure"
      rescue Cql::NoHostsAvailable
        puts "success"
      end
      """
    When it is executed
    Then its output should contain:
      """
      success
      """
    