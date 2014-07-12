# encoding: utf-8

@wip
Feature: custom load balancing policies

  Upon creation of a cluster object, a custom load balancing policy can be
  provided that will be used for all driver operations.

  Load balancing policies must also be state listeners and will receive updates
  of node membership and availability changes.

  Scenario: a policy that ignores a certain keyspace
    Given a running cassandra cluster
    And a file named "policy.rb" with:
      """ruby
      class Policy < Cql::LoadBalancing::Policies::RoundRobin
        def initialize(keyspace_to_ignore)
          @keyspace = keyspace_to_ignore
          super()
        end

        def plan(keyspace, statement)
          return [].to_enum if keyspace == @keyspace
          super
        end
      end
      """
    And the following example:
      """ruby
      require 'cql'
      require 'policy'
      
      cluster = Cql.cluster(load_balancing_policy: Policy.new('simplex')).build
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
    