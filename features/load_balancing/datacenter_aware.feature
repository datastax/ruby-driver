# encoding: utf-8

@wip
Feature: Datacenter-aware Round Robin Policy

  A specialized Round Robin load balancing policy allows for querying remote datacenters when all local nodes are down.

  The effects of the policy can be seen by enabling requests tracing. The
  coordinator node that served every traced request can be retrieved from the
  system_traces.session table.

  @todo
  Scenario: Datacenter-aware Round Robin policy limits queries to a "local" datacenter
    Given a running cassandra cluster in 2 datacenters with 2 nodes in each
    And a schema "simplex"
    And a table "songs"
    And the following example:
    """ruby
      require 'cql'

      cluster = Cql.cluster
        .with_load_balancing_policy(Cql::LoadBalancing::Policies::DCAwareRoundRobin.new("dc2"))
        .build
      session = cluster.connect('simplex')

      trace_ids = []
      4.times do
        trace_ids.push session.execute("SELECT * FROM songs", :trace => true).trace_id.to_s
      end

      coordinators = session.execute(
        "SELECT coordinator
         FROM system_traces.sessions
         WHERE session_id IN (#{trace_ids.join(",")})"
      )
      ips = coordinators.map do |row|
        row["coordinator"].to_s
      end

      puts ips.sort
      """
    When it is executed
    Then its output should contain:
    """
      127.0.1.3
      127.0.1.4
      """

  @todo
  Scenario: Datacenter-aware Round Robin policy is used
    Given a running cassandra cluster in 2 datacenters with 2 nodes in each
    And a schema "simplex"
    And a table "songs"
    And node 1 stops
    And node 2 stops
    And the following example:
    """ruby
      require 'cql'

      cluster = Cql.cluster
        .with_load_balancing_policy(Cql::LoadBalancing::Policies::DCAwareRoundRobin.new("dc1", 1))
        .build
      session = cluster.connect('simplex')

      trace_ids = []
      4.times do
        trace_ids.push session.execute("SELECT * FROM songs", :trace => true).trace_id.to_s
      end

      coordinators = session.execute(
        "SELECT coordinator
         FROM system_traces.sessions
         WHERE session_id IN (#{trace_ids.join(",")})"
      )
      ips = coordinators.map do |row|
        row["coordinator"].to_s
      end

      puts ips.sort
      """
    When it is executed
    Then its output should match:
    """
      127.0.1.(3|4)
      """
