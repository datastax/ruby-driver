# encoding: utf-8

Feature: Datacenter-aware Round Robin Policy

  A specialized Round Robin load balancing policy allows for querying remote datacenters when all local nodes are down.

  The effects of the policy can be seen by enabling requests tracing. The
  coordinator node that served every traced request can be retrieved from the
  system_traces.session table.

  Scenario: Datacenter-aware Round Robin policy prefers hosts from a local datacenter
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
    127.0.0.3
    127.0.0.4
    """
