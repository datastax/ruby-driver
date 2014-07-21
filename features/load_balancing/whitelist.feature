# encoding: utf-8

@todo
Feature: White List Policy

  The White List load balancing policy wraps a subpolicy and ensure that only
  hosts from a provided white list will ever be returned.

  The effects of the policy can be seen by enabling requests tracing. The
  coordinator node that served every traced request can be retrieved from the
  system_traces.session table.

  Scenario: A Round Robin policy is wrapped with a White List policy
    Given a running cassandra cluster with a schema "simplex" and a table "songs"
    And the following example:
    """ruby
      require 'cql'

      allowed_ips = ["127.0.0.1", "127.0.0.3"]
      round_robin = Cql::LoadBalancing::Policies:RoundRobin.new
      whitelist = Cql::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
      cluster = Cql.cluster().with_load_balancing_policy(whitelist).build
      session = cluster.connect('simplex')

      results_1 = session.execute("SELECT * FROM songs", :trace => true)
      results_2 = session.execute("SELECT * FROM songs", :trace => true)
      results_3 = session.execute("SELECT * FROM songs", :trace => true)

      coordinators = session.execute(
        "SELECT coordinator
         FROM system_traces.sessions
         WHERE session_id IN (?, ?, ?)",
         results_1.trace_id,
         results_2.trace_id,
         results_3.trace_id
      )
      ips = coordinators.map do |row|
        row["coordinator"].to_s
      end

      puts ips.sort
      """
    When it is executed
    Then its output should contain:
    """
      127.0.0.1
      127.0.0.3
      """
