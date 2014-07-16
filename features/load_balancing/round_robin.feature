# encoding: utf-8

@wip
Feature: Round Robin Policy

  The Round Robin load balancing policy dispatches requests evenly on cluster
  nodes.

  Scenario: the default load balancing policy is Round Robin
    Given a cassandra cluster with schema "simplex" with table "songs"
    And the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster().build
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
      127.0.0.2
      127.0.0.3
      """
