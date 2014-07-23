# encoding: utf-8

Feature: Datacenter-aware Round Robin Policy

  A specialized Round Robin load balancing policy allows for querying remotedatacenters when all local nodes are down.

  The effects of the policy can be seen by enabling requests tracing. The
  coordinator node that served every request can be retrieved from the
  execution info of the result.

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

    coordinator_ips = 4.times.map do
      info = session.execute("SELECT * FROM songs").execution_info
      info.hosts.last.ip
    end

    puts coordinator_ips.sort.uniq
    """
    When it is executed
    Then its output should contain:
    """
    127.0.0.3
    127.0.0.4
    """
