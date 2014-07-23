Feature: White List Policy

  The White List load balancing policy wraps a subpolicy and ensure that only
  hosts from a provided white list will ever be returned.

  The effects of the policy can be seen by enabling requests tracing. The
  coordinator node that served every request is the last host in
  execution info.

  Scenario: A Round Robin policy is wrapped with a White List policy
    Given a running cassandra cluster with a schema "simplex" and a table "songs"
    And the following example:
    """ruby
    require 'cql'

    allowed_ips = ["127.0.0.1", "127.0.0.3"]
    round_robin = Cql::LoadBalancing::Policies::RoundRobin.new
    whitelist   = Cql::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
    cluster     = Cql.cluster.with_load_balancing_policy(whitelist).build
    session     = cluster.connect('simplex')

    coordinator_ips = 3.times.map do
      info = session.execute("SELECT * FROM songs").execution_info
      info.hosts.last.ip
    end

    puts coordinator_ips.sort.uniq
    """
    When it is executed
    Then its output should contain:
    """
    127.0.0.1
    127.0.0.3
    """
