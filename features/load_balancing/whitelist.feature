Feature: White List Policy

  The White List load balancing policy wraps a subpolicy and ensures that only
  hosts from a provided white list are used. This policy can be used to limit
  effects of automatic peer discovery to executing queries only on a given set
  of hosts.

  Scenario: Prevent queries from running on non-whitelisted hosts
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"
    And the following example:
      """ruby
      require 'cql'

      allowed_ips = ["127.0.0.1", "127.0.0.3"]
      round_robin = Cql::LoadBalancing::Policies::RoundRobin.new
      whitelist   = Cql::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
      cluster     = Cql.connect(load_balancing_policy: whitelist)
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
