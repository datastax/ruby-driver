Feature: Round Robin Policy

  The Round Robin load balancing policy dispatches requests evenly on cluster
  nodes.

  The effects of the policy can be seen by enabling requests tracing. The
  coordinator node that served every request is the last host in
  execution info.

  Scenario: Round Robin policy is used by default
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"
    And the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster.build
      session = cluster.connect('simplex')

      coordinator_ips = 3.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.2
      127.0.0.3
      """

  Scenario: Round Robin policy is used explicitly
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"
    And the following example:
      """ruby
      require 'cql'

      cluster = Cql.cluster
        .with_load_balancing_policy(Cql::LoadBalancing::Policies::RoundRobin.new)
        .build
      session = cluster.connect('simplex')

      coordinator_ips = 3.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort

      puts ips.sort
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.2
      127.0.0.3
      """

  Scenario: Round Robin policy ignores datacenters
    Given a running cassandra cluster in 2 datacenters with 2 nodes in each
    And a keyspace "simplex"
    And a table "songs"
    And the following example:
    """ruby
      require 'cql'

      cluster = Cql.cluster
        .with_load_balancing_policy(Cql::LoadBalancing::Policies::RoundRobin.new)
        .build
      session = cluster.connect('simplex')

      coordinator_ips = 4.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end

      puts coordinator_ips.sort
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.2
      127.0.0.3
      127.0.0.4
      """
