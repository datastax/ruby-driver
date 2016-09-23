Feature: Execution profiles

  Execution profiles allow a user to group various execution options into a 'profile'. A user can then execute
  statements with different profiles by specifying the profile name.

  Background:
    Given a running cassandra cluster

  Scenario: Configure different load balancing policies with profiles.
    Given the following example:
      """ruby
      require 'cassandra'

      include Cassandra::LoadBalancing::Policies
      profiles = {
          p1: Cassandra::Execution::Profile.new(load_balancing_policy: WhiteList.new(['127.0.0.1'], RoundRobin.new)),
          p2: Cassandra::Execution::Profile.new(load_balancing_policy: WhiteList.new(['127.0.0.2'], RoundRobin.new))
      }
      cluster = Cassandra.cluster(execution_profiles: profiles)
      session = cluster.connect

      puts "Running with default profile"
      3.times do
        rs = session.execute('select rpc_address from system.local')
        puts rs.first['rpc_address']
      end

      puts "Running with profile p1"
      3.times do
        rs = session.execute('select rpc_address from system.local', execution_profile: :p1)
        puts rs.first['rpc_address']
      end

      puts "Running with profile p2"
      3.times do
        rs = session.execute('select rpc_address from system.local', execution_profile: :p2)
        puts rs.first['rpc_address']
      end
      """
    When it is executed
    Then its output should contain:
      """
      Running with default profile
      127.0.0.2
      127.0.0.3
      127.0.0.1
      Running with profile p1
      127.0.0.1
      127.0.0.1
      127.0.0.1
      Running with profile p2
      127.0.0.2
      127.0.0.2
      127.0.0.2
      """

