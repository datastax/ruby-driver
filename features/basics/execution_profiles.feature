Feature: Execution profiles

  Execution profiles allow a user to group various execution options into a 'profile'. A user defines profiles when
  initializing the cluster object. A user can then execute statements with different profiles by specifying the profile
  name in calls to `Session.execute*`.

  Profile names should be strings or symbols. In this release, a profile encapsulates load-balancing policy,
  retry-policy, consistency-level, and timeout primitive options. Execution profiles are immutable once created.

  If a user specifies simple options to `Cassandra.cluster`, the options mentioned above are stored in a default
  execution profile named `:default`. This execution profile is used by default by `Session.execute*` methods.
  User-defined execution profiles fall back to the same system defaults that past versions of the driver fell back to
  for unspecified options:

  * load_balancing_policy: `LoadBalancing::Policies::TokenAware.new(LoadBalancing::Policies::DCAwareRoundRobin.new, true)`
  * retry-policy: `Retry::Policies::Default.new`
  * consistency: `:local_one`
  * timeout: `12`

  In particular, note that user-defined execution profiles do not fall back to options specified in a (possibly
  user-defined) `:default` profile.

  If you declare execution profiles, it is illegal to also include the primitive options mentioned above:

  ```ruby
  puts "This is bad"
  cluster = Cassandra.cluster(timeout: 7, execution_profiles: {
      my_profile: Cassandra::Execution::Profile.new(...)
  })
  ```

  To change default execution profile attributes and also declare other execution profiles, you must explicitly declare
  the :default profile when initializing the cluster:

  ```ruby
  puts "This is legal"
  cluster = Cassandra.cluster(execution_profiles: {
      default: Cassandra::Execution::Profile.new(timeout: 7)
      my_profile: Cassandra::Execution::Profile.new(...)
  })
  ```

  Unspecified attributes fall back to the system defaults mentioned above.

  Finally, options specified to `Session.execute*` methods override options specified in the desired execution profile.

  Background:
    Given a running cassandra cluster

  Scenario: Creating and inspecting execution profiles
    Given the following example:
      """ruby
      require 'cassandra'

      profile_1 = Cassandra::Execution::Profile.new(load_balancing_policy: Cassandra::LoadBalancing::Policies::RoundRobin.new,
                                                    retry_policy: Cassandra::Retry::Policies::DowngradingConsistency.new,
                                                    consistency: :all,
                                                    timeout: 32
      )

      cluster = Cassandra.cluster(execution_profiles: {'my_profile' => profile_1})
      puts "There are #{cluster.execution_profiles.size} execution profiles in the cluster:"
      puts ""

      cluster.execution_profiles.each do |name, profile|
        puts "Name: #{name}"
        puts "Load_balancing_policy: #{profile.load_balancing_policy.class}"
        puts "Retry policy: #{profile.retry_policy.class}"
        puts "Consistency: #{profile.consistency}"
        puts "Timeout: #{profile.timeout}"
        puts ""
      end
      """
    When it is executed
    Then its output should contain:
      """
      There are 2 execution profiles in the cluster:

      Name: my_profile
      Load_balancing_policy: Cassandra::LoadBalancing::Policies::RoundRobin
      Retry policy: Cassandra::Retry::Policies::DowngradingConsistency
      Consistency: all
      Timeout: 32

      Name: default
      Load_balancing_policy: Cassandra::LoadBalancing::Policies::TokenAware
      Retry policy: Cassandra::Retry::Policies::Default
      Consistency: local_one
      Timeout: 12
      """

  Scenario: Configure different load balancing policies with profiles
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

      # By default, the driver uses a token-aware, round-robin load balancing policy.
      ip_list = []
      3.times do
        rs = session.execute('select rpc_address from system.local')
        ip_list << rs.first['rpc_address'].to_s
      end
      puts ip_list.sort.join("\n")

      # p2 and p3 set up load-balancing policies that will match only one node.
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
      127.0.0.1
      127.0.0.2
      127.0.0.3
      Running with profile p1
      127.0.0.1
      127.0.0.1
      127.0.0.1
      Running with profile p2
      127.0.0.2
      127.0.0.2
      127.0.0.2
      """

