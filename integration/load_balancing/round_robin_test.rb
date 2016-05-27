# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require File.dirname(__FILE__) + '/../integration_test_case.rb'
require File.dirname(__FILE__) + '/try_next_host_retry_policy.rb'

class RoundRobinTest < IntegrationTestCase
  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(2, 2)
  end

  def setup_schema
    @@ccm_cluster.setup_schema(<<-CQL)
    CREATE KEYSPACE simplex WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '2', 'dc2': '2'};
    USE simplex;
    CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
    CQL
  end

  # Test for basic round robin load balancing policy
  #
  # test_round_robin_used_explicitly tests the round robin policy by explicitly using it during the cluster connection.
  # 4 queries are made, and each query should go to a different node in the Cassandra cluster. Since each query goes
  # to a different Cassandra node, this test also verifies that the round robin policy ignores datacenters.
  #
  # @since 1.0.0
  # @expected_result Each of the 4 queries should be routed to a different node in the cluster, across datacenters.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_round_robin_used_explicitly_and_ignores_datacenters
    setup_schema
    policy  = Cassandra::LoadBalancing::Policies::RoundRobin.new
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      assert_equal 1, info.hosts.size
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2', '127.0.0.3', '127.0.0.4'], hosts_used.sort
    cluster.close
  end

  # Test for whitelisted round robin load balancing policy
  #
  # test_whitelisted_round_robin tests the round robin policy using a whitelist. 4 queries are made, and each query
  # should go to a whitelisted node in the Cassandra cluster.
  #
  # @since 1.0.0
  # @expected_result Each of the 4 queries should be routed to a whitelisted node in the cluster.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_whitelisted_round_robin
    setup_schema
    allowed_ips = ['127.0.0.1', '127.0.0.3']
    round_robin = Cassandra::LoadBalancing::Policies::RoundRobin.new
    whitelist = Cassandra::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
    cluster = Cassandra.cluster(load_balancing_policy: whitelist)
    session = cluster.connect('simplex')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      assert_equal 1, info.hosts.size
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.3'], hosts_used.sort.uniq
    cluster.close
  end

  # Test for basic dc-aware round robin load balancing policy
  #
  # test_dc_aware_round_robin_queries_to_local_dc tests the round robin policy wrapping around a datacenter-aware
  # policy. Using dc2 (node3, node4), 4 queries are made, and each query should go to a one of these two nodes in the
  # Cassandra cluster. Note the default consistency is :local_one.
  #
  # @since 1.0.0
  # @expected_result Each of the 4 queries should be routed to a node in the local dc, dc2 (node3 or node4).
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_dc_aware_round_robin_queries_to_local_dc
    setup_schema
    datacenter = 'dc2'
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.3', '127.0.0.4'], hosts_used.sort.uniq
    cluster.close
  end

  # Test for dc-aware round robin load balancing policy with local down
  #
  # test_dc_aware_round_robin_does_not_query_to_remote_dc_by_default_if_local_down tests the dc-aware round robin
  # policy's behavior when the local dc is down. It performs a query and verifies that a NoHostsAvailable error is
  # raised as no local nodes are available. Note the default consistency is :local_one.
  #
  # @expected_errors [Cassandra::Errors::NoHostsAvailable] When local dc is down.
  #
  # @since 1.0.0
  # @expected_result A NoHostsAvailable should be raised as the local dc is down, and no remotes are specified.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_dc_aware_round_robin_does_not_query_to_remote_dc_by_default_if_local_down
    setup_schema
    datacenter = 'dc1'
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    @@ccm_cluster.stop_node('node1')
    @@ccm_cluster.stop_node('node2')

    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
    end

    cluster.close
  end

  # Test for dc-aware round robin lbp with local down but remotes enabled
  #
  # test_dc_aware_round_robin_can_query_to_remote_dc_if_local_down tests the dc-aware round robin policy's behavior
  # when the local dc is down, but remote hosts are enabled. It first specifies the load balancing policy with
  # max_remote_hosts_to_use = nil to allow unlimited remote hosts to be used for non-local consistency queries. It then
  # performs 4 queries and verifies that hosts from dc2 (node3, node4) are used. Note the consistency is :one to allow
  # for remote hosts to be used for the query.
  #
  # @since 1.0.0
  # @expected_result Each of the 4 queries should be routed a different node in the remote dc, dc2 (node3 or node4).
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_dc_aware_round_robin_can_query_to_remote_dc_if_local_down
    setup_schema
    datacenter = 'dc1'
    max_remote_hosts_to_use = nil
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use)
    cluster = Cassandra.cluster(load_balancing_policy: policy, consistency: :one)
    session = cluster.connect('simplex')

    @@ccm_cluster.stop_node('node1')
    @@ccm_cluster.stop_node('node2')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.3', '127.0.0.4'], hosts_used.sort.uniq
    cluster.close
  end

  # Test for dc-aware round robin lbp with local down but one remote enabled
  #
  # test_dc_aware_round_robin_routes_up_to_max_hosts_in_remote tests the dc-aware round robin policy's behavior
  # when the local dc is down, but one remote host is enabled. It first specifies the load balancing policy with
  # max_remote_hosts_to_use = 1 to allow up to 1 remote host to be used for non-local consistency queries. It then
  # performs 4 queries and verifies that one host from dc2 (node3, node4) is used. Note the consistency is :one to allow
  # for remote hosts to be used for the query.
  #
  # @since 1.0.0
  # @expected_result Each of the 4 queries should be routed one node in the remote dc, dc2 (node3 or node4).
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_dc_aware_round_robin_routes_up_to_max_hosts_in_remote
    setup_schema
    datacenter = 'dc1'
    max_remote_hosts_to_use = 1
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use)
    cluster = Cassandra.cluster(load_balancing_policy: policy, consistency: :one)
    session = cluster.connect('simplex')

    @@ccm_cluster.stop_node('node1')
    @@ccm_cluster.stop_node('node2')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal 1, hosts_used.uniq.size
    assert_includes(['127.0.0.3', '127.0.0.4'], hosts_used.uniq.first)
    cluster.close
  end

  # Test for dc-aware round robin lbp with local down but remotes disabled for local consistencies
  #
  # test_dc_aware_round_robin_does_not_query_to_remote_dc_for_local_consistency_by_default_if_local_down tests the
  # dc-aware round robin policy's behavior for local consistency queries when the local dc is down, but only remote
  # hosts are enabled. It first specifies the load balancing policy with max_remote_hosts_to_use = 2 to allow
  # up to 2 remote hosts to be used for non-local consistency queries. It then performs a query and verifies that a
  # NoHostsAvailable error is raised, as no local nodes are available for the local consistency query. Note the default
  # consistency is :local_one.
  #
  # @expected_errors [Cassandra::Errors::NoHostsAvailable] When local dc is down, local consistency is not enabled for remotes.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-211
  # @expected_result A NoHostsAvailable should be raised as the local dc is down, and no remotes are specified for local consistency.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_dc_aware_round_robin_does_not_query_to_remote_dc_for_local_consistency_by_default_if_local_down
    setup_schema
    datacenter = 'dc1'
    max_remote_hosts_to_use = 2
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    @@ccm_cluster.stop_node('node1')
    @@ccm_cluster.stop_node('node2')

    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
    end

    cluster.close
  end

  # Test for dc-aware round robin lbp with local down but remotes enabled for local consistencies
  #
  # test_dc_aware_round_robin_can_query_to_remote_dc_for_local_if_local_down tests the dc-aware round robin policy's
  # behavior when the local dc is down, but remote hosts are enabled with local consistencies. It first specifies the
  # load balancing policy with max_remote_hosts_to_use = 2, and use_remote_hosts_for_local_consistency = true, to allow
  # up to 2 remote hosts to be used for local consistency queries. It then performs 4 queries and verifies that hosts
  # from dc2 (node3, node4) are used. Note the default consistency is :local_one.
  #
  # @since 1.0.0
  # @expected_result Each of the 4 local-consistency queries should be routed a different node in the remote dc, dc2.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_dc_aware_round_robin_can_query_to_remote_dc_for_local_consistency_if_local_down
    setup_schema
    datacenter = 'dc1'
    max_remote_hosts_to_use = 2
    use_remote_hosts_for_local_consistency = true
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use,
                                                                       use_remote_hosts_for_local_consistency)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    @@ccm_cluster.stop_node('node1')
    @@ccm_cluster.stop_node('node2')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.3', '127.0.0.4'], hosts_used.sort.uniq
    cluster.close
  end

  # Test for dc-aware round robin lbp with local down but zero remotes enabled for local consistencies
  #
  # test_dc_aware_round_robin_cannot_query_to_remote_dc_for_local_consistency_if_max_hosts_zero tests the dc-aware
  # round robin policy's behavior when the local dc is down, but zero remote hosts are enabled with local consistencies.
  # It attempts to create a load balancing policy with max_remote_hosts_to_use = 0,
  # and use_remote_hosts_for_local_consistency = true, and verifies than an ArgumentError is raised.
  #
  # @expected_errors [ArgumentError] When zero max_remote_hosts_to_use is specified with use_remote_hosts_for_local_consistency.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-211
  # @expected_result An ArgumentError should be raised.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_dc_aware_round_robin_cannot_query_to_remote_dc_for_local_consistency_if_max_hosts_zero
    setup_schema
    datacenter = 'dc1'
    max_remote_hosts_to_use = 0
    use_remote_hosts_for_local_consistency = true

    assert_raises(ArgumentError) do
      Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use,
                                                                use_remote_hosts_for_local_consistency)
    end
  end

  # Test for TryNextHost retry decision
  #
  # test_try_next_host_retry_decision tests that TryNextHost retry decision is being properly used when returned from the
  # retry policy. It uses the TryNextHostRetryPolicy class defined in try_next_host_retry_policy.rb as the retry policy.
  # It performs the same query against the cluster, shutting down one host at a time and making sure the next host is
  # used. Finally, it checks that a NoHostsAvailable error is raised when all hosts are down.
  #
  # @expected_errors [Cassandra::Errors::NoHostsAvailable] When all hosts are down.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-104
  # @expected_result Each of the queries should be fulfilled by an available host.
  #
  # @test_assumptions Existing Cassandra cluster with keyspace 'simplex' and table 'users'.
  # @test_category connection:retry_policy
  #
  def test_try_next_host_retry_decision
    setup_schema
    policy  = Cassandra::LoadBalancing::Policies::RoundRobin.new
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy, retry_policy: TryNextHostRetryPolicy.new)
    session = cluster.connect("simplex")

    @@ccm_cluster.stop_node('node1')
    info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)",
                            :consistency => :one).execution_info
    assert ['127.0.0.2', '127.0.0.3', '127.0.0.4'].include? info.hosts.last.ip.to_s

    @@ccm_cluster.stop_node('node2')
    info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)",
                            :consistency => :one).execution_info
    assert ['127.0.0.3', '127.0.0.4'].include? info.hosts.last.ip.to_s

    @@ccm_cluster.stop_node('node3')
    info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)",
                            :consistency => :one).execution_info
    assert_equal '127.0.0.4', info.hosts.last.ip.to_s

    @@ccm_cluster.stop_node('node4')
    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)",
                      :consistency => :one).execution_info
    end

    cluster.close
  end
end
