# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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

class TokenAwareTest < IntegrationTestCase
  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(2, 2)
  end

  def setup_schema
    @@ccm_cluster.setup_schema(<<-CQL)
    CREATE KEYSPACE simplex WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '1', 'dc2': '1'};
    USE simplex;
    CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
    INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40);
    INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35);
    INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32);
    INSERT INTO users (user_id, first, last, age) VALUES (3, 'Apache', 'Cassandra', 7);
    CQL
  end

  def test_token_aware_datacenter_aware_is_used_by_default
    setup_schema
    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      assert_equal 1, info.hosts.size
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.1', '127.0.0.2', '127.0.0.2'], hosts_used.sort
    cluster.close
  end

  def test_token_aware_routes_to_primary_replica_in_primary_dc
    setup_schema
    base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new('dc1')
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect("simplex")

    Retry.with_attempts(5, Cassandra::Errors::InvalidError) do
      select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }
    
      result  = Retry.with_attempts(5) { session.execute(select, arguments: [0]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal 2945182322382062539, result.first.values.first
      assert_equal "127.0.0.1", result.execution_info.hosts.last.ip.to_s

      result  = Retry.with_attempts(5) { session.execute(select, arguments: [1]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal 6292367497774912474, result.first.values.first
      assert_equal "127.0.0.1", result.execution_info.hosts.last.ip.to_s

      result  = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal -8218881827949364593, result.first.values.first
      assert_equal "127.0.0.2", result.execution_info.hosts.last.ip.to_s

      result  = Retry.with_attempts(5) { session.execute(select, arguments: [3]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal -8048510690352527683, result.first.values.first
      assert_equal "127.0.0.2", result.execution_info.hosts.last.ip.to_s
    end

    cluster.close
  end

  # Test for token-aware load balancing policy with primary dc down
  #
  # test_token_aware_does_not_by_default_route_to_next_replica_if_primary_dc_down tests the token aware policy's
  # behavior when the primary dc is down. It performs a query and verifies that a NoHostsAvailable error is
  # raised as no primary replicas are available. Note the default consistency is :local_one.
  #
  # @expected_errors [Cassandra::Errors::NoHostsAvailable] When primary dc is down.
  #
  # @since 1.0.0
  # @expected_result A NoHostsAvailable should be raised as the primary dc is down, and no remotes are specified.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:token_aware
  #
  def test_token_aware_does_not_by_default_route_to_next_replica_if_primary_dc_down
    setup_schema
    datacenter = 'dc1'
    base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }

    @@ccm_cluster.stop_node("node1")
    @@ccm_cluster.stop_node("node2")

    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
    end

    cluster.close
  end

  def test_token_aware_can_route_to_next_replica_if_primary_dc_down
    setup_schema
    max_remote_hosts_to_use = nil
    base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new('dc1', max_remote_hosts_to_use)
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
    cluster = Cassandra.cluster(load_balancing_policy: policy, consistency: :one)
    session = cluster.connect("simplex")

    Retry.with_attempts(5, Cassandra::Errors::InvalidError) do
      select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }

      result  = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal "127.0.0.2", result.execution_info.hosts.last.ip.to_s

      @@ccm_cluster.stop_node("node2")

      result  = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal "127.0.0.1", result.execution_info.hosts.last.ip.to_s

      @@ccm_cluster.stop_node("node1")

      result  = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_includes(["127.0.0.3", "127.0.0.4"], result.execution_info.hosts.last.ip.to_s)
    end

    cluster.close
  end

  def test_token_aware_routes_to_next_whitelisted_replica_if_primary_down
    setup_schema
    allowed_ips = ["127.0.0.1", "127.0.0.3"]
    round_robin = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new('dc1')
    whitelist = Cassandra::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(whitelist)
    cluster = Cassandra.cluster(load_balancing_policy: policy, consistency: :one)
    session = cluster.connect("simplex")

    Retry.with_attempts(5, Cassandra::Errors::InvalidError) do
      select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }

      result = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal "127.0.0.1", result.execution_info.hosts.last.ip.to_s
    end

    cluster.close
  end

  # Test for token-aware lbp with primary down but remotes disabled for local consistencies
  #
  # test_token_aware_does_not_route_to_next_replica_for_local_consistency_by_default_if_primary_dc_down tests the
  # token-aware policy's behavior for local consistency queries when the primary dc is down, but only remote
  # hosts are enabled. It first specifies the load balancing policy with max_remote_hosts_to_use = nil to allow
  # unlimited remote hosts to be used for non-local consistency queries. It then performs a query and verifies that a
  # NoHostsAvailable error is raised, as no primary replicas are available for the local consistency query. Note the
  # default consistency is :local_one.
  #
  # @expected_errors [Cassandra::Errors::NoHostsAvailable] When primary dc is down, local consistency is not enabled for remotes.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-211
  # @expected_result A NoHostsAvailable should be raised as the primary dc is down, and no remotes are specified for local consistency.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:token_aware
  #
  def test_token_aware_does_not_route_to_next_replica_for_local_consistency_by_default_if_primary_dc_down
    setup_schema
    datacenter = 'dc1'
    max_remote_hosts_to_use = nil
    base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use)
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }

    @@ccm_cluster.stop_node("node1")
    @@ccm_cluster.stop_node("node2")

    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
    end

    cluster.close
  end

  # Test for token-aware lbp with primary down but remotes enabled for local consistencies
  #
  # test_token_aware_can_route_to_next_replica_for_local_consistency_if_primary_dc_down tests the token-aware policy's
  # behavior when the primary dc is down, but remote replicas are enabled with local consistencies. It first specifies the
  # load balancing policy with max_remote_hosts_to_use = nil, and use_remote_hosts_for_local_consistency = true, to allow
  # unlimited remote hosts to be used for local consistency queries. It then performs 4 queries and verifies that replicas
  # from dc2 (node3, node4) are used. Note the default consistency is :local_one.
  #
  # @since 1.0.0
  # @expected_result Each of the 4 local-consistency queries should be routed a different replica in the remote dc, dc2.
  #
  # @test_assumptions Existing Cassandra cluster with two datacenters, keyspace 'simplex', and table 'users'.
  # @test_category load_balancing:token_aware
  #
  def test_token_aware_can_route_to_next_replica_for_local_consistency_if_primary_dc_down
    setup_schema
    datacenter = 'dc1'
    max_remote_hosts_to_use = nil
    use_remote_hosts_for_local_consistency = true
    base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use,
                                                                            use_remote_hosts_for_local_consistency)
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }

    @@ccm_cluster.stop_node("node1")
    @@ccm_cluster.stop_node("node2")

    result = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
    assert_equal 1, result.execution_info.hosts.size
    assert_includes(["127.0.0.3", "127.0.0.4"], result.execution_info.hosts.last.ip.to_s)

    cluster.close
  end
end
