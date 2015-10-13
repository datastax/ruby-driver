# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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

class RoundRobinTest < IntegrationTestCase
  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(2, 2)
  end

  def setup_schema
    @@ccm_cluster.setup_schema(<<-CQL)
    CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    USE simplex;
    CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
    CQL
  end

  # Test for basic round robin load balancing policy
  #
  # test_round_robin_used_explicitly tests the round robin policy by explicitly using it
  # during the cluster connection. 4 queries are made, and each query should go to a different
  # node in the Cassandra cluster.
  #
  # @return [String] List of hosts used in the query execution.
  #
  # @since 1.0.0
  # @expected_result Each of the 4 queries should be routed to a different node in the cluster.
  #
  # @test_assumptions Existing Cassandra cluster with keyspace 'simplex' and table 'users'.
  # @test_category load_balancing:round_robin
  #
  def test_round_robin_used_explicitly
    setup_schema
    policy  = Cassandra::LoadBalancing::Policies::RoundRobin.new
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2', '127.0.0.3', '127.0.0.4'], hosts_used.sort
    cluster.close
  end

  def test_whitelisted_round_robin
    setup_schema
    allowed_ips = ["127.0.0.1", "127.0.0.3"]
    round_robin = Cassandra::LoadBalancing::Policies::RoundRobin.new
    whitelist = Cassandra::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
    cluster = Cassandra.cluster(load_balancing_policy: whitelist, consistency: :one)
    session = cluster.connect("simplex")

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      assert_equal 1, info.hosts.size
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.3'], hosts_used.sort.uniq
    cluster.close
  end

  def test_round_robin_ignores_datacenters
    setup_schema
    policy = Cassandra::LoadBalancing::Policies::RoundRobin.new
    cluster = Cassandra.cluster(load_balancing_policy: policy, consistency: :one)
    session = cluster.connect("simplex")

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      assert_equal 1, info.hosts.size
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2', '127.0.0.3', '127.0.0.4'], hosts_used.sort
    cluster.close
  end

  def test_dc_aware_round_robin_queries_to_local_dc
    setup_schema
    datacenter = "dc1"
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    cluster = Cassandra.cluster(load_balancing_policy: policy, consistency: :one)
    session = cluster.connect("simplex")

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2'], hosts_used.sort.uniq
    cluster.close
  end

  def test_dc_aware_round_robin_queries_to_remote_dc_if_local_down
    setup_schema
    datacenter = "dc2"
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @@ccm_cluster.stop_node('node3')
    @@ccm_cluster.stop_node('node4')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2'], hosts_used.sort.uniq
    cluster.close
  end

  def test_raise_error_on_dc_aware_round_robin_unable_to_query_to_required_dc
    setup_schema
    datacenter = "dc1"
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @@ccm_cluster.stop_node('node1')
    @@ccm_cluster.stop_node('node2')

    assert_raises(Cassandra::Errors::UnavailableError) do
      Retry.with_attempts(5, Cassandra::Errors::WriteTimeoutError) { session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)") }
    end

    @@ccm_cluster.start_node('node1')
    cluster.close
  end

  def test_dc_aware_round_robin_routes_up_to_max_hosts_in_remote
    setup_schema
    datacenter = "dc2"
    remotes_to_try = 1
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, remotes_to_try)
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @@ccm_cluster.stop_node('node3')
    @@ccm_cluster.stop_node('node4')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal 1, hosts_used.uniq.size
    refute_includes(hosts_used, "127.0.0.3")
    refute_includes(hosts_used, "127.0.0.4")
    cluster.close
  end

  def test_raise_error_on_dc_aware_round_robin_unable_to_route_local_consistencies_to_remote
    setup_schema
    datacenter = "dc2"
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @@ccm_cluster.stop_node('node3')
    @@ccm_cluster.stop_node('node4')

    assert_raises(Cassandra::Errors::NoHostsAvailable) do 
      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)", :consistency => :local_one)
    end

    cluster.close
  end

  def test_dc_aware_round_robin_can_route_local_consistencies_to_remote
    setup_schema
    datacenter = "dc2"
    use_remote = true
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, nil, use_remote)
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @@ccm_cluster.stop_node('node3')
    @@ccm_cluster.stop_node('node4')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)", :consistency => :local_one).execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2'], hosts_used.sort.uniq
    cluster.close
  end
end