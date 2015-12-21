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

  def test_token_aware_routes_to_primary_replica
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

  def test_token_aware_routes_to_next_replica_if_primary_down
    setup_schema
    base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new('dc1')
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
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
      assert ["127.0.0.3", "127.0.0.4"].include?(result.execution_info.hosts.last.ip.to_s)
    end

    cluster.close
  end

  def test_token_aware_routes_to_next_whitelisted_replica_if_primary_down
    setup_schema
    allowed_ips = ["127.0.0.1", "127.0.0.3"]
    round_robin = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new('dc1')
    whitelist = Cassandra::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(whitelist)
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    Retry.with_attempts(5, Cassandra::Errors::InvalidError) do
      select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }

      result = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal "127.0.0.1", result.execution_info.hosts.last.ip.to_s
    end

    cluster.close
  end

  def test_token_aware_routes_to_primary_replica_in_primary_dc
    setup_schema
    datacenter = "dc2"
    base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect("simplex")
    
    Retry.with_attempts(5, Cassandra::Errors::InvalidError) do
      select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }

      result = Retry.with_attempts(5) { session.execute(select, arguments: [0]) }
      assert_equal 2945182322382062539, result.first.values.first
      assert_equal "127.0.0.3", result.execution_info.hosts.last.ip.to_s

      result = Retry.with_attempts(5) { session.execute(select, arguments: [1]) }
      assert_equal 6292367497774912474, result.first.values.first
      assert_equal "127.0.0.3", result.execution_info.hosts.last.ip.to_s

      result = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal -8218881827949364593, result.first.values.first
      assert_equal "127.0.0.4", result.execution_info.hosts.last.ip.to_s

      result = Retry.with_attempts(5) { session.execute(select, arguments: [3]) }
      assert_equal -8048510690352527683, result.first.values.first
      assert_equal "127.0.0.4", result.execution_info.hosts.last.ip.to_s
    end

    cluster.close
  end

  def test_token_aware_routes_to_secondary_replica_if_primary_dc_down
    setup_schema
    datacenter = "dc2"
    base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
    cluster = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    Retry.with_attempts(5, Cassandra::Errors::InvalidError) do
      select = Retry.with_attempts(5) { session.prepare("SELECT token(user_id) FROM simplex.users WHERE user_id = ?") }

      result = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal "127.0.0.4", result.execution_info.hosts.last.ip.to_s

      @@ccm_cluster.stop_node("node4")

      result = Retry.with_attempts(5) { session.execute(select, arguments: [2]) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal "127.0.0.3", result.execution_info.hosts.last.ip.to_s

      @@ccm_cluster.stop_node("node3")

      result = Retry.with_attempts(5) { session.execute(select, arguments: [2], consistency: :one) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal "127.0.0.2", result.execution_info.hosts.last.ip.to_s

      result = Retry.with_attempts(5) { session.execute(select, arguments: [2], consistency: :one) }
      assert_equal 1, result.execution_info.hosts.size
      assert_equal "127.0.0.1", result.execution_info.hosts.last.ip.to_s
    end

    cluster.close
  end
end