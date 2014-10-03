# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
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
  def setup
    @ccm_cluster = CCM.setup_cluster(2, 2)

    $stop_cluster ||= begin
      at_exit do
        @ccm_cluster.stop
      end
    end
  end

  def setup_schema
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("DROP KEYSPACE simplex") rescue nil
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")
    session.execute("CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT)")
    cluster.close
  end

  def test_round_robin_is_default_policy
    setup_schema
    cluster = Cassandra.connect
    session = cluster.connect("simplex")

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2', '127.0.0.3', '127.0.0.4'], hosts_used.sort
    cluster.close
  end

  def test_round_robin_used_explicitly
    setup_schema
    policy  = Cassandra::LoadBalancing::Policies::RoundRobin.new
    cluster = Cassandra.connect(load_balancing_policy: policy)
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
    cluster = Cassandra.connect(load_balancing_policy: whitelist)
    session = cluster.connect("simplex")

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.3'], hosts_used.sort.uniq
    cluster.close
  end

  def test_round_robin_ignores_datacenters
    setup_schema
    policy = Cassandra::LoadBalancing::Policies::RoundRobin.new
    cluster = Cassandra.connect(load_balancing_policy: policy)
    session = cluster.connect("simplex")

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)").execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2', '127.0.0.3', '127.0.0.4'], hosts_used.sort
    cluster.close
  end

  def test_dc_aware_round_robin_queries_to_local_dc
    setup_schema
    datacenter = "dc1"
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
    cluster = Cassandra.connect(load_balancing_policy: policy)
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
    cluster = Cassandra.connect(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @ccm_cluster.stop_node('node3')
    @ccm_cluster.stop_node('node4')

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
    cluster = Cassandra.connect(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @ccm_cluster.stop_node('node1')
    @ccm_cluster.stop_node('node2')

    assert_raises(Cassandra::Errors::QueryError) do
      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)")
    end

    @ccm_cluster.start_node('node1')
    cluster.close
  end

  def test_dc_aware_round_robin_routes_up_to_max_hosts_in_remote
    setup_schema
    datacenter = "dc2"
    remotes_to_try = 1
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, remotes_to_try)
    cluster = Cassandra.connect(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @ccm_cluster.stop_node('node3')
    @ccm_cluster.stop_node('node4')

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
    cluster = Cassandra.connect(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @ccm_cluster.stop_node('node3')
    @ccm_cluster.stop_node('node4')

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
    cluster = Cassandra.connect(consistency: :one, load_balancing_policy: policy)
    session = cluster.connect("simplex")

    @ccm_cluster.stop_node('node3')
    @ccm_cluster.stop_node('node4')

    hosts_used = []
    4.times do
      info =  session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)", :consistency => :local_one).execution_info
      hosts_used.push(info.hosts.last.ip.to_s)
    end

    assert_equal ['127.0.0.1', '127.0.0.2'], hosts_used.sort.uniq
    cluster.close
  end
end