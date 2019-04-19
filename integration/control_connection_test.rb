# encoding: utf-8

#--
# Copyright DataStax, Inc.
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

require File.dirname(__FILE__) + '/integration_test_case.rb'

class ControlConnectionTest < IntegrationTestCase

  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(1, 2)
  end

  def self.after_suite
    CCM.remove_cluster(@@ccm_cluster.name)
  end

  def remove_peer_info(info)
    # Make sure to only connect to node1, as each node has its own peers table
    allowed_ips = ['127.0.0.1']
    round_robin = Cassandra::LoadBalancing::Policies::RoundRobin.new
    whitelist = Cassandra::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
    cluster = Cassandra.cluster(load_balancing_policy: whitelist)
    session = cluster.connect

    value = session.execute("SELECT #{info} FROM system.peers WHERE peer = '127.0.0.2'").first[info]
    session.execute("DELETE #{info} FROM system.peers WHERE peer = '127.0.0.2'")
    result = session.execute("SELECT #{info} FROM system.peers WHERE peer = '127.0.0.2'").first
    assert_nil result[info]

    cluster.close
    value
  end

  def restore_peer_info(info, value)
    # Make sure to only connect to node1, as each node has its own peers table
    allowed_ips = ['127.0.0.1']
    round_robin = Cassandra::LoadBalancing::Policies::RoundRobin.new
    whitelist = Cassandra::LoadBalancing::Policies::WhiteList.new(allowed_ips, round_robin)
    cluster = Cassandra.cluster(load_balancing_policy: whitelist)
    session = cluster.connect

    session.execute("UPDATE system.peers SET #{info}=? WHERE peer = '127.0.0.2'", arguments: [value])
    result = session.execute("SELECT #{info} FROM system.peers WHERE peer = '127.0.0.2'").first
    assert_equal value, result[info]

    cluster.close
  end

  # Test for null columns in peer
  #
  # test_missing_peer_columns tests that the control connection ignores any peers which have missing peer columns.
  # Using a simple 2-node cluster, it first removes one of the peer columns of node2 from node1's system.peers
  # table. It then uses node1 explicitly as the control connection and verifies that node2 has not been used as a host.
  # It finally restores the peer column in node1 so the next test case can continue.g
  #
  # @since 2.1.7
  # @jira_ticket RUBY-255
  # @expected_result Node2 should not be used as a host
  #
  # @test_assumptions A 2-node Cassandra cluster.
  # @test_category control_connection
  #
  def test_missing_peer_columns
    peer_info = ['host_id', 'data_center', 'rack', 'rpc_address', 'tokens']

    peer_info.each do |info|
      begin
        original_value = remove_peer_info(info)
        cluster = Cassandra.cluster(hosts: ['127.0.0.1'])
        assert_equal ['127.0.0.1'], cluster.hosts.map { |h| h.ip.to_s }
        cluster.close
      ensure
        restore_peer_info(info, original_value)
      end
    end
  end

  # Test for repreparing statements on another host
  #
  # test_can_reprepare_statements_automatically tests that prepared statements are automatically reprepared on a host
  # if that host does not already have the prepared statement in its cache. It first creates a simple keyspace
  # and table to be used. It then prepares an insert statement on node2, by keeping node1 down. It then brings node1
  # back up but brings node2 down. Finally it executes the prepared statement on node1, and verifies that the query
  # is executed successfully using node1.
  #
  # @since 3.1.0
  # @jira_ticket RUBY-257
  # @expected_result Node1 should be able to be used to execute the prepared statement
  #
  # @test_assumptions A 2-node Cassandra cluster.
  # @test_category prepared_statements:preparation
  #
  def test_can_reprepare_statements_automatically
    cluster = Cassandra.cluster
    session = cluster.connect

    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    session.execute("USE simplex")
    session.execute("CREATE TABLE test (k int, v int, PRIMARY KEY (k, v))")

    # Prepare on node2
    @@ccm_cluster.stop_node('node1')
    insert = session.prepare("INSERT INTO test (k,v) VALUES (?, ?)")
    assert_equal 1, insert.execution_info.hosts.size
    assert_equal '127.0.0.2', insert.execution_info.hosts.first.ip.to_s
    @@ccm_cluster.start_node('node1')

    # Insert using node1
    @@ccm_cluster.stop_node('node2')
    info = session.execute(insert, arguments: [0,0]).execution_info
    assert_equal 1, info.hosts.size
    assert_equal '127.0.0.1', info.hosts.first.ip.to_s
  ensure
    cluster.close
  end
end
