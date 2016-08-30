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
end
