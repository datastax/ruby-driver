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

class IdempotencyTest < IntegrationTestCase
  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(2, 1)
  end

  def setup
    @@ccm_cluster.setup_schema(<<-CQL)
    CREATE KEYSPACE simplex WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '1', 'dc2': '1'};
    USE simplex;
    CREATE TABLE test (k int, v int, PRIMARY KEY (k, v));
    INSERT INTO test (k, v) VALUES (0, 0)
    CQL
  end

  # Test for retrying idempotent statements on timeout
  #
  # test_statement_idempotency_on_timeout tests that idempotent statements are retried automatically on the next host.
  # It first blocks the first host such that it is unreachable. It then attempts a simple SELECT query and verifies that
  # a Cassandra::Errors::TimeoutError is raised, and the next host is not tried. It finally executes the same query
  # once more with idempotent: true and verifies that the statement executes successfully on another host.
  #
  # @expected_errors [Cassandra::Errors::TimeoutError] When a host is unavailable on a non-idempotent query
  #
  # @since 3.0.0
  # @jira_ticket RUBY-146
  # @expected_result Idempotent queries should be retried on the next host automatically
  #
  # @test_assumptions A 2-dc Cassandra cluster 1 node in each dc.
  # @test_category queries:timeout
  #
  def test_statement_idempotency_on_timeout
    datacenter = 'dc1'
    max_remote_hosts_to_use = 1
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect('simplex')

    @@ccm_cluster.block_node('node1')

    assert_raises(Cassandra::Errors::TimeoutError) do
      session.execute('SELECT * FROM test', consistency: :one)
    end

    info = nil
    begin
      info = session.execute('SELECT * FROM test', consistency: :one, idempotent: true).execution_info
      assert_equal 1, info.retries
      assert_equal 2, info.hosts.size
      assert_equal '127.0.0.1', info.hosts[0].ip.to_s
      assert_equal '127.0.0.2', info.hosts[1].ip.to_s
    rescue Cassandra::Errors::ReadTimeoutError => e
      # Every once in a while, the test fails with a ReadTimeoutError. Try to report extra info in that case that
      # may help us track down the core issue.
      info = e.execution_info
      assert_equal('', info.inspect, 'Got ReadTimeoutError when statement should have succeeded')
    end
  ensure
    @@ccm_cluster.unblock_nodes
    cluster && cluster.close
  end

  # Test for retrying idempotent statements on timeout
  #
  # test_statement_idempotency_on_timeout tests that idempotent statements are retried automatically on the next host,
  # when the keyspace is not predefined. It first blocks the first host such that it is unreachable. It then attempts a
  # simple SELECT query and verifies that a Cassandra::Errors::TimeoutError is raised, and the next host is not tried.
  # It finally executes the same query once more with idempotent: true and verifies that the statement executes
  # successfully on another host.
  #
  # @expected_errors [Cassandra::Errors::TimeoutError] When a host is unavailable on a non-idempotent query
  #
  # @since 3.0.1
  # @jira_ticket RUBY-233
  # @expected_result Idempotent queries should be retried on the next host automatically
  #
  # @test_assumptions A 2-dc Cassandra cluster 1 node in each dc.
  # @test_category queries:timeout
  #
  def test_statement_idempotency_on_timeout_no_keyspace_predefined
    datacenter = 'dc1'
    max_remote_hosts_to_use = 1
    policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use)
    cluster = Cassandra.cluster(load_balancing_policy: policy)
    session = cluster.connect

    @@ccm_cluster.block_node('node1')

    assert_raises(Cassandra::Errors::TimeoutError) do
      session.execute('SELECT * FROM simplex.test', consistency: :one)
    end

    info = nil
    begin
      info = session.execute('SELECT * FROM simplex.test', consistency: :one, idempotent: true).execution_info
      assert_equal 1, info.retries
      assert_equal 2, info.hosts.size
      assert_equal '127.0.0.1', info.hosts[0].ip.to_s
      assert_equal '127.0.0.2', info.hosts[1].ip.to_s
    rescue Cassandra::Errors::ReadTimeoutError => e
      # Every once in a while, the test fails with a ReadTimeoutError. Try to report extra info in that case that
      # may help us track down the core issue.
      info = e.execution_info
      assert_equal('', info.inspect, 'Got ReadTimeoutError when statement should have succeeded')
    end
  ensure
    @@ccm_cluster.unblock_nodes
    cluster && cluster.close
  end
end
