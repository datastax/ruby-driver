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
    @@ccm_cluster = CCM.setup_cluster(1, 2)
  end

  def setup
    @@ccm_cluster.setup_schema(<<-CQL)
    CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
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
  # once more with idempotent: true and verifies that the statement executes successfully.
  #
  # @expected_errors [Cassandra::Errors::TimeoutError] When a host is unavailable on a non-idempotent query
  #
  # @since 3.0.0
  # @jira_ticket RUBY-146
  # @expected_result Idempotent queries should be retried on the next host automatically
  #
  # @test_assumptions A 2-node Cassandra cluster with RF=2.
  # @test_category queries:timeout
  #
  def test_statement_idempotency_on_timeout
    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    @@ccm_cluster.block_node("node1")

    assert_raises(Cassandra::Errors::TimeoutError) do
      session.execute("SELECT * FROM test", consistency: :one)
    end

    session.execute("SELECT * FROM test", consistency: :one, idempotent: true)
  ensure
    @@ccm_cluster.unblock_nodes
    cluster && cluster.close
  end

end
