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

require File.dirname(__FILE__) + '/integration_test_case.rb'

class ClientErrorTest < IntegrationTestCase
  @@currently_failing_nodes = []

  def self.before_suite
    if CCM.cassandra_version < '2.2.0'
      puts "C* > 2.2 required for client failure error tests, skipping setup."
    else
      @@ccm_cluster = CCM.setup_cluster(1, 3)
      @@ccm_cluster.change_tombstone_thresholds
    end
  end

  def set_failing_nodes(failing_nodes, keyspace)
    # Ensure all desired nodes have failures enabled
    failing_nodes.each do |node|
      unless @@currently_failing_nodes.include?(node)
        @@ccm_cluster.stop_node(node)
        @@ccm_cluster.start_node(node, "-Dcassandra.test.fail_writes_ks=#{keyspace}")
        @@currently_failing_nodes.push(node)
      end
    end

    # Disable failing for other nodes
    @@currently_failing_nodes.each do |node|
      unless failing_nodes.include?(node)
        @@ccm_cluster.stop_node(node)
        @@ccm_cluster.start_node(node)
        @@currently_failing_nodes.delete(node)
      end
    end
  end

  # Test for validating WriteError
  #
  # test_write_failures_from_coordinator tests that the proper WriteError error is thrown when a WriteFailure exception
  # is returned from Cassandra. It first creates a test keyspace and table with consistency ALL. It then enables the
  # cassandra.test.fail_writes_ks JVM option in Cassandra for one of the three nodes. This option triggers a WriteFailure
  # for that specific node. It then performs an INSERT with consistency ALL and verifies that a WriteError is thrown.
  # It then performs the same query with quorum and verifies that the query is executed without any errors. Finally, it
  # clears the set JVM option and drops the keyspace.
  #
  # @expected_errors [Cassandra::Errors::WriteError] When a WriteFailure is triggered on Cassandra.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-105
  # @expected_result A WriteError should be thrown from the first node
  #
  # @test_assumptions A Cassandra cluster with version 2.2.0 or higher.
  # @test_category error_codes
  #
  def test_raise_error_on_write_failure
    skip("Client failure errors are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    # Create a cluster object that tries to use the beta protocol. If it succeeds, the write failure response
    # will have a map of <node-ip, failure-code> instead of num-failures. When v5 is officially released, we
    # can remove the allow_beta_protocol arg in this test.
    cluster = Retry.with_attempts(5, Cassandra::Errors::NoHostsAvailable) {
      Cassandra.cluster
    }
    session = cluster.connect

    session.execute("CREATE KEYSPACE testwritefail WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}",
                    consistency: :all) rescue nil
    session.execute("CREATE TABLE IF NOT EXISTS testwritefail.test (k int PRIMARY KEY, v int)", consistency: :all)

    # Disable one node
    set_failing_nodes(["node1"], "testwritefail")

    # One node disabled should trigger a WriteFailure
    ex = assert_raises(Cassandra::Errors::WriteError) do
      session.execute("INSERT INTO testwritefail.test (k, v) VALUES (1, 0)", consistency: :all)
    end

    # If we're speaking protocol version > 4, verify that we have a failure map in the exception.
    connection_options = cluster.instance_variable_get(:@connection_options)
    assert_operator(ex.failed, :>=, 1)
    if connection_options.protocol_version > 4
      assert_equal(ex.failed, ex.failures_by_node.size)
      assert_equal(0, ex.failures_by_node.values.first)
    end

    # Quorum should still work with two nodes
    session.execute("INSERT INTO testwritefail.test (k, v) VALUES (1, 0)", consistency: :quorum)

    # Restart the node to clear jvm settings
    set_failing_nodes([], "testwritefail")
  ensure
    cluster && cluster.close
  end

  # Test for validating ReadError
  #
  # test_tombstone_overflow_read_failure tests that the proper ReadError error is thrown when a ReadFailure exception
  # is returned from Cassandra. It depends on tombstone_failure_threshold and tombstone_warn_threshold being changed in
  # Cassandra to trigger the failure. It first creates a test keyspace and table with consistency ALL. It then inserts
  # many wide rows into Cassandra, and then immediately deletes all these rows, creating many tombstones. Finally, it
  # performs a simple read query to trigger the ReadFailure on Cassandra, and subsequently a ReadError from the driver.
  #
  # @expected_errors [Cassandra::Errors::ReadError] When a ReadFailure is triggered on Cassandra.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-105
  # @expected_result A ReadError should be thrown
  #
  # @test_assumptions A Cassandra cluster with version 2.2.0 or higher, with tombstone threshold settings lowered.
  # @test_category error_codes
  #
  def test_raise_error_on_read_failure
    skip("Client failure errors are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    # Create a cluster object that tries to use the beta protocol. If it succeeds, the write failure response
    # will have a map of <node-ip, failure-code> instead of num-failures. When v5 is officially released, we
    # can remove the allow_beta_protocol arg in this test.
    cluster = Retry.with_attempts(5, Cassandra::Errors::NoHostsAvailable) {
      Cassandra.cluster
    }
    session = cluster.connect

    session.execute("CREATE KEYSPACE testreadfail WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}",
                    consistency: :all) rescue nil
    session.execute("CREATE TABLE IF NOT EXISTS testreadfail.test2 (k int, v0 int, v1 int, PRIMARY KEY (k, v0))",
                    consistency: :all)

    # Insert wide rows
    insert = session.prepare("INSERT INTO testreadfail.test2 (k, v0, v1) VALUES (1, ?, 1)")
    (0..3000).each do |num|
      session.execute(insert, arguments: [num])
    end

    # Delete wide rows
    delete = session.prepare("DELETE v1 FROM testreadfail.test2 WHERE k = 1 AND v0 =?")
    (0..2001).each do |num|
      session.execute(delete, arguments: [num])
    end

    # Tombstones should trigger ReadFailure
    ex = assert_raises(Cassandra::Errors::ReadError) do
      session.execute("SELECT * FROM testreadfail.test2 WHERE k = 1")
    end

    # If we're speaking protocol version > 4, verify that we have a failure map in the exception.
    connection_options = cluster.instance_variable_get(:@connection_options)
    assert_operator(ex.failed, :>=, 1)
    if connection_options.protocol_version > 4
      assert_equal(ex.failed, ex.failures_by_node.size)
      assert_equal(1, ex.failures_by_node.values.first)
    end
  ensure
    cluster && cluster.close
  end

  # Test for validating FunctionCallError
  #
  # test_raise_error_on_function_failure tests that the proper FunctionCallError error is thrown when a FunctionFailure
  # exception is returned from Cassandra. It first creates a test keyspace and table with consistency ALL. It also creates
  # an UDF that throws an exception. It then inserts a simple value to be called by the UDF. Finally, it calls the UDF and
  # triggers a FunctionFailure on Cassandra, and subsequently a FunctionCallError from the driver.
  #
  # @expected_errors [Cassandra::Errors::FunctionCallError] When a FunctionFailure is triggered on Cassandra.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-105
  # @expected_result A FunctionCallError should be thrown
  #
  # @test_assumptions A Cassandra cluster with version 2.2.0 or higher.
  # @test_category error_codes
  #
  def test_raise_error_on_function_failure
    skip("Client failure errors are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Retry.with_attempts(5, Cassandra::Errors::NoHostsAvailable) { Cassandra.cluster }
    session = cluster.connect

    session.execute("CREATE KEYSPACE testfunctionfail WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}",
                    consistency: :all) rescue nil
    session.execute("CREATE TABLE IF NOT EXISTS testfunctionfail.d (k int PRIMARY KEY , d double)", consistency: :all)

    # Create a UDF that throws an exception
    session.execute("CREATE FUNCTION IF NOT EXISTS testfunctionfail.test_failure(d double)
                      RETURNS NULL ON NULL INPUT
                      RETURNS double
                      LANGUAGE java AS 'throw new RuntimeException(\"failure\");'",
                    consistency: :all)

    # Insert value to use for function
    session.execute("INSERT INTO testfunctionfail.d (k, d) VALUES (0, 5.12)")

    # FunctionFailure should be triggered
    assert_raises(Cassandra::Errors::FunctionCallError) do
      session.execute("SELECT test_failure(d) FROM testfunctionfail.d WHERE k = 0")
    end
  ensure
    cluster && cluster.close
  end
end
