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

class ClientWarningsTest < IntegrationTestCase
  def self.before_suite
    if CCM.cassandra_version < '2.2.0'
      puts "C* > 2.2 required for client warnings tests, skipping setup."
    else
      super
      @@ccm_cluster.stop
      @@ccm_cluster.start("-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler")
    end
  end

  def setup
    unless CCM.cassandra_version < '2.2.0'
      @@ccm_cluster.setup_schema(<<-CQL)
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
      USE simplex;
      CREATE TABLE test (k int, v text, PRIMARY KEY (k, v));
      CQL

      @query = "BEGIN UNLOGGED BATCH
                INSERT INTO test (k, v) VALUES (0, '#{'a' * 5 * 1025}')
                INSERT INTO test (k, v) VALUES (1, '#{'a' * 5 * 1025}')
                APPLY BATCH"
      @exceeding_warning = Regexp.new(/Batch.* for .* is of size .*, exceeding specified threshold of 5120/)
      @partition_warning = Regexp.new(/Unlogged batch covering 2 partitions detected against table .* You should use a \
logged batch for atomicity, or asynchronous writes for performance/)
    end
  end

  # Test for empty client warnings
  #
  # test_warnings_nil tests that client warnings are empty on queries that don't return any.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-133
  # @expected_result Client warnings should be empty
  #
  # @test_assumptions A Cassandra cluster >= 2.2
  # @test_category queries:client_warning
  #
  def test_warnings_nil
    skip("Client warnings are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster
    session = cluster.connect

    execution_info = session.execute("SELECT * FROM system.local").execution_info
    assert_nil execution_info.warnings
  end

  # Test for batch exceeding client warnings
  #
  # test_batch_exceeding_length tests that client warnings related to batch exceeding length are presented by the driver.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-133
  # @expected_result Client warnings should not empty
  #
  # @test_assumptions A Cassandra cluster >= 2.2
  # @test_category queries:client_warning
  #
  def test_batch_exceeding_length
    skip("Client warnings are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    execution_info = session.execute(@query).execution_info

    assert_equal 1, execution_info.warnings.size
    assert_match @exceeding_warning, execution_info.warnings.first
  end

  # Test for batch exceeding client warnings and trace
  #
  # test_batch_exceeding_length_with_trace tests that client warnings related to batch exceeding length are presented
  # by the driver, even with tracing enabled.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-133
  # @expected_result Client warnings should not empty, with trace enabled
  #
  # @test_assumptions A Cassandra cluster >= 2.2
  # @test_category queries:client_warning
  #
  def test_batch_exceeding_length_with_trace
    skip("Client warnings are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    execution_info = session.execute(@query, trace: true).execution_info

    assert_equal 1, execution_info.warnings.size
    refute_nil execution_info.trace
    assert_match @exceeding_warning, execution_info.warnings.first
  end

  # Test for batch exceeding client warnings and custom payload
  #
  # test_batch_exceeding_length_with_custom_payload tests that client warnings related to batch exceeding length are
  # presented by the driver, even with a custom payload.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-133
  # @expected_result Client warnings should not empty, with a custom payload
  #
  # @test_assumptions A Cassandra cluster >= 2.2 with CustomPayloadMirroringQueryHandler enabled.
  # @test_category queries:client_warning
  #
  def test_batch_exceeding_length_with_custom_payload
    skip("Client warnings are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    custom_payload = {'test' => 'test_return'}
    execution_info = session.execute(@query, payload: custom_payload).execution_info

    assert_equal 1, execution_info.warnings.size
    refute_nil execution_info.payload
    assert_match @exceeding_warning, execution_info.warnings.first
    assert_equal custom_payload, execution_info.payload
  end

  # Test for batch exceeding client warnings, trace, and custom payload
  #
  # test_batch_exceeding_length_with_trace_and_custom_payload tests that client warnings related to batch exceeding
  # length are presented by the driver, even with tracing enabled and a custom payload.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-133
  # @expected_result Client warnings should not empty, with tracing and a custom payload
  #
  # @test_assumptions A Cassandra cluster >= 2.2 with CustomPayloadMirroringQueryHandler enabled.
  # @test_category queries:client_warning
  #
  def test_batch_exceeding_length_with_trace_and_custom_payload
    skip("Client warnings are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    custom_payload = {'test' => 'test_return'}
    execution_info = session.execute(@query, trace: true, payload: custom_payload).execution_info

    assert_equal 1, execution_info.warnings.size
    refute_nil execution_info.trace
    refute_nil execution_info.payload
    assert_match @exceeding_warning, execution_info.warnings.first
    assert_equal custom_payload, execution_info.payload
  end
end