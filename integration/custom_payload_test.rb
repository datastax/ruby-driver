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

class CustomPayloadTest < IntegrationTestCase
  def self.before_suite
    unless CCM.cassandra_version < '2.2.0'
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
      CREATE TABLE test (k int, v int, PRIMARY KEY (k, v));
      INSERT INTO test (k, v) VALUES (0, 0)
      CQL
    end
  end

  def validate_custom_payloads(query)
    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Simple key-value
      custom_payload = {'test' => 'test_return'}
      mirrored_payload = session.execute(query, payload: custom_payload).execution_info.payload
      assert_equal custom_payload, mirrored_payload

      # No key-value
      custom_payload = {'' => ''}
      mirrored_payload = session.execute(query, payload: custom_payload).execution_info.payload
      assert_equal custom_payload, mirrored_payload

      # Space key-value
      custom_payload = {' ' => ' '}
      mirrored_payload = session.execute(query, payload: custom_payload).execution_info.payload
      assert_equal custom_payload, mirrored_payload

      # Long key-value pair
      value = "X" * 10
      custom_payload = {value => value}
      mirrored_payload = session.execute(query, payload: custom_payload).execution_info.payload
      assert_equal custom_payload, mirrored_payload

      # Largest supported key-value pairs supported (65,535)
      custom_payload = {}
      (0..65534).each do |i|
        custom_payload[i.to_s] = i.to_s
      end
      mirrored_payload = session.execute(query, payload: custom_payload).execution_info.payload
      assert_equal custom_payload, mirrored_payload

      # Largest supported + 1 pair
      custom_payload["65535"] = "65535"
      assert_raises(ArgumentError) do
        session.execute(query, payload: custom_payload)
      end
    ensure
      cluster && cluster.close
    end
  end

  # Test for custom payloads in basic queries
  #
  # test_custom_payload_basic_query tests that custom payloads work with basic queries.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-109
  # @expected_result Custom payloads should work with basic queries.
  #
  # @test_assumptions A Cassandra cluster >= 2.2 with CustomPayloadMirroringQueryHandler enabled.
  # @test_category queries:custom_payload
  #
  def test_custom_payload_basic_query
    skip("Custom payloads are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    query = "SELECT * FROM test"
    validate_custom_payloads(query)
  end

  # Test for custom payloads with prepared statements
  #
  # test_custom_payload_basic_query tests that custom payloads work with prepared statements.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-109
  # @expected_result Custom payloads should work with prepared statements.
  #
  # @test_assumptions A Cassandra cluster >= 2.2 with CustomPayloadMirroringQueryHandler enabled.
  # @test_category queries:custom_payload
  #
  def test_custom_payload_prepared_statement
    skip("Custom payloads are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      select = session.prepare("SELECT * FROM test WHERE k=?")
      validate_custom_payloads(select.bind([0]))
    ensure
      cluster && cluster.close
    end
  end

  # Test for custom payloads with batch statements
  #
  # test_custom_payload_basic_query tests that custom payloads work with batch statements.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-109
  # @expected_result Custom payloads should work with batch statements.
  #
  # @test_assumptions A Cassandra cluster >= 2.2 with CustomPayloadMirroringQueryHandler enabled.
  # @test_category queries:custom_payload
  #
  def test_custom_payload_batch_statement
    skip("Custom payloads are only available in C* after 2.2") if CCM.cassandra_version < '2.2.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      insert = session.prepare("INSERT INTO test (k, v) VALUES (?, ?)")

      batch = session.batch do |b|
        b.add("INSERT INTO test (k, v) VALUES (1, 1)")
        b.add(insert, arguments: [2, 2])
      end

      validate_custom_payloads(batch)
    ensure
      cluster && cluster.close
    end
  end
end
