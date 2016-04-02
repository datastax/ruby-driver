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

class MetadataTest < IntegrationTestCase

  def setup
    @@ccm_cluster.setup_schema("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

    @cluster = Cassandra.cluster(
        schema_refresh_delay: 0.1,
        schema_refresh_timeout: 0.1
    )
    @listener = SchemaChangeListener.new(@cluster)
    @session = @cluster.connect('simplex')
    @session.execute("CREATE TABLE simplex.users (user_id bigint, first text, last text, age int, PRIMARY KEY (user_id, last))")
    @listener.wait_for_table('simplex', 'users')
  end

  def teardown
    @cluster && @cluster.close
  end

  # Test for retrieving table metadata
  #
  # test_can_retrieve_table_metadata tests that all pieces of table metadata can be retrieved. It goes through each piece
  # of table metadata and verifies that each piece is as expected.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-181
  # @expected_result table metadata should be retrieved.
  #
  # @test_category metadata
  #
  def test_can_retrieve_table_metadata
    assert @cluster.keyspace('simplex').has_table?('users')
    table_meta = @cluster.keyspace('simplex').table('users')
    assert_equal 'users', table_meta.name
    assert_equal 'simplex', table_meta.keyspace.name
    assert_empty table_meta.indexes
    refute_nil table_meta.id unless CCM.cassandra_version < '3.0.0'
    refute_nil table_meta.options

    assert_equal 2, table_meta.primary_key.size
    assert_equal 'user_id', table_meta.primary_key[0].name
    assert_equal :bigint, table_meta.primary_key[0].type.kind
    assert_equal 'last', table_meta.primary_key[1].name
    assert_equal :text, table_meta.primary_key[1].type.kind

    assert_equal 1, table_meta.partition_key.size
    assert_equal 'user_id', table_meta.partition_key[0].name
    assert_equal :bigint, table_meta.partition_key[0].type.kind
    assert_equal 1, table_meta.clustering_columns.size
    assert_equal 'last', table_meta.clustering_columns[0].name
    assert_equal :text, table_meta.clustering_columns[0].type.kind
    assert_equal :asc, table_meta.clustering_order.first

    assert_equal 4, table_meta.columns.size
    table_meta.each_column do |column|
      assert ['user_id', 'first', 'last', 'age'].any? { |name| name == column.name }
      assert [:bigint, :text, :int].any? { |type| type == column.type.kind }
    end
  end

  # Test for column ordering in table metadata
  #
  # test_column_ordering_is_deterministic tests that the metadata relating to the columns are retrieved in the proper
  # order. This proper order is: partition key, clustering columns, and then all other columns alphanumerically.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-180
  # @expected_result column ordering should be correct in table metadata.
  #
  # @test_category metadata
  #
  def test_column_ordering_is_deterministic
    assert @cluster.keyspace('simplex').has_table?('users')
    table_meta = @cluster.keyspace('simplex').table('users')
    table_cql = Regexp.new(/CREATE TABLE simplex\.users \(
  user_id bigint,
  last text,
  age int,
  first text,
  PRIMARY KEY \(user_id, last\)
\)/)

    assert_equal 0, table_meta.to_cql =~ table_cql

    col_names = ['user_id', 'last', 'age', 'first']
    table_meta.each_column do |column|
      assert_equal col_names[0], column.name
      col_names.delete_at(0)
    end
  end

  # Test for retrieving crc_check_balance property
  #
  # test_table_metadata_contains_crc_check_balance tests that the 'crc_check_balance' property of table metadata is able
  # to be retrieved.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-179
  # @expected_result crc_check_balance property should be retrieved from the table metadata
  #
  # @test_category metadata
  #
  def test_table_metadata_contains_crc_check_balance
    skip("The crc_check_balance property on a table was introduced in Cassandra 3.0") if CCM.cassandra_version < '3.0.0'

    assert @cluster.keyspace('simplex').has_table?('users')
    table_meta = @cluster.keyspace('simplex').table('users')
    assert_equal 1.0, table_meta.options.crc_check_chance
  end

  # Test for retrieving extensions property
  #
  # test_table_metadata_contains_extensions tests that the 'extensions' property of table metadata is able to be
  # retrieved.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-170
  # @expected_result extensions property should be retrieved from the table metadata
  #
  # @test_category metadata
  #
  def test_table_metadata_contains_extensions
    skip("The extensions property on a table was introduced in Cassandra 3.0") if CCM.cassandra_version < '3.0.0'

    assert @cluster.keyspace('simplex').has_table?('users')
    table_meta = @cluster.keyspace('simplex').table('users')
    assert_empty table_meta.options.extensions
  end

end
