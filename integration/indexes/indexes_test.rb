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

require File.dirname(__FILE__) + '/../integration_test_case.rb'

class IndexesTest < IntegrationTestCase

  def setup
    @@ccm_cluster.setup_schema("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

    @cluster = Cassandra.cluster(
        schema_refresh_delay: 0.1,
        schema_refresh_timeout: 0.1
    )
    @listener = SchemaChangeListener.new(@cluster)
    @session = @cluster.connect('simplex')
  end

  def teardown
    @cluster && @cluster.close
  end

  # Test for creating indexes
  #
  # test_can_create_index tests that indexes can be created using the driver. It first creates a simple table and a
  # simplex index on that table. It then verifies that the index metadata is being properly retrieved.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-178
  # @expected_result index should be created and its metadata should be retrieved.
  #
  # @test_category indexes
  #
  def test_can_create_index
    @session.execute("CREATE TABLE simplex.test (a text PRIMARY KEY, b text)")
    @session.execute("CREATE INDEX b_index ON simplex.test (b)")

    @listener.wait_for_index('simplex', 'test', 'b_index')

    assert @cluster.keyspace('simplex').table('test').has_index?('b_index')
    index = @cluster.keyspace('simplex').table('test').index('b_index')
    assert_equal 'b_index', index.name
    assert_equal 'test', index.table.name
    assert_equal :composites, index.kind
    assert_equal 'b', index.target
  end

  # Test for creating indexes on partial collections
  #
  # test_can_create_index_on_partial_collections tests that indexes can be created on partial (non-frozen) collections.
  # The indexes can be created on either the keys or the values, but not both at the same time. It first creates a table
  # and an index on the keys of the collection, verifying the metadata once created. It then drops the original index and
  # creates another index, but this time on the values of the collection.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-178
  # @expected_result partial collection indexes should be created and their metadata should be retrieved.
  #
  # @test_category indexes
  #
  def test_can_create_index_on_partial_collections
    skip("Secondary index on partial collections were introduced in Cassandra 2.1") if CCM.cassandra_version < '2.1.0'

    @session.execute("CREATE TABLE simplex.collection_test (a int PRIMARY KEY, b map<text, text>)")
    @session.execute("CREATE INDEX b_index ON simplex.collection_test (keys(b))")

    @listener.wait_for_index('simplex', 'collection_test', 'b_index')

    assert @cluster.keyspace('simplex').table('collection_test').has_index?('b_index')
    index = @cluster.keyspace('simplex').table('collection_test').index('b_index')
    assert_equal 'b_index', index.name
    assert_equal 'collection_test', index.table.name
    assert_equal :composites, index.kind
    assert_equal 'keys(b)', index.target

    @session.execute("DROP INDEX b_index")
    @session.execute("CREATE INDEX b_index ON simplex.collection_test (b)")

    @listener.wait_for_index('simplex', 'collection_test', 'b_index')

    assert @cluster.keyspace('simplex').table('collection_test').has_index?('b_index')
    index = @cluster.keyspace('simplex').table('collection_test').index('b_index')
    assert_equal 'b_index', index.name
    assert_equal 'collection_test', index.table.name
    assert_equal :composites, index.kind
    if CCM.cassandra_version < '3.0.0'
      assert_equal 'b', index.target
    else
      assert_equal 'values(b)', index.target
    end
  end

  # Test for creating indexes on full collections
  #
  # test_can_create_index_on_full_collections tests that indexes can be created on full (frozen) collections. It first
  # creates a table which includes a frozen collection. It then creates a full index on the collection, verifying that
  # the metadata is properly retrieved.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-178
  # @expected_result full collection indexes should be created and their metadata should be retrieved.
  #
  # @test_category indexes
  #
  def test_can_create_index_on_full_collections
    skip("Secondary index on full collections were introduced in Cassandra 2.1.3") if CCM.cassandra_version.to_i < '2.1.3'.to_i

    @session.execute("CREATE TABLE simplex.collection_test (a int PRIMARY KEY, b frozen<map<text, text>>)")
    @session.execute("CREATE INDEX b_index ON simplex.collection_test (full(b))")

    @listener.wait_for_index('simplex', 'collection_test', 'b_index')

    assert @cluster.keyspace('simplex').table('collection_test').has_index?('b_index')
    index = @cluster.keyspace('simplex').table('collection_test').index('b_index')
    assert_equal 'b_index', index.name
    assert_equal 'collection_test', index.table.name
    assert_equal :composites, index.kind
    if CCM.cassandra_version < '3.0.0'
      assert_equal 'b', index.target
    else
      assert_equal 'full(b)', index.target
    end
  end

  # Test for creating multiple indexes on the same column
  #
  # test_can_create_multiple_indexes_same_column tests that multiple indexes can be created on the same column. It first
  # creates a table which includes a non-frozen collection. It then creates two indexes: one for the key and another for
  # the value of the collection. It then verifies the metadata associated with each index.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-178
  # @expected_result multiple indexes should be created and their metadata should be retrieved.
  #
  # @test_category indexes
  #
  def test_can_create_multiple_indexes_same_column
    skip("Multiple indexes on same column were introduced in Cassandra 3.0.0") if CCM.cassandra_version < '3.0.0'

    @session.execute("CREATE TABLE simplex.multi_index_test (a int PRIMARY KEY, b map<text, text>)")
    @session.execute("CREATE INDEX key_index ON simplex.multi_index_test (keys(b))")
    @session.execute("CREATE INDEX value_index ON simplex.multi_index_test (values(b))")

    @listener.wait_for_index('simplex', 'multi_index_test', 'key_index')
    @listener.wait_for_index('simplex', 'multi_index_test', 'value_index')

    assert @cluster.keyspace('simplex').table('multi_index_test').has_index?('key_index')
    assert @cluster.keyspace('simplex').table('multi_index_test').has_index?('value_index')

    key_index = @cluster.keyspace('simplex').table('multi_index_test').index('key_index')
    assert_equal 'key_index', key_index.name
    assert_equal 'multi_index_test', key_index.table.name
    assert_equal :composites, key_index.kind
    assert_equal 'keys(b)', key_index.target

    value_index = @cluster.keyspace('simplex').table('multi_index_test').index('value_index')
    assert_equal 'value_index', value_index.name
    assert_equal 'multi_index_test', value_index.table.name
    assert_equal :composites, value_index.kind
    assert_equal 'values(b)', value_index.target
  end

end
