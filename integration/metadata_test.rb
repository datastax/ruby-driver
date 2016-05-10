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
require File.dirname(__FILE__) + '/datatype_utils.rb'

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
    @session.execute("CREATE TABLE simplex.blobby (key blob PRIMARY KEY, f1 blob, f2 blob) WITH COMPACT STORAGE")
    @listener.wait_for_table('simplex', 'blobby')
    @session.execute("CREATE TABLE simplex.dense (f1 int, f2 int, f3 int, PRIMARY KEY (f1, f2)) WITH COMPACT STORAGE")
    @listener.wait_for_table('simplex', 'dense')
    @session.execute("CREATE TABLE simplex.custom (f1 int PRIMARY KEY," \
    " f2 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)')")
    @listener.wait_for_table('simplex', 'custom')
  end

  def teardown
    @cluster && @cluster.close
  end

  # Test for retrieving keyspace metadata
  #
  # test_can_retrieve_keyspace_metadata tests that all pieces of keyspace metadata can be retrieved. It goes through
  # each piece of keyspace metadata and verifies that each piece is as expected.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-181
  # @expected_result keyspace metadata should be retrieved.
  #
  # @test_category metadata
  #
  def test_can_retrieve_keyspace_metadata
    ks_meta = @cluster.keyspace('simplex')
    assert_equal 'simplex', ks_meta.name
    assert_equal 'SimpleStrategy', ks_meta.replication.klass
    assert_equal 1, ks_meta.replication.options['replication_factor'].to_i
    assert ks_meta.durable_writes?
    assert ks_meta.has_table?('users')
    assert_equal 4, ks_meta.tables.size

    ks_cql = Regexp.new(/CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', \
'replication_factor': '1'} AND durable_writes = true;/)

    assert_match ks_cql, ks_meta.to_cql
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

    assert_columns([['user_id', :bigint], ['last', :text]], table_meta.primary_key)
    assert_columns([['user_id', :bigint]], table_meta.partition_key)
    assert_columns([ ['last', :text]], table_meta.clustering_columns)
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
    table_cql = Regexp.new(/CREATE TABLE simplex\."users" \(
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

  # Test for retrieving table metadata with quoted identifiers
  #
  # test_can_retrieve_quoted_table_metadata tests that all pieces of table metadata can be retrieved, when the
  # table has quoted identifiers. It goes through each piece of table view metadata and verifies that each piece
  # is as expected.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-175
  # @expected_result table metadata with quoted identifiers should be retrieved.
  #
  # @test_category metadata
  #
  def test_can_retrieve_quoted_table_metadata
    # Check upper-case chars, unescaped upper-case chars, quoted numbers, quoted identifier with single quote,
    # quoted identifier with double quotes
    @session.execute("CREATE TABLE roles (\"FOO\" text, BAR ascii, \"10\" int, \"'20'\" int, \"\"\"30\"\"\" int,
                     \"f00\"\"b4r\" text, PRIMARY KEY (\"FOO\", BAR, \"10\", \"'20'\", \"\"\"30\"\"\", \"f00\"\"b4r\"))")
    @listener.wait_for_table('simplex', 'roles')

    assert @cluster.keyspace('simplex').has_table?('roles')
    table_meta = @cluster.keyspace('simplex').table('roles')

    assert_equal 'roles', table_meta.name
    assert_equal 'simplex', table_meta.keyspace.name
    assert_empty table_meta.indexes
    refute_nil table_meta.id unless CCM.cassandra_version < '3.0.0'
    refute_nil table_meta.options

    assert_columns([['FOO', :text], ['bar', :ascii], ['10', :int], ["'20'", :int], ["\"30\"", :int], ["f00\"b4r", :text]],
                   table_meta.primary_key)
    assert_columns([['FOO', :text]], table_meta.partition_key)
    assert_columns([['bar', :ascii], ['10', :int], ["'20'", :int], ["\"30\"", :int], ["f00\"b4r", :text]],
                   table_meta.clustering_columns)
    assert_equal :asc, table_meta.clustering_order.first

    assert_equal 6, table_meta.columns.size
    table_meta.each_column do |column|
      assert ['FOO', 'bar', '10', "'20'", "\"30\"", "f00\"b4r"].any? { |name| name == column.name }
      assert [:text, :ascii, :int].any? { |type| type == column.type.kind }
    end

    table_cql = Regexp.new(/CREATE TABLE simplex\."roles" \(
  "FOO" text,
  bar ascii,
  "10" int,
  "'20'" int,
  """30""" int,
  "f00""b4r" text,
  PRIMARY KEY \("FOO", bar, "10", "'20'", """30""", "f00""b4r"\)
\)/)

    assert_equal 0, table_meta.to_cql =~ table_cql
    @session.execute("DROP TABLE roles")

    # Check all the reserved words
    reserved_word_int_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.reserved_words.each do |word|
      reserved_word_int_list.push("\"#{word}\" int")
    end

    @session.execute("CREATE TABLE reserved_words (#{reserved_word_int_list.join(',')})")
    @listener.wait_for_table('simplex', 'reserved_words')

    assert @cluster.keyspace('simplex').has_table?('reserved_words')
    table_meta = @cluster.keyspace('simplex').table('reserved_words')
    refute_nil table_meta.to_cql
    @session.execute("DROP TABLE reserved_words")
  end

  # Test for skipping internal columns in static-compact tables
  #
  # test_skip_internal_columns_for_static_compact_table tests that the "column1 text" clustering
  # column and "value blob" regular columns are excluded from table metadata for static-compact tables.
  # It also coerces columns marked static to be regular instead.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-185
  # @expected_result the metadata should only report columns we've consciously added to the table.
  #
  # @test_category metadata
  #
  def test_skip_internal_columns_for_static_compact_table
    assert @cluster.keyspace('simplex').has_table?('blobby')
    table_meta = @cluster.keyspace('simplex').table('blobby')
    table_cql = Regexp.new(/CREATE TABLE simplex\.blobby \(
  "key" blob PRIMARY KEY,
  "f1" blob,
  "f2" blob
\)/)

    assert_equal 0, table_meta.to_cql =~ table_cql, "actual cql: #{table_meta.to_cql}"
    assert_equal 3, table_meta.columns.size

    table_meta.each_column do |column|
      assert ['key', 'f1', 'f2'].any? { |name| name == column.name }
      assert_equal :blob, column.type.kind
      refute column.static?
    end
  end

  # Test for skipping internal columns in dense tables
  #
  # test_skip_internal_columns_for_dense_table tests that "value <empty-type>" column is excluded from table metadata
  # for dense tables.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-185
  # @expected_result the metadata should only report columns we've consciously added to the table.
  #
  # @test_category metadata
  #
  def test_skip_internal_columns_for_dense_table
    # NOTE: It seems that the dense table does not have an empty-type column. The Java driver has logic to
    # handle that, but maybe it's outdated and unnecessary. This test serves the purpose of keeping us on guard,
    # in case some version of C* does create the internal column; if we encounter such a C*, the test will fail and
    # we'll go to the effort of fixing the issue.

    assert @cluster.keyspace('simplex').has_table?('dense')
    table_meta = @cluster.keyspace('simplex').table('dense')
    table_cql = Regexp.new(/CREATE TABLE simplex\.dense \(
  "f1" int,
  "f2" int,
  "f3" int,
  PRIMARY KEY \("f1", "f2"\)
\)
WITH COMPACT STORAGE/)

    assert_equal 0, table_meta.to_cql =~ table_cql, "actual cql: #{table_meta.to_cql}"
    assert_equal 3, table_meta.columns.size

    table_meta.each_column do |column|
      assert ['f1', 'f2', 'f3'].any? { |name| name == column.name }
      assert_equal :int, column.type.kind
      refute column.static?
    end
  end

  # Test for handling custom type columns in table metadata
  #
  # test_custom_type_column_in_table tests that a custom type column in a table is processed properly
  # when collecting table metadata.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-186
  # @expected_result the metadata should correctly report the custom type column.
  #
  # @test_category metadata
  #
  def test_custom_type_column_in_table
    skip("Custom type representation was changed in Cassandra 3.0 to be a single-quoted string") if CCM.cassandra_version < '3.0.0'

    assert @cluster.keyspace('simplex').has_table?('custom')
    table_meta = @cluster.keyspace('simplex').table('custom')
    table_cql = Regexp.new(/CREATE TABLE simplex\."custom" \(
  "f1" int PRIMARY KEY,
  "f2" 'org.apache.cassandra.db.marshal.CompositeType\(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type\)'
\)/)

    assert_equal 0, table_meta.to_cql =~ table_cql, "actual cql: #{table_meta.to_cql}"
    assert_equal 2, table_meta.columns.size
    column = table_meta.columns[0]
    assert_equal 'f1', column.name
    assert_equal :int, column.type.kind
    column = table_meta.columns[1]
    assert_equal 'f2', column.name
    assert_equal :custom, column.type.kind
    assert_equal 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)',
                 column.type.name
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
