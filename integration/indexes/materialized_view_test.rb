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

class MaterializedViewTest < IntegrationTestCase

  def setup
    return if CCM.cassandra_version < '3.0.0'

    @@ccm_cluster.setup_schema("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

    @cluster = Cassandra.cluster(
        schema_refresh_delay: 0.1,
        schema_refresh_timeout: 0.1
    )
    @listener = SchemaChangeListener.new(@cluster)
    @session = @cluster.connect('simplex')

    @session.execute("CREATE TABLE simplex.scores(
                        user TEXT,
                        game TEXT,
                        year INT,
                        month INT,
                        day INT,
                        score INT,
                        PRIMARY KEY (user, game, year, month, day)
                        )")
    @session.execute("CREATE MATERIALIZED VIEW simplex.monthlyhigh AS
                        SELECT game, year, month, score, user, day FROM simplex.scores
                        WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY ((game, year, month), score, user, day)
                        WITH CLUSTERING ORDER BY (score DESC, user ASC, day ASC)")
    @listener.wait_for_materialized_view('simplex', 'monthlyhigh')
  end

  def teardown
    @cluster && @cluster.close
  end

  # Test for retrieving materialized view metadata
  #
  # test_can_retrieve_materialized_view_metadata tests that all pieces of materialized view metadata can be retrieved.
  # It goes through each piece of materialized view metadata and verifies that each piece is as expected.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-167
  # @expected_result materialized view metadata should be retrieved.
  #
  # @test_category materialized_view
  #
  def test_can_retrieve_materialized_view_metadata
    skip("Materialized views were introduced in Cassandra 3.0.0") if CCM.cassandra_version < '3.0.0'

    assert @cluster.keyspace('simplex').has_materialized_view?('monthlyhigh')
    mv_meta = @cluster.keyspace('simplex').materialized_view('monthlyhigh')

    table = @cluster.keyspace('simplex').table('scores')
    assert table.has_materialized_view?('monthlyhigh')
    assert_same(mv_meta, table.materialized_view('monthlyhigh'))

    assert_equal 'monthlyhigh', mv_meta.name
    refute_nil mv_meta.id
    assert_equal 'scores', mv_meta.base_table.name
    assert_equal 'simplex', mv_meta.keyspace.name
    refute_nil mv_meta.options

    assert_columns([['game', :text], ['year', :int], ['month', :int], ['score', :int], ['user', :text], ['day', :int]],
                   mv_meta.primary_key)
    assert_columns([['game', :text], ['year', :int], ['month', :int]], mv_meta.partition_key)
    assert_columns([['score', :int], ['user', :text], ['day', :int]], mv_meta.clustering_columns)

    assert_equal 6, mv_meta.columns.size
    mv_meta.each_column do |column|
      assert ['game', 'year', 'month', 'score', 'user', 'day'].any? { |name| name == column.name }
      assert [:text, :int].any? { |type| type == column.type.kind }
    end
  end

  # Test for retrieving mv metadata with quoted identifiers
  #
  # test_can_retrieve_quoted_mv_metadata tests that all pieces of materialized view metadata can be retrieved, when the
  # view has quoted identifiers. It goes through each piece of materialized view metadata and verifies that each piece
  # is as expected.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-167
  # @expected_result mv metadata with quoted identifiers should be retrieved.
  #
  # @test_category materialized_view
  #
  def test_can_retrieve_quoted_mv_metadata
    skip("Materialized views were introduced in Cassandra 3.0.0") if CCM.cassandra_version < '3.0.0'

    @session.execute("CREATE TABLE simplex.test (
                      \"theKey\" int,
                      \"the;Clustering\" int,
                      \"the Value\" int,
                      PRIMARY KEY (\"theKey\", \"the;Clustering\"))")
    @session.execute("CREATE MATERIALIZED VIEW simplex.mv1 AS
                      SELECT \"theKey\", \"the;Clustering\", \"the Value\"
                      FROM simplex.test
                      WHERE \"theKey\" IS NOT NULL AND \"the;Clustering\" IS NOT NULL AND \"the Value\" IS NOT NULL
                      PRIMARY KEY (\"theKey\", \"the;Clustering\")")

    @listener.wait_for_materialized_view('simplex', 'mv1')

    assert @cluster.keyspace('simplex').has_materialized_view?('mv1')
    mv_meta = @cluster.keyspace('simplex').materialized_view('mv1')

    assert_equal 'mv1', mv_meta.name
    refute_nil mv_meta.id
    assert_equal 'test', mv_meta.base_table.name
    assert_equal 'simplex', mv_meta.keyspace.name
    refute_nil mv_meta.options

    assert_columns([['theKey', :int], ['the;Clustering', :int]], mv_meta.primary_key)
    assert_columns([['theKey', :int]], mv_meta.partition_key)
    assert_columns([['the;Clustering', :int]], mv_meta.clustering_columns)

    assert_equal 3, mv_meta.columns.size
    mv_meta.each_column do |column|
      assert ['theKey', 'the;Clustering', 'the Value'].any? { |name| name == column.name }
      assert_equal :int, column.type.kind
    end
  end

  # Test for column ordering in materialized view metadata
  #
  # test_column_ordering_is_deterministic tests that the metadata relating to the columns are retrieved in the proper
  # order. This proper order is: partition key, clustering columns, and then all other columns alphanumerically.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-167
  # @expected_result column ordering should be correct in materialized view metadata.
  #
  # @test_category materialized_view
  #
  def test_column_ordering_is_deterministic
    skip("Materialized views were introduced in Cassandra 3.0.0") if CCM.cassandra_version < '3.0.0'

    assert @cluster.keyspace('simplex').has_materialized_view?('monthlyhigh')
    mv_cql = Regexp.new(/CREATE MATERIALIZED VIEW simplex.monthlyhigh AS
SELECT game, year, month, score, "user", day
FROM simplex.scores
WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL
PRIMARY KEY \(\(game, year, month\), score, "user", day\)
WITH bloom_filter_fp_chance = 0.01
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
 AND comment = ''
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
 AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
 AND crc_check_chance = 1.0
 AND dclocal_read_repair_chance = 0.1
 AND default_time_to_live = 0
 AND gc_grace_seconds = 864000
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 0
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99PERCENTILE';/)

    mv_meta = @cluster.keyspace('simplex').materialized_view('monthlyhigh')
    assert_match mv_cql, mv_meta.to_cql

    col_names = ['game', 'year', 'month', 'score', 'user', 'day']
    mv_meta.each_column do |column|
      assert_equal col_names[0], column.name
      col_names.delete_at(0)
    end
  end

  # Test for retrieving mv metadata updates
  #
  # test_materialized_view_metadata_updates tests that materialized view metadata is updated when there is any update
  # to the materialized view. It first creates a simple materialized view and verifies that the default compaction
  # strategy is SizeTieredCompactionStrategy. It then alters the compaction strategy to LeveledCompactionStrategy and
  # verifies that the metadata is properly updated.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-167
  # @expected_result mv metadata should be updated when the mv is updated.
  #
  # @test_category materialized_view
  #
  def test_materialized_view_metadata_updates
    skip("Materialized views were introduced in Cassandra 3.0.0") if CCM.cassandra_version < '3.0.0'

    @session.execute("CREATE TABLE simplex.test (pk int PRIMARY KEY, c int)")
    @session.execute("CREATE MATERIALIZED VIEW simplex.mv1 AS SELECT c FROM simplex.test WHERE c IS NOT NULL PRIMARY KEY (pk, c)")

    @listener.wait_for_materialized_view('simplex', 'mv1')

    assert @cluster.keyspace('simplex').has_materialized_view?('mv1')
    mv_meta = @cluster.keyspace('simplex').materialized_view('mv1')
    assert_equal 'SizeTieredCompactionStrategy', mv_meta.options.compaction_strategy.class_name

    @session.execute("ALTER MATERIALIZED VIEW simplex.mv1 WITH compaction = { 'class' : 'LeveledCompactionStrategy' }")
    @cluster.refresh_schema
    mv_meta = @cluster.keyspace('simplex').materialized_view('mv1')
    assert_equal 'LeveledCompactionStrategy', mv_meta.options.compaction_strategy.class_name
  end

  # Test for retrieving mv metadata drops
  #
  # test_materialized_view_metadata_drop tests that materialized view metadata is removed when the materialized view
  # is dropped. It first creates a simple materialized view and verifies that its metadata can be accessed. It then
  # drops this materialized view and verifies that the metadata for the view no longer exists.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-167
  # @expected_result mv metadata should be removed when the mv is dropped.
  #
  # @test_category materialized_view
  #
  def test_materialized_view_metadata_drop
    skip("Materialized views were introduced in Cassandra 3.0.0") if CCM.cassandra_version < '3.0.0'

    @session.execute("CREATE TABLE simplex.test (pk int PRIMARY KEY, c int)")
    @session.execute("CREATE MATERIALIZED VIEW simplex.mv1 AS SELECT c FROM simplex.test WHERE c IS NOT NULL PRIMARY KEY (pk, c)")

    @listener.wait_for_materialized_view('simplex', 'mv1')
    assert @cluster.keyspace('simplex').has_materialized_view?('mv1')

    @session.execute("DROP MATERIALIZED VIEW simplex.mv1")
    @cluster.refresh_schema
    refute @cluster.keyspace('simplex').has_materialized_view?('mv1')
  end

  # Test for retrieving mv metadata updates from base table changes
  #
  # test_base_table_column_addition tests that materialized view metadata is properly updated when there is a change to
  # the underlying base table. It first creates a basic table and a materialized view. It then alters the table to add
  # a new column, verifying that the alteration is in the table's metadata. Finally, it verifies that the alteration
  # has been propagated to the materialized view's metadata as well.
  #
  # @since 3.0.0
  # @jira_ticket RUBY-167
  # @expected_result mv metadata should be updated when the base table is updated.
  #
  # @test_category materialized_view
  #
  def test_base_table_column_addition
    skip("Materialized views were introduced in Cassandra 3.0.0") if CCM.cassandra_version < '3.0.0'

    @session.execute("CREATE TABLE simplex.scores2(
                        user TEXT,
                        game TEXT,
                        year INT,
                        month INT,
                        day INT,
                        score INT,
                        PRIMARY KEY (user, game, year, month, day)
                        )")
    @session.execute("CREATE MATERIALIZED VIEW simplex.alltimehigh AS
                        SELECT * FROM simplex.scores2
                        WHERE game IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY (game, score, user, year, month, day)
                        WITH CLUSTERING ORDER BY (score DESC)")

    @listener.wait_for_materialized_view('simplex', 'alltimehigh')

    assert @cluster.keyspace('simplex').has_materialized_view?('alltimehigh')
    mv_meta = @cluster.keyspace('simplex').materialized_view('alltimehigh')
    refute mv_meta.has_column?('fouls')

    @session.execute("ALTER TABLE simplex.scores2 ADD fouls INT")
    @cluster.refresh_schema

    table_meta = @cluster.keyspace('simplex').table('scores2')
    assert table_meta.has_column?('fouls')
    assert_equal :int, table_meta.column('fouls').type.kind

    mv_meta = @cluster.keyspace('simplex').materialized_view('alltimehigh')
    assert mv_meta.has_column?('fouls')
    assert_equal :int, mv_meta.column('fouls').type.kind
  end
end
