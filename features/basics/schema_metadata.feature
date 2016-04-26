Feature: Schema Metadata

  Ruby Driver allows inspecting schema metadata

  Background:
    Given a running cassandra cluster

  Scenario: Getting keyspace metadata
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      ks_meta = cluster.keyspace('simplex')
      puts "Name: #{ks_meta.name}"
      puts "Replication class: #{ks_meta.replication.klass}"
      puts "Replication factor: #{ks_meta.replication.options['replication_factor'].to_i}"
      puts "Durable writes?: #{ks_meta.durable_writes?}"
      puts "# tables: #{ks_meta.tables.size}"

      puts ""
      puts ks_meta.to_cql
      """
    When it is executed
    Then its output should contain:
      """cql
      Name: simplex
      Replication class: SimpleStrategy
      Replication factor: 3
      Durable writes?: true
      # tables: 0

      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = true;
      """

  Scenario: Getting table metadata
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.users (user_id bigint, first text, last text, age int, PRIMARY KEY (user_id, last))
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      table_meta = cluster.keyspace('simplex').table('users')
      puts "Name: #{table_meta.name}"
      puts "Keyspace: #{table_meta.keyspace.name}"
      puts "Partition key: #{table_meta.partition_key[0].name}"
      puts "Clustering column: #{table_meta.clustering_columns[0].name}"
      puts "Clustering order: #{table_meta.clustering_order.first}"
      puts "Num columns: #{table_meta.columns.size}"

      puts ""
      puts table_meta.to_cql
      """
    When it is executed
    Then its output should contain:
      """cql
      Name: users
      Keyspace: simplex
      Partition key: user_id
      Clustering column: last
      Clustering order: asc
      Num columns: 4

      CREATE TABLE simplex."users" (
        user_id bigint,
        last text,
        age int,
        first text,
        PRIMARY KEY (user_id, last)
      )
      """

  Scenario: Getting index metadata
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.test_table (a int primary key, b int);
      CREATE INDEX ind1 ON simplex.test_table (b);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      index = cluster.keyspace('simplex').table('test_table').index('ind1')
      puts index.to_cql

      puts ""
      puts "Name: #{index.name}"
      puts "Table name: #{index.table.name}"
      puts "Kind: #{index.kind}"
      puts "Target: #{index.target}"
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE INDEX "ind1" ON simplex.test_table (b);

      Name: ind1
      Table name: test_table
      Kind: composites
      Target: b
      """

  @cassandra-version-specific @cassandra-version-3.0
  Scenario: Getting index metadata on full collections
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.test_table (a int PRIMARY KEY, b frozen<map<text, text>>);
      CREATE INDEX ind1 ON simplex.test_table (full(b));
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      index = cluster.keyspace('simplex').table('test_table').index('ind1')
      puts index.to_cql

      puts ""
      puts "Name: #{index.name}"
      puts "Table name: #{index.table.name}"
      puts "Kind: #{index.kind}"
      puts "Target: #{index.target}"
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE INDEX "ind1" ON simplex.test_table (full(b));

      Name: ind1
      Table name: test_table
      Kind: composites
      Target: full(b)
      """

  @cassandra-version-specific @cassandra-version-3.0
  Scenario: Getting multiple index metadata on same column
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.test_table (a int PRIMARY KEY, b map<text, text>);
      CREATE INDEX ind1 ON simplex.test_table (keys(b));
      CREATE INDEX ind2 ON simplex.test_table (values(b));
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      index = cluster.keyspace('simplex').table('test_table').index('ind1')
      puts index.to_cql

      puts ""
      puts "Name: #{index.name}"
      puts "Target: #{index.target}"

      puts ""
      index = cluster.keyspace('simplex').table('test_table').index('ind2')
      puts index.to_cql

      puts ""
      puts "Name: #{index.name}"
      puts "Target: #{index.target}"
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE INDEX "ind1" ON simplex.test_table (keys(b));

      Name: ind1
      Target: keys(b)

      CREATE INDEX "ind2" ON simplex.test_table (values(b));

      Name: ind2
      Target: values(b)
      """

  @cassandra-version-specific @cassandra-version-2.1
  Scenario: Getting user-defined type metadata
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TYPE simplex.address (street text, zipcode int);
      CREATE TYPE simplex.check_in (
        location frozen <address>,
        time timestamp,
        data frozen <tuple<int, text, float>>
      );
      CREATE TABLE simplex.users (id int PRIMARY KEY, location frozen<address>);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      cluster.keyspace('simplex').each_type do |type|
        puts type.to_cql
      end
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE TYPE simplex.address (
        street text,
        zipcode int
      );
      CREATE TYPE simplex.check_in (
        location frozen <address>,
        time timestamp,
        data frozen <tuple<int, text, float>>
      );
      """

  @cassandra-version-specific @cassandra-version-2.2
  Scenario: Getting user-defined functions and metadata
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE OR REPLACE FUNCTION simplex.fLog(input double)
        CALLED ON NULL INPUT
        RETURNS double
        LANGUAGE java
        AS 'return Double.valueOf(Math.log(input.doubleValue()));'
      """
    And the following example:
      """ruby
      require 'cassandra'
      include Cassandra::Types

      cluster = Cassandra.cluster

      puts cluster.keyspace('simplex').function('fLog', double).to_cql

      function = cluster.keyspace('simplex').function('fLog', double)
      puts ""
      puts "Name: #{function.name}"
      puts "Language: #{function.language}"
      puts "Return type: #{function.type}"
      puts "Called on null?: #{function.called_on_null?}"
      puts "Argument 'input'?: #{function.has_argument?("input")}"
      function.each_argument { |arg| puts "Argument type: #{arg.type}" }
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE FUNCTION simplex.flog(input double)
        CALLED ON NULL INPUT
        RETURNS double
        LANGUAGE java
        AS $$return Double.valueOf(Math.log(input.doubleValue()));$$;

      Name: flog
      Language: java
      Return type: double
      Called on null?: true
      Argument 'input'?: true
      Argument type: double
      """

  @cassandra-version-specific @cassandra-version-2.2
  Scenario: Getting user-defined aggregates and metadata
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE OR REPLACE FUNCTION simplex.avgState(state tuple<int, bigint>, val int)
        CALLED ON NULL INPUT
        RETURNS tuple<int, bigint>
        LANGUAGE java
        AS 'if (val !=null) { state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue()); } return state;';
      CREATE OR REPLACE FUNCTION simplex.avgFinal(state tuple<int, bigint>)
        CALLED ON NULL INPUT
        RETURNS double
        LANGUAGE java
        AS 'double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r/= state.getInt(0); return Double.valueOf(r);';
      CREATE OR REPLACE AGGREGATE simplex.average(int)
        SFUNC avgState
        STYPE tuple<int, bigint>
        FINALFUNC avgFinal
        INITCOND (0, 0);
      """
    And the following example:
      """ruby
      require 'cassandra'
      include Cassandra::Types

      cluster = Cassandra.cluster

      aggregate = cluster.keyspace('simplex').aggregate('average', int)
      puts aggregate.to_cql
      puts ""
      puts "Name: #{aggregate.name}"
      puts "Return type: #{aggregate.type}"
      puts "Argument type: #{aggregate.argument_types[0].kind}"
      puts "State type: #{aggregate.state_type}"
      puts "Initial condition: #{aggregate.initial_state}"
      puts "State function: #{aggregate.state_function.name}"
      puts "Final function: #{aggregate.final_function.name}"
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE AGGREGATE simplex.average(int)
        SFUNC avgstate
        STYPE tuple<int, bigint>
        FINALFUNC avgfinal
        INITCOND (0, 0);

      Name: average
      Return type: double
      Argument type: int
      State type: tuple<int, bigint>
      Initial condition: (0, 0)
      State function: avgstate
      Final function: avgfinal
      """

  @cassandra-version-specific @cassandra-version-3.0
  Scenario: Getting materialized view metadata
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.test_table (f1 int PRIMARY KEY, f2 int, f3 int) ;
      CREATE MATERIALIZED VIEW simplex.test_view AS
       SELECT f1, f2 FROM simplex.test_table WHERE f2 IS NOT NULL
       PRIMARY KEY (f1, f2);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      mv_meta = cluster.keyspace('simplex').materialized_view('test_view')
      puts "Name: #{mv_meta.name}"
      puts "Base table: #{mv_meta.base_table.name}"
      puts "Keyspace: #{mv_meta.keyspace.name}"
      puts "Partition key: #{mv_meta.partition_key[0].name}"
      puts "Clustering column: #{mv_meta.clustering_columns[0].name}"
      puts "Num columns: #{mv_meta.columns.size}"

      puts ""
      puts mv_meta.to_cql
      """
    When it is executed
    Then its output should contain:
      """cql
      Name: test_view
      Base table: test_table
      Keyspace: simplex
      Partition key: f1
      Clustering column: f2
      Num columns: 2

      CREATE MATERIALIZED VIEW simplex.test_view AS
      SELECT "f1", "f2"
      FROM simplex.test_table
      WHERE f2 IS NOT NULL
      PRIMARY KEY (("f1"), "f2")
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
       AND speculative_retry = '99PERCENTILE';
      """
