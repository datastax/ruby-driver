Feature: Schema Metadata

  Ruby Driver allows inspecting schema metadata

  Background:
    Given a running cassandra cluster

  @cassandra-version-specific @cassandra-version-less-3.0
  Scenario: Getting table metadata
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      puts cluster.keyspace('system').table("IndexInfo").to_cql
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE TABLE system."IndexInfo" (
        table_name text,
        index_name text,
        PRIMARY KEY (table_name, index_name)
      )
      """

  @cassandra-version-specific @cassandra-version-3.0
  Scenario: Getting table metadata on 3.0
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      puts cluster.keyspace('system').table("IndexInfo").to_cql
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE TABLE system."IndexInfo" (
        table_name text,
        index_name text,
        value 'org.apache.cassandra.db.marshal.EmptyType',
        PRIMARY KEY (table_name, index_name)
      )
      """

  Scenario: Getting index metadata
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      CREATE TABLE simplex.test_table (f1 int primary key, f2 int);
      CREATE INDEX ind1 ON simplex.test_table (f2);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      cluster.keyspace('simplex').table('test_table').each_index do |index|
        puts index.to_cql
      end
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE INDEX "ind1" ON simplex.test_table (f2);
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

      view = cluster.keyspace('simplex').materialized_view('test_view')
      puts view.to_cql
      """
    When it is executed
    Then its output should contain:
      """cql
      CREATE MATERIALIZED VIEW simplex.test_view AS
      SELECT "f1", "f2"
      FROM simplex.test_table
      WHERE f2 IS NOT NULL
      PRIMARY KEY(("f1"),"f2")
      WITH bloom_filter_fp_chance = 0.01
      AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
      AND comment = ''
      AND compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
      AND compression = {'chunk_length_in_kb': '64', 'class': 'LZ4Compressor'}
      AND dclocal_read_repair_chance = 0.1
      AND default_time_to_live = 0
      AND gc_grace_seconds = 864000
      AND max_index_interval = 2048
      AND memtable_flush_period_in_ms = 0
      AND min_index_interval = 128
      AND read_repair_chance = 0.0
      AND speculative_retry = '99PERCENTILE'
      AND crc_check_chance = 1.0;
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
