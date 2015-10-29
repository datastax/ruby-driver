# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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

class SessionTest < IntegrationTestCase
  def setup_schema
    @@ccm_cluster.setup_schema(<<-CQL)
    CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    USE simplex;
    CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
    CREATE TABLE test (k text, v int, PRIMARY KEY (k, v));
    CQL
  end

  # Test for assuring initial keyspace is nil
  #
  # test_session_keyspace_is_initially_nil connects to a Cassandra cluster without specifiying
  # an initial keyspace to connect with. The resulting session object shouldn't have a keyspace.
  #
  # @since 1.0.0
  # @expected_result The resulting session object shouldn't have an associated keyspace.
  # @test_category session:basic
  #
  def test_session_keyspace_is_initially_nil
    cluster = Cassandra.cluster
    session = cluster.connect()

    assert_nil session.keyspace
  ensure
    cluster && cluster.close
  end

  def test_can_select_from_an_existing_keyspace
    cluster = Cassandra.cluster
    session = cluster.connect()
    if CCM.cassandra_version.start_with?('3.0')
      results = session.execute("SELECT * FROM system_schema.keyspaces")
    else
      results = session.execute("SELECT * FROM system.schema_keyspaces")
    end

    refute_nil results
  ensure
    cluster && cluster.close
  end

  def test_use_keyspace_changes_current_keyspace
    cluster = Cassandra.cluster
    session = cluster.connect()
    session.execute("USE system")
    assert_equal session.keyspace, "system"

    session.execute("USE system_traces")
    assert_equal session.keyspace, "system_traces"
  ensure
    cluster && cluster.close
  end

  def test_connect_session_with_existing_keyspace
    cluster = Cassandra.cluster
    session = cluster.connect("system")

    assert_equal session.keyspace, "system"
  ensure
    cluster && cluster.close
  end

  def test_can_create_new_keyspace
    setup_schema

    cluster = Cassandra.cluster
    session = cluster.connect()

    assert cluster.has_keyspace?("simplex"), "Expected cluster to have keyspace 'simplex'"
    
    session.execute("USE simplex")
    assert_equal session.keyspace, "simplex"
  ensure
    cluster && cluster.close
  end

  def test_execute_errors_on_invalid_keyspaces
    cluster = Cassandra.cluster
    session = cluster.connect()

    assert_raises(Cassandra::Errors::InvalidError) do 
      session.execute("CREATE TABLE users (user_id INT PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)")
    end

    session.execute("USE system")
    assert_raises(Cassandra::Errors::UnauthorizedError) do 
      session.execute("CREATE TABLE users (user_id INT PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)")
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_after_creating_a_table
    setup_schema

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    assert cluster.keyspace("simplex").has_table?("users"), "Expected to keyspace 'simplex' to have table 'users'"

    session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)")
    result = session.execute("SELECT * FROM users").first

    assert_equal result, {"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}
  ensure
    cluster && cluster.close
  end

  def test_can_insert_after_creating_a_table_async
    setup_schema

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    future = session.execute_async("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)")
    refute_nil future.get

    future = session.execute_async("SELECT * FROM users")
    result = future.get.first
    assert_equal result, {"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}
  ensure
    cluster && cluster.close
  end

  def test_can_prepare_insert_and_select_statements
    setup_schema

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.users (user_id, first, last, age) VALUES (?, ?, ?, ?)") }
    select = Retry.with_attempts(5) { session.prepare("SELECT * FROM simplex.users") }
    refute_nil insert
    refute_nil select

    Retry.with_attempts(5) { session.execute(insert, arguments: [0, 'John', 'Doe', 40]) }
    result = Retry.with_attempts(5) { session.execute(select).first }
    assert_equal result, {"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}
  ensure
    cluster && cluster.close
  end

  def test_prepare_errors_on_invalid_arguments
    setup_schema

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.users (user_id, first, last, age) VALUES (?, ?, ?, ?)") }
    refute_nil insert

    assert_raises(ArgumentError) do
      session.execute(insert, [0, 'John', 'Doe', 40])
    end

    assert_raises(ArgumentError) do
      session.execute(insert, arguments: [])
    end

    assert_raises(ArgumentError) do
      session.execute(insert, arguments: ['John', 'Doe', 40, 0])
    end
  ensure
    cluster && cluster.close
  end

  def test_can_use_named_parameters_with_prepared_statement
    skip("Named parameters are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    setup_schema

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.users (user_id, first, last, age) VALUES (:a, :b, :c, :d)") }
      Retry.with_attempts(5) { session.execute(insert, arguments: {:a => 0, :b => 'John', :c => 'Doe', :d => 40}) }

      select = Retry.with_attempts(5) { session.prepare("SELECT * FROM simplex.users WHERE user_id=:id") }
      result = Retry.with_attempts(5) { session.execute(select, arguments: {:id => 0}).first }

      assert_equal result, {"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}

      batch = session.batch do |b|
        b.add(insert, {:a => 1, :b => 'Jane', :c => 'Doe', :d => 30})
        b.add(insert, {:a => 2, :b => 'Agent', :c => 'Smith', :d => 20})
      end

      Retry.with_attempts(5) { session.execute(batch) }

      results = session.execute("SELECT * FROM users")
      assert_equal 3, results.size
    ensure
      cluster && cluster.close
    end
  end

  def test_can_use_named_parameters_with_simple_statement
    skip("Named parameters are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    setup_schema

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (:a, :b, :c, :d)",
                      arguments: {:a => 0, :b => 'John', :c => 'Doe', :d => 40})

      result = session.execute("SELECT * FROM users WHERE user_id=:id", arguments: {:id => 0}).first

      assert_equal result, {"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}

      batch = session.batch do |b|
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (:a, :b, :c, :d)", {:a => 1, :b => 'Jane', :c => 'Doe', :d => 30})
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (:a, :b, :c, :d)", {:a => 2, :b => 'Agent', :c => 'Smith', :d => 20})
      end

      session.execute(batch)

      results = session.execute("SELECT * FROM users")
      assert_equal 3, results.size
    ensure
      cluster && cluster.close
    end
  end

  def test_raise_error_on_invalid_named_parameters
    skip("Named parameters are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'
    skip("Missing named parameters are ignored in C* after 2.2") if CCM.cassandra_version >= '2.2.0'

    setup_schema

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.users (user_id, first, last, age) VALUES (:a, :b, :c, :d)") }

      assert_raises(ArgumentError) do
        session.execute(insert, arguments: {:a => 0, :b => 'John', :c => 'Doe'})
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_prepare_errors_on_non_existent_table
    setup_schema

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    assert_raises(Cassandra::Errors::InvalidError) do
      session.prepare("INSERT INTO simplex.badtable (user_id, first, last, age) VALUES (?, ?, ?, ?)")
    end

    assert_raises(Cassandra::Errors::InvalidError) do
      session.prepare("SELECT * FROM simplex.badtable")
    end
  ensure
    cluster && cluster.close
  end

  def test_can_execute_simple_batch_statements
    skip("Batch statements are only available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    setup_schema

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      batch = session.batch do |b|
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)")
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35)")
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32)")
      end

      session.execute(batch)
      results = session.execute("SELECT * FROM users")
      assert_equal 3, results.size
    ensure
      cluster && cluster.close
    end
  end

  def test_can_execute_batch_statements_with_parameters
    skip("Batch statements are only available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    setup_schema

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      batch = session.batch do |b|
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)", [3, 'Apache', 'Cassandra', 8])
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)", [4, 'DataStax', 'Ruby-Driver', 1])
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)", [5, 'Cassandra', 'Community', 8])
      end

      session.execute(batch)
      results = session.execute("SELECT * FROM users")
      assert_equal 3, results.size
    ensure
      cluster && cluster.close
    end
  end

  def test_can_execute_batch_statements_with_prepared_statements
    skip("Batch statements are only available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    setup_schema

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")
    
      insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.users (user_id, first, last, age) VALUES (?, ?, ?, ?)") }
      refute_nil insert

      batch = session.batch do |b|
        b.add(insert, [6, 'Joséphine', 'Baker', 108])
        b.add(insert, [7, 'Stefan', 'Löfven', 57])
        b.add(insert, [8, 'Mick', 'Jager', 71])
      end

      Retry.with_attempts(5) { session.execute(batch) }
      results = session.execute("SELECT * FROM users")
      assert_equal 3, results.size
    ensure
      cluster && cluster.close
    end
  end

  def test_result_paging
    skip("Paging is only available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    setup_schema

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.test (k, v) VALUES (?, ?)") }
      ("a".."z").each_with_index do |letter, number|
        Retry.with_attempts(5) { session.execute(insert, arguments: [letter, number]) }
      end

      # Small page_size
      results  = session.execute("SELECT * FROM test", page_size: 5)
      assert_equal 5, results.size
      refute results.last_page?
      count = 0

      loop do
        results.each do |row|
          refute_nil row
          count += 1
        end
      
        break if results.last_page?
        assert_equal 5, results.size
        results = results.next_page
      end
      assert results.last_page?
      assert_equal 26, count

      # Lage page_size
      results  = session.execute("SELECT * FROM test", page_size: 500)
      assert_equal 26, results.size
      assert results.last_page?

      # Invalid page_size
      assert_raises(ArgumentError) do
        session.execute("SELECT * FROM test", page_size: 0)
      end
    ensure
      cluster && cluster.close
    end
  end

  def page_through(future, count)
    future.then do |results|
      results.each do |row|
        refute_nil row
        count += 1
      end

      if results.last_page? 
        count 
      else
        page_through(results.next_page_async, count)
      end
    end
  end

  def test_result_paging_async
    skip("Paging is only available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    setup_schema

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.test (k, v) VALUES (?, ?)") }
      ("a".."z").each_with_index do |letter, number|
        Retry.with_attempts(5) { session.execute(insert, arguments: [letter, number]) }
      end

      select = Retry.with_attempts(5) { session.prepare("SELECT * FROM simplex.test") }
      future = session.execute_async(select, page_size: 5)
      count = 0

      count = page_through(future, count).get
      assert_equal 26, count
    ensure
      cluster && cluster.close
    end
  end

  def test_query_tracing
    setup_schema

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")
    
    # Void returning operations
    result = session.execute("INSERT INTO test (k, v) VALUES ('a', 1)", trace: true)
    refute_nil result.execution_info.trace
    assert_instance_of Cassandra::Uuid, result.execution_info.trace.id

    # Row returning operations
    result = session.execute("SELECT * FROM test", trace: true)
    refute_nil result.execution_info.trace
    assert_instance_of Cassandra::Uuid, result.execution_info.trace.id

    if CCM.cassandra_version >= '2.0'
      # Batch operations
      batch = session.batch do |b|
        b.add("INSERT INTO test (k, v) VALUES ('b', 2)")
        b.add("INSERT INTO test (k, v) VALUES ('c', 3)")
        b.add("INSERT INTO test (k, v) VALUES ('d', 4)")
      end
      result = session.execute(batch, trace: true)
      refute_nil result.execution_info.trace
      assert_instance_of Cassandra::Uuid, result.execution_info.trace.id
    end
  ensure
    cluster && cluster.close
  end

  # Test for verifying schema synchronization can be disabled
  #
  # test_can_disable_synchronize_schema tests that schema synchronization can be disabled by passing in synchronize_schema
  # = false when the cluster is created, preventing schema metadata population at cluster initialization. This test first
  # creates a cluster object with synchronize_schema = false. It then verifies that the cluster does not have any schema
  # metadata by querying for an existing keyspace, "simplex". It proceeds to manually synchronize the schema by calling
  # cluster.refresh_schema. Finally, it verifies schema metadata has been updated by checking that the cluster has metadata
  # regarding the keyspace "simplex".
  #
  # @since 2.1.4
  # @jira_ticket RUBY-97
  # @expected_result Schema metadata should be initially nil, and then populated after calling cluster.refresh_schema
  #
  # @test_category metadata
  #
  def test_can_disable_synchronize_schema
    setup_schema

    begin
      cluster = Cassandra.cluster(synchronize_schema: false)
      refute cluster.has_keyspace?("simplex"), "Expected cluster metadata to not include keyspace 'simplex'"

      cluster.refresh_schema
      assert cluster.has_keyspace?("simplex"), "Expected cluster metadata to include keyspace 'simplex'"
    ensure
      cluster && cluster.close
    end
  end

  def curr_time_in_micro(time)
    time.tv_sec * 1000000 + time.tv_usec
  end

  # Test for executing queries with client-side timestamps
  #
  # test_client_side_timestamps_with_cql_future tests client-side timestamps can be enabled and used with any simple,
  # prepared or batch query. It first inserts a row into Cassandra with a future timestamp using "USING TIMESTAMP"
  # CQL. It then attempts to do an insert using a simple statement and verifies that the insert did not get accepted
  # by Cassandra by checking the timestamp for that row. It then repeats this process for prepared and batch statements.
  #
  # @since 2.1.4
  # @jira_ticket RUBY-70
  # @expected_result Each query with client-side timestamp should fail, as the server timestamp is newer
  #
  # @test_assumptions A Cassandra cluster with version 2.1.0 or higher.
  # @test_category queries:timestamp
  #
  def test_client_side_timestamps_with_cql_future
    skip("Client-side timestamps are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    setup_schema
    begin
      cluster = Cassandra.cluster(client_timestamps: true)
      session = cluster.connect("simplex")

      # Insert from the future
      timestamp = curr_time_in_micro(Time.now + 1000)
      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40) USING TIMESTAMP #{timestamp}")
      result = session.execute("SELECT writetime(first) FROM users WHERE user_id = 0").first
      assert_equal timestamp, result["writetime(first)"]

      # Current client-side timestamp won't update the future row
      # Simple statements
      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'Mary', 'Holler', 22)")
      result = session.execute("SELECT writetime(first) FROM users WHERE user_id = 0").first
      assert_equal timestamp, result["writetime(first)"]

      # Prepared statements
      insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.users (user_id, first, last, age) VALUES (?, ?, ?, ?)") }
      Retry.with_attempts(5) { session.execute(insert, arguments: [0, 'Jane', 'Smith', 30]) }
      result = session.execute("SELECT writetime(first) FROM users WHERE user_id = 0").first
      assert_equal timestamp, result["writetime(first)"]

      # Batch statements
      batch = session.batch do |b|
        b.add("INSERT INTO users (user_id, first, last, age) VALUES (0, 'Apache', 'Cassandra', 6)")
        b.add(insert, [0, 'Ruby', 'Driver', 2])
      end

      Retry.with_attempts(5) { session.execute(batch) }
      result = session.execute("SELECT writetime(first) FROM users WHERE user_id = 0").first
      assert_equal timestamp, result["writetime(first)"]
    ensure
      cluster && cluster.close
    end
  end

  # Test for executing queries with past client-side timestamps
  #
  # test_client_side_timestamps_with_past_timestamp tests client-side timestamps can be enabled and used with any simple,
  # prepared or batch query. It first inserts a row into Cassandra with the current timestamp. It then changes the current
  # system time to be 1 minute slow, emulating queries with past timestamps. It attempts to do an insert using a simple
  # statement and verifies that the insert did not get accepted by Cassandra by checking the value for that row is still
  # the original value. It then repeats this process for prepared and batch statements.
  #
  # @since 2.1.4
  # @jira_ticket RUBY-70
  # @expected_result Each query with client-side timestamp should fail, as the server timestamp is newer
  #
  # @test_assumptions A Cassandra cluster with version 2.1.0 or higher.
  # @test_category queries:timestamp
  #
  def test_client_side_timestamps_with_past_timestamp
    skip("Client-side timestamps are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    setup_schema
    begin
      cluster = Cassandra.cluster(client_timestamps: true)
      session = cluster.connect("simplex")

      # Insert in the present
      session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)")

      # Set current time to the past
      Delorean.time_travel_to "1 minute ago" do
        # Old client-side timestamp won't update the row
        # Simple statements
        session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'Mary', 'Holler', 22)")
        result = session.execute("SELECT * FROM users WHERE user_id = 0").first
        assert_equal "John", result["first"]

        # Prepared statements
        insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.users (user_id, first, last, age) VALUES (?, ?, ?, ?)") }
        Retry.with_attempts(5) { session.execute(insert, arguments: [0, 'Jane', 'Smith', 30]) }
        result = session.execute("SELECT * FROM users WHERE user_id = 0").first
        assert_equal "John", result["first"]

        # Batch statements
        batch = session.batch do |b|
          b.add(insert, [0, 'Ruby', 'Driver', 2])
        end

        Retry.with_attempts(5) { session.execute(batch) }
        result = session.execute("SELECT * FROM users WHERE user_id = 0").first
        assert_equal "John", result["first"]
      end
    ensure
      cluster && cluster.close
    end
  end

end
