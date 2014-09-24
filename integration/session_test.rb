# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
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

  def test_initialially_nil
    cluster = Cassandra.connect
    session = cluster.connect()

    assert_nil session.keyspace
  end

  def test_keyspaces_exist
    cluster = Cassandra.connect
    session = cluster.connect()
    results = session.execute("SELECT * FROM system.schema_keyspaces")

    refute_nil results
  end

  def test_use_keyspaces
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("USE system")
    assert_equal session.keyspace, "system"

    session.execute("USE system_traces")
    assert_equal session.keyspace, "system_traces"
  end

  def test_connect_with_keyspace
    cluster = Cassandra.connect
    session = cluster.connect("system")

    assert_equal session.keyspace, "system"
  end

  def test_keyspace_creation
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    sleep(1)
    assert cluster.has_keyspace?("simplex"), "Expected cluster to have keyspace 'simplex'"
    
    session.execute("USE simplex")
    assert_equal session.keyspace, "simplex"
  end

  def test_schema_creation_and_insert
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")

    session.execute("CREATE TABLE users (user_id INT PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)")
    sleep(1)
    assert cluster.keyspace("simplex").has_table?("users"), "Expected to keyspace 'simplex' to have table 'users'"

    session.execute("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)")
    result = session.execute("SELECT * FROM users").first

    assert_equal result, {"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}
  end

  def test_schema_creation_and_insert_async
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")

    session.execute("CREATE TABLE users (user_id INT PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)")
    future = session.execute_async("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)")
    refute_nil future.get

    future = session.execute_async("SELECT * FROM users")
    result = future.get.first
    assert_equal result, {"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}
  end

  def test_prepared_statements
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")
    session.execute("CREATE TABLE users (user_id INT PRIMARY KEY, first VARCHAR, last VARCHAR, age INT)")

    insert = session.prepare("INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)")
    select = session.prepare("SELECT * FROM users")
    refute_nil insert
    refute_nil select

    session.execute(insert, 0, 'John', 'Doe', 40)
    result = session.execute(select).first
    assert_equal result, {"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}
  end

  def test_batch_statements
    skip("Batch statements are only available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")
    session.execute("CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT)")

    # Simple statement
    batch = session.batch do |b|
      b.add("INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)")
      b.add("INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35)")
      b.add("INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32)")
    end

    session.execute(batch)
    results = session.execute("SELECT * FROM users")
    assert_equal 3, results.size

    # Parametrized simple statement
    batch = session.batch do |b|
      b.add("INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)", 3, 'Apache', 'Cassandra', 8)
      b.add("INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)", 4, 'DataStax', 'Ruby-Driver', 1)
      b.add("INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)", 5, 'Cassandra', 'Community', 8)
    end

    session.execute(batch)
    results = session.execute("SELECT * FROM users")
    assert_equal 6, results.size

    # Prepared statement
    insert = session.prepare("INSERT INTO users (user_id, first, last, age) VALUES (?, ?, ?, ?)")
    refute_nil insert

    batch = session.batch do |b|
      b.add(insert, 6, 'Joséphine', 'Baker', 108)
      b.add(insert, 7, 'Stefan', 'Löfven', 57)
      b.add(insert, 8, 'Mick', 'Jager', 71)
    end

    session.execute(batch)
    results = session.execute("SELECT * FROM users")
    assert_equal 9, results.size
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

  def test_paging
    skip("Paging is only available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")
    
    session.execute("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))")
    insert = session.prepare("INSERT INTO test (k, v) VALUES (?, ?)")
    ("a".."z").each_with_index do |letter, number|
      session.execute(insert, letter, number)
    end

    # Synchronous paging
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

    # Asynchronous paging
    select = session.prepare("SELECT * FROM test")
    future = session.execute_async(select, page_size: 5)
    count = 0

    count = page_through(future, count).get
    assert_equal 26, count
  end

  def test_tracing
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")
    session.execute("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))")
    
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
  end
end