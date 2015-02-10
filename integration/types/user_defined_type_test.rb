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

require File.dirname(__FILE__) + '/../integration_test_case.rb'
require File.dirname(__FILE__) + '/../datatype_utils.rb'

class UserDefinedTypeTest < IntegrationTestCase

  def setup
    @@ccm_cluster.setup_schema("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
  end

  def test_raise_error_on_nonexisting_udt
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      assert_raises(Cassandra::Errors::InvalidError) do
        session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_udts
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      session.execute("CREATE TYPE user (age int, name text, gender text)")
      session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

      # Test non-prepared statement
      session.execute("INSERT INTO mytable (a, b) VALUES (0, {age: 30, name: 'John', gender: 'male'})")
      user_value = session.execute("SELECT b FROM mytable where a=0").first['b']
      assert_equal 30, user_value.age
      assert_equal 'John', user_value.name
      assert_equal 'male', user_value.gender

      # Test prepared statement
      insert = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
      session.execute(insert, arguments: [1, Cassandra::UDT.new(age: 25, name: 'Jane', gender: 'female')])

      user_value = session.execute("SELECT b FROM mytable where a=1").first['b']
      assert_equal 25, user_value.age
      assert_equal 'Jane', user_value.name
      assert_equal 'female', user_value.gender
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_same_udt_different_keyspaces
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # First insert UDT in primary keyspace
      session.execute("CREATE TYPE user (age int, name text)")
      session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

      session.execute("INSERT INTO mytable (a, b) VALUES (0, {age: 30, name: 'John'})")
      user_value = session.execute("SELECT b FROM mytable where a=0")
      assert_equal 1, user_value.size

      # Switch to secondary keyspace and insert UDT
      session.execute("CREATE KEYSPACE simplex2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
      session.execute("USE simplex2")
      session.execute("CREATE TYPE user (state text, is_cool boolean)")
      session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

      session.execute("INSERT INTO mytable (a, b) VALUES (0, {state: 'CA', is_cool: true})")
      user_value = session.execute("SELECT b FROM mytable where a=0")
      assert_equal 1, user_value.size
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_partial_udts
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      session.execute("CREATE TYPE user (age int, name text, gender text)")
      session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

      # Test non-prepared statements
      session.execute("INSERT INTO mytable (a, b) VALUES (0, {age: 30, name: 'John'})")
      user_value = session.execute("SELECT b FROM mytable where a=0").first['b']
      assert_equal 30, user_value.age
      assert_equal 'John', user_value.name
      assert_nil user_value.gender

      session.execute("INSERT INTO mytable (a, b) VALUES (1, {name: 'Jane'})")
      user_value = session.execute("SELECT b FROM mytable where a=1").first['b']
      assert_nil user_value.age
      assert_equal 'Jane', user_value.name
      assert_nil user_value.gender

      # Test prepared statements
      insert = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
      session.execute(insert, arguments: [2, Cassandra::UDT.new(age: 35, name: 'James')])
      session.execute(insert, arguments: [3, Cassandra::UDT.new(name: 'Jess')])

      user_value = session.execute("SELECT b FROM mytable where a=2").first['b']
      assert_equal 35, user_value.age
      assert_equal 'James', user_value.name
      assert_nil user_value.gender

      user_value = session.execute("SELECT b FROM mytable where a=3").first['b']
      assert_nil user_value.age
      assert_equal 'Jess', user_value.name
      assert_nil user_value.gender
    ensure
      cluster && cluster.close
    end
  end

  def test_raise_error_on_nonexistent_udt_field
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      session.execute("CREATE TYPE user (age int, name text, gender text)")
      session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

      assert_raises(Cassandra::Errors::InvalidError) do
        session.execute("INSERT INTO mytable (a, b) VALUES (0, {bad_field: 30})")
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_udts_with_varying_lengths
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the UDT
      max_udt_length = 1024
      udt = (0...max_udt_length).map do |letter|
        "v_#{letter} bigint"
      end.join(',')

      session.execute("CREATE TYPE lengthy_udt (#{udt})")

      # Create the table
      session.execute("CREATE TABLE mytable (zz int PRIMARY KEY, a frozen<lengthy_udt>)")

      # Create the input
      input = Hash.new
      [0, max_udt_length - 1].each do |length|
        input["v_#{length}"] = length
      end

      # Insert into table
      insert = session.prepare("INSERT INTO mytable (zz, a) VALUES (?, ?)")
      session.execute(insert, arguments: [0, Cassandra::UDT.new(input)])

      # Verify UDT written correctly
      user_value = session.execute("SELECT a FROM mytable WHERE zz=0").first['a']

      [0, max_udt_length - 1].each do |length|
        assert_equal input["v_#{length}"], user_value["v_#{length}"]
      end
    ensure
      cluster && cluster.close
    end
  end

  def nested_udt_helper(udt, depth)
    if depth == 0
      udt
    else
      nested_udt_helper(udt['value'], depth - 1)
    end
  end

  def test_can_insert_nested_udts
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the UDTs
      max_udt_depth = 16
      session.execute("CREATE TYPE depth_0 (age int, name text)")

      (0...max_udt_depth).each do |depth|
        session.execute("CREATE TYPE depth_#{depth + 1} (value frozen<depth_#{depth}>)")
      end

      # Create the table
      session.execute("CREATE TABLE mytable (
                      zz int PRIMARY KEY,
                      v_0 frozen<depth_0>,
                      v_1 frozen<depth_1>,
                      v_2 frozen<depth_2>,
                      v_3 frozen<depth_3>,
                      v_#{max_udt_depth} frozen<depth_#{max_udt_depth}>
                      )")

      # Create the input
      udts = [Cassandra::UDT.new(age: 30, name: 'John')]
      (1..max_udt_depth).each do |depth|
        udts.push(Cassandra::UDT.new(value: udts[depth - 1]))
      end

      # Insert into table
      [0, 1, 2, 3, max_udt_depth].each do |depth|
        input = udts[depth]
        insert = session.prepare("INSERT INTO mytable (zz, v_#{depth}) VALUES (0, ?)")
        session.execute(insert, arguments: [input])

        user_value = session.execute("SELECT v_#{depth} FROM mytable WHERE zz=0").first
        expected = nested_udt_helper(input, depth)
        actual = nested_udt_helper(user_value["v_#{depth}"], depth)

        assert_equal expected['age'], actual['age']
        assert_equal expected['name'], actual['name']
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_udt_all_datatypes_nil
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the UDT and table
      alpha_type_list = DatatypeUtils.primitive_datatypes.zip('a'..'z').map do |datatype, letter|
        "#{letter} #{datatype}"
      end

      session.execute("CREATE TYPE alldatatypes (#{alpha_type_list.join(",")})")
      session.execute("CREATE TABLE mytable (zz int PRIMARY KEY, a frozen<alldatatypes>)")

      # Create the input
      input = Hash.new
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter|
        input["#{letter}"] = nil
      end

      # Insert into table
      insert = session.prepare("INSERT INTO mytable (zz, a) VALUES (?, ?)")
      session.execute(insert, arguments: [0, Cassandra::UDT.new(input)])

      # Verify results
      result = session.execute("SELECT * FROM mytable").first['a']

      DatatypeUtils.primitive_datatypes.zip('a'..'z').map do |datatype, letter|
        assert_equal input[letter], result[letter]
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_udt_all_datatypes
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the UDT and table
      alpha_type_list = DatatypeUtils.primitive_datatypes.zip('a'..'z').map do |datatype, letter|
        "#{letter} #{datatype}"
      end

      session.execute("CREATE TYPE alldatatypes (#{alpha_type_list.join(",")})")
      session.execute("CREATE TABLE mytable (zz int PRIMARY KEY, a frozen<alldatatypes>)")

      # Create the input
      input = Hash.new
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter|
        input["#{letter}"] = DatatypeUtils.get_sample(datatype)
      end

      # Insert into table
      insert = session.prepare("INSERT INTO mytable (zz, a) VALUES (?, ?)")
      session.execute(insert, arguments: [0, Cassandra::UDT.new(input)])

      # Verify results
      result = session.execute("SELECT * FROM mytable").first['a']

      DatatypeUtils.primitive_datatypes.zip('a'..'z').map do |datatype, letter|
        assert_equal input[letter], result[letter]
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_udt_all_collection_datatypes_nil
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the UDT and table
      alpha_type_list = []
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          if collection_type == 'Map'
            alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}, #{datatype}>")
          elsif collection_type == 'Tuple'
            alpha_type_list.push("#{letter1}_#{letter2} frozen<#{collection_type}<#{datatype}>>")
          else
            alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}>")
          end
        end
      end

      session.execute("CREATE TYPE alldatatypes (#{alpha_type_list.join(",")})")
      session.execute("CREATE TABLE mytable (zz int PRIMARY KEY, a frozen<alldatatypes>)")

      # Create the input
      input = Hash.new
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          input["#{letter1}_#{letter2}"] = nil
        end
      end

      # Insert into table
      insert = session.prepare("INSERT INTO mytable (zz, a) VALUES (?, ?)")
      session.execute(insert, arguments: [0, Cassandra::UDT.new(input)])

      # Verify results
      result = session.execute("SELECT * FROM mytable").first['a']

      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          assert_equal input["#{letter1}_#{letter2}"], result["#{letter1}_#{letter2}"]
        end
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_udt_all_collection_datatypes
    skip("UDTs are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the UDT and table
      alpha_type_list = []
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          if collection_type == 'Map'
            alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}, #{datatype}>")
          elsif collection_type == 'Tuple'
            alpha_type_list.push("#{letter1}_#{letter2} frozen<#{collection_type}<#{datatype}>>")
          else
            alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}>")
          end
        end
      end

      session.execute("CREATE TYPE alldatatypes (#{alpha_type_list.join(",")})")
      session.execute("CREATE TABLE mytable (zz int PRIMARY KEY, a frozen<alldatatypes>)")

      # Create the input
      input = Hash.new
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          input["#{letter1}_#{letter2}"] = DatatypeUtils.get_collection_sample(collection_type, datatype)
        end
      end

      # Insert into table
      insert = session.prepare("INSERT INTO mytable (zz, a) VALUES (?, ?)")
      session.execute(insert, arguments: [0, Cassandra::UDT.new(input)])

      # Verify results
      result = session.execute("SELECT * FROM mytable").first['a']

      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          assert_equal input["#{letter1}_#{letter2}"], result["#{letter1}_#{letter2}"]
        end
      end
    ensure
      cluster && cluster.close
    end
  end

end