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

  # Test for inserting udts inside collections
  #
  # test_can_insert_collection_datatypes_udts tests that UDTs can be inserted into collection datatypes.
  # It first creates a simple UDT. It then creates a table that has a mapping of each collection type to
  # the created udt (such as List<frozen<udt>>). It then generates all data parameters and inserts the values
  # into Cassandra. Finally, it verifies that all inserted elements match the resulting output.
  #
  # @since 2.1.4
  # @jira_ticket RUBY-94
  # @expected_result Collection type with nested udts should be successfully inserted into the table
  #
  # @test_assumptions A Cassandra cluster with version 2.1.3 or higher.
  # @test_category data_types:udt
  #
  def test_can_insert_collection_datatypes_udts
    skip("Nested collections are only available in C* after 2.1.3") if CCM.cassandra_version < '2.1.3'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the type
      session.execute("CREATE TYPE udt (age int, name text)")

      # Create the table
      alpha_type_list = ["zz int PRIMARY KEY"]
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        if collection_type == 'Map'
          alpha_type_list.push("#{letter1} #{collection_type}<frozen<udt>, frozen<udt>>")
        else
          alpha_type_list.push("#{letter1} #{collection_type}<frozen<udt>>")
        end
      end

      session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

      # Create the input
      udt_input = Cassandra::UDT.new(age: 42, name: "John")

      params = [0]
      DatatypeUtils.collection_types.each do |collection_type|
        if collection_type == 'Map'
          params.push({udt_input => udt_input})
        elsif collection_type == 'List'
          params.push([udt_input])
        elsif collection_type == 'Set'
          params.push(Set.new([udt_input]))
        else
          params.push(Cassandra::Tuple.new(udt_input))
        end
      end

      # Insert into table
      parameters = ["zz"]
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        parameters.push("#{letter1}")
      end

      arguments = []
      (DatatypeUtils.collection_types.size+1).times { arguments.push('?') }

      insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
              VALUES (#{arguments.join(",")})")
      session.execute(insert, arguments: params)

      # Verify results
      result = session.execute("SELECT * FROM mytable").first
      result.each_value.zip(params) do |actual, expected|
        if expected.is_a?(Hash) && expected.keys.first.is_a?(Cassandra::UDT)
          next # Temporary fix until RUBY-120 is resolved
        elsif expected.is_a?(Set) && expected.first.is_a?(Cassandra::UDT)
          next # Temporary fix until RUBY-120 is resolved
        else
          assert_equal expected, actual
        end
      end
    ensure
      cluster && cluster.close
    end
  end

  # Test for inserting nested collections inside udts
  #
  # test_can_insert_udt_nested_collection_datatypes tests that nested collection datatypes can be inserted into
  # UDTs. It first creates a UDT that has a mapping of each collection type to each collection
  # type (such as List<frozen<List<int>>>). It then generates all data parameters and inserts the values
  # into Cassandra. Finally, it verifies that all inserted elements match the resulting output.
  #
  # @since 2.1.4
  # @jira_ticket RUBY-94
  # @expected_result UDTs with nested collection types should be successfully inserted into the table
  #
  # @test_assumptions A Cassandra cluster with version 2.1.3 or higher.
  # @test_category data_types:udt
  #
  def test_can_insert_udt_nested_collection_datatypes
    skip("Nested collections are only available in C* after 2.1.3") if CCM.cassandra_version < '2.1.3'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the UDT and table
      alpha_type_list = []
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type2, letter2|
          if collection_type2 == 'Map'
            if collection_type == 'Map'
              alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<frozen<#{collection_type2}<int,int>>,
                                    frozen<#{collection_type2}<int,int>>>")
            else
              alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<frozen<#{collection_type2}<int,int>>>")
            end
          elsif collection_type == 'Map'
            alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<frozen<#{collection_type2}<int>>,
                                  frozen<#{collection_type2}<int>>>")
          else
            alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<frozen<#{collection_type2}<int>>>")
          end
        end
      end

      session.execute("CREATE TYPE nesteddatatypes (#{alpha_type_list.join(",")})")
      session.execute("CREATE TABLE mytable (zz int PRIMARY KEY, a frozen<nesteddatatypes>)")

      # Create the input
      input = Hash.new
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type2, letter2|
          if collection_type == 'Map'
            input["#{letter1}_#{letter2}"] = {DatatypeUtils.get_collection_sample(collection_type2, 'int') =>
                                              DatatypeUtils.get_collection_sample(collection_type2, 'int')}
          elsif collection_type == 'List'
            input["#{letter1}_#{letter2}"] = [DatatypeUtils.get_collection_sample(collection_type2, 'int')]
          elsif collection_type == 'Set'
            input["#{letter1}_#{letter2}"] = Set.new([DatatypeUtils.get_collection_sample(collection_type2, 'int')])
          else
            input["#{letter1}_#{letter2}"] = Cassandra::Tuple.new(DatatypeUtils.get_collection_sample(collection_type2, 'int'))
          end
        end
      end

      # Insert into table
      insert = session.prepare("INSERT INTO mytable (zz, a) VALUES (?, ?)")
      session.execute(insert, arguments: [0, Cassandra::UDT.new(input)])

      # Verify results
      result = session.execute("SELECT * FROM mytable").first['a']

      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          if input["#{letter1}_#{letter2}"].is_a?(Hash) && input["#{letter1}_#{letter2}"].keys.first.is_a?(Cassandra::Tuple)
            next # Temporary fix until RUBY-120 is resolved
          elsif input["#{letter1}_#{letter2}"].is_a?(Set) && input["#{letter1}_#{letter2}"].first.is_a?(Cassandra::Tuple)
            next # Temporary fix until RUBY-120 is resolved
          else
            assert_equal input["#{letter1}_#{letter2}"], result["#{letter1}_#{letter2}"]
          end
        end
      end
    ensure
      cluster && cluster.close
    end
  end

end