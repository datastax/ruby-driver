# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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

class DatatypeTest < IntegrationTestCase

  def setup
    @@ccm_cluster.setup_schema("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
  end

  # Test for assuring unassigned primitive datatypes are nil
  #
  # test_all_primitive_datatypes_initially_nil creates a table with each datatype. It
  # then inserts an "empty" row with only the primary key. Finally, it verifies with a read
  # that each cell (each datatype) in the row is initialized to nil.
  #
  # @since 1.0.0
  # @expected_result Each datatype should be initialized nil.
  #
  # @test_assumptions "primitive_datatypes" list of primitive datatypes.
  # @test_category data_types:primitive
  #
  def test_all_primitive_datatypes_initially_nil
    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z') do |datatype, letter|
      alpha_type_list.push("#{letter} #{datatype}")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Insert into table
    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (zz) VALUES (?)") }
    Retry.with_attempts(5) { session.execute(insert, arguments: [0]) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.delete("zz")
    result.each_value do |actual|
      assert_nil actual
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_all_primitive_datatypes_nil_values
    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z') do |datatype, letter|
      alpha_type_list.push("#{letter} #{datatype}")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.primitive_datatypes.each do
      params.push(nil)
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z') do |_, letter|
      parameters.push(letter)
    end

    arguments = []
    (DatatypeUtils.primitive_datatypes.size + 1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_each_primitive_datatype
    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z') do |datatype, letter|
      alpha_type_list.push("#{letter} #{datatype}")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.primitive_datatypes.each do |datatype|
      params.push(DatatypeUtils.get_sample(datatype))
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z') do |_, letter|
      parameters.push(letter)
    end

    arguments = []
    (DatatypeUtils.primitive_datatypes.size + 1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster.close
  end

  def test_all_collection_types_initially_nil
    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
        if collection_type == 'Map'
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}, #{datatype}>")
        else
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}>")
        end
      end
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Insert into table
    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (zz) VALUES (?)") }
    Retry.with_attempts(5) { session.execute(insert, arguments: [0]) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.delete("zz")
    result.each_value do |actual|
      assert_nil actual
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_all_collection_types_nil_values
    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
        if collection_type == 'Map'
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}, #{datatype}>")
        else
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}>")
        end
      end
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.collection_types.each do |_|
      DatatypeUtils.primitive_datatypes.each do |_|
        params.push(nil)
      end
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |_, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |_, letter2|
        parameters.push("#{letter1}_#{letter2}")
      end
    end

    arguments = []
    (DatatypeUtils.collection_types.size*DatatypeUtils.primitive_datatypes.size + 1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_each_collection_type
    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
        if collection_type == 'Map'
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}, #{datatype}>")
        else
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}>")
        end
      end
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.collection_types.each do |collection_type|
      DatatypeUtils.primitive_datatypes.each do |datatype|
        params.push(DatatypeUtils.get_collection_sample(collection_type, datatype))
      end
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |_, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |_, letter2|
        parameters.push("#{letter1}_#{letter2}")
      end
    end

    arguments = []
    (DatatypeUtils.collection_types.size*DatatypeUtils.primitive_datatypes.size + 1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_tuple_type
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<tuple<ascii, bigint, boolean>>)")

    # Test non-prepared statement
    complete = Cassandra::Tuple.new('foo', 123, true)
    session.execute("INSERT INTO mytable (a, b) VALUES (0, ?)", arguments: [complete])
    result = session.execute("SELECT b FROM mytable WHERE a=0").first
    assert_equal complete, complete
    assert_equal complete, result['b']

    # Test partial tuples
    partial = Cassandra::Tuple.new('foo', 123)
    session.execute("INSERT INTO mytable (a, b) VALUES (1, ?)", arguments: [partial])
    result = session.execute("SELECT b FROM mytable WHERE a=1").first
    assert_equal Cassandra::Tuple.new(*(partial.to_a << nil)), result['b']

    subpartial = Cassandra::Tuple.new('foo')
    session.execute("INSERT INTO mytable (a, b) VALUES (2, ?)", arguments: [subpartial])
    result = session.execute("SELECT b FROM mytable WHERE a=2").first
    assert_equal Cassandra::Tuple.new(*(subpartial.to_a << nil << nil)), result['b']

    # Test prepared statement
    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (a, b) VALUES (?, ?)") }
    Retry.with_attempts(5) { session.execute(insert, arguments: [3, complete]) }
    Retry.with_attempts(5) { session.execute(insert, arguments: [4, partial]) }
    Retry.with_attempts(5) { session.execute(insert, arguments: [5, subpartial]) }

    result = session.execute("SELECT b FROM mytable WHERE a=3").first
    assert_equal complete, result['b']
    result = session.execute("SELECT b FROM mytable WHERE a=4").first
    assert_equal Cassandra::Tuple.new(*(partial.to_a << nil)), result['b']
    result = session.execute("SELECT b FROM mytable WHERE a=5").first
    assert_equal Cassandra::Tuple.new(*(subpartial.to_a << nil << nil)), result['b']
  ensure
    cluster && cluster.close
  end

  def test_raise_error_on_unmatched_tuple_lengths
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<tuple<ascii, bigint, boolean>>)")
    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (a, b) VALUES (?, ?)") }

    # Tuple with extra value
    assert_raises(ArgumentError) do
      session.execute(insert, arguments: [0, Cassandra::Tuple.new('foo', 123, true, 'extra')])
    end

    # Mismatched types in tuple
    assert_raises(ArgumentError) do
      session.execute(insert, arguments: [0, Cassandra::Tuple.new(true, 123, 'foo')])
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_tuples_with_varying_lengths
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    lengths = [1, 2, 3, 384]
    alpha_type_list = ["zz int PRIMARY KEY"]
    lengths.zip('a'..'z') do |length, letter|
      alpha_type_list.push("#{letter} frozen<Tuple<#{(['bigint']*length).join(',')}>>")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Insert into table
    lengths.zip('a'..'z') do |length, letter|
      input = Cassandra::Tuple.new(*(0...length).to_a)
      session.execute("INSERT INTO mytable (zz, #{letter}) VALUES (0, ?)", arguments: [input])
      result = session.execute("SELECT #{letter} FROM mytable WHERE zz=0").first
      assert_equal input, result["#{letter}"]

      bigger_input = Cassandra::Tuple.new((0..length).to_a)
      assert_raises(Cassandra::Errors::InvalidError) do
        session.execute("INSERT INTO mytable (zz, #{letter}) VALUES (0, ?)", arguments: [bigger_input])
      end
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_tuple_type_nil_values
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter|
      alpha_type_list.push("a_#{letter} frozen<Tuple<#{datatype}>>")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.primitive_datatypes.each do
      params.push(nil)
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |_, letter|
      parameters.push("a_#{letter}")
    end

    arguments = []
    (DatatypeUtils.primitive_datatypes.size + 1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_tuple_type_all_datatypes
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter|
      alpha_type_list.push("a_#{letter} frozen<Tuple<#{datatype}>>")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.primitive_datatypes.each do |datatype|
      params.push(DatatypeUtils.get_collection_sample('Tuple', datatype))
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |_, letter|
      parameters.push("a_#{letter}")
    end

    arguments = []
    (DatatypeUtils.primitive_datatypes.size + 1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_tuple_type_all_collection_datatypes_nil
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
        if collection_type == 'Map'
          alpha_type_list.push("#{letter1}_#{letter2} frozen<tuple<#{collection_type}<#{datatype}, #{datatype}>>>")
        else
          alpha_type_list.push("#{letter1}_#{letter2} frozen<tuple<#{collection_type}<#{datatype}>>>")
        end
      end
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.collection_types.each do
      DatatypeUtils.primitive_datatypes.each do
        params.push(nil)
      end
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |_, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |_, letter2|
        parameters.push("#{letter1}_#{letter2}")
      end
    end

    arguments = []
    (DatatypeUtils.collection_types.size*DatatypeUtils.primitive_datatypes.size + 1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster && cluster.close
  end

  def test_can_insert_tuple_type_all_collection_datatypes
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
        if collection_type == 'Map'
          alpha_type_list.push("#{letter1}_#{letter2} frozen<tuple<#{collection_type}<#{datatype}, #{datatype}>>>")
        else
          alpha_type_list.push("#{letter1}_#{letter2} frozen<tuple<#{collection_type}<#{datatype}>>>")
        end
      end
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.collection_types.each do |collection_type|
      DatatypeUtils.primitive_datatypes.each do |datatype|
        params.push(Cassandra::Tuple.new(DatatypeUtils.get_collection_sample(collection_type, datatype)))
      end
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |_, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |_, letter2|
        parameters.push("#{letter1}_#{letter2}")
      end
    end

    arguments = []
    ((DatatypeUtils.collection_types.size)*DatatypeUtils.primitive_datatypes.size + 1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster && cluster.close
  end

  def nested_tuples_schema_helper(depth)
    if depth == 0
      "bigint"
    else
      "tuple<#{nested_tuples_schema_helper(depth - 1)}>"
    end
  end

  def nested_tuples_creator_helper(depth)
    if depth == 0
      303
    else
      Cassandra::Tuple.new(nested_tuples_creator_helper(depth - 1))
    end
  end

  def test_can_insert_nested_tuples
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    lengths = [1, 2, 3, 128]
    alpha_type_list = ["zz int PRIMARY KEY"]
    lengths.zip('a'..'z') do |depth, letter|
      alpha_type_list.push("#{letter} frozen<#{nested_tuples_schema_helper(depth)}>")
    end

    # Occasionally returns Cassandra::Errors::ServerError, Invalid definition for comparator org.apache.cassandra.db.marshal.TupleType
    Retry.with_attempts(5, Cassandra::Errors::NoHostsAvailable) do
      session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")
    end

    # Insert into table
    lengths.zip('a'..'z') do |depth, letter|
      input = nested_tuples_creator_helper(depth)

      choice = rand(2)
      if choice == 0    # try simple statement
        session.execute("INSERT INTO mytable (zz, #{letter}) VALUES (0, ?)", arguments: [input])
      else              # try prepared statement
        insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (zz, #{letter}) VALUES (0, ?)") }
        Retry.with_attempts(5) { session.execute(insert, arguments: [input]) }
      end

      result = session.execute("SELECT #{letter} FROM mytable WHERE zz=0").first
      assert_equal input, result["#{letter}"]
    end
  ensure
    cluster && cluster.close
  end

  # Test for inserting nested collections
  #
  # test_can_insert_nested_collections tests that collection datatypes can be inserted into a Cassandra
  # cluster. It first creates a table that has a mapping of each collection type to each collection
  # type (such as List<frozen<List<int>>>). It then generates all data parameters and inserts the values
  # into Cassandra. Finally, it verifies that all inserted elements match the resulting output.
  #
  # @since 2.1.4
  # @jira_ticket RUBY-94
  # @expected_result Each nested collection type should be successfully insert into the table
  #
  # @test_assumptions A Cassandra cluster with version 2.1.3 or higher.
  # @test_category data_types:collections
  #
  def test_can_insert_nested_collections
    skip("Nested collections are only available in C* after 2.1.3") if CCM.cassandra_version < '2.1.3'

    cluster = Cassandra.cluster
    session = cluster.connect("simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
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

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    DatatypeUtils.collection_types.each do |collection_type|
      DatatypeUtils.collection_types.each do |collection_type2|
        if collection_type == 'Map'
          params.push({DatatypeUtils.get_collection_sample(collection_type2, 'int') =>
                           DatatypeUtils.get_collection_sample(collection_type2, 'int')})
        elsif collection_type == 'List'
          params.push([DatatypeUtils.get_collection_sample(collection_type2, 'int')])
        elsif collection_type == 'Set'
          params.push(Set.new([DatatypeUtils.get_collection_sample(collection_type2, 'int')]))
        else
          params.push(Cassandra::Tuple.new(DatatypeUtils.get_collection_sample(collection_type2, 'int')))
        end
      end
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |_, letter1|
      DatatypeUtils.collection_types.zip('a'..'z').each do |_, letter2|
        parameters.push("#{letter1}_#{letter2}")
      end
    end

    arguments = []
    (DatatypeUtils.collection_types.size*DatatypeUtils.collection_types.size+1).times { arguments.push('?') }

    insert = Retry.with_attempts(5) { session.prepare("INSERT INTO simplex.mytable (#{parameters.join(",")}) VALUES (#{arguments.join(",")})") }
    Retry.with_attempts(5) { session.execute(insert, arguments: params) }

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster && cluster.close
  end

end
