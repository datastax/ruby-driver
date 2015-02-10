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

class DatatypeTest < IntegrationTestCase

  def setup
    @@ccm_cluster.setup_schema("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
  end

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
    insert = session.prepare("INSERT INTO mytable (zz)
            VALUES (?)")
    session.execute(insert, arguments: [0])

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.delete("zz")
    result.each_value do |actual|
      assert_nil actual
    end
  ensure
    cluster.close
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
    DatatypeUtils.primitive_datatypes.each do |datatype|
      params.push(nil)
    end

    # Insert into table
    parameters = ["zz"]
    parameters.push(('a'..'o').to_a)
    arguments = []
    16.times { arguments.push('?') }

    insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
            VALUES (#{arguments.join(",")})")
    session.execute(insert, arguments: params)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster.close
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
    parameters.push(('a'..'o').to_a)
    arguments = []
    16.times { arguments.push('?') }

    insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
            VALUES (#{arguments.join(",")})")
    session.execute(insert, arguments: params)

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
        elsif collection_type == 'Tuple'
        else
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}>")
        end
      end
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Insert into table
    insert = session.prepare("INSERT INTO mytable (zz)
            VALUES (?)")
    session.execute(insert, arguments: [0])

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.delete("zz")
    result.each_value do |actual|
      assert_nil actual
    end
  ensure
    cluster.close
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
        elsif collection_type == 'Tuple'
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
        unless collection_type == 'Tuple'
          params.push(nil)
        end
      end
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
        unless collection_type == 'Tuple'
          parameters.push("#{letter1}_#{letter2}")
        end
      end
    end

    arguments = []
    46.times { arguments.push('?') }

    insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
            VALUES (#{arguments.join(",")})")
    session.execute(insert, arguments: params)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster.close
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
        elsif collection_type == 'Tuple'
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
        unless collection_type == 'Tuple'
          params.push(DatatypeUtils.get_collection_sample(collection_type, datatype))
        end
      end
    end

    # Insert into table
    parameters = ["zz"]
    DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
        unless collection_type == 'Tuple'
          parameters.push("#{letter1}_#{letter2}")
        end
      end
    end

    arguments = []
    46.times { arguments.push('?') }

    insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
            VALUES (#{arguments.join(",")})")
    session.execute(insert, arguments: params)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end
  ensure
    cluster.close
  end

  def test_can_insert_tuple_type
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<tuple<ascii, bigint, boolean>>)")

      # Test non-prepared statement
      complete = ['foo', 123, true]
      session.execute("INSERT INTO mytable (a, b) VALUES (0, (?, ?, ?))", arguments: complete)
      result = session.execute("SELECT b FROM mytable WHERE a=0").first
      assert_equal complete, result['b']

      # Test partial tuples
      partial = ['foo', 123]
      session.execute("INSERT INTO mytable (a, b) VALUES (1, (?, ?))", arguments: partial)
      result = session.execute("SELECT b FROM mytable WHERE a=1").first
      assert_equal partial.push(nil), result['b']

      subpartial = ['foo']
      session.execute("INSERT INTO mytable (a, b) VALUES (2, (?))", arguments: subpartial)
      result = session.execute("SELECT b FROM mytable WHERE a=2").first
      assert_equal subpartial.push(nil).push(nil), result['b']

      # Test prepared statement
      insert = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
      session.execute(insert, arguments: [3, complete])
      session.execute(insert, arguments: [4, partial[0..1]])
      session.execute(insert, arguments: [5, [subpartial[0]]])

      result = session.execute("SELECT b FROM mytable WHERE a=3").first
      assert_equal complete, result['b']
      result = session.execute("SELECT b FROM mytable WHERE a=4").first
      assert_equal partial, result['b']
      result = session.execute("SELECT b FROM mytable WHERE a=5").first
      assert_equal subpartial, result['b']
    ensure
      cluster && cluster.close
    end
  end

  def test_raise_error_on_unmatched_tuple_lengths
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<tuple<ascii, bigint, boolean>>)")
      insert = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")

      assert_raises(ArgumentError) do
        session.execute(insert, arguments: [0, ['foo', 123, true, 'extra']])
      end

      input = ['foo', 123]
      assert_raises(Cassandra::Errors::InvalidError) do
        session.execute("INSERT INTO mytable (a, b) VALUES (0, (?, ?, ?))", arguments: input)
      end

      input = ['foo', 123, true, 'extra']
      assert_raises(Cassandra::Errors::InvalidError) do
        session.execute("INSERT INTO mytable (a, b) VALUES (0, (?, ?, ?))", arguments: input)
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_tuples_with_varying_lengths
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
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
        input = (0...length).to_a
        session.execute("INSERT INTO mytable (zz, #{letter}) VALUES (0, (#{(['?']*length).join(',')}))", arguments: input)
        result = session.execute("SELECT #{letter} FROM mytable WHERE zz=0").first
        assert_equal input, result["#{letter}"]

        bigger_input = (0..length).to_a
        assert_raises(Cassandra::Errors::InvalidError) do
          session.execute("INSERT INTO mytable (zz, #{letter}) VALUES (0, (#{(['?']*length).join(',')}))", arguments: bigger_input)
        end
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_tuple_type_nil_values
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
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
        params.push(nil)
      end

      # Insert into table
      parameters = ["zz"]
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter|
        parameters.push("a_#{letter}")
      end

      arguments = []
      (DatatypeUtils.primitive_datatypes.size+1).times { arguments.push('?') }

      insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
                VALUES (#{arguments.join(",")})")
      session.execute(insert, arguments: params)

      # Verify results
      result = session.execute("SELECT * FROM mytable").first
      result.each_value.zip(params) do |actual, expected|
        assert_equal expected, actual
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_tuple_type_all_datatypes
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
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
      DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter|
        parameters.push("a_#{letter}")
      end

      arguments = []
      (DatatypeUtils.primitive_datatypes.size+1).times { arguments.push('?') }

      insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
              VALUES (#{arguments.join(",")})")
      session.execute(insert, arguments: params)

      # Verify results
      result = session.execute("SELECT * FROM mytable").first
      result.each_value.zip(params) do |actual, expected|
        assert_equal expected, actual
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_tuple_type_all_collection_datatypes_nil
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the table
      alpha_type_list = ["zz int PRIMARY KEY"]
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          if collection_type == 'Map'
            alpha_type_list.push("#{letter1}_#{letter2} frozen<tuple<#{collection_type}<#{datatype}, #{datatype}>>>")
          elsif collection_type == 'Tuple'
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
          unless collection_type == 'Tuple'
            params.push([DatatypeUtils.get_collection_sample(collection_type, datatype)])
          end
        end
      end

      # Insert into table
      parameters = ["zz"]
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          unless collection_type == 'Tuple'
            parameters.push("#{letter1}_#{letter2}")
          end
        end
      end

      arguments = []
      ((DatatypeUtils.collection_types.size-1)*DatatypeUtils.primitive_datatypes.size+1).times { arguments.push('?') }

      insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
              VALUES (#{arguments.join(",")})")
      session.execute(insert, arguments: params)

      # Verify results
      result = session.execute("SELECT * FROM mytable").first
      result.each_value.zip(params) do |actual, expected|
        assert_equal expected, actual
      end
    ensure
      cluster && cluster.close
    end
  end

  def test_can_insert_tuple_type_all_collection_datatypes
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the table
      alpha_type_list = ["zz int PRIMARY KEY"]
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          if collection_type == 'Map'
            alpha_type_list.push("#{letter1}_#{letter2} frozen<tuple<#{collection_type}<#{datatype}, #{datatype}>>>")
          elsif collection_type == 'Tuple'
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
          unless collection_type == 'Tuple'
            params.push(nil)
          end
        end
      end

      # Insert into table
      parameters = ["zz"]
      DatatypeUtils.collection_types.zip('a'..'z').each do |collection_type, letter1|
        DatatypeUtils.primitive_datatypes.zip('a'..'z').each do |datatype, letter2|
          unless collection_type == 'Tuple'
            parameters.push("#{letter1}_#{letter2}")
          end
        end
      end

      arguments = []
      ((DatatypeUtils.collection_types.size-1)*DatatypeUtils.primitive_datatypes.size+1).times { arguments.push('?') }

      insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
              VALUES (#{arguments.join(",")})")
      session.execute(insert, arguments: params)

      # Verify results
      result = session.execute("SELECT * FROM mytable").first
      result.each_value.zip(params) do |actual, expected|
        assert_equal expected, actual
      end
    ensure
      cluster && cluster.close
    end
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
      [nested_tuples_creator_helper(depth - 1)]
    end
  end

  def test_can_insert_nested_tuples
    skip("Tuples are only available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Create the table
      lengths = [1, 2, 3, 128]
      alpha_type_list = ["zz int PRIMARY KEY"]
      lengths.zip('a'..'z') do |depth, letter|
        alpha_type_list.push("#{letter} frozen<#{nested_tuples_schema_helper(depth)}>")
      end

      session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

      # Insert into table
      lengths.zip('a'..'z') do |depth, letter|
        insert = session.prepare("INSERT INTO mytable (zz, #{letter}) VALUES (0, (?))")
        input = nested_tuples_creator_helper(depth)
        session.execute(insert, arguments: input)

        result = session.execute("SELECT #{letter} FROM mytable WHERE zz=0").first
        assert_equal input, result["#{letter}"]
      end
    ensure
      cluster && cluster.close
    end
  end

end