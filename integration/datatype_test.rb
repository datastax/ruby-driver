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

class DatatypeTest < IntegrationTestCase

  def primitive_datatypes
    [ 'ascii',
      'bigint',
      'blob',
      'boolean',
      'decimal',
      'double',
      'float',
      'inet',
      'int',
      'text',
      'timestamp',
      'timeuuid',
      'uuid',
      'varchar',
      'varint'
    ]
  end

  def collection_types
    [ 'List',
      'Map',
      'Set'
    ]
  end

  def get_sample(datatype)
    case datatype
    when 'ascii' then 'ascii'
    when 'bigint' then 765438000
    when 'blob' then '0x626c6f62'
    when 'boolean' then true
    when 'decimal' then BigDecimal.new('1313123123.234234234234234234123')
    when 'double' then 3.141592653589793
    when 'float' then 1.25
    when 'inet' then IPAddr.new('200.199.198.197')
    when 'int' then 4
    when 'text' then 'text'
    when 'timestamp' then Time.at(1358013521.123)
    when 'timeuuid' then Cassandra::Uuid.new('FE2B4360-28C6-11E2-81C1-0800200C9A66')
    when 'uuid' then Cassandra::Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66')
    when 'varchar' then 'varchar'
    when 'varint' then 67890656781923123918798273492834712837198237
    else raise "Missing handling of: " + datatype
    end
  end

  def get_collection_sample(complex_type, datatype)
    case complex_type
    when 'List' then [get_sample(datatype), get_sample(datatype)]
    when 'Set' then Set.new([get_sample(datatype)])
    when 'Map' then
        if datatype == 'blob'
            {get_sample('ascii') => get_sample(datatype)}
        else
            {get_sample(datatype) => get_sample(datatype)}
        end
    else raise "Missing handling of non-primitive type: " + complex_type
    end
  end

  def test_all_primitive_datatypes_initially_nil
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    primitive_datatypes.zip('a'..'z') do |datatype, letter|
      alpha_type_list.push("#{letter} #{datatype}")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Insert into table
    insert = session.prepare("INSERT INTO mytable (zz)
            VALUES (?)")
    session.execute(insert, 0)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.delete("zz")
    result.each_value do |actual|
      assert_nil actual
    end

    cluster.close
  end

  def test_can_insert_all_primitive_datatypes_nil_values
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    primitive_datatypes.zip('a'..'z') do |datatype, letter|
      alpha_type_list.push("#{letter} #{datatype}")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    primitive_datatypes.each do |datatype|
      params.push(nil)
    end

    # Insert into table
    parameters = ["zz"]
    parameters.push(('a'..'o').to_a)
    arguments = []
    16.times { arguments.push('?') }

    insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
            VALUES (#{arguments.join(",")})")
    session.execute(insert, *params)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end

    cluster.close
  end

  def test_can_insert_each_primitive_datatype
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    primitive_datatypes.zip('a'..'z') do |datatype, letter|
      alpha_type_list.push("#{letter} #{datatype}")
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Create the input
    params = [0]
    primitive_datatypes.each do |datatype|
      params.push(get_sample(datatype))
    end

    # Insert into table
    parameters = ["zz"]
    parameters.push(('a'..'o').to_a)
    arguments = []
    16.times { arguments.push('?') }

    insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
            VALUES (#{arguments.join(",")})")
    session.execute(insert, *params)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end

    cluster.close
  end

  def test_all_collection_types_initially_nil
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    collection_types.zip('a'..'z').each do |collection_type, letter1|
      primitive_datatypes.zip('a'..'z').each do |datatype, letter2|    
        if collection_type == 'Map'
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}, #{datatype}>")
        else
          alpha_type_list.push("#{letter1}_#{letter2} #{collection_type}<#{datatype}>")
        end
      end
    end

    session.execute("CREATE TABLE mytable (#{alpha_type_list.join(",")})")

    # Insert into table
    insert = session.prepare("INSERT INTO mytable (zz)
            VALUES (?)")
    session.execute(insert, 0)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.delete("zz")
    result.each_value do |actual|
      assert_nil actual
    end

    cluster.close
  end

  def test_can_insert_all_collection_types_nil_values
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    collection_types.zip('a'..'z').each do |collection_type, letter1|
      primitive_datatypes.zip('a'..'z').each do |datatype, letter2|    
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
    collection_types.each do |collection_type|
      primitive_datatypes.each do |datatype|
        params.push(nil)
      end
    end

    # Insert into table
    parameters = ["zz"]
    collection_types.zip('a'..'z').each do |collection_type, letter1|
      primitive_datatypes.zip('a'..'z').each do |datatype, letter2|   
        parameters.push("#{letter1}_#{letter2}")
      end
    end

    arguments = []
    46.times { arguments.push('?') }

    insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
            VALUES (#{arguments.join(",")})")
    session.execute(insert, *params)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end

    cluster.close
  end

  def test_can_insert_each_collection_type
    cluster = Cassandra.connect
    session = cluster.connect()
    session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("USE simplex")

    # Create the table
    alpha_type_list = ["zz int PRIMARY KEY"]
    collection_types.zip('a'..'z').each do |collection_type, letter1|
      primitive_datatypes.zip('a'..'z').each do |datatype, letter2|    
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
    collection_types.each do |collection_type|
      primitive_datatypes.each do |datatype|
        params.push(get_collection_sample(collection_type, datatype))
      end
    end

    # Insert into table
    parameters = ["zz"]
    collection_types.zip('a'..'z').each do |collection_type, letter1|
      primitive_datatypes.zip('a'..'z').each do |datatype, letter2|   
        parameters.push("#{letter1}_#{letter2}")
      end
    end

    arguments = []
    46.times { arguments.push('?') }

    insert = session.prepare("INSERT INTO mytable (#{parameters.join(",")})
            VALUES (#{arguments.join(",")})")
    session.execute(insert, *params)

    # Verify results
    result = session.execute("SELECT * FROM mytable").first
    result.each_value.zip(params) do |actual, expected|
      assert_equal expected, actual
    end

    cluster.close
  end
end