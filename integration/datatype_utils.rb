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

class DatatypeUtils

  def self.primitive_datatypes
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

  def self.collection_types
    [ 'List',
      'Map',
      'Set',
      'Tuple'
    ]
  end

  def self.get_sample(datatype)
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
      when 'timestamp' then Time.at(1358013521, 123000)
      when 'timeuuid' then Cassandra::TimeUuid.new('FE2B4360-28C6-11E2-81C1-0800200C9A66')
      when 'uuid' then Cassandra::Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66')
      when 'varchar' then 'varchar'
      when 'varint' then 67890656781923123918798273492834712837198237
      else raise "Missing handling of: " + datatype
    end
  end

  def self.get_collection_sample(complex_type, datatype)
    case complex_type
      when 'List' then [get_sample(datatype), get_sample(datatype)]
      when 'Set' then Set.new([get_sample(datatype)])
      when 'Map' then
        if datatype == 'blob'
          {get_sample('ascii') => get_sample(datatype)}
        else
          {get_sample(datatype) => get_sample(datatype)}
        end
      when 'Tuple' then Cassandra::Tuple.new(get_sample(datatype))
      else raise "Missing handling of non-primitive type: " + complex_type
    end
  end

end