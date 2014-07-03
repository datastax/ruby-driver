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

module Cassandra
  module Protocol
    class RowsResultResponse < ResultResponse
      attr_reader :rows, :metadata, :paging_state

      def initialize(rows, metadata, paging_state, trace_id)
        super(trace_id)
        @rows, @metadata, @paging_state = rows, metadata, paging_state
      end

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        original_buffer_length = buffer.length
        column_specs, columns_count, paging_state = read_metadata(protocol_version, buffer)
        if column_specs.nil?
          consumed_bytes = original_buffer_length - buffer.length
          remaining_bytes = CqlByteBuffer.new(buffer.read(length - consumed_bytes))
          RawRowsResultResponse.new(protocol_version, remaining_bytes, paging_state, trace_id)
        else
          new(read_rows(protocol_version, buffer, column_specs), column_specs, paging_state, trace_id)
        end
      end

      def to_s
        %(RESULT ROWS #@metadata #@rows)
      end

      private

      RESULT_TYPES[0x02] = self

      COLUMN_TYPES = [
        nil,
        :ascii,
        :bigint,
        :blob,
        :boolean,
        :counter,
        :decimal,
        :double,
        :float,
        :int,
        :text,
        :timestamp,
        :uuid,
        :varchar,
        :varint,
        :timeuuid,
        :inet,
      ].freeze

      TYPE_CONVERTER = TypeConverter.new

      GLOBAL_TABLES_SPEC_FLAG = 0x01
      HAS_MORE_PAGES_FLAG = 0x02
      NO_METADATA_FLAG = 0x04

      MARSHAL_TYPE_MAP = {
        'org.apache.cassandra.db.marshal.AsciiType' => :ascii,
        'org.apache.cassandra.db.marshal.BooleanType' => :boolean,
        'org.apache.cassandra.db.marshal.BytesType' => :bytes,
        'org.apache.cassandra.db.marshal.CounterColumnType' => :counter,
        'org.apache.cassandra.db.marshal.DateType' => :date,
        'org.apache.cassandra.db.marshal.DecimalType' => :decimal,
        'org.apache.cassandra.db.marshal.DoubleType' => :double,
        'org.apache.cassandra.db.marshal.FloatType' => :float,
        'org.apache.cassandra.db.marshal.InetAddressType' => :inet,
        'org.apache.cassandra.db.marshal.Int32Type' => :int,
        'org.apache.cassandra.db.marshal.IntegerType' => :int,
        'org.apache.cassandra.db.marshal.ListType' => :list,
        'org.apache.cassandra.db.marshal.LongType' => :long,
        'org.apache.cassandra.db.marshal.MapType' => :map,
        'org.apache.cassandra.db.marshal.SetType' => :set,
        'org.apache.cassandra.db.marshal.TimeUUIDType' => :time_uuid,
        'org.apache.cassandra.db.marshal.TimestampType' => :timestamp,
        'org.apache.cassandra.db.marshal.UTF8Type' => :text,
        'org.apache.cassandra.db.marshal.UUIDType' => :uuid,
      }.freeze

      def self.read_column_type(buffer)
        id, type = buffer.read_option do |id, b|
          if id == 0
            decode_custom_type_description(buffer.read_string)
          elsif id > 0 && id <= 0x10
            COLUMN_TYPES[id]
          elsif id == 0x20
            sub_type = read_column_type(buffer)
            [:list, sub_type]
          elsif id == 0x21
            key_type = read_column_type(buffer)
            value_type = read_column_type(buffer)
            [:map, key_type, value_type]
          elsif id == 0x22
            sub_type = read_column_type(buffer)
            [:set, sub_type]
          else
            raise UnsupportedColumnTypeError, %(Unsupported column type: #{id})
          end
        end
        type
      end

      def self.decode_custom_type_description(description)
        parenthesis_index = description.index('(')
        type = description[0, parenthesis_index]
        if type == 'org.apache.cassandra.db.marshal.UserType'
          rest = description[parenthesis_index + 1, description.bytesize - parenthesis_index - 2]
          skip_index = rest.index(',', rest.index(',') + 1)
          field_descriptions = rest[skip_index + 1, rest.bytesize - skip_index]
          [:udt, decode_fields({}, field_descriptions)]
        else
          [:custom, description]
        end
      end

      def self.decode_fields(acc, str)
        next_index = nil
        if (index = str.index(','))
          field_description = str[0, index]
          next_index = index + 1
        else
          field_description = str
          next_index = str.bytesize
        end
        name, type = field_description.split(':')
        name = [name].pack('H*')
        if (parenthesis_index = type.index('('))
          subtype = type[parenthesis_index + 1, type.bytesize - parenthesis_index - 2]
          type = type[0, parenthesis_index]
          type = [MARSHAL_TYPE_MAP[type], MARSHAL_TYPE_MAP[subtype]]
        else
          type = MARSHAL_TYPE_MAP[type]
        end
        acc[name] = type
        if next_index == str.bytesize
          acc
        else
          decode_fields(acc, str[next_index, str.bytesize - next_index])
        end
      end

      def self.read_metadata(protocol_version, buffer)
        flags = buffer.read_int
        columns_count = buffer.read_int
        paging_state = nil
        column_specs = nil
        if flags & HAS_MORE_PAGES_FLAG != 0
          paging_state = buffer.read_bytes
        end
        if flags & NO_METADATA_FLAG == 0
          if flags & GLOBAL_TABLES_SPEC_FLAG != 0
            global_keyspace_name = buffer.read_string
            global_table_name = buffer.read_string
          end
          column_specs = columns_count.times.map do
            if global_keyspace_name
              keyspace_name = global_keyspace_name
              table_name = global_table_name
            else
              keyspace_name = buffer.read_string
              table_name = buffer.read_string
            end
            column_name = buffer.read_string
            type = read_column_type(buffer)
            [keyspace_name, table_name, column_name, type]
          end
        end
        [column_specs, columns_count, paging_state]
      end

      def self.read_rows(protocol_version, buffer, column_specs)
        rows_count = buffer.read_int
        rows = []
        rows_count.times do |row_index|
          row = {}
          column_specs.each do |column_spec|
            row[column_spec[2]] = TYPE_CONVERTER.from_bytes(buffer, column_spec[3])
          end
          rows << row
        end
        rows
      end
    end
  end
end
