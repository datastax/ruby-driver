# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
    module Coder
      module_function

      GLOBAL_TABLES_SPEC_FLAG = 0x01
      HAS_MORE_PAGES_FLAG     = 0x02
      NO_METADATA_FLAG        = 0x04

      def write_values_v4(buffer, values, types, names = EMPTY_LIST)
        if values && !values.empty?
          buffer.append_short(values.size)
          values.zip(types, names) do |(value, type, name)|
            buffer.append_string(name) if name
            write_value_v4(buffer, value, type)
          end
          buffer
        else
          buffer.append_short(0)
        end
      end

      def write_list_v4(buffer, list, type)
        raw = CqlByteBuffer.new

        raw.append_int(list.size)
        list.each do |element|
          write_value_v4(raw, element, type)
        end

        buffer.append_bytes(raw)
      end

      def write_map_v4(buffer, map, key_type, value_type)
        raw = CqlByteBuffer.new

        raw.append_int(map.size)
        map.each do |key, value|
          write_value_v4(raw, key, key_type)
          write_value_v4(raw, value, value_type)
        end

        buffer.append_bytes(raw)
      end

      def write_udt_v4(buffer, value, fields)
        raw = CqlByteBuffer.new

        fields.each do |field|
          write_value_v4(raw, value[field.name], field.type)
        end

        buffer.append_bytes(raw)
      end

      def write_tuple_v4(buffer, value, members)
        raw = CqlByteBuffer.new

        members.each_with_index do |type, i|
          write_value_v4(raw, value[i], type)
        end

        buffer.append_bytes(raw)
      end

      def write_value_v4(buffer, value, type)
        if value.nil?
          buffer.append_int(-1)
          return
        end

        if NOT_SET.eql?(value)
          buffer.append_int(-2)
          return
        end

        case type.kind
        when :ascii            then write_ascii(buffer, value)
        when :bigint, :counter then write_bigint(buffer, value)
        when :blob             then write_blob(buffer, value)
        when :boolean          then write_boolean(buffer, value)
        when :custom           then write_custom(buffer, value, type)
        when :decimal          then write_decimal(buffer, value)
        when :double           then write_double(buffer, value)
        when :float            then write_float(buffer, value)
        when :int              then write_int(buffer, value)
        when :inet             then write_inet(buffer, value)
        when :timestamp        then write_timestamp(buffer, value)
        when :uuid, :timeuuid  then write_uuid(buffer, value)
        when :text             then write_text(buffer, value)
        when :varint           then write_varint(buffer, value)
        when :tinyint          then write_tinyint(buffer, value)
        when :smallint         then write_smallint(buffer, value)
        when :time             then write_time(buffer, value)
        when :date             then write_date(buffer, value)
        when :list, :set       then write_list_v4(buffer, value, type.value_type)
        when :map              then write_map_v4(buffer, value,
                                                 type.key_type,
                                                 type.value_type)
        when :udt              then write_udt_v4(buffer, value, type.fields)
        when :tuple            then write_tuple_v4(buffer, value, type.members)
        else
          raise Errors::EncodingError, %(Unsupported value type: #{type})
        end
      end

      def read_prepared_metadata_v4(buffer)
        flags         = buffer.read_int
        columns_count = buffer.read_int
        pk_count      = buffer.read_int
        pk_specs      = ::Array.new(pk_count) {|_i| buffer.read_short}

        if flags & GLOBAL_TABLES_SPEC_FLAG == GLOBAL_TABLES_SPEC_FLAG
          keyspace_name = buffer.read_string
          table_name    = buffer.read_string

          column_specs = ::Array.new(columns_count) do |_i|
            [keyspace_name, table_name, buffer.read_string, read_type_v4(buffer)]
          end
        else
          column_specs = ::Array.new(columns_count) do |_i|
            [
              buffer.read_string,
              buffer.read_string,
              buffer.read_string,
              read_type_v4(buffer)
            ]
          end
        end

        [pk_specs, column_specs]
      end

      def read_metadata_v4(buffer)
        flags = buffer.read_int
        count = buffer.read_int

        paging_state = nil
        paging_state = buffer.read_bytes if flags & HAS_MORE_PAGES_FLAG != 0
        column_specs = nil

        if flags & NO_METADATA_FLAG == 0
          if flags & GLOBAL_TABLES_SPEC_FLAG != 0
            keyspace_name = buffer.read_string
            table_name    = buffer.read_string

            column_specs = ::Array.new(count) do |_i|
              [keyspace_name, table_name, buffer.read_string, read_type_v4(buffer)]
            end
          else
            column_specs = ::Array.new(count) do |_i|
              [
                buffer.read_string,
                buffer.read_string,
                buffer.read_string,
                read_type_v4(buffer)
              ]
            end
          end
        end

        [column_specs, paging_state]
      end

      def read_type_v4(buffer)
        id = buffer.read_unsigned_short
        case id
        when 0x0000 then Types.custom(buffer.read_string)
        when 0x0001 then Types.ascii
        when 0x0002 then Types.bigint
        when 0x0003 then Types.blob
        when 0x0004 then Types.boolean
        when 0x0005 then Types.counter
        when 0x0006 then Types.decimal
        when 0x0007 then Types.double
        when 0x0008 then Types.float
        when 0x0009 then Types.int
        when 0x000B then Types.timestamp
        when 0x000C then Types.uuid
        when 0x000D then Types.text
        when 0x000E then Types.varint
        when 0x000F then Types.timeuuid
        when 0x0010 then Types.inet
        when 0x0011 then Types.date
        when 0x0012 then Types.time
        when 0x0013 then Types.smallint
        when 0x0014 then Types.tinyint
        when 0x0020 then Types.list(read_type_v4(buffer))
        when 0x0021 then Types.map(read_type_v4(buffer), read_type_v4(buffer))
        when 0x0022 then Types.set(read_type_v4(buffer))
        when 0x0030
          keyspace = buffer.read_string
          name     = buffer.read_string
          fields   = ::Array.new(buffer.read_short) do
            [buffer.read_string, read_type_v4(buffer)]
          end

          Types.udt(keyspace, name, fields)
        when 0x0031 then Types.tuple(
          *::Array.new(buffer.read_short) { read_type_v4(buffer) }
        )
        else
          raise Errors::DecodingError, %(Unsupported column type: #{id})
        end
      end

      def read_values_v4(buffer, column_metadata, custom_type_handlers)
        ::Array.new(buffer.read_int) do |_i|
          row = ::Hash.new

          column_metadata.each do |(_, _, column, type)|
            row[column] = read_value_v4(buffer, type, custom_type_handlers)
          end

          row
        end
      end

      def read_value_v4(buffer, type, custom_type_handlers)
        case type.kind
        when :ascii            then read_ascii(buffer)
        when :bigint, :counter then read_bigint(buffer)
        when :blob             then buffer.read_bytes
        when :boolean          then read_boolean(buffer)
        when :decimal          then read_decimal(buffer)
        when :double           then read_double(buffer)
        when :float            then read_float(buffer)
        when :int              then read_int(buffer)
        when :timestamp        then read_timestamp(buffer)
        when :uuid             then read_uuid(buffer)
        when :timeuuid         then read_uuid(buffer, TimeUuid)
        when :text             then read_text(buffer)
        when :varint           then read_varint(buffer)
        when :inet             then read_inet(buffer)
        when :tinyint          then read_tinyint(buffer)
        when :smallint         then read_smallint(buffer)
        when :time             then read_time(buffer)
        when :date             then read_date(buffer)
        when :custom           then read_custom(buffer, type, custom_type_handlers)
        when :list
          return nil unless read_size(buffer)

          value_type = type.value_type
          ::Array.new(buffer.read_signed_int) { read_value_v4(buffer, value_type, custom_type_handlers) }
        when :map
          return nil unless read_size(buffer)

          key_type   = type.key_type
          value_type = type.value_type
          value      = ::Hash.new

          buffer.read_signed_int.times do
            value[read_value_v4(buffer, key_type, custom_type_handlers)] = read_value_v4(buffer, value_type, custom_type_handlers)
          end

          value
        when :set
          return nil unless read_size(buffer)

          value_type = type.value_type
          value      = ::Set.new

          buffer.read_signed_int.times do
            value << read_value_v4(buffer, value_type, custom_type_handlers)
          end

          value
        when :udt
          size = read_size(buffer)
          return nil unless size

          length   = buffer.length
          keyspace = type.keyspace
          name     = type.name
          fields   = type.fields
          values   = ::Hash.new

          fields.each do |field|
            values[field.name] = if length - buffer.length >= size
                                   nil
                                 else
                                   read_value_v4(buffer, field.type, custom_type_handlers)
                                 end
          end

          Cassandra::UDT::Strict.new(keyspace, name, fields, values)
        when :tuple
          return nil unless read_size(buffer)

          members = type.members
          values  = ::Array.new

          members.each do |member_type|
            break if buffer.empty?
            values << read_value_v4(buffer, member_type, custom_type_handlers)
          end

          values.fill(nil, values.length, (members.length - values.length))

          Cassandra::Tuple::Strict.new(members, values)
        else
          raise Errors::DecodingError, %(Unsupported value type: #{type})
        end
      end

      def write_values_v3(buffer, values, types, names = EMPTY_LIST)
        if values && !values.empty?
          buffer.append_short(values.size)
          values.zip(types, names) do |(value, type, name)|
            buffer.append_string(name) if name
            write_value_v3(buffer, value, type)
          end
          buffer
        else
          buffer.append_short(0)
        end
      end

      def write_list_v3(buffer, list, type)
        raw = CqlByteBuffer.new

        raw.append_int(list.size)
        list.each do |element|
          write_value_v3(raw, element, type)
        end

        buffer.append_bytes(raw)
      end

      def write_map_v3(buffer, map, key_type, value_type)
        raw = CqlByteBuffer.new

        raw.append_int(map.size)
        map.each do |key, value|
          write_value_v3(raw, key, key_type)
          write_value_v3(raw, value, value_type)
        end

        buffer.append_bytes(raw)
      end

      def write_udt_v3(buffer, value, fields)
        raw = CqlByteBuffer.new

        fields.each do |field|
          write_value_v3(raw, value[field.name], field.type)
        end

        buffer.append_bytes(raw)
      end

      def write_tuple_v3(buffer, value, members)
        raw = CqlByteBuffer.new

        members.each_with_index do |type, i|
          write_value_v3(raw, value[i], type)
        end

        buffer.append_bytes(raw)
      end

      def write_value_v3(buffer, value, type)
        if value.nil?
          buffer.append_int(-1)
          return
        end

        case type.kind
        when :ascii            then write_ascii(buffer, value)
        when :bigint, :counter then write_bigint(buffer, value)
        when :blob             then write_blob(buffer, value)
        when :boolean          then write_boolean(buffer, value)
        when :decimal          then write_decimal(buffer, value)
        when :double           then write_double(buffer, value)
        when :float            then write_float(buffer, value)
        when :int              then write_int(buffer, value)
        when :inet             then write_inet(buffer, value)
        when :timestamp        then write_timestamp(buffer, value)
        when :uuid, :timeuuid  then write_uuid(buffer, value)
        when :text             then write_text(buffer, value)
        when :varint           then write_varint(buffer, value)
        when :list, :set       then write_list_v3(buffer, value, type.value_type)
        when :map              then write_map_v3(buffer,
                                                 value,
                                                 type.key_type,
                                                 type.value_type)
        when :udt              then write_udt_v3(buffer, value, type.fields)
        when :tuple            then write_tuple_v3(buffer, value, type.members)
        else
          raise Errors::EncodingError, %(Unsupported value type: #{type})
        end
      end

      def read_values_v3(buffer, column_metadata)
        ::Array.new(buffer.read_int) do |_i|
          row = ::Hash.new

          column_metadata.each do |(_, _, column, type)|
            row[column] = read_value_v3(buffer, type)
          end

          row
        end
      end

      def read_value_v3(buffer, type)
        case type.kind
        when :ascii            then read_ascii(buffer)
        when :bigint, :counter then read_bigint(buffer)
        when :blob             then buffer.read_bytes
        when :boolean          then read_boolean(buffer)
        when :decimal          then read_decimal(buffer)
        when :double           then read_double(buffer)
        when :float            then read_float(buffer)
        when :int              then read_int(buffer)
        when :timestamp        then read_timestamp(buffer)
        when :uuid             then read_uuid(buffer)
        when :timeuuid         then read_uuid(buffer, TimeUuid)
        when :text             then read_text(buffer)
        when :varint           then read_varint(buffer)
        when :inet             then read_inet(buffer)
        when :list
          return nil unless read_size(buffer)

          value_type = type.value_type
          ::Array.new(buffer.read_signed_int) { read_value_v3(buffer, value_type) }
        when :map
          return nil unless read_size(buffer)

          key_type   = type.key_type
          value_type = type.value_type
          value      = ::Hash.new

          buffer.read_signed_int.times do
            value[read_value_v3(buffer, key_type)] = read_value_v3(buffer, value_type)
          end

          value
        when :set
          return nil unless read_size(buffer)

          value_type = type.value_type
          value      = ::Set.new

          buffer.read_signed_int.times do
            value << read_value_v3(buffer, value_type)
          end

          value
        when :udt
          size = read_size(buffer)
          return nil unless size

          length   = buffer.length
          keyspace = type.keyspace
          name     = type.name
          fields   = type.fields
          values   = ::Hash.new

          fields.each do |field|
            values[field.name] = if length - buffer.length >= size
                                   nil
                                 else
                                   read_value_v3(buffer, field.type)
                                 end
          end

          Cassandra::UDT::Strict.new(keyspace, name, fields, values)
        when :tuple
          return nil unless read_size(buffer)

          members = type.members
          values  = ::Array.new

          members.each do |member_type|
            break if buffer.empty?
            values << read_value_v3(buffer, member_type)
          end

          values.fill(nil, values.length, (members.length - values.length))

          Cassandra::Tuple::Strict.new(members, values)
        else
          raise Errors::DecodingError, %(Unsupported value type: #{type})
        end
      end

      def read_metadata_v3(buffer)
        flags = buffer.read_int
        count = buffer.read_int

        paging_state = nil
        paging_state = buffer.read_bytes if flags & HAS_MORE_PAGES_FLAG != 0
        column_specs = nil

        if flags & NO_METADATA_FLAG == 0
          if flags & GLOBAL_TABLES_SPEC_FLAG != 0
            keyspace_name = buffer.read_string
            table_name    = buffer.read_string

            column_specs = ::Array.new(count) do |_i|
              [keyspace_name, table_name, buffer.read_string, read_type_v3(buffer)]
            end
          else
            column_specs = ::Array.new(count) do |_i|
              [
                buffer.read_string,
                buffer.read_string,
                buffer.read_string,
                read_type_v3(buffer)
              ]
            end
          end
        end

        [column_specs, paging_state]
      end

      def read_type_v3(buffer)
        id = buffer.read_unsigned_short
        case id
        when 0x0000 then Types.custom(buffer.read_string)
        when 0x0001 then Types.ascii
        when 0x0002 then Types.bigint
        when 0x0003 then Types.blob
        when 0x0004 then Types.boolean
        when 0x0005 then Types.counter
        when 0x0006 then Types.decimal
        when 0x0007 then Types.double
        when 0x0008 then Types.float
        when 0x0009 then Types.int
        when 0x000B then Types.timestamp
        when 0x000C then Types.uuid
        when 0x000D then Types.text
        when 0x000E then Types.varint
        when 0x000F then Types.timeuuid
        when 0x0010 then Types.inet
        when 0x0020 then Types.list(read_type_v3(buffer))
        when 0x0021 then Types.map(read_type_v3(buffer), read_type_v3(buffer))
        when 0x0022 then Types.set(read_type_v3(buffer))
        when 0x0030
          keyspace = buffer.read_string
          name     = buffer.read_string
          fields   = ::Array.new(buffer.read_short) do
            [buffer.read_string, read_type_v3(buffer)]
          end

          Types.udt(keyspace, name, fields)
        when 0x0031 then Types.tuple(
          *::Array.new(buffer.read_short) { read_type_v3(buffer) }
        )
        else
          raise Errors::DecodingError, %(Unsupported column type: #{id})
        end
      end

      def write_values_v1(buffer, values, types)
        if values && !values.empty?
          buffer.append_short(values.size)
          values.each_with_index do |value, index|
            write_value_v1(buffer, value, types[index])
          end
          buffer
        else
          buffer.append_short(0)
        end
      end

      def write_list_v1(buffer, list, type)
        raw = CqlByteBuffer.new

        raw.append_short(list.size)
        list.each do |element|
          write_short_value(raw, element, type)
        end

        buffer.append_bytes(raw)
      end

      def write_map_v1(buffer, map, key_type, value_type)
        raw = CqlByteBuffer.new

        raw.append_short(map.size)
        map.each do |key, value|
          write_short_value(raw, key, key_type)
          write_short_value(raw, value, value_type)
        end

        buffer.append_bytes(raw)
      end

      def write_value_v1(buffer, value, type)
        if value.nil?
          buffer.append_int(-1)
          return
        end

        case type.kind
        when :ascii            then write_ascii(buffer, value)
        when :bigint, :counter then write_bigint(buffer, value)
        when :blob             then write_blob(buffer, value)
        when :boolean          then write_boolean(buffer, value)
        when :decimal          then write_decimal(buffer, value)
        when :double           then write_double(buffer, value)
        when :float            then write_float(buffer, value)
        when :int              then write_int(buffer, value)
        when :inet             then write_inet(buffer, value)
        when :text             then write_text(buffer, value)
        when :timestamp        then write_timestamp(buffer, value)
        when :timeuuid, :uuid  then write_uuid(buffer, value)
        when :varint           then write_varint(buffer, value)
        when :list, :set       then write_list_v1(buffer, value, type.value_type)
        when :map              then write_map_v1(buffer,
                                                 value,
                                                 type.key_type,
                                                 type.value_type)
        else
          raise Errors::EncodingError, %(Unsupported value type: #{type})
        end
      end

      def read_values_v1(buffer, column_metadata)
        ::Array.new(buffer.read_int) do |_i|
          row = ::Hash.new

          column_metadata.each do |(_, _, column, type)|
            row[column] = read_value_v1(buffer, type)
          end

          row
        end
      end

      def read_value_v1(buffer, type)
        case type.kind
        when :ascii            then read_ascii(buffer)
        when :bigint, :counter then read_bigint(buffer)
        when :blob             then buffer.read_bytes
        when :boolean          then read_boolean(buffer)
        when :decimal          then read_decimal(buffer)
        when :double           then read_double(buffer)
        when :float            then read_float(buffer)
        when :int              then read_int(buffer)
        when :timestamp        then read_timestamp(buffer)
        when :text             then read_text(buffer)
        when :varint           then read_varint(buffer)
        when :uuid             then read_uuid(buffer)
        when :timeuuid         then read_uuid(buffer, TimeUuid)
        when :inet             then read_inet(buffer)
        when :list
          return nil unless read_size(buffer)

          value_type = type.value_type
          ::Array.new(buffer.read_short) { read_short_value(buffer, value_type) }
        when :map
          return nil unless read_size(buffer)

          key_type   = type.key_type
          value_type = type.value_type

          value = ::Hash.new

          buffer.read_short.times do
            value[read_short_value(buffer, key_type)] =
              read_short_value(buffer, value_type)
          end

          value
        when :set
          return nil unless read_size(buffer)

          value_type = type.value_type

          value = ::Set.new

          buffer.read_short.times do
            value << read_short_value(buffer, value_type)
          end

          value
        else
          raise Errors::DecodingError, %(Unsupported value type: #{type})
        end
      end

      def read_metadata_v1(buffer)
        flags = buffer.read_int
        count = buffer.read_int

        paging_state = nil
        paging_state = buffer.read_bytes if flags & HAS_MORE_PAGES_FLAG != 0
        column_specs = nil

        if flags & NO_METADATA_FLAG == 0
          if flags & GLOBAL_TABLES_SPEC_FLAG != 0
            keyspace_name = buffer.read_string
            table_name    = buffer.read_string

            column_specs = ::Array.new(count) do |_i|
              [keyspace_name, table_name, buffer.read_string, read_type_v1(buffer)]
            end
          else
            column_specs = ::Array.new(count) do |_i|
              [
                buffer.read_string,
                buffer.read_string,
                buffer.read_string,
                read_type_v1(buffer)
              ]
            end
          end
        end

        [column_specs, paging_state]
      end

      def read_type_v1(buffer)
        kind = buffer.read_unsigned_short

        case kind
        when 0x0000 then Types.custom(buffer.read_string)
        when 0x0001 then Types.ascii
        when 0x0002 then Types.bigint
        when 0x0003 then Types.blob
        when 0x0004 then Types.boolean
        when 0x0005 then Types.counter
        when 0x0006 then Types.decimal
        when 0x0007 then Types.double
        when 0x0008 then Types.float
        when 0x0009 then Types.int
        when 0x000A then Types.text
        when 0x000B then Types.timestamp
        when 0x000C then Types.uuid
        when 0x000D then Types.text
        when 0x000E then Types.varint
        when 0x000F then Types.timeuuid
        when 0x0010 then Types.inet
        when 0x0020 then Types.list(read_type_v1(buffer))
        when 0x0021 then Types.map(read_type_v1(buffer), read_type_v1(buffer))
        when 0x0022 then Types.set(read_type_v1(buffer))
        else
          raise Errors::DecodingError, %(Unsupported column type: #{kind})
        end
      end

      def read_ascii(buffer)
        value = buffer.read_bytes
        value && value.force_encoding(::Encoding::ASCII)
      end

      def read_bigint(buffer)
        read_size(buffer) && buffer.read_long
      end

      alias read_counter read_bigint

      def read_boolean(buffer)
        read_size(buffer) && buffer.read(1) == Constants::TRUE_BYTE
      end

      def read_custom(buffer, type, custom_type_handlers)
        # Lookup the type-name to get the Class that can deserialize buffer data into a custom domain object.
        unless custom_type_handlers && custom_type_handlers.key?(type)
          raise Errors::DecodingError, %(Unsupported custom column type: #{type.name})
        end
        num_bytes = read_size(buffer)
        custom_type_handlers[type].deserialize(buffer.read(num_bytes)) if num_bytes && num_bytes > 0
      end

      def read_decimal(buffer)
        size = read_size(buffer)
        size && buffer.read_decimal(size)
      end

      def read_double(buffer)
        read_size(buffer) && buffer.read_double
      end

      def read_float(buffer)
        read_size(buffer) && buffer.read_float
      end

      def read_int(buffer)
        read_size(buffer) && buffer.read_signed_int
      end

      def read_timestamp(buffer)
        return nil unless read_size(buffer)

        timestamp     = buffer.read_long
        seconds       = timestamp / 1_000
        microsenconds = (timestamp % 1_000) * 1_000

        ::Time.at(seconds, microsenconds)
      end

      def read_uuid(buffer, klass = Uuid)
        read_size(buffer) && buffer.read_uuid(klass)
      end

      def read_text(buffer)
        value = buffer.read_bytes
        value && value.force_encoding(::Encoding::UTF_8)
      end

      def read_varint(buffer)
        size = read_size(buffer)
        size && buffer.read_varint(size)
      end

      def read_inet(buffer)
        size = read_size(buffer)
        size && ::IPAddr.new_ntoh(buffer.read(size))
      end

      def read_tinyint(buffer)
        read_size(buffer) && buffer.read_tinyint
      end

      def read_smallint(buffer)
        read_size(buffer) && buffer.read_smallint
      end

      def read_time(buffer)
        return nil unless read_size(buffer)

        Time.new(buffer.read_long)
      end

      def read_date(buffer)
        return nil unless read_size(buffer)

        ::Date.jd(DATE_OFFSET + buffer.read_int, ::Date::GREGORIAN)
      end

      def write_ascii(buffer, value)
        buffer.append_bytes(value.encode(::Encoding::ASCII))
      end

      def write_bigint(buffer, value)
        buffer.append_int(8)
        buffer.append_long(value)
      end

      alias write_counter write_bigint

      def write_blob(buffer, value)
        buffer.append_bytes(value.encode(::Encoding::BINARY))
      end

      def write_boolean(buffer, value)
        buffer.append_int(1)
        buffer.append(value ? Constants::TRUE_BYTE : Constants::FALSE_BYTE)
      end

      def write_custom(buffer, value, type)
        # Verify that the given type-name matches the value's cql type name.
        if value.class.type != type
          raise Errors::EncodingError, "type mismatch: value is a #{value.type} and column is a #{type}"
        end

        buffer.append_bytes(value.serialize)
      end

      def write_decimal(buffer, value)
        buffer.append_bytes(CqlByteBuffer.new.append_decimal(value))
      end

      def write_double(buffer, value)
        buffer.append_int(8)
        buffer.append_double(value)
      end

      def write_float(buffer, value)
        buffer.append_int(4)
        buffer.append_float(value)
      end

      def write_int(buffer, value)
        buffer.append_int(4)
        buffer.append_int(value)
      end

      def write_inet(buffer, value)
        buffer.append_int(value.ipv6? ? 16 : 4)
        buffer.append(value.hton)
      end

      def write_timestamp(buffer, value)
        ms = (value.to_r.to_f * 1000).to_i
        buffer.append_int(8)
        buffer.append_long(ms)
      end

      def write_text(buffer, value)
        buffer.append_bytes(value.encode(::Encoding::UTF_8))
      end

      def write_uuid(buffer, value)
        buffer.append_int(16)
        buffer.append_uuid(value)
      end

      def write_varint(buffer, value)
        buffer.append_bytes(CqlByteBuffer.new.append_varint(value))
      end

      def write_tinyint(buffer, value)
        buffer.append_int(1)
        buffer.append_tinyint(value)
      end

      def write_smallint(buffer, value)
        buffer.append_int(2)
        buffer.append_smallint(value)
      end

      def write_time(buffer, value)
        ns = value.to_nanoseconds
        buffer.append_int(8)
        buffer.append_long(ns)
      end

      def write_date(buffer, value)
        buffer.append_int(4)
        buffer.append_int(value.gregorian.jd - DATE_OFFSET)
      end

      def read_short_size(buffer)
        size = buffer.read_short

        return nil if size & 0x8000 == 0x8000 || (size == 0)

        size
      end

      def read_short_value(buffer, type)
        case type.kind
        when :ascii
          value = buffer.read_short_bytes
          value && value.force_encoding(::Encoding::ASCII)
        when :bigint, :counter
          read_short_size(buffer) && buffer.read_long
        when :blob
          value = buffer.read_short_bytes
          value && value.force_encoding(::Encoding::BINARY)
        when :boolean
          read_short_size(buffer) && buffer.read(1) == Constants::TRUE_BYTE
        when :decimal
          size = read_short_size(buffer)
          size && buffer.read_decimal(size)
        when :double
          read_short_size(buffer) && buffer.read_double
        when :float
          read_short_size(buffer) && buffer.read_float
        when :int
          read_short_size(buffer) && buffer.read_signed_int
        when :inet
          size = read_short_size(buffer)
          size && ::IPAddr.new_ntoh(buffer.read(size))
        when :text
          value = buffer.read_short_bytes
          value && value.force_encoding(::Encoding::UTF_8)
        when :timestamp
          return nil unless read_short_size(buffer)

          timestamp     = buffer.read_long
          seconds       = timestamp / 1_000
          microsenconds = (timestamp % 1_000) * 1_000

          ::Time.at(seconds, microsenconds)
        when :timeuuid
          read_short_size(buffer) && buffer.read_uuid(TimeUuid)
        when :uuid
          read_short_size(buffer) && buffer.read_uuid
        when :varint
          size = read_short_size(buffer)
          size && buffer.read_varint(size)
        else
          raise Errors::EncodingError, %(Unsupported short value type: #{type})
        end
      end

      def write_short_value(buffer, value, type)
        case type.kind
        when :ascii
          buffer.append_short_bytes(value && value.encode(::Encoding::ASCII))
        when :bigint, :counter
          if value
            buffer.append_short(8)
            buffer.append_long(value)
          else
            buffer.append_short(-1)
          end
        when :blob
          buffer.append_short_bytes(value && value.encode(::Encoding::BINARY))
        when :boolean
          if !value.nil?
            buffer.append_short(1)
            buffer.append(value ? Constants::TRUE_BYTE : Constants::FALSE_BYTE)
          else
            buffer.append_short(-1)
          end
        when :decimal
          buffer.append_short_bytes(value && CqlByteBuffer.new.append_decimal(value))
        when :double
          if value
            buffer.append_short(8)
            buffer.append_double(value)
          else
            buffer.append_short(-1)
          end
        when :float
          if value
            buffer.append_short(4)
            buffer.append_float(value)
          else
            buffer.append_short(-1)
          end
        when :inet
          if value
            buffer.append_short(value.ipv6? ? 16 : 4)
            buffer.append(value.hton)
          else
            buffer.append_short(-1)
          end
        when :int
          if value
            buffer.append_short(4)
            buffer.append_int(value)
          else
            buffer.append_short(-1)
          end
        when :text
          buffer.append_short_bytes(value && value.encode(::Encoding::UTF_8))
        when :timestamp
          if value
            buffer.append_short(8)
            buffer.append_long((value.to_f * 1000).to_i)
          else
            buffer.append_short(-1)
          end
        when :timeuuid, :uuid
          if value
            buffer.append_short(16)
            buffer.append_uuid(value)
          else
            buffer.append_short(-1)
          end
        when :varint
          buffer.append_short_bytes(value && CqlByteBuffer.new.append_varint(value))
        else
          raise Errors::EncodingError, %(Unsupported short value type: #{type})
        end
      end

      def read_size(buffer)
        size = buffer.read_signed_int

        return nil if (size & 0x80000000 == 0x80000000) || (size == 0)

        size
      end
    end
  end
end
