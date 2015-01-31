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
    module Coder; extend self
      GLOBAL_TABLES_SPEC_FLAG = 0x01
      HAS_MORE_PAGES_FLAG     = 0x02
      NO_METADATA_FLAG        = 0x04

      def write_values_v3(buffer, values, types)
        if values && values.size > 0
          buffer.append_short(values.size)
          values.each_with_index do |value, index|
            write_value_v3(buffer, value, types[index])
          end
          buffer
        else
          buffer.append_short(0)
        end
      end

      def write_value_v3(buffer, value, type)
        case type
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
        when :varchar          then write_varchar(buffer, value)
        when :varint           then write_varint(buffer, value)
        when ::Array
          case type.first
          when :list, :set
            if value
              raw        = CqlByteBuffer.new
              value_type = type[1]

              raw.append_int(value.size)
              value.each do |element|
                write_value_v3(raw, element, value_type)
              end

              buffer.append_bytes(raw)
            else
              buffer.append_int(-1)
            end
          when :map
            if value
              raw        = CqlByteBuffer.new
              key_type   = type[1]
              value_type = type[2]

              raw.append_int(value.size)
              value.each do |key, value|
                write_value_v3(raw, key, key_type)
                write_value_v3(raw, value, value_type)
              end

              buffer.append_bytes(raw)
            else
              buffer.append_int(-1)
            end
          when :udt
            if value
              raw    = CqlByteBuffer.new
              fields = type[3]

              fields.each do |(field_name, field_type)|
                write_value_v3(raw, value.send(field_name), field_type)
              end

              buffer.append_bytes(raw)
            else
              buffer.append_int(-1)
            end
          when :tuple
            if value
              raw     = CqlByteBuffer.new
              members = type[1]

              members.each_with_index do |member_type, i|
                write_value_v3(raw, value[i], member_type)
              end

              buffer.append_bytes(raw)
            else
              buffer.append_int(-1)
            end
          when :custom
            if value
              buffer.append_bytes(value)
            else
              buffer.append_int(-1)
            end
          else
            raise Errors::EncodingError, %(Unsupported complex value type: #{type})
          end
        else
          raise Errors::EncodingError, %(Unsupported value type: #{type})
        end
      end

      def read_values_v3(buffer, column_metadata)
        ::Array.new(buffer.read_int) do |i|
          row = ::Hash.new

          column_metadata.each do |(_, _, column, type)|
            row[column] = read_value_v3(buffer, type)
          end

          row
        end
      end

      def read_value_v3(buffer, type)
        case type
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
        when :varchar          then read_varchar(buffer)
        when :varint           then read_varint(buffer)
        when :inet             then read_inet(buffer)
        when ::Array
          case type.first
          when :list
            return nil unless read_size(buffer)

            value_type = type[1]
            ::Array.new(buffer.read_signed_int) { read_value_v3(buffer, value_type) }
          when :map
            return nil unless read_size(buffer)

            key_type   = type[1]
            value_type = type[2]

            value = ::Hash.new

            buffer.read_signed_int.times do
              value[read_value_v3(buffer, key_type)] = read_value_v3(buffer, value_type)
            end

            value
          when :set
            return nil unless read_size(buffer)

            value_type = type[1]

            value = ::Set.new

            buffer.read_signed_int.times do
              value << read_value_v3(buffer, value_type)
            end

            value
          when :udt
            return nil unless read_size(buffer)

            keyspace = type[1]
            name     = type[2]
            fields   = type[3]

            values = ::Hash.new

            fields.each do |(field_name, field_type)|
              if buffer.empty?
                values[field_name] = nil
              else
                values[field_name] = read_value_v3(buffer, field_type)
              end
            end

            UserValue.new(keyspace, name, values)
          when :tuple
            return nil unless read_size(buffer)

            members = type[1]
            values  = ::Array.new

            members.each do |member_type|
              break if buffer.empty?
              values << read_value_v3(buffer, member_type)
            end

            values.fill(nil, values.length, (members.length - values.length))
          when :custom
            buffer.read_bytes
          else
            raise Errors::DecodingError, %(Unsupported complex value type: #{type})
          end
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

            column_specs = ::Array.new(count) do |i|
              [keyspace_name, table_name, buffer.read_string, read_type_v3(buffer)]
            end
          else
            column_specs = ::Array.new(count) do |i|
              [buffer.read_string, buffer.read_string, buffer.read_string, read_type_v3(buffer)]
            end
          end
        end

        [column_specs, paging_state]
      end

      def read_type_v3(buffer)
        case buffer.read_unsigned_short
        when 0x0000 then [:custom, buffer.read_string]
        when 0x0001 then :ascii
        when 0x0002 then :bigint
        when 0x0003 then :blob
        when 0x0004 then :boolean
        when 0x0005 then :counter
        when 0x0006 then :decimal
        when 0x0007 then :double
        when 0x0008 then :float
        when 0x0009 then :int
        when 0x000B then :timestamp
        when 0x000C then :uuid
        when 0x000D then :varchar
        when 0x000E then :varint
        when 0x000F then :timeuuid
        when 0x0010 then :inet
        when 0x0020 then [:list, read_type_v3(buffer)]
        when 0x0021 then [:map, read_type_v3(buffer), read_type_v3(buffer)]
        when 0x0022 then [:set, read_type_v3(buffer)]
        when 0x0030 then [:udt, *read_user_defined_type(buffer)]
        when 0x0031 then [:tuple, read_tuple(buffer)]
        else
          raise Errors::DecodingError, %(Unsupported column type: #{id})
        end
      end

      def write_values_v1(buffer, values, types)
        if values && values.size > 0
          buffer.append_short(values.size)
          values.each_with_index do |value, index|
            write_value_v1(buffer, value, types[index])
          end
          buffer
        else
          buffer.append_short(0)
        end
      end

      def write_value_v1(buffer, value, type)
        case type
        when :ascii            then write_ascii(buffer, value)
        when :bigint, :counter then write_bigint(buffer, value)
        when :blob             then write_blob(buffer, value)
        when :boolean          then write_boolean(buffer, value)
        when :decimal          then write_decimal(buffer, value)
        when :double           then write_double(buffer, value)
        when :float            then write_float(buffer, value)
        when :int              then write_int(buffer, value)
        when :inet             then write_inet(buffer, value)
        when :varchar, :text   then write_varchar(buffer, value)
        when :timestamp        then write_timestamp(buffer, value)
        when :timeuuid, :uuid  then write_uuid(buffer, value)
        when :varint           then write_varint(buffer, value)
        when ::Array
          case type.first
          when :list, :set
            if value
              raw        = CqlByteBuffer.new
              value_type = type[1]

              raw.append_short(value.size)
              value.each do |element|
                write_short_value(raw, element, value_type)
              end

              buffer.append_bytes(raw)
            else
              buffer.append_int(-1)
            end
          when :map
            if value
              raw        = CqlByteBuffer.new
              key_type   = type[1]
              value_type = type[2]

              raw.append_short(value.size)
              value.each do |key, value|
                write_short_value(raw, key, key_type)
                write_short_value(raw, value, value_type)
              end

              buffer.append_bytes(raw)
            else
              buffer.append_int(-1)
            end
          else
            raise Errors::EncodingError, %(Unsupported value type: #{type})
          end
        else
          raise Errors::EncodingError, %(Unsupported value type: #{type})
        end
      end

      def read_values_v1(buffer, column_metadata)
        ::Array.new(buffer.read_int) do |i|
          row = ::Hash.new

          column_metadata.each do |(_, _, column, type)|
            row[column] = read_value_v1(buffer, type)
          end

          row
        end
      end

      def read_value_v1(buffer, type)
        case type
        when :ascii            then read_ascii(buffer)
        when :bigint, :counter then read_bigint(buffer)
        when :blob             then buffer.read_bytes
        when :boolean          then read_boolean(buffer)
        when :decimal          then read_decimal(buffer)
        when :double           then read_double(buffer)
        when :float            then read_float(buffer)
        when :int              then read_int(buffer)
        when :timestamp        then read_timestamp(buffer)
        when :varchar, :text   then read_varchar(buffer)
        when :varint           then read_varint(buffer)
        when :uuid             then read_uuid(buffer)
        when :timeuuid         then read_uuid(buffer, TimeUuid)
        when :inet             then read_inet(buffer)
        when ::Array
          case type.first
          when :list
            return nil unless read_size(buffer)

            value_type = type[1]
            ::Array.new(buffer.read_short) { read_short_value(buffer, value_type) }
          when :map
            return nil unless read_size(buffer)

            key_type   = type[1]
            value_type = type[2]

            value = ::Hash.new

            buffer.read_short.times do
              value[read_short_value(buffer, key_type)] = read_short_value(buffer, value_type)
            end

            value
          when :set
            return nil unless read_size(buffer)

            value_type = type[1]

            value = ::Set.new

            buffer.read_short.times do
              value << read_short_value(buffer, value_type)
            end

            value
          when :custom
            buffer.read_bytes
          else
            raise Errors::DecodingError, %(Unsupported complex value type: #{type})
          end
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

            column_specs = ::Array.new(count) do |i|
              [keyspace_name, table_name, buffer.read_string, read_type_v1(buffer)]
            end
          else
            column_specs = ::Array.new(count) do |i|
              [buffer.read_string, buffer.read_string, buffer.read_string, read_type_v1(buffer)]
            end
          end
        end

        [column_specs, paging_state]
      end

      def read_type_v1(buffer)
        case buffer.read_unsigned_short
        when 0x0000 then [:custom, buffer.read_string]
        when 0x0001 then :ascii
        when 0x0002 then :bigint
        when 0x0003 then :blob
        when 0x0004 then :boolean
        when 0x0005 then :counter
        when 0x0006 then :decimal
        when 0x0007 then :double
        when 0x0008 then :float
        when 0x0009 then :int
        when 0x000A then :text
        when 0x000B then :timestamp
        when 0x000C then :uuid
        when 0x000D then :varchar
        when 0x000E then :varint
        when 0x000F then :timeuuid
        when 0x0010 then :inet
        when 0x0020 then [:list, read_type_v1(buffer)]
        when 0x0021 then [:map, read_type_v1(buffer), read_type_v1(buffer)]
        when 0x0022 then [:set, read_type_v1(buffer)]
        else
          raise Errors::DecodingError, %(Unsupported column type: #{id})
        end
      end

      def read_ascii(buffer)
        value  = buffer.read_bytes
        value && value.force_encoding(::Encoding::ASCII)
      end

      def read_bigint(buffer)
        read_size(buffer) && buffer.read_long
      end

      alias :read_counter :read_bigint

      def read_boolean(buffer)
        read_size(buffer) && buffer.read(1) == Constants::TRUE_BYTE
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

      def read_varchar(buffer)
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

      def write_ascii(buffer, value)
        buffer.append_bytes(value && value.encode(::Encoding::ASCII))
      end

      def write_bigint(buffer, value)
        if value
          buffer.append_int(8)
          buffer.append_long(value)
        else
          buffer.append_int(-1)
        end
      end

      alias :write_counter :write_bigint

      def write_blob(buffer, value)
        buffer.append_bytes(value && value.encode(::Encoding::BINARY))
      end

      def write_boolean(buffer, value)
        if !value.nil?
          buffer.append_int(1)
          buffer.append(value ? Constants::TRUE_BYTE : Constants::FALSE_BYTE)
        else
          buffer.append_int(-1)
        end
      end

      def write_decimal(buffer, value)
        buffer.append_bytes(value && CqlByteBuffer.new.append_decimal(value))
      end

      def write_double(buffer, value)
        if value
          buffer.append_int(8)
          buffer.append_double(value)
        else
          buffer.append_int(-1)
        end
      end

      def write_float(buffer, value)
        if value
          buffer.append_int(4)
          buffer.append_float(value)
        else
          buffer.append_int(-1)
        end
      end

      def write_int(buffer, value)
        if value
          buffer.append_int(4)
          buffer.append_int(value)
        else
          buffer.append_int(-1)
        end
      end

      def write_inet(buffer, value)
        if value
          buffer.append_int(value.ipv6? ? 16 : 4)
          buffer.append(value.hton)
        else
          buffer.append_int(-1)
        end
      end

      def write_timestamp(buffer, value)
        if value
          ms = (value.to_r.to_f * 1000).to_i
          buffer.append_int(8)
          buffer.append_long(ms)
        else
          buffer.append_int(-1)
        end
      end

      def write_varchar(buffer, value)
        buffer.append_bytes(value && value.encode(::Encoding::UTF_8))
      end

      def write_uuid(buffer, value)
        if value
          buffer.append_int(16)
          buffer.append_uuid(value)
        else
          buffer.append_int(-1)
        end
      end

      def write_varint(buffer, value)
        buffer.append_bytes(value && CqlByteBuffer.new.append_varint(value))
      end

      def read_short_size(buffer)
        size = buffer.read_short

        return nil if size & 0x8000 == 0x8000 || (size == 0)

        size
      end

      def read_short_value(buffer, type)
        case type
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
        when :varchar, :text
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
        case type
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
        when :varchar, :text
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

      def read_tuple(buffer)
        ::Array.new(buffer.read_short) { read_type_v3(buffer) }
      end

      def read_user_defined_type(buffer)
        keyspace = buffer.read_string
        name     = buffer.read_string
        fields   = ::Array.new(buffer.read_short) { [buffer.read_string, read_type_v3(buffer)] }

        [keyspace, name, fields]
      end
    end
  end
end
