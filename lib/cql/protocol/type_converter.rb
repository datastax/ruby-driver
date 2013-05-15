# encoding: utf-8

require 'ipaddr'
require 'set'


module Cql
  module Protocol
    class TypeConverter
      include Decoding

      def initialize
        @conversions = conversions
      end

      def convert_type(buffer, type, size_bytes=4)
        return nil if buffer.empty?
        case type
        when Array
          return nil unless read_size(buffer, size_bytes)
          case type.first
          when :list
            convert_list(buffer, @conversions[type[1]])
          when :map
            convert_map(buffer, @conversions[type[1]], @conversions[type[2]])
          when :set
            convert_set(buffer, @conversions[type[1]])
          end
        else
          @conversions[type].call(buffer, size_bytes)
        end
      end

      def conversions
        {
          :ascii => method(:convert_ascii),
          :bigint => method(:convert_bigint),
          :blob => method(:convert_blob),
          :boolean => method(:convert_boolean),
          :counter => method(:convert_bigint),
          :decimal => method(:convert_decimal),
          :double => method(:convert_double),
          :float => method(:convert_float),
          :int => method(:convert_int),
          :timestamp => method(:convert_timestamp),
          :varchar => method(:convert_varchar),
          :text => method(:convert_varchar),
          :varint => method(:convert_varint),
          :timeuuid => method(:convert_uuid),
          :uuid => method(:convert_uuid),
          :inet => method(:convert_inet),
        }
      end

      def convert_ascii(buffer, size_bytes)
        bytes = size_bytes == 4 ? read_bytes!(buffer) : read_short_bytes!(buffer)
        bytes ? bytes.force_encoding(::Encoding::ASCII) : nil
      end

      def convert_bigint(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_long!(buffer)
      end

      def convert_blob(buffer, size_bytes)
        bytes = size_bytes == 4 ? read_bytes!(buffer) : read_short_bytes!(buffer)
        bytes ? bytes : nil
      end

      def convert_boolean(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        buffer.read(1) == Constants::TRUE_BYTE
      end

      def convert_decimal(buffer, size_bytes)
        size = read_size(buffer, size_bytes)
        return nil unless size
        read_decimal!(buffer, size)
      end

      def convert_double(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_double!(buffer)
      end

      def convert_float(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_float!(buffer)
      end

      def convert_int(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_int!(buffer)
      end

      def convert_timestamp(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        timestamp = read_long!(buffer)
        Time.at(timestamp/1000.0)
      end

      def convert_varchar(buffer, size_bytes)
        bytes = size_bytes == 4 ? read_bytes!(buffer) : read_short_bytes!(buffer)
        bytes ? bytes.force_encoding(::Encoding::UTF_8) : nil
      end

      def convert_varint(buffer, size_bytes)
        size = read_size(buffer, size_bytes)
        return nil unless size
        read_varint!(buffer, size)
      end

      def convert_uuid(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_uuid!(buffer)
      end

      def convert_inet(buffer, size_bytes)
        size = read_size(buffer, size_bytes)
        return nil unless size
        IPAddr.new_ntoh(buffer.read(size))
      end

      def convert_list(buffer, value_converter)
        list = []
        size = buffer.read_short
        size.times do
          list << value_converter.call(buffer, 2)
        end
        list
      end

      def convert_map(buffer, key_converter, value_converter)
        map = {}
        size = buffer.read_short
        size.times do
          key = key_converter.call(buffer, 2)
          value = value_converter.call(buffer, 2)
          map[key] = value
        end
        map
      end

      def convert_set(buffer, value_converter)
        set = Set.new
        size = buffer.read_short
        size.times do
          set << value_converter.call(buffer, 2)
        end
        set
      end

      def read_size(buffer, size_bytes)
        if size_bytes == 2
          size = buffer.read_short
          return nil if size & 0x8000 == 0x8000
        else
          size = buffer.read_int
          return nil if size & 0x80000000 == 0x80000000
        end
        size
      end
    end
  end
end