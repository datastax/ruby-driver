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

      def from_bytes(buffer, type, size_bytes=4)
        return nil if buffer.empty?
        case type
        when Array
          return nil unless read_size(buffer, size_bytes)
          case type.first
          when :list
            bytes_to_list(buffer, @conversions[type[1]])
          when :map
            bytes_to_map(buffer, @conversions[type[1]], @conversions[type[2]])
          when :set
            bytes_to_set(buffer, @conversions[type[1]])
          end
        else
          @conversions[type].call(buffer, size_bytes)
        end
      end

      def conversions
        {
          :ascii => method(:bytes_to_ascii),
          :bigint => method(:bytes_to_bigint),
          :blob => method(:bytes_to_blob),
          :boolean => method(:bytes_to_boolean),
          :counter => method(:bytes_to_bigint),
          :decimal => method(:bytes_to_decimal),
          :double => method(:bytes_to_double),
          :float => method(:bytes_to_float),
          :int => method(:bytes_to_int),
          :timestamp => method(:bytes_to_timestamp),
          :varchar => method(:bytes_to_varchar),
          :text => method(:bytes_to_varchar),
          :varint => method(:bytes_to_varint),
          :timeuuid => method(:bytes_to_uuid),
          :uuid => method(:bytes_to_uuid),
          :inet => method(:bytes_to_inet),
        }
      end

      def bytes_to_ascii(buffer, size_bytes)
        bytes = size_bytes == 4 ? read_bytes!(buffer) : read_short_bytes!(buffer)
        bytes ? bytes.force_encoding(::Encoding::ASCII) : nil
      end

      def bytes_to_bigint(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_long!(buffer)
      end

      def bytes_to_blob(buffer, size_bytes)
        bytes = size_bytes == 4 ? read_bytes!(buffer) : read_short_bytes!(buffer)
        bytes ? bytes : nil
      end

      def bytes_to_boolean(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        buffer.read(1) == Constants::TRUE_BYTE
      end

      def bytes_to_decimal(buffer, size_bytes)
        size = read_size(buffer, size_bytes)
        return nil unless size
        read_decimal!(buffer, size)
      end

      def bytes_to_double(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_double!(buffer)
      end

      def bytes_to_float(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_float!(buffer)
      end

      def bytes_to_int(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_int!(buffer)
      end

      def bytes_to_timestamp(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        timestamp = read_long!(buffer)
        Time.at(timestamp/1000.0)
      end

      def bytes_to_varchar(buffer, size_bytes)
        bytes = size_bytes == 4 ? read_bytes!(buffer) : read_short_bytes!(buffer)
        bytes ? bytes.force_encoding(::Encoding::UTF_8) : nil
      end

      def bytes_to_varint(buffer, size_bytes)
        size = read_size(buffer, size_bytes)
        return nil unless size
        read_varint!(buffer, size)
      end

      def bytes_to_uuid(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_uuid!(buffer)
      end

      def bytes_to_inet(buffer, size_bytes)
        size = read_size(buffer, size_bytes)
        return nil unless size
        IPAddr.new_ntoh(buffer.read(size))
      end

      def bytes_to_list(buffer, value_converter)
        list = []
        size = buffer.read_short
        size.times do
          list << value_converter.call(buffer, 2)
        end
        list
      end

      def bytes_to_map(buffer, key_converter, value_converter)
        map = {}
        size = buffer.read_short
        size.times do
          key = key_converter.call(buffer, 2)
          value = value_converter.call(buffer, 2)
          map[key] = value
        end
        map
      end

      def bytes_to_set(buffer, value_converter)
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