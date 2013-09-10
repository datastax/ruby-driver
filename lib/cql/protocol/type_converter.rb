# encoding: utf-8

require 'ipaddr'
require 'set'


module Cql
  module Protocol
    class TypeConverter
      include Decoding
      include Encoding

      def initialize
        @from_bytes_converters = from_bytes_converters
        @to_bytes_converters = to_bytes_converters
      end

      def from_bytes(buffer, type, size_bytes=4)
        return nil if buffer.empty?
        case type
        when Array
          return nil unless read_size(buffer, size_bytes)
          case type.first
          when :list
            bytes_to_list(buffer, @from_bytes_converters[type[1]])
          when :map
            bytes_to_map(buffer, @from_bytes_converters[type[1]], @from_bytes_converters[type[2]])
          when :set
            bytes_to_set(buffer, @from_bytes_converters[type[1]])
          end
        else
          @from_bytes_converters[type].call(buffer, size_bytes)
        end
      end

      def to_bytes(io, type, value, size_bytes=4)
        case type
        when Array
          unless value.nil? || value.is_a?(Enumerable)
            raise InvalidValueError, 'Value for collection must be enumerable'
          end
          case type.first
          when :list, :set
            _, sub_type = type
            if value
              raw = ''
              write_short(raw, value.size)
              value.each do |element|
                to_bytes(raw, sub_type, element, 2)
              end
              write_bytes(io, raw)
            else
              nil_to_bytes(io, size_bytes)
            end
          when :map
            _, key_type, value_type = type
            if value
              raw = ''
              write_short(raw, value.size)
              value.each do |key, value|
                to_bytes(raw, key_type, key, 2)
                to_bytes(raw, value_type, value, 2)
              end
              write_bytes(io, raw)
            else
              nil_to_bytes(io, size_bytes)
            end
          else
            raise UnsupportedColumnTypeError, %(Unsupported column collection type: #{type.first})
          end
        else
          converter = @to_bytes_converters[type]
          unless converter
            raise UnsupportedColumnTypeError, %(Unsupported column type: #{type})
          end
          converter.call(io, value, size_bytes)
        end
      rescue TypeError => e
        raise TypeError, %("#{value}" cannot be encoded as #{type.to_s.upcase}: #{e.message}), e.backtrace
      end

      private

      def from_bytes_converters
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
          :timeuuid => method(:bytes_to_timeuuid),
          :uuid => method(:bytes_to_uuid),
          :inet => method(:bytes_to_inet),
        }
      end

      def to_bytes_converters
        {
          :ascii => method(:ascii_to_bytes),
          :bigint => method(:bigint_to_bytes),
          :blob => method(:blob_to_bytes),
          :boolean => method(:boolean_to_bytes),
          :counter => method(:bigint_to_bytes),
          :decimal => method(:decimal_to_bytes),
          :double => method(:double_to_bytes),
          :float => method(:float_to_bytes),
          :inet => method(:inet_to_bytes),
          :int => method(:int_to_bytes),
          :text => method(:varchar_to_bytes),
          :varchar => method(:varchar_to_bytes),
          :timestamp => method(:timestamp_to_bytes),
          :timeuuid => method(:uuid_to_bytes),
          :uuid => method(:uuid_to_bytes),
          :varint => method(:varint_to_bytes),
        }
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

      def bytes_to_timeuuid(buffer, size_bytes)
        return nil unless read_size(buffer, size_bytes)
        read_uuid!(buffer, TimeUuid)
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

      def ascii_to_bytes(io, value, size_bytes)
        v = value && value.encode(::Encoding::ASCII)
        if size_bytes == 4
          write_bytes(io, v)
        else
          write_short_bytes(io, v)
        end
      end

      def bigint_to_bytes(io, value, size_bytes)
        if value
          size_to_bytes(io, 8, size_bytes)
          write_long(io, value)
        else
          nil_to_bytes(io, size_bytes)
        end
      end

      def blob_to_bytes(io, value, size_bytes)
        v = value && value.encode(::Encoding::BINARY)
        if size_bytes == 4
          write_bytes(io, v)
        else
          write_short_bytes(io, v)
        end
      end

      def boolean_to_bytes(io, value, size_bytes)
        if !value.nil?
          size_to_bytes(io, 1, size_bytes)
          io << (value ? Constants::TRUE_BYTE : Constants::FALSE_BYTE)
        else
          nil_to_bytes(io, size_bytes)
        end
      end

      def decimal_to_bytes(io, value, size_bytes)
        raw = value && write_decimal('', value)
        if size_bytes == 4
          write_bytes(io, raw)
        else
          write_short_bytes(io, raw)
        end
      end

      def double_to_bytes(io, value, size_bytes)
        if value
          size_to_bytes(io, 8, size_bytes)
          write_double(io, value)
        else
          nil_to_bytes(io, size_bytes)
        end
      end

      def float_to_bytes(io, value, size_bytes)
        if value
          size_to_bytes(io, 4, size_bytes)
          write_float(io, value)
        else
          nil_to_bytes(io, size_bytes)
        end
      end

      def inet_to_bytes(io, value, size_bytes)
        if value
          size_to_bytes(io, value.ipv6? ? 16 : 4, size_bytes)
          io << value.hton
        else
          nil_to_bytes(io, size_bytes)
        end
      end

      def int_to_bytes(io, value, size_bytes)
        if value
          size_to_bytes(io, 4, size_bytes)
          write_int(io, value)
        else
          nil_to_bytes(io, size_bytes)
        end
      end

      def varchar_to_bytes(io, value, size_bytes)
        v = value && value.encode(::Encoding::UTF_8)
        if size_bytes == 4
          write_bytes(io, v)
        else
          write_short_bytes(io, v)
        end
      end

      def timestamp_to_bytes(io, value, size_bytes)
        if value
          ms = (value.to_f * 1000).to_i
          size_to_bytes(io, 8, size_bytes)
          write_long(io, ms)
        else
          nil_to_bytes(io, size_bytes)
        end
      end

      def uuid_to_bytes(io, value, size_bytes)
        if value
          size_to_bytes(io, 16, size_bytes)
          write_uuid(io, value)
        else
          nil_to_bytes(io, size_bytes)
        end
      end

      def varint_to_bytes(io, value, size_bytes)
        raw = value && write_varint('', value)
        if size_bytes == 4
          write_bytes(io, raw)
        else
          write_short_bytes(io, raw)
        end
      end

      def size_to_bytes(io, size, size_bytes)
        if size_bytes == 4
          write_int(io, size)
        else
          write_short(io, size)
        end
      end

      def nil_to_bytes(io, size_bytes)
        if size_bytes == 4
          write_int(io, -1)
        else
          write_short(io, -1)
        end
      end
    end
  end
end