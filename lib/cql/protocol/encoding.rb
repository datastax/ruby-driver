# encoding: utf-8

module Cql
  module Protocol
    module Encoding
      extend self

      def write_int(buffer, n)
        buffer << [n].pack(Formats::INT_FORMAT)
      end

      def write_short(buffer, n)
        buffer << [n].pack(Formats::SHORT_FORMAT)
      end

      def write_string(buffer, str)
        str = str.to_s
        buffer << [str.bytesize].pack(Formats::SHORT_FORMAT)
        buffer << str
        buffer
      end

      def write_long_string(buffer, str)
        buffer << [str.bytesize].pack(Formats::INT_FORMAT)
        buffer << str
        buffer
      end

      def write_uuid(buffer, uuid)
        v = uuid.value
        write_int(buffer, (v >> 96) & 0xffffffff)
        write_int(buffer, (v >> 64) & 0xffffffff)
        write_int(buffer, (v >> 32) & 0xffffffff)
        write_int(buffer, v & 0xffffffff)
      end

      def write_string_list(buffer, strs)
        buffer << [strs.size].pack(Formats::SHORT_FORMAT)
        strs.each do |str|
          write_string(buffer, str)
        end
        buffer
      end

      def write_bytes(buffer, bytes)
        if bytes
          write_int(buffer, bytes.bytesize)
          buffer << bytes
        else
          write_int(buffer, -1)
        end
        buffer
      end

      def write_short_bytes(buffer, bytes)
        if bytes
          write_short(buffer, bytes.bytesize)
          buffer << bytes
        else
          write_short(buffer, -1)
        end
        buffer
      end

      def write_consistency(buffer, consistency)
        index = CONSISTENCIES.index(consistency)
        raise EncodingError, %(Unknown consistency "#{consistency}") unless index
        write_short(buffer, index)
      end

      def write_string_map(buffer, map)
        buffer << [map.size].pack(Formats::SHORT_FORMAT)
        map.each do |key, value|
          write_string(buffer, key)
          write_string(buffer, value)
        end
        buffer
      end

      def write_long(buffer, n)
        top = n >> 32
        bottom = n & 0xffffffff
        write_int(buffer, top)
        write_int(buffer, bottom)
      end

      def write_varint(buffer, n)
        num = n
        bytes = []
        until num == 0 || num == -1
          bytes << (num & 0xff)
          num = num >> 8
        end
        buffer << bytes.reverse.pack(Formats::BYTES_FORMAT)
      end

      def write_decimal(buffer, n)
        sign, number_string, _, size = n.split
        num = number_string.to_i
        raw = write_varint('', num)
        write_int(buffer, number_string.length - size)
        buffer << raw
      end

      def write_double(buffer, n)
        buffer << [n].pack(Formats::DOUBLE_FORMAT)
      end

      def write_float(buffer, n)
        buffer << [n].pack(Formats::FLOAT_FORMAT)
      end
    end
  end
end