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
        buffer << [str.length].pack(Formats::SHORT_FORMAT)
        buffer << str
        buffer
      end

      def write_long_string(buffer, str)
        buffer << [str.length].pack(Formats::INT_FORMAT)
        buffer << str
        buffer
      end

      def write_uuid(buffer, uuid)
        raise NotImplementedError
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
          write_int(buffer, bytes.length)
          buffer << bytes
        else
          write_int(buffer, -1)
        end
        buffer
      end

      def write_short_bytes(buffer, bytes)
        if bytes
          write_short(buffer, bytes.length)
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
    end
  end
end