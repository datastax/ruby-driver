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
    class CqlByteBuffer < Ione::ByteBuffer
      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} #{to_str.inspect}>"
      end

      def read_unsigned_byte
        read_byte
      rescue RangeError => e
        raise Errors::DecodingError, e.message, e.backtrace
      end
      
      def read_varint(len=bytesize, signed=true)
        bytes = read(len)
        n = 0
        bytes.each_byte do |b|
          n = (n << 8) | b
        end
        if signed && bytes.getbyte(0) & 0x80 == 0x80
          n -= 2**(bytes.length * 8)
        end
        n
      rescue RangeError => e
        raise Errors::DecodingError, e.message, e.backtrace
      end

      def read_decimal(len=bytesize)
        size = read_signed_int
        number_string = read_varint(len - 4).to_s
        if number_string.length <= size
          if number_string.start_with?(MINUS)
            number_string = number_string[1, number_string.length - 1]
            fraction_string = MINUS + ZERO << DECIMAL_POINT
          else
            fraction_string = ZERO + DECIMAL_POINT
          end
          (size - number_string.length).times { fraction_string << ZERO }
          fraction_string << number_string
        else
          fraction_string = number_string[0, number_string.length - size]
          fraction_string << DECIMAL_POINT
          fraction_string << number_string[number_string.length - size, number_string.length]
        end
        BigDecimal.new(fraction_string)
      rescue Errors::DecodingError => e
        raise Errors::DecodingError, e.message, e.backtrace
      end

      def read_long
        top, bottom = read(8).unpack(Formats::TWO_INTS_FORMAT)
        return (top << 32) | bottom if top <= 0x7fffffff
        top ^= 0xffffffff
        bottom ^= 0xffffffff
        -((top << 32) | bottom) - 1
      rescue RangeError => e
        raise Errors::DecodingError, e.message, e.backtrace
      end

      def read_double
        read(8).unpack(Formats::DOUBLE_FORMAT).first
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode a double: #{e.message}", e.backtrace
      end

      def read_float
        read(4).unpack(Formats::FLOAT_FORMAT).first
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode a float: #{e.message}", e.backtrace
      end

      def read_signed_int
        n = read_int
        return n if n <= 0x7fffffff
        n - 0xffffffff - 1
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode an int: #{e.message}", e.backtrace
      end
      
      def read_unsigned_short
        read_short
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode a short: #{e.message}", e.backtrace
      end

      def read_string
        length = read_unsigned_short
        string = read(length)
        string.force_encoding(::Encoding::UTF_8)
        string
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode a string: #{e.message}", e.backtrace
      end

      def read_long_string
        length = read_signed_int
        string = read(length)
        string.force_encoding(::Encoding::UTF_8)
        string
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode a long string: #{e.message}", e.backtrace
      end

      def read_uuid(impl=Uuid)
        impl.new(read_varint(16, false))
      rescue Errors::DecodingError => e
        raise Errors::DecodingError, "Not enough bytes available to decode a UUID: #{e.message}", e.backtrace
      end

      def read_string_list
        size = read_unsigned_short
        Array.new(size) { read_string }
      end

      def read_bytes
        size = read_signed_int
        return nil if size & 0x80000000 == 0x80000000
        read(size)
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode a bytes: #{e.message}", e.backtrace
      end

      def read_short_bytes
        size = read_unsigned_short
        return nil if size & 0x8000 == 0x8000
        read(size)
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode a short bytes: #{e.message}", e.backtrace
      end

      def read_option
        id = read_unsigned_short
        value = nil
        if block_given?
          value = yield id, self
        end
        [id, value]
      end

      def read_inet
        size = read_byte
        ip_addr = IPAddr.new_ntoh(read(size))
        port = read_int
        [ip_addr, port]
      rescue RangeError => e
        raise Errors::DecodingError, "Not enough bytes available to decode an INET: #{e.message}", e.backtrace
      end

      def read_consistency
        index = read_unsigned_short
        raise Errors::DecodingError, "Unknown consistency index #{index}" if index >= CONSISTENCIES.size || CONSISTENCIES[index].nil?
        CONSISTENCIES[index]
      end

      def read_string_map
        map = {}
        map_size = read_unsigned_short
        map_size.times do
          key = read_string
          map[key] = read_string
        end
        map
      end

      def read_string_multimap
        map = {}
        map_size = read_unsigned_short
        map_size.times do
          key = read_string
          map[key] = read_string_list
        end
        map
      end

      def append_int(n)
        append([n].pack(Formats::INT_FORMAT))
      end

      def append_short(n)
        append([n].pack(Formats::SHORT_FORMAT))
      end

      def append_string(str)
        str = str.to_s
        append_short(str.bytesize)
        append(str)
      end

      def append_long_string(str)
        append_int(str.bytesize)
        append(str)
      end

      def append_uuid(uuid)
        v = uuid.value
        append_int((v >> 96) & 0xffffffff)
        append_int((v >> 64) & 0xffffffff)
        append_int((v >> 32) & 0xffffffff)
        append_int((v >>  0) & 0xffffffff)
      end

      def append_string_list(strs)
        append_short(strs.size)
        strs.each do |str|
          append_string(str)
        end
        self
      end

      def append_bytes(bytes)
        if bytes
          append_int(bytes.bytesize)
          append(bytes)
        else
          append_int(-1)
        end
      end

      def append_short_bytes(bytes)
        if bytes
          append_short(bytes.bytesize)
          append(bytes)
        else
          append_short(-1)
        end
      end

      def append_consistency(consistency)
        index = CONSISTENCIES.index(consistency)
        raise Errors::EncodingError, %(Unknown consistency "#{consistency}") if index.nil? || CONSISTENCIES[index].nil?
        append_short(index)
      end

      def append_string_map(map)
        append_short(map.size)
        map.each do |key, value|
          append_string(key)
          append_string(value)
        end
        self
      end

      def append_long(n)
        top = n >> 32
        bottom = n & 0xffffffff
        append_int(top)
        append_int(bottom)
      end

      def append_varint(n)
        num = n
        bytes = []
        begin
          bytes << (num & 0xff)
          num >>= 8
        end until (num == 0 || num == -1) && (bytes.last[7] == num[7])
        append(bytes.reverse.pack(Formats::BYTES_FORMAT))
      end

      def append_decimal(n)
        str = n.to_s(FLOAT_STRING_FORMAT)
        size = str.index(DECIMAL_POINT)
        number_string = str.gsub(DECIMAL_POINT, NO_CHAR)

        num = number_string.to_i
        raw = self.class.new.append_varint(num)
        append_int(number_string.length - size)
        append(raw)
      end

      def append_double(n)
        append([n].pack(Formats::DOUBLE_FORMAT))
      end

      def append_float(n)
        append([n].pack(Formats::FLOAT_FORMAT))
      end

      def eql?(other)
        other.eql?(to_str)
      end
      alias_method :==, :eql?

      private

      MINUS = '-'.freeze
      ZERO = '0'.freeze
      DECIMAL_POINT = '.'.freeze
      FLOAT_STRING_FORMAT = 'F'.freeze
      NO_CHAR = ''.freeze
    end
  end
end