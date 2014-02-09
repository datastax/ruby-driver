# encoding: utf-8

require 'bigdecimal'


module Cql
  module Protocol
    module Decoding
      extend self

      def read_byte!(buffer)
        buffer.read_byte
      rescue RangeError => e
        raise DecodingError, e.message, e.backtrace
      end

      def read_varint!(buffer, length=buffer.length, signed=true)
        bytes = buffer.read(length)
        n = 0
        bytes.each_byte do |b|
          n = (n << 8) | b
        end
        if signed && bytes.getbyte(0) & 0x80 == 0x80
          n -= 2**(bytes.length * 8)
        end
        n
      rescue RangeError => e
        raise DecodingError, e.message, e.backtrace
      end

      def read_decimal!(buffer, length=buffer.length)
        size = read_int!(buffer)
        number_string = read_varint!(buffer, length - 4).to_s
        if number_string.length < size
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
      rescue DecodingError => e
        raise DecodingError, e.message, e.backtrace
      end

      def read_long!(buffer)
        top, bottom = buffer.read(8).unpack(Formats::TWO_INTS_FORMAT)
        return (top << 32) | bottom if top <= 0x7fffffff
        top ^= 0xffffffff
        bottom ^= 0xffffffff
        -((top << 32) | bottom) - 1
      rescue RangeError => e
        raise DecodingError, e.message, e.backtrace
      end

      def read_double!(buffer)
        buffer.read(8).unpack(Formats::DOUBLE_FORMAT).first
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode a double: #{e.message}", e.backtrace
      end

      def read_float!(buffer)
        buffer.read(4).unpack(Formats::FLOAT_FORMAT).first
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode a float: #{e.message}", e.backtrace
      end

      def read_int!(buffer)
        n = buffer.read_int
        return n if n <= 0x7fffffff
        n - 0xffffffff - 1
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode an int: #{e.message}", e.backtrace
      end

      def read_short!(buffer)
        buffer.read_short
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode a short: #{e.message}", e.backtrace
      end

      def read_string!(buffer)
        length = read_short!(buffer)
        string = buffer.read(length)
        string.force_encoding(::Encoding::UTF_8)
        string
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode a string: #{e.message}", e.backtrace
      end

      def read_long_string!(buffer)
        length = read_int!(buffer)
        string = buffer.read(length)
        string.force_encoding(::Encoding::UTF_8)
        string
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode a long string: #{e.message}", e.backtrace
      end

      def read_uuid!(buffer, impl=Uuid)
        impl.new(read_varint!(buffer, 16, false))
      rescue DecodingError => e
        raise DecodingError, "Not enough bytes available to decode a UUID: #{e.message}", e.backtrace
      end

      def read_string_list!(buffer)
        size = read_short!(buffer)
        size.times.map do
          read_string!(buffer)
        end
      end

      def read_bytes!(buffer)
        size = read_int!(buffer)
        return nil if size & 0x80000000 == 0x80000000
        buffer.read(size)
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode a bytes: #{e.message}", e.backtrace
      end

      def read_short_bytes!(buffer)
        size = read_short!(buffer)
        return nil if size & 0x8000 == 0x8000
        buffer.read(size)
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode a short bytes: #{e.message}", e.backtrace
      end

      def read_option!(buffer)
        id = read_short!(buffer)
        value = nil
        if block_given?
          value = yield id, buffer
        end
        [id, value]
      end

      def read_inet!(buffer)
        size = read_byte!(buffer)
        ip_addr = IPAddr.new_ntoh(buffer.read(size))
        port = read_int!(buffer)
        [ip_addr, port]
      rescue RangeError => e
        raise DecodingError, "Not enough bytes available to decode an INET: #{e.message}", e.backtrace
      end

      def read_consistency!(buffer)
        index = read_short!(buffer)
        raise DecodingError, "Unknown consistency index #{index}" if index >= CONSISTENCIES.size || CONSISTENCIES[index].nil?
        CONSISTENCIES[index]
      end

      def read_string_map!(buffer)
        map = {}
        map_size = read_short!(buffer)
        map_size.times do
          key = read_string!(buffer)
          map[key] = read_string!(buffer)
        end
        map
      end

      def read_string_multimap!(buffer)
        map = {}
        map_size = read_short!(buffer)
        map_size.times do
          key = read_string!(buffer)
          map[key] = read_string_list!(buffer)
        end
        map
      end

      private

      MINUS = '-'.freeze
      ZERO = '0'.freeze
      DECIMAL_POINT = '.'.freeze
    end
  end
end