# encoding: utf-8

module Cql
  module Protocol
    module Decoding
      extend self

      def read_byte!(buffer)
        raise DecodingError, 'No byte available to decode' if buffer.empty?
        b = buffer.slice!(0, 1)
        b.getbyte(0)
      end

      def read_varint!(buffer, length=buffer.length, signed=true)
        raise DecodingError, "Length #{length} specifed but only #{buffer.bytesize} bytes given" if buffer.bytesize < length
        bytes = buffer.slice!(0, length)
        n = 0
        bytes.each_byte do |b|
          n = (n << 8) | b
        end
        if signed && bytes.getbyte(0) & 0x80 == 0x80
          n -= 2**(bytes.length * 8)
        end
        n
      end

      def read_decimal!(buffer, length=buffer.length)
        raise DecodingError, "Length #{length} specifed but only #{buffer.bytesize} bytes given" if buffer.bytesize < length
        size = read_int!(buffer)
        number_bytes = buffer.slice!(0, length - 4)
        number_string = read_varint!(number_bytes).to_s
        fraction_string = number_string[0, number_string.length - size] << DECIMAL_POINT << number_string[number_string.length - size, number_string.length]
        BigDecimal.new(fraction_string)
      end

      def read_long!(buffer)
        raise DecodingError, "Need eight bytes to decode long, only #{buffer.bytesize} bytes given" if buffer.bytesize < 8
        top, bottom = buffer.slice!(0, 8).unpack(Formats::TWO_INTS_FORMAT)
        (top << 32) | bottom
      end

      def read_double!(buffer)
        raise DecodingError, "Need eight bytes to decode double, only #{buffer.bytesize} bytes given" if buffer.bytesize < 8
        buffer.slice!(0, 8).unpack(Formats::DOUBLE_FORMAT).first
      end

      def read_float!(buffer)
        raise DecodingError, "Need four bytes to decode float, only #{buffer.bytesize} bytes given" if buffer.bytesize < 4
        buffer.slice!(0, 4).unpack(Formats::FLOAT_FORMAT).first
      end

      def read_int!(buffer)
        raise DecodingError, "Need four bytes to decode an int, only #{buffer.bytesize} bytes given" if buffer.bytesize < 4
        buffer.slice!(0, 4).unpack(Formats::INT_FORMAT).first
      end

      def read_short!(buffer)
        raise DecodingError, "Need two bytes to decode a short, only #{buffer.bytesize} bytes given" if buffer.bytesize < 2
        buffer.slice!(0, 2).unpack(Formats::SHORT_FORMAT).first
      end

      def read_string!(buffer)
        length = read_short!(buffer)
        raise DecodingError, "String length is #{length}, but only #{buffer.bytesize} bytes given" if buffer.bytesize < length
        string = buffer.slice!(0, length)
        string.force_encoding(::Encoding::UTF_8)
        string
      end

      def read_long_string!(buffer)
        length = read_int!(buffer)
        raise DecodingError, "String length is #{length}, but only #{buffer.bytesize} bytes given" if buffer.bytesize < length
        string = buffer.slice!(0, length)
        string.force_encoding(::Encoding::UTF_8)
        string
      end

      def read_uuid!(buffer)
        raise DecodingError, "UUID requires 16 bytes, but only #{buffer.bytesize} bytes given" if buffer.bytesize < 16
        Uuid.new(read_varint!(buffer, 16, false))
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
        raise DecodingError, "Byte array length is #{size}, but only #{buffer.bytesize} bytes given" if buffer.bytesize < size
        bytes = buffer.slice!(0, size)
        bytes.force_encoding(::Encoding::BINARY)
        bytes
      end

      def read_short_bytes!(buffer)
        size = read_short!(buffer)
        return nil if size & 0x8000 == 0x8000
        raise DecodingError, "Byte array length is #{size}, but only #{buffer.bytesize} bytes given" if buffer.bytesize < size
        bytes = buffer.slice!(0, size)
        bytes.force_encoding(::Encoding::BINARY)
        bytes
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
        raise DecodingError, "Inet requires #{size} bytes, but only #{buffer.bytesize} bytes given" if buffer.bytesize < size
        ip_addr = IPAddr.new_ntoh(buffer.slice!(0, size))
        port = read_int!(buffer)
        [ip_addr, port]
      end

      def read_consistency!(buffer)
        index = read_short!(buffer)
        raise DecodingError, "Unknown consistency index #{index}" unless index < CONSISTENCIES.size
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

      DECIMAL_POINT = '.'.freeze
    end
  end
end