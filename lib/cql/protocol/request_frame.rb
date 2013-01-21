# encoding: utf-8

module Cql
  module Protocol
    InvalidStreamIdError = Class.new(CqlError)

    class RequestFrame
      def initialize(body, stream_id=0)
        @body = body
        @stream_id = stream_id
        raise InvalidStreamIdError, 'The stream ID must be between 0 and 127' unless 0 <= @stream_id && @stream_id < 128
      end

      def write(io)
        buffer = [1, 0, @stream_id, @body.opcode, 0].pack(HEADER_FORMAT)
        buffer = @body.write(buffer)
        buffer[4, 4] = [buffer.length - 8].pack(INT_FORMAT)
        io << buffer
      end

      private

      INT_FORMAT = 'N'.freeze
    end

    class RequestBody
      include Encoding

      attr_reader :opcode

      def initialize(opcode)
        @opcode = opcode
      end
    end

    class StartupRequest < RequestBody
      def initialize(cql_version='3.0.0', compression=nil)
        super(1)
        @arguments = {CQL_VERSION => cql_version}
        @arguments[COMPRESSION] = compression if compression
      end

      def write(io)
        write_string_map(io, @arguments)
        io
      end

      def to_s
        %(STARTUP #@arguments)
      end

      private

      CQL_VERSION = 'CQL_VERSION'.freeze
      COMPRESSION = 'COMPRESSION'.freeze
    end

    class OptionsRequest < RequestBody
      def initialize
        super(5)
      end

      def write(io)
        io
      end

      def to_s
        %(OPTIONS)
      end
    end

    class RegisterRequest < RequestBody
      def initialize(*events)
        super(11)
        @events = events
      end

      def write(io)
        write_string_list(io, @events)
      end

      def to_s
        %(REGISTER #@events)
      end
    end

    class QueryRequest < RequestBody
      def initialize(cql, consistency)
        super(7)
        @cql = cql
        @consistency = consistency
      end

      def write(io)
        write_long_string(io, @cql)
        write_consistency(io, @consistency)
      end

      def to_s
        %(QUERY "#@cql" #{@consistency.to_s.upcase})
      end
    end

    class PrepareRequest < RequestBody
      def initialize(cql)
        super(9)
        @cql = cql
      end

      def write(io)
        write_long_string(io, @cql)
      end

      def to_s
        %(PREPARE "#@cql")
      end
    end

    class ExecuteRequest < RequestBody
      def initialize(id, metadata, values, consistency)
        super(10)
        raise ArgumentError, "Metadata for #{metadata.size} columns, but #{values.size} values given" if metadata.size != values.size
        @id = id
        @metadata = metadata
        @values = values
        @consistency = consistency
      end

      def write(io)
        write_short_bytes(io, @id)
        write_short(io, @metadata.size)
        @metadata.each_with_index do |(_, _, _, type), index|
          write_value(io, @values[index], type)
        end
        write_consistency(io, @consistency)
      end

      def to_s
        id = @id.each_byte.map { |x| x.to_s(16) }.join('')
        %(EXECUTE #{id} #@values #{@consistency.to_s.upcase})
      end

      private

      BYTES_FORMAT = 'C*'.freeze
      DOUBLE_FORMAT = 'G'.freeze
      FLOAT_FORMAT = 'g'.freeze
      INT_FORMAT = 'N'.freeze
      TRUE_BYTE = "\x01".freeze
      FALSE_BYTE = "\x00".freeze
      EMPTY_STRING = ''.freeze

      def write_value(io, value, type)
        case type
        when :ascii
          write_bytes(io, value.encode(::Encoding::ASCII))
        when :bigint
          write_long(io, value)
        when :blob
          write_bytes(io, value.encode(::Encoding::BINARY))
        when :boolean
          write_int(io, 1)
          io << (value ? TRUE_BYTE : FALSE_BYTE)
        when :decimal
          sign, number_string, _, size = value.split
          num = number_string.to_i
          raw = write_bignum('', num)
          write_int(io, 4 + raw.size)
          write_int(io, number_string.length - size)
          io << raw
        when :double
          write_int(io, 8)
          io << [value].pack(DOUBLE_FORMAT)
        when :float
          write_int(io, 4)
          io << [value].pack(FLOAT_FORMAT)
        when :inet
          if value.ipv6?
            write_int(io, 16)
            io << value.hton
          else
            write_int(io, 4)
            io << value.hton
          end
        when :int
          write_int(io, 4)
          io << [value].pack(INT_FORMAT)
        when :text, :varchar
          write_bytes(io, value.encode(::Encoding::UTF_8))
        when :timestamp
          ms = (value.to_f * 1000).to_i
          write_long(io, ms)
        when :timeuuid, :uuid
          write_int(io, 16)
          write_bignum(io, value.value)
        when :varint
          raw = write_bignum('', value)
          write_int(io, raw.length)
          io << raw
        else
          raise UnsupportedColumnTypeError, %(Unsupported column type: #{type})
        end
      end

      def write_long(io, n)
        top = n >> 32
        bottom = n & 0xffffffff
        write_int(io, 8)
        write_int(io, top)
        write_int(io, bottom)
      end

      def write_bignum(io, n)
        num = n
        bytes = []
        until num == 0 || num == -1
          bytes << (num & 0xff)
          num = num >> 8
        end
        io << bytes.reverse.pack(BYTES_FORMAT)
      end
    end
  end
end