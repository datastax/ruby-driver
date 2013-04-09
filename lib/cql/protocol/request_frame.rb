# encoding: utf-8

module Cql
  module Protocol
    class RequestFrame
      def initialize(body, stream_id=0)
        @body = body
        @stream_id = stream_id
        raise InvalidStreamIdError, 'The stream ID must be between 0 and 127' unless 0 <= @stream_id && @stream_id < 128
      end

      def write(io)
        buffer = [1, 0, @stream_id, @body.opcode, 0].pack(Formats::HEADER_FORMAT)
        buffer = @body.write(buffer)
        buffer[4, 4] = [buffer.length - 8].pack(Formats::INT_FORMAT)
        io << buffer
      end
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

    class CredentialsRequest < RequestBody
      attr_reader :credentials

      def initialize(credentials)
        super(4)
        @credentials = credentials.dup.freeze
      end

      def write(io)
        write_string_map(io, @credentials)
      end

      def to_s
        %(CREDENTIALS #{@credentials})
      end

      def eql?(rq)
        self.class === rq && rq.credentials.eql?(@credentials)
      end
      alias_method :==, :eql?

      def hash
        @h ||= @credentials.hash
      end
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
      attr_reader :cql, :consistency

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

      def eql?(rq)
        self.class === rq && rq.cql.eql?(self.cql) && rq.consistency.eql?(self.consistency)
      end
      alias_method :==, :eql?

      def hash
        @h ||= (@cql.hash * 31) ^ consistency.hash
      end
    end

    class PrepareRequest < RequestBody
      attr_reader :cql

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

      def eql?(rq)
        self.class === rq && rq.cql == self.cql
      end
      alias_method :==, :eql?

      def hash
        @h ||= @cql.hash
      end
    end

    class ExecuteRequest < RequestBody
      attr_reader :id, :metadata, :values, :consistency

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

      def eql?(rq)
        self.class === rq && rq.id == self.id && rq.metadata == self.metadata && rq.values == self.values && rq.consistency == self.consistency
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 0
          h = ((h & 33554431) * 31) ^ @id.hash
          h = ((h & 33554431) * 31) ^ @metadata.hash
          h = ((h & 33554431) * 31) ^ @values.hash
          h = ((h & 33554431) * 31) ^ @consistency.hash
          h
        end
      end

      private

      def write_value(io, value, type)
        if Array === type
          raise InvalidValueError, 'Value for collection must be enumerable' unless value.is_a?(Enumerable)
          case type.first
          when :list, :set
            _, sub_type = type
            raw = ''
            write_short(raw, value.size)
            value.each do |element|
              rr = ''
              write_value(rr, element, sub_type)
              raw << rr[2, rr.length - 2]
            end
            write_bytes(io, raw)
          when :map
            _, key_type, value_type = type
            raw = ''
            write_short(raw, value.size)
            value.each do |key, value|
              rr = ''
              write_value(rr, key, key_type)
              raw << rr[2, rr.length - 2]
              rr = ''
              write_value(rr, value, value_type)
              raw << rr[2, rr.length - 2]
            end
            write_bytes(io, raw)
          else
            raise UnsupportedColumnTypeError, %(Unsupported column collection type: #{type.first})
          end
        else
          case type
          when :ascii
            write_bytes(io, value.encode(::Encoding::ASCII))
          when :bigint
            write_int(io, 8)
            write_long(io, value)
          when :blob
            write_bytes(io, value.encode(::Encoding::BINARY))
          when :boolean
            write_int(io, 1)
            io << (value ? Constants::TRUE_BYTE : Constants::FALSE_BYTE)
          when :decimal
            raw = write_decimal('', value)
            write_int(io, raw.size)
            io << raw
          when :double
            write_int(io, 8)
            write_double(io, value)
          when :float
            write_int(io, 4)
            write_float(io, value)
          when :inet
            write_int(io, value.ipv6? ? 16 : 4)
            io << value.hton
          when :int
            write_int(io, 4)
            write_int(io, value)
          when :text, :varchar
            write_bytes(io, value.encode(::Encoding::UTF_8))
          when :timestamp
            ms = (value.to_f * 1000).to_i
            write_int(io, 8)
            write_long(io, ms)
          when :timeuuid, :uuid
            write_int(io, 16)
            write_uuid(io, value)
          when :varint
            raw = write_varint('', value)
            write_int(io, raw.length)
            io << raw
          else
            raise UnsupportedColumnTypeError, %(Unsupported column type: #{type})
          end
        end
      rescue TypeError => e
        raise TypeError, %("#{value}" cannot be encoded as #{type.to_s.upcase}: #{e.message}), e.backtrace
      end
    end
  end
end