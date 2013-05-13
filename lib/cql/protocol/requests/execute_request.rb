# encoding: utf-8

module Cql
  module Protocol
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
