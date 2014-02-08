# encoding: utf-8

module Cql
  module Protocol
    class ExecuteRequest < Request
      attr_reader :id, :metadata, :values, :request_metadata, :consistency, :serial_consistency

      def initialize(id, metadata, values, request_metadata, consistency, serial_consistency=nil, trace=false)
        raise ArgumentError, "Metadata for #{metadata.size} columns, but #{values.size} values given" if metadata.size != values.size
        super(10, trace)
        @id = id
        @metadata = metadata
        @values = values
        @request_metadata = request_metadata
        @consistency = consistency
        @serial_consistency = serial_consistency
        @encoded_values = self.class.encode_values('', @metadata, @values)
      end

      def write(protocol_version, io)
        write_short_bytes(io, @id)
        if protocol_version > 1
          write_consistency(io, @consistency)
          flags = 0
          flags |= @values.size > 0 ? 1 : 0
          flags |= @request_metadata ? 0 : 2
          flags |= 0x10 if @serial_consistency
          io << flags.chr
          if @values.size > 0
            io << @encoded_values
          end
          write_consistency(io, @serial_consistency) if @serial_consistency
          io
        else
          io << @encoded_values
          write_consistency(io, @consistency)
        end
      end

      def to_s
        id = @id.each_byte.map { |x| x.to_s(16) }.join('')
        %(EXECUTE #{id} #@values #{@consistency.to_s.upcase})
      end

      def eql?(rq)
        self.class === rq && rq.id == self.id && rq.metadata == self.metadata && rq.values == self.values && rq.consistency == self.consistency && rq.serial_consistency == self.serial_consistency
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 0
          h = ((h & 33554431) * 31) ^ @id.hash
          h = ((h & 33554431) * 31) ^ @metadata.hash
          h = ((h & 33554431) * 31) ^ @values.hash
          h = ((h & 33554431) * 31) ^ @consistency.hash
          h = ((h & 33554431) * 31) ^ @serial_consistency.hash
          h
        end
      end

      def self.encode_values(buffer, metadata, values)
        Encoding.write_short(buffer, metadata.size)
        metadata.each_with_index do |(_, _, _, type), index|
          TYPE_CONVERTER.to_bytes(buffer, type, values[index])
        end
        buffer
      end

      private

      TYPE_CONVERTER = TypeConverter.new
    end
  end
end
