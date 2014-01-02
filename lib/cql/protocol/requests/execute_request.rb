# encoding: utf-8

module Cql
  module Protocol
    class ExecuteRequest < Request
      attr_reader :id, :metadata, :values, :consistency

      def initialize(id, metadata, values, consistency, request_metadata, trace=false)
        raise ArgumentError, "Metadata for #{metadata.size} columns, but #{values.size} values given" if metadata.size != values.size
        super(10, trace)
        @id = id
        @metadata = metadata
        @values = values
        @consistency = consistency
        @request_metadata = request_metadata
        @encoded_values = encode_values
      end

      def write(protocol_version, io)
        write_short_bytes(io, @id)
        if protocol_version > 1
          write_consistency(io, @consistency)
          flags = 0
          flags |= @values.size > 0 ? 1 : 0
          flags |= @request_metadata ? 0 : 2
          io << flags.chr
          if @values.size > 0
            io << @encoded_values
          end
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

      def encode_values
        buffer = ''
        write_short(buffer, @metadata.size)
        @metadata.each_with_index do |(_, _, _, type), index|
          TYPE_CONVERTER.to_bytes(buffer, type, @values[index])
        end
        buffer
      end

      TYPE_CONVERTER = TypeConverter.new
    end
  end
end
