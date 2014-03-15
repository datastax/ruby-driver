# encoding: utf-8

module Cql
  module Protocol
    class PreparedResultResponse < ResultResponse
      attr_reader :id, :metadata, :result_metadata

      def initialize(id, metadata, result_metadata, trace_id)
        super(trace_id)
        @id, @metadata, @result_metadata = id, metadata, result_metadata
      end

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        id = buffer.read_short_bytes
        metadata, _ = RowsResultResponse.read_metadata(protocol_version, buffer)
        result_metadata = nil
        if protocol_version > 1
          result_metadata, _, _ = RowsResultResponse.read_metadata(protocol_version, buffer)
        end
        new(id, metadata, result_metadata, trace_id)
      end

      def eql?(other)
        self.id == other.id && self.metadata == other.metadata && self.trace_id == other.trace_id
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 0
          h = ((h & 0x01ffffff) * 31) ^ @id.hash
          h = ((h & 0x01ffffff) * 31) ^ @metadata.hash
          h = ((h & 0x01ffffff) * 31) ^ @trace_id.hash
          h
        end
      end

      def to_s
        hex_id = @id.each_byte.map { |x| x.to_s(16).rjust(2, '0') }.join('')
        %(RESULT PREPARED #{hex_id} #@metadata)
      end

      private

      RESULT_TYPES[0x04] = self
    end
  end
end
