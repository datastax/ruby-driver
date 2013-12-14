# encoding: utf-8

module Cql
  module Protocol
    class PreparedResultResponse < ResultResponse
      attr_reader :id, :metadata

      def initialize(id, metadata, trace_id)
        super(trace_id)
        @id, @metadata = id, metadata
      end

      def self.decode!(buffer, trace_id=nil)
        id = read_short_bytes!(buffer)
        metadata = RowsResultResponse.read_metadata!(buffer)
        new(id, metadata, trace_id)
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
        %(RESULT PREPARED #{id.each_byte.map { |x| x.to_s(16) }.join('')} #@metadata)
      end

      private

      RESULT_TYPES[0x04] = self
    end
  end
end
