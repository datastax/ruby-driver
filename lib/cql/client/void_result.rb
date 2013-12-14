# encoding: utf-8

module Cql
  module Client
    class VoidResult
      include Enumerable

      INSTANCE = self.new

      # @return [ResultMetadata]
      attr_reader :metadata

      # The ID of the query trace associated with the query, if any.
      #
      # @return [Cql::Uuid]
      attr_reader :trace_id

      # @private
      def initialize(trace_id=nil)
        @trace_id = trace_id
        @metadata = EMPTY_METADATA
      end

      # Always returns true
      def empty?
        true
      end

      # No-op for API compatibility with {QueryResult}.
      #
      # @return [Enumerable]
      def each(&block)
        self
      end
      alias_method :each_row, :each

      private

      EMPTY_METADATA = ResultMetadata.new([])
    end
  end
end