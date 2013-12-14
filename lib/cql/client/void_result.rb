# encoding: utf-8

module Cql
  module Client
    class VoidResult
      include Enumerable

      # The ID of the query trace associated with the query, if any.
      #
      # @return [Cql::Uuid]
      attr_reader :trace_id

      # @private
      def initialize(trace_id)
        @trace_id = trace_id
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
    end
  end
end