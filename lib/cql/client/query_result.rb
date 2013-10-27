# encoding: utf-8

module Cql
  module Client
    class QueryResult
      include Enumerable

      # @return [ResultMetadata]
      attr_reader :metadata

      # @private
      def initialize(metadata, rows, trace_loader)
        @metadata = ResultMetadata.new(metadata)
        @rows = rows
        @trace_loader = trace_loader
      end

      # Load the query trace associated with the query, if any.
      #
      # If the trace has not yet been fully written (e.g. the duration field
      # has not yet been populated), a IncompleteTraceError is returned. When
      # that happens, try loading the trace again after a short pause.
      #
      # @note This method is only available as an asynchronous operation and
      #   is therefore not part of the public API yet.
      #
      # @private
      # @return [Future<QueryTrace>]
      def trace
        @trace_loader.load
      end

      # Returns whether or not there are any rows in this result set
      def empty?
        @rows.empty?
      end

      # Iterates over each row in the result set.
      #
      # @yieldparam [Hash] row each row in the result set as a hash
      # @return [Enumerable<Hash>]
      def each(&block)
        @rows.each(&block)
      end
      alias_method :each_row, :each
    end
  end
end