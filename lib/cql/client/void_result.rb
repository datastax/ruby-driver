# encoding: utf-8

module Cql
  module Client
    # Many CQL queries do not return any rows, but they can still return
    # data about the query, for example the trace ID. This class exist to make
    # that data available.
    #
    # It has the exact same API as {Cql::Client::QueryResult} so that you don't
    # need to check the return value of for example {Cql::Client::Client#execute}.
    #
    # @see Cql::Client::QueryResult
    # @see Cql::Client::Client#execute
    # @see Cql::Client::PreparedStatement#execute
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

      # Always returns true
      def last_page?
        true
      end

      # Always returns nil
      def next_page
        nil
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