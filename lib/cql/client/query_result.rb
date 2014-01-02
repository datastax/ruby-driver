# encoding: utf-8

module Cql
  module Client
    class QueryResult
      include Enumerable

      # @return [ResultMetadata]
      attr_reader :metadata

      # The ID of the query trace associated with the query, if any.
      #
      # @return [Cql::Uuid]
      attr_reader :trace_id

      # @private
      def initialize(metadata, rows, trace_id)
        @metadata = ResultMetadata.new(metadata)
        @rows = rows
        @trace_id = trace_id
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

    # @private
    class LazyQueryResult < QueryResult
      def initialize(metadata, lazy_rows, trace_id)
        super(metadata, nil, trace_id)
        @raw_metadata = metadata
        @lazy_rows = lazy_rows
        @lock = Mutex.new
      end

      def empty?
        ensure_materialized
        super
      end

      def each(&block)
        ensure_materialized
        super
      end
      alias_method :each_row, :each

      private

      def ensure_materialized
        unless @rows
          @lock.synchronize do
            unless @rows
              @rows = @lazy_rows.materialize(@raw_metadata)
            end
          end
        end
      end
    end
  end
end