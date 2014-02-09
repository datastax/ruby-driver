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
      attr_reader :paging_state

      # @private
      def initialize(metadata, rows, trace_id, paging_state)
        @metadata = ResultMetadata.new(metadata)
        @rows = rows
        @trace_id = trace_id
        @paging_state = paging_state
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

    class PagedQueryResult < QueryResult
      def metadata
        @result.metadata
      end

      def trace_id
        @result.trace_id
      end

      def paging_state
        @result.paging_state
      end

      def empty?
        @result.empty?
      end

      def each(&block)
        @result.each(&block)
      end
      alias_method :each_row, :each

      def last_page?
        @last_page
      end

      def next_page
      end
    end

    # @private
    class AsynchronousPagedQueryResult < PagedQueryResult
      def initialize(request, result, options)
        @request = request
        @result = result
        @result = result
        @options = options.merge(paging_state: result.paging_state)
        @last_page = !result.paging_state
      end
    end

    # @private
    class AsynchronousQueryPagedQueryResult < AsynchronousPagedQueryResult
      def initialize(client, request, result, options)
        super(request, result, options)
        @client = client
      end

      def next_page
        @client.execute(@request.cql, *@request.values, @options)
      end
    end

    # @private
    class AsynchronousPreparedPagedQueryResult < AsynchronousPagedQueryResult
      def initialize(prepared_statement, request, result, options)
        super(request, result, options)
        @prepared_statement = prepared_statement
      end

      def next_page
        @prepared_statement.execute(*@request.values, @options)
      end
    end

    # @private
    class SynchronousPagedQueryResult < PagedQueryResult
      include SynchronousBacktrace

      def initialize(asynchronous_result)
        @result = asynchronous_result
      end

      def async
        @result
      end

      def last_page?
        @result.last_page?
      end

      def next_page
        synchronous_backtrace { self.class.new(@result.next_page.value) }
      end
    end

    # @private
    class LazyQueryResult < QueryResult
      def initialize(metadata, lazy_rows, trace_id, paging_state)
        super(metadata, nil, trace_id, paging_state)
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