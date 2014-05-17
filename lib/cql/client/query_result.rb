# encoding: utf-8

module Cql
  module Client
    # Query results encapsulate the rows returned by a query.
    #
    # In addition to containing the rows it contains metadata about the data
    # types of the columns of the rows, and it knows the ID of the trace,
    # if tracing was requested for the query.
    #
    # When paging over a big result you can use {#last_page?} to find out if the
    # page is the last, or {#next_page} to retrieve the next page.
    #
    # `QueryResult` is an `Enumerable` so it can be mapped, filtered, reduced, etc.
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

      # Returns true when there are no more pages to load.
      #
      # This is only relevant when you have requested paging of the results with
      # the `:page_size` option to {Cql::Client::Client#execute} or
      # {Cql::Client::PreparedStatement#execute}.
      #
      # @see Cql::Client::Client#execute
      def last_page?
        true
      end

      # Returns the next page or nil when there is no next page.
      #
      # This is only relevant when you have requested paging of the results with
      # the `:page_size` option to {Cql::Client::Client#execute} or
      # {Cql::Client::PreparedStatement#execute}.
      #
      # @see Cql::Client::Client#execute
      def next_page
        nil
      end
    end

    # @private
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
        return Future.resolved(nil) if last_page?
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
        return Future.resolved(nil) if last_page?
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
        synchronous_backtrace do
          asynchronous_result = @result.next_page.value
          asynchronous_result && self.class.new(asynchronous_result)
        end
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