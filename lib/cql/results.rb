# encoding: utf-8

module Cql
  # @private
  module Result
    FULFILLED_FUTURE = Futures::Fulfilled.new(nil)

    include Enumerable

    # The ID of the query trace associated with the query, if any.
    #
    # @return [Cql::Execution::Info]
    def execution_info
      @info ||= Execution::Info.new(@keyspace, @statement, @options, @hosts, @consistency, @retries, @trace_id ? Execution::Trace.new(@trace_id, @client) : nil)
    end

    def empty?
      raise ::NotImplementedError, "must be implemented by a child"
    end

    def size
      raise ::NotImplementedError, "must be implemented by a child"
    end
    alias :length :size

    def each
      raise ::NotImplementedError, "must be implemented by a child"
    end
    alias :rows :each
    alias :each_row :each

    def last_page?
      raise ::NotImplementedError, "must be implemented by a child"
    end

    def next_page
      raise ::NotImplementedError, "must be implemented by a child"
    end

    def next_page_async
      raise ::NotImplementedError, "must be implemented by a child"
    end
  end

  # @private
  module Results
    class Paged
      include Result

      # @private
      def initialize(rows, paging_state, trace_id, keyspace, statement, options, hosts, consistency, retries, client)
        @rows           = rows
        @paging_state   = paging_state
        @trace_id       = trace_id
        @keyspace       = keyspace
        @statement      = statement
        @options        = options
        @hosts          = hosts
        @consistency    = consistency
        @retries        = retries
        @client         = client
      end

      # Returns whether or not there are any rows in this result set
      def empty?
        @rows.empty?
      end

      # Returns count of underlying rows
      def size
        @rows.size
      end
      alias :length :size

      def each(&block)
        @rows.each(&block)
      end
      alias :rows :each
      alias :each_row :each

      # Returns true when there are no more pages to load.
      def last_page?
        @paging_state.nil?
      end

      # Returns the next page or nil when there is no next page.
      #
      # @return [Cql::Result]
      def next_page
        next_page_async.get
      end

      def next_page_async
        return FULFILLED_FUTURE if @paging_state.nil?

        if @statement.is_a?(Statements::Simple)
          @client.query(@statement, @options, @paging_state)
        else
          @client.execute(@statement, @options, @paging_state)
        end
      end
    end

    class Void
      include Result

      # @private
      def initialize(trace_id, keyspace, statement, options, hosts, consistency, retries, client)
        @trace_id    = trace_id
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
        @client      = client
      end

      # Returns whether or not there are any rows in this result set
      def empty?
        true
      end

      # Returns count of underlying rows
      def size
        0
      end
      alias :length :size

      # Iterates over each row in the result set.
      #
      # @yieldparam [Hash] row each row in the result set as a hash
      # @return [Cql::Result]
      def each(&block)
        NO_ROWS.each(&block)
      end
      alias :rows :each
      alias :each_row :each

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
      def next_page_async
        FULFILLED_FUTURE
      end

      def next_page
        nil
      end

      private

      NO_ROWS = [].freeze
    end
  end
end
