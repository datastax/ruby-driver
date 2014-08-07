# encoding: utf-8

module Cql
  class Result
    FULFILLED_FUTURE = Futures::Fulfilled.new(nil)

    include Enumerable

    # Query execution information, such as number of retries and all tried hosts, etc.
    # @return [Cql::Execution::Info]
    def execution_info
      @info ||= Execution::Info.new(@keyspace, @statement, @options, @hosts, @consistency, @retries, @trace_id ? Execution::Trace.new(@trace_id, @client) : nil)
    end

    # @return [Boolean] whether it has any rows
    def empty?
    end

    # @return [Integer] rows count
    def size
    end
    alias :length :size

    # @yieldparam [Hash] row
    # @return [Enumerator, Cql::Result]
    def each
    end
    alias :rows :each
    alias :each_row :each

    # @return [Boolean] whether no more pages are available
    def last_page?
    end

    # Loads next page synchronously
    # @see Cql::Session#execute
    def next_page
    end

    # Loads next page asynchronously
    # @see Cql::Session#execute_async
    def next_page_async
    end
  end

  # @private
  module Results
    class Paged < Result
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
        if block_given?
          @rows.each(&block)
          self
        else
          @rows.each
        end
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

    class Void < Result
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
        if block_given?
          NO_ROWS.each(&block)
          self
        else
          NO_ROWS.each
        end
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
