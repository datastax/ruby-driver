# encoding: utf-8

module Cql
  module Result
    # The ID of the query trace associated with the query, if any.
    #
    # @return [Cql::Execution::Info]
    attr_reader :execution_info
  end

  module Results
    class Paged
      include Result, Enumerable

      # @private
      def initialize(metadata, rows, paging_state, execution_info)
        @raw_metadata   = metadata
        @rows           = rows
        @paging_state   = paging_state
        @execution_info = execution_info
      end

      # @return [ResultMetadata]
      def metadata
        @metadata ||= Client::ResultMetadata.new(@raw_metadata)
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

      # Iterates over each row in the result set.
      #
      # @yieldparam [Hash] row each row in the result set as a hash
      # @return [Cql::Result]
      def each(&block)
        @rows.each(&block)
        self
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
        @paging_state.nil?
      end

      # Returns the next page or nil when there is no next page.
      #
      # This is only relevant when you have requested paging of the results with
      # the `:page_size` option to {Cql::Client::Client#execute} or
      # {Cql::Client::PreparedStatement#execute}.
      #
      # @see Cql::Client::Client#execute
      def next_page
        return Future.resolved if last_page?
        @client.execute(@request.cql, *@request.values, @options)
      end
    end

    class Void
      include Result, Enumerable

      def initialize(execution_info)
        @execution_info = execution_info
      end

      def metadata
        EMPTY_METADATA
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
        self
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
        return Future.resolved
      end

      private

      EMPTY_ROWS     = [].freeze
      EMPTY_METADATA = Client::ResultMetadata.new(EMPTY_ROWS)
    end
  end
end
