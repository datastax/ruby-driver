# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Cassandra
  class Result
    # @private
    FULFILLED_FUTURE = Futures::Fulfilled.new(nil)

    include Enumerable

    # Query execution information, such as number of retries and all tried hosts, etc.
    # @return [Cassandra::Execution::Info]
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

    # @yieldparam row [Hash] current row
    # @return [Enumerator, self] returns Enumerator if no block given
    def each
    end
    alias :rows :each
    alias :each_row :each

    # @return [Boolean] whether no more pages are available
    def last_page?
    end

    # Loads next page synchronously
    # @see Cassandra::Session#execute
    def next_page
    end

    # Loads next page asynchronously
    # @return [Cassandra::Result, nil] returns `nil` if last page
    # @see Cassandra::Session#execute_async
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
      # @return [Cassandra::Result]
      def next_page(options = nil)
        next_page_async(options).get
      end

      def next_page_async(options = nil)
        return FULFILLED_FUTURE if @paging_state.nil?

        options = options ? @options.override(options) : @options

        if @statement.is_a?(Statements::Simple)
          @client.query(@statement, options, @paging_state)
        else
          @client.execute(@statement, options, @paging_state)
        end
      end

      def inspect
        "#<Cassandra::Result:0x#{self.object_id.to_s(16)}>"
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
      # @yieldparam row [Hash] each row in the result set as a hash
      # @return [Cassandra::Result]
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
      # the `:page_size` option to {Cassandra::Client::Client#execute} or
      # {Cassandra::Client::PreparedStatement#execute}.
      #
      # @see Cassandra::Client::Client#execute
      def last_page?
        true
      end

      # Returns the next page or nil when there is no next page.
      #
      # This is only relevant when you have requested paging of the results with
      # the `:page_size` option to {Cassandra::Client::Client#execute} or
      # {Cassandra::Client::PreparedStatement#execute}.
      #
      # @see Cassandra::Client::Client#execute
      def next_page_async(options = nil)
        FULFILLED_FUTURE
      end

      def next_page(options = nil)
        nil
      end

      def inspect
        "#<Cassandra::Result:0x#{self.object_id.to_s(16)}>"
      end

      private

      NO_ROWS = [].freeze
    end
  end
end
