# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
#++

module Cassandra
  class Result
    include Enumerable

    # Query execution information, such as number of retries and all tried hosts, etc.
    # @return [Cassandra::Execution::Info]
    def execution_info
      @info ||= Execution::Info.new(@payload,
                                    @warnings,
                                    @keyspace,
                                    @statement,
                                    @options,
                                    @hosts,
                                    @consistency,
                                    @retries,
                                    @trace_id ?
                                        Execution::Trace.new(@trace_id, @client, @options.load_balancing_policy) :
                                        nil)
    end

    # @return [Boolean] whether it has any rows
    def empty?
    end

    # @return [Integer] rows count
    def size
    end
    alias length size

    # @yieldparam row [Hash] current row
    # @return [Enumerator, self] returns Enumerator if no block given
    def each
    end
    alias rows each
    alias each_row each

    # @return [Boolean] whether no more pages are available
    def last_page?
    end

    # Loads next page synchronously
    #
    # @param options [Hash] additional options, just like the ones for
    #   {Cassandra::Session#execute}
    #
    # @note `:paging_state` option will be ignored.
    #
    # @return [Cassandra::Result, nil] returns `nil` if last page
    #
    # @see Cassandra::Session#execute
    def next_page(options = nil)
    end

    # Loads next page asynchronously
    #
    # @param options [Hash] additional options, just like the ones for
    #   {Cassandra::Session#execute_async}
    #
    # @note `:paging_state` option will be ignored.
    #
    # @return [Cassandra::Future<Cassandra::Result>] a future that resolves to a new Result if there is a new page,
    #   `nil` otherwise.
    #
    # @see Cassandra::Session#execute
    def next_page_async(options = nil)
    end

    # Exposes current paging state for stateless pagination.
    #
    # @return [String, nil] current paging state as a `String` or `nil`.
    #
    # @note Although this feature exists to allow web applications to store
    #   paging state in an [HTTP cookie](http://en.wikipedia.org/wiki/HTTP_cookie),
    #   **it is not safe to expose without encrypting or otherwise securing it**.
    #   Paging state contains information internal to the Apache Cassandra cluster,
    #   such as partition key and data. Additionally, if a paging state is sent with CQL
    #   statement, different from the original, the behavior of Cassandra is
    #   undefined and will likely cause a server process of the coordinator of
    #   such request to abort.
    #
    # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v2.spec#L482-L487 Paging State
    #   description in Cassandra Native Protocol v2 specification
    def paging_state
    end
  end

  # @private
  module Results
    class Paged < Result
      attr_reader :paging_state

      def initialize(payload,
                     warnings,
                     rows,
                     paging_state,
                     trace_id,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries,
                     client,
                     futures_factory)
        @payload        = payload
        @warnings       = warnings
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
        @futures        = futures_factory
      end

      # Returns whether or not there are any rows in this result set
      def empty?
        @rows.empty?
      end

      # Returns count of underlying rows
      def size
        @rows.size
      end
      alias length size

      def each(&block)
        if block_given?
          @rows.each(&block)
          self
        else
          @rows.each
        end
      end
      alias rows each
      alias each_row each

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
        return @futures.value(nil) if @paging_state.nil?

        options = @options.override(options, paging_state: @paging_state)

        if @statement.is_a?(Statements::Simple)
          @client.query(@statement, options)
        else
          @client.execute(@statement, options)
        end
      end

      # @private
      def inspect
        "#<Cassandra::Result:0x#{object_id.to_s(16)} " \
            "@rows=#{@rows.inspect} " \
            "@last_page=#{@paging_state.nil?}>"
      end
    end

    class Void < Result
      def initialize(payload,
                     warnings,
                     trace_id,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries,
                     client,
                     futures_factory)
        @payload     = payload
        @warnings    = warnings
        @trace_id    = trace_id
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
        @client      = client
        @futures     = futures_factory
      end

      # Returns whether or not there are any rows in this result set
      def empty?
        true
      end

      # Returns count of underlying rows
      def size
        0
      end
      alias length size

      # Iterates over each row in the result set.
      #
      # @yieldparam row [Hash] each row in the result set as a hash
      # @return [Cassandra::Result]
      def each(&block)
        if block_given?
          EMPTY_LIST.each(&block)
          self
        else
          EMPTY_LIST.each
        end
      end
      alias rows each
      alias each_row each

      # Returns true when there are no more pages to load.
      #
      # This is only relevant when you have requested paging of the results with
      # the `:page_size` option to {Cassandra::Session#execute}.
      #
      # @see Cassandra::Session#execute
      def last_page?
        true
      end

      # Returns the next page or nil when there is no next page.
      #
      # This is only relevant when you have requested paging of the results with
      # the `:page_size` option to {Cassandra::Session#execute_async}.
      #
      # @see Cassandra::Session#execute_async
      def next_page_async(options = nil)
        @futures.value(nil)
      end

      def next_page(options = nil)
        nil
      end

      def paging_state
        nil
      end

      def inspect
        "#<Cassandra::Result:0x#{object_id.to_s(16)} @rows=[] @last_page=true>"
      end
    end
  end
end
