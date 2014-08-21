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
  # Sessions are used for query execution. Each session tracks its current keyspace. A session should be reused as much as possible, however it is ok to create several independent session for interacting with different keyspaces in the same application.
  class Session
    # @private
    def initialize(client, default_options)
      @client  = client
      @options = default_options
    end

    # Executes a given statement and returns a future result
    # @!method execute_async(statement, *args, options = {})
    #
    # @param statement [String, Cassandra::Statements::Simple,
    #   Cassandra::Statements::Bound, Cassandra::Statements::Prepared] statement to
    #   execute
    # @param args [*Object] arguments to paramterized query or prepared
    #   statement
    #
    # @option options [Symbol] :consistency (:one) consistency level for the
    #   request, one of {Cassandra::CONSISTENCIES}
    # @option options [Integer] :page_size (50000) size of results page, you
    #   can page through results using {Cassandra::Result#next_page} or
    #   {Cassandra::Result#next_page_async}
    # @option options [Boolean] :trace (false) whether to enable request
    #   tracing
    # @option options [Numeric] :timeout (nil) if specified, it is a number
    #   of seconds after which to time out the request if it hasn't completed
    # @option options [Symbol] :serial_consistency (nil) this option is
    #   relevant for conditional updates and specifies a serial consistency to
    #   be used, one of {Cassandra::SERIAL_CONSISTENCIES}
    #
    # @return [Cassandra::Future<Cassandra::Result>]
    def execute_async(statement, *args)
      if args.last.is_a?(::Hash)
        options = @options.override(args.pop)
      else
        options = @options
      end

      case statement
      when ::String
        @client.query(Statements::Simple.new(statement, *args), options)
      when Statements::Simple
        @client.query(statement, options)
      when Statements::Prepared
        @client.execute(statement.bind(*args), options)
      when Statements::Bound
        @client.execute(statement, options)
      when Statements::Batch
        @client.batch(statement, options)
      else
        Futures::Broken.new(::ArgumentError.new("unsupported statement #{statement.inspect}"))
      end
    end

    # A blocking wrapper around {Cassandra::Session#execute_async}
    # @!method execute(statement, *args, options = {})
    # @see Cassandra::Session#execute_async
    # @see Cassandra::Future#get
    #
    # @return [Cassandra::Result] query result
    # @raise [Cassandra::Errors::NoHostsAvailable] if none of the hosts can be reached
    # @raise [Cassandra::Errors::QueryError] if Cassandra returns an error response
    def execute(*args)
      execute_async(*args).get
    end

    def prepare_async(statement, options = nil)
      if options.is_a?(::Hash)
        options = @options.override(options)
      else
        options = @options
      end

      case statement
      when ::String
        @client.prepare(statement, options)
      when Statements::Simple
        @client.prepare(statement.cql, options)
      else
        Futures::Broken.new(::ArgumentError.new("unsupported statement #{statement.inspect}"))
      end
    end

    def prepare(*args)
      prepare_async(*args).get
    end

    # Returns a new {Statements::Batch} instance and optionally yields it to a
    # given block
    # @yieldparam batch [Statements::Batch] a logged batch
    # @return [Statements::Batch] a logged batch
    def logged_batch(&block)
      statement = Statements::Batch::Logged.new
      yield(statement) if block_given?
      statement
    end
    alias :batch :logged_batch

    def unlogged_batch
      statement = Statements::Batch::Unlogged.new
      yield(statement) if block_given?
      statement
    end

    def counter_batch
      statement = Statements::Batch::Counter.new
      yield(statement) if block_given?
      statement
    end

    def close_async
      promise = Promise.new

      @client.close.on_complete do |f|
        if f.resolved?
          promise.fulfill(self)
        else
          f.on_failure {|e| promise.break(e)}
        end
      end

      promise.future
    end

    def close
      close_async.get
    end

    def inspect
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
    end
  end
end
