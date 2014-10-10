# encoding: utf-8

#--
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
#++

module Cassandra
  # Sessions are used for query execution. Each session tracks its current keyspace. A session should be reused as much as possible, however it is ok to create several independent session for interacting with different keyspaces in the same application.
  class Session
    extend Forwardable

    # @!method keyspace
    #   Returns current keyspace
    #   @return [String] current keyspace
    def_delegators :@client, :keyspace

    # @private
    def initialize(client, default_options, futures_factory)
      @client  = client
      @options = default_options
      @futures = futures_factory
    end

    # Executes a given statement and returns a future result
    # @!method execute_async(statement, *args, options = {})
    #
    # @param statement [String, Cassandra::Statements::Simple,
    #   Cassandra::Statements::Bound, Cassandra::Statements::Prepared]
    #   statement to execute
    # @param args [*Object] arguments to paramterized query or prepared
    #   statement
    #
    # @option options [Symbol] :consistency consistency level for the request.
    #   Must be one of {Cassandra::CONSISTENCIES}
    # @option options [Integer] :page_size size of results page. You can page
    #   through results using {Cassandra::Result#next_page} or
    #   {Cassandra::Result#next_page_async}
    # @option options [Boolean] :trace (false) whether to enable request tracing
    # @option options [Numeric] :timeout (nil) if specified, it is a number of
    #   seconds after which to time out the request if it hasn't completed
    # @option options [Symbol] :serial_consistency (nil) this option is only
    #   relevant for conditional updates and specifies a serial consistency to
    #   be used, one of {Cassandra::SERIAL_CONSISTENCIES}
    #
    # @see Cassandra.connect Options that can be specified on the cluster-level
    #   and their default values.
    #
    # @note Last argument will be treated as `options` if it is a {Hash}.
    #   Therefore, make sure to pass empty `options` when executing a statement
    #   with the last parameter required to be a map datatype.
    #
    # @return [Cassandra::Future<Cassandra::Result>]
    #
    # @see Cassandra::Session#execute A list of errors this future can be
    #   resolved with
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
        @futures.error(::ArgumentError.new("unsupported statement #{statement.inspect}"))
      end
    end

    # A blocking wrapper around {Cassandra::Session#execute_async}
    # @!method execute(statement, *args, options = {})
    # @see Cassandra::Session#execute_async
    # @see Cassandra::Future#get
    #
    # @return [Cassandra::Result] query result
    # @raise [Cassandra::Errors::NoHostsAvailable] if all hosts fail
    # @raise [Cassandra::Errors::ExecutionError] if Cassandra fails to execute
    # @raise [Cassandra::Errors::ValidationError] if Cassandra fails to validate
    # @raise [Cassandra::Errors::TimeoutError] if request has not completed
    #   within the `:timeout` given
    def execute(*args)
      execute_async(*args).get
    end

    # Prepares a given statement and returns a future prepared statement
    #
    # @param statement [String, Cassandra::Statements::Simple] a statement to
    #   prepare
    #
    # @option options [Boolean] :trace (false) whether to enable request tracing
    # @option options [Numeric] :timeout (nil) if specified, it is a number of
    #   seconds after which to time out the request if it hasn't completed
    #
    # @return [Cassandra::Future<Cassandra::Statements::Prepared>] future
    #   prepared statement
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
        @futures.error(::ArgumentError.new("unsupported statement #{statement.inspect}"))
      end
    end

    # A blocking wrapper around {Cassandra::Session#prepare_async}
    # @see Cassandra::Session#prepare_async
    # @see Cassandra::Future#get
    #
    # @return [Cassandra::Statements::Prepared] prepared statement
    # @raise [Cassandra::Errors::NoHostsAvailable] if none of the hosts can be reached
    # @raise [Cassandra::Errors::ExecutionError] if Cassandra returns an error response
    def prepare(*args)
      prepare_async(*args).get
    end

    # Returns a logged {Statements::Batch} instance and optionally yields it to
    # a given block
    # @yieldparam batch [Statements::Batch] a logged batch
    # @return [Statements::Batch] a logged batch
    def logged_batch(&block)
      statement = Statements::Batch::Logged.new
      yield(statement) if block_given?
      statement
    end
    alias :batch :logged_batch

    # Returns a unlogged {Statements::Batch} instance and optionally yields it
    # to a given block
    # @yieldparam batch [Statements::Batch] an unlogged batch
    # @return [Statements::Batch] an unlogged batch
    def unlogged_batch
      statement = Statements::Batch::Unlogged.new
      yield(statement) if block_given?
      statement
    end

    # Returns a counter {Statements::Batch} instance and optionally yields it
    # to a given block
    # @yieldparam batch [Statements::Batch] a counter batch
    # @return [Statements::Batch] a counter batch
    def counter_batch
      statement = Statements::Batch::Counter.new
      yield(statement) if block_given?
      statement
    end

    # Asynchronously closes current session
    #
    # @return [Cassandra::Future<Cassandra::Session>] a future that resolves to
    #   self once closed
    def close_async
      promise = @futures.promise

      @client.close.on_complete do |f|
        if f.resolved?
          promise.fulfill(self)
        else
          f.on_failure {|e| promise.break(e)}
        end
      end

      promise.future
    end

    # Synchronously closes current session
    #
    # @return [self] this session
    #
    # @see Cassandra::Session#close_async
    def close
      close_async.get
    end

    # @return [String] a CLI-friendly session representation
    def inspect
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
    end
  end
end
