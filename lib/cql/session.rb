# encoding: utf-8

module Cql
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
    # @param statement [String, Cql::Statements::Simple,
    #   Cql::Statements::Bound, Cql::Statements::Prepared] statement to
    #   execute
    # @param args [*Object] arguments to paramterized query or prepared
    #   statement
    #
    # @option options [Symbol] :consistency (:one) consistency level for the
    #   request, one of {Cql::CONSISTENCIES}
    # @option options [Integer] :page_size (50000) size of results page, you
    #   can page through results using {Cql::Result#next_page} or
    #   {Cql::Result#next_page_async}
    # @option options [Boolean] :trace (false) whether to enable request
    #   tracing
    # @option options [Numeric] :timeout (nil) if specified, it is a number
    #   of seconds after which to time out the request if it hasn't completed
    # @option options [Symbol] :serial_consistency (nil) this option is
    #   relevant for conditional updates and specifies a serial consistency to
    #   be used, one of {Cql::SERIAL_CONSISTENCIES}
    #
    # @return [Cql::Future<Cql::Result>]
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

    # A blocking wrapper around {Cql::Session#execute_async}
    # @!method execute(statement, *args, options = {})
    # @see Cql::Session#execute_async
    # @see Cql::Future#get
    #
    # @return [Cql::Result] query result
    # @raise [Cql::Errors::NoHostsAvailable] if none of the hosts can be reached
    # @raise [Cql::Errors::QueryError] if Cassandra returns an error response
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
