# encoding: utf-8

module Cql
  class Session
    # @private
    def initialize(client, default_options)
      @client  = client
      @options = default_options
    end

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

    def batch(&block)
      statement = Statements::Batch::Logged.new
      yield(statement) if block_given?
      statement
    end
    alias :logged_batch :batch

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
  end
end
