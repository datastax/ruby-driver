# encoding: utf-8

module Cql
  class Session
    def initialize(client)
      @client = client
    end

    def execute_async(cql, *args)
      case cql
      when Client::Batch, Client::PreparedStatement
        cql.execute(*args)
      else
        @client.execute(cql, *args)
      end
    end

    def execute(cql, *args)
      execute_async(cql, *args).get
    end

    def prepare_async(cql)
      @client.prepare(cql)
    end

    def prepare(cql)
      prepare_async(cql).get
    end

    def batch
      batch = @client.batch(:logged)
      yield(batch) if block_given?
      batch
    end
    alias :logged_batch :batch

    def unlogged_batch
      batch = @client.batch(:unlogged)
      yield(batch) if block_given?
      batch
    end

    def counter_batch
      batch = @client.batch(:counter)
      yield(batch) if block_given?
      batch
    end

    def close_async
      @client.close
    end

    def close
      close_async.get
    end
  end
end
