# encoding: utf-8

module Cql
  class Cluster
    def initialize(options)
      @options  = ThreadSafe.new(options)
      @sessions = ThreadSafe.new([])
    end

    def connect_async(keyspace)
      options = @options.merge(:keyspace => keyspace).freeze
      client  = Client::AsynchronousClient.new(options)
      session = Session.new(client)

      client.connect.map { @sessions << session; session }
    end

    def connect(keyspace = nil)
      connect_async(keyspace).value
    end

    def close_async
      f = Future.all(*@sessions.map(&:close_async))
      f.on_complete {@sessions.clear}
      f
    end

    def close
      close_async.value

      self
    end
  end
end
