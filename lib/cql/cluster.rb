# encoding: utf-8

module Cql
  class Cluster
    def initialize(io_reactor, control_connection, cluster_registry, client_options)
      @io_reactor         = io_reactor
      @control_connection = control_connection
      @registry           = cluster_registry
      @options            = client_options
      @clients            = ThreadSafe.new(::Set.new)
    end

    def hosts
      @registry.hosts
    end

    def register(listener)
      @registry.add_listener(listener)
      self
    end

    def connect_async(keyspace = nil)
      options = @options.merge({:keyspace => keyspace})

      client  = Client::AsynchronousClient.new(options)
      session = Session.new(client)

      client.connect.map { @clients << client; session }
    end

    def connect(keyspace = nil)
      connect_async(keyspace).get
    end

    def close_async
      if @clients.empty?
        f = @control_connection.close_async
      else
        f = Future.all(*(@clients.map { |c| c.shutdown }), @control_connection.close_async)

        @clients.clear
      end

      f.flat_map { @io_reactor.stop }

      f.map(self)
    end

    def close
      close_async.get
    end
  end
end

require 'cql/cluster/control_connection'
require 'cql/cluster/registry'
