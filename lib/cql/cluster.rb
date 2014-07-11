# encoding: utf-8

module Cql
  class Cluster
    def initialize(io_reactor, control_connection, cluster_state, client_options)
      @io_reactor         = io_reactor
      @control_connection = control_connection
      @state              = cluster_state
      @options            = client_options
    end

    def hosts
      @state.hosts
    end

    def connect_async(keyspace = nil)
      options = @options.merge({
        :hosts                => @state.ips,
        :keyspace             => keyspace,
      })

      client  = Client::AsynchronousClient.new(options)
      session = Session.new(client)

      client.on_close { @state.remove_client(client) }
      client.connect.map { @state.add_client(client); session }
    end

    def connect(keyspace = nil)
      connect_async(keyspace).get
    end

    def close_async
      if @state.has_clients?
        futures = @state.each_client.map { |c| c.shutdown }
        futures << @control_connection.close_async

        f = Future.all(*futures)
      else
        f = @control_connection.close_async
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
require 'cql/cluster/host'
require 'cql/cluster/state'
