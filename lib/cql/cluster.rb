# encoding: utf-8

module Cql
  class Cluster
    def initialize(control_connection, cluster_state, client_options)
      @control_connection = control_connection
      @state              = cluster_state
      @options            = client_options
    end

    def hosts
      @state.hosts
    end

    def connect_async(keyspace = nil)
      options = @options.merge({
        :hosts                => @state.hosts.values.map {|host| host.ip},
        :keyspace             => keyspace,
      })

      client  = Client::AsynchronousClient.new(options)
      session = Session.new(@state, client)

      client.connect.map { @state.add_client(client); session }
    end

    def connect(keyspace = nil)
      connect_async(keyspace).get
    end

    def close_async
      if @state.has_clients?
        f = @control_connection.close_async
      else
        futures = @state.each_client.map do |client|
                    f = client.close
                    f.on_complete { @state.remove_client(client) }
                    f.map(self)
                  end

        f = Future.all(*futures)
        f = f.flat_map { @control_connection.close_async }
      end

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
