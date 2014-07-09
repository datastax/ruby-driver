# encoding: utf-8

module Cql
  class Cluster
    State = Struct.new(:hosts, :clients)
    class Host
      attr_reader   :ip
      attr_accessor :id, :rack, :datacenter, :release_version

      def initialize(ip, data = {})
        @ip              = ip
        @id              = data['host_id']
        @release_version = data['release_version']
        @rack            = data['rack']
        @datacenter      = data['data_center']
        @status          = :unknown
      end

      def up?
        @status == :up
      end

      def up!
        @status = :up
        self
      end

      def down?
        @status == :down
      end

      def down!
        @status = :down
        self
      end
    end

    def initialize(control_connection, cluster_state, client_options)
      @control_connection = control_connection
      @state              = cluster_state
      @options            = client_options
    end

    def hosts
      @state.hosts.map {|_, h| Cql::Host.new(h.ip, h.id, h.rack, h.datacenter, h.release_version)}
    end

    def connect_async(keyspace = nil)
      options = @options.merge({
        :hosts                => @state.hosts.values.map {|host| host.ip},
        :keyspace             => keyspace,
      })

      client  = Client::AsynchronousClient.new(options)
      session = Session.new(@state.clients, client)

      client.connect.map { @state.clients << client; session }
    end

    def connect(keyspace = nil)
      connect_async(keyspace).get
    end

    def close_async
      if @state.clients.empty?
        f = @control_connection.close_async
      else
        futures = @state.clients.map do |client|
                    f = client.close
                    f.on_complete { @state.clients.delete(client) }
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
