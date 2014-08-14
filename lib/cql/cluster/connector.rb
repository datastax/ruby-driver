# encoding: utf-8

module Cql
  class Cluster
    # @private
    class Connector
      def initialize(logger, io_reactor, eviction_policy, connection_options)
        @logger             = logger
        @reactor            = io_reactor
        @eviction_policy    = eviction_policy
        @connection_options = connection_options
      end

      def connect(host, distance)
        if distance.ignore?
          return NO_CONNECTIONS
        elsif distance.local?
          pool_size = @connection_options.connections_per_local_node
        else
          pool_size = @connection_options.connections_per_remote_node
        end

        f = create_cluster_connector.connect_all([host.ip.to_s], pool_size)

        f.on_complete do |f|
          if f.resolved?
            f.value.each do |connection|
              connection.on_closed do |cause|
                @eviction_policy.disconnected(host, cause)
              end

              @eviction_policy.connected(host)
            end
          else
            f.on_failure do |e|
              @eviction_policy.connection_error(host, e)
            end
          end
        end

        f
      end

      def connect_to_host(host)
        f = create_connector.connect(host.ip.to_s)

        f.on_complete do |f|
          if f.resolved?
            f.value.on_closed do |cause|
              @eviction_policy.disconnected(host, cause)
            end

            @eviction_policy.connected(host)
          else
            f.on_failure do |e|
              @eviction_policy.connection_error(host, e)
            end
          end
        end

        f
      end

      private

      NO_CONNECTIONS = Ione::Future.resolved([])

      def create_cluster_connector
        Cql::Client::ClusterConnector.new(create_connector, @logger)
      end

      def create_connector
        authentication_step = @connection_options.protocol_version == 1 ? Cql::Client::CredentialsAuthenticationStep.new(@connection_options.credentials) : Cql::Client::SaslAuthenticationStep.new(@connection_options.auth_provider)
        protocol_handler_factory = lambda { |connection| Protocol::CqlProtocolHandler.new(connection, @reactor, @connection_options.protocol_version, @connection_options.compressor) }

        Cql::Client::Connector.new([
          Cql::Client::ConnectStep.new(@reactor, protocol_handler_factory, @connection_options.port, @connection_options.connection_timeout, @logger),
          Cql::Client::CacheOptionsStep.new,
          Cql::Client::InitializeStep.new(@connection_options.compressor, @logger),
          authentication_step,
          Cql::Client::CachePropertiesStep.new,
        ])
      end
    end
  end
end
