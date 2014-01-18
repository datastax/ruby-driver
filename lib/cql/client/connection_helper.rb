# encoding: utf-8

module Cql
  module Client
    # @private
    class ConnectionHelper
      def initialize(io_reactor, port, authenticator, protocol_version, connections_per_node, connection_timeout, compressor, logger)
        @connections_per_node = connections_per_node
        @logger = logger
        @connection_pipeline = ConnectionPipeline.new(io_reactor, port, connection_timeout, protocol_version, compressor, authenticator, logger)
      end

      def connect(hosts, initial_keyspace)
        f = connect_to_hosts(hosts, initial_keyspace, true)
        f = f.map do |connections|
          connected_connections = connections.select(&:connected?)
          if connected_connections.empty?
            e = connections.first.error
            if e.is_a?(Cql::QueryError) && e.code == 0x100
              e = AuthenticationError.new(e.message)
            end
            raise e
          end
          connected_connections
        end
        f
      end

      def discover_peers(seed_connections, initial_keyspace)
        return Future.resolved([]) if seed_connections.empty?
        @logger.debug('Looking for additional nodes')
        peer_discovery = PeerDiscovery.new(seed_connections)
        peer_discovery.new_hosts.flat_map do |hosts|
          if hosts.empty?
            @logger.debug('No additional nodes found')
            Future.resolved([])
          else
            @logger.debug('%d additional nodes found' % hosts.size)
            connect_to_hosts(hosts, initial_keyspace, false)
          end
        end
      end

      private

      def connect_to_hosts(hosts, initial_keyspace, peer_discovery)
        connection_futures = hosts.flat_map do |host|
          Array.new(@connections_per_node) do
            @connection_pipeline.run(PendingConnection.new(host, initial_keyspace))
          end
        end
        if peer_discovery
          Future.all(*connection_futures).flat_map do |connections|
            f = discover_peers(connections.select(&:connected?), initial_keyspace)
            f.map do |peer_connections|
              connections + peer_connections
            end
          end
        else
          Future.all(*connection_futures)
        end
      end

      class ConnectionPipeline
        def initialize(io_reactor, port, connection_timeout, protocol_version, compressor, authenticator, logger)
          @connection_phases = [
            ConnectPhase.new(io_reactor, port, connection_timeout),
            CacheOptionsPhase.new,
            StartUpPhase.new(compressor, logger),
            AuthenticationPhase.new(authenticator, protocol_version),
            NodeIdentificationPhase.new,
            KeyspaceChangingPhase.new,
          ]
          @port = port
          @logger = logger
        end

        def run(pending_connection)
          @logger.debug('Connecting to node at %s:%d' % [pending_connection.host, @port])
          f = @connection_phases.reduce(Future.resolved(pending_connection)) do |f, phase|
            f.flat_map do |pending_connection|
              phase.run(pending_connection)
            end
          end
          f = f.map do |pending_connection|
            pending_connection.connection
          end
          f.on_value do |connection|
            @logger.info('Connected to node %s at %s:%d in data center %s' % [connection[:host_id], connection.host, connection.port, connection[:data_center]])
            register_close_logger(connection)
          end
          f.on_failure do |error|
            @logger.warn('Failed connecting to node at %s:%d: %s' % [pending_connection.host, @port, error.message])
          end
          f.recover do |error|
            FailedConnection.new(error, pending_connection.host, @port)
          end
        end

        private

        def register_close_logger(connection)
          connection.on_closed do |cause|
            message = 'Connection to node %s at %s:%d in data center %s ' % [connection[:host_id], connection.host, connection.port, connection[:data_center]]
            if cause
              message << ('unexpectedly closed: %s' % cause.message)
              @logger.warn(message)
            else
              message << 'closed'
              @logger.info(message)
            end
          end
        end
      end

      class ConnectPhase
        def initialize(io_reactor, port, connection_timeout)
          @io_reactor = io_reactor
          @port = port
          @connection_timeout = connection_timeout
        end

        def run(pending_connection)
          @io_reactor.connect(pending_connection.host, @port, @connection_timeout).map do |connection|
            pending_connection.with_connection(connection)
          end
        end
      end

      class CacheOptionsPhase
        def run(pending_connection)
          f = pending_connection.execute(Protocol::OptionsRequest.new)
          f.on_value do |supported_options|
            pending_connection[:cql_version] = supported_options['CQL_VERSION']
            pending_connection[:compression] = supported_options['COMPRESSION']
          end
          f.map(pending_connection)
        end
      end

      class StartUpPhase
        def initialize(compressor, logger)
          @compressor = compressor
          @logger = logger
        end

        def run(pending_connection)
          compression = @compressor && @compressor.algorithm
          supported_algorithms = pending_connection[:compression]
          if @compressor && !supported_algorithms.include?(@compressor.algorithm)
            @logger.warn(%[Compression algorithm "#{@compressor.algorithm}" not supported (server supports "#{supported_algorithms.join('", "')}")])
            compression = nil
          elsif @compressor
            @logger.debug('Using "%s" compression' % @compressor.algorithm)
          end
          f = pending_connection.execute(Protocol::StartupRequest.new(nil, compression))
          f.map do |startup_response|
            if startup_response.is_a?(AuthenticationRequired)
              pending_connection.with_authentication_class(startup_response.authentication_class)
            else
              pending_connection
            end
          end
        end
      end

      class AuthenticationPhase
        def initialize(authenticator, protocol_version)
          @authenticator = authenticator
          @protocol_version = protocol_version
        end

        def run(pending_connection)
          if pending_connection.authentication_class
            if @authenticator && @authenticator.supports?(pending_connection.authentication_class, @protocol_version)
              auth_request = @authenticator.initial_request(@protocol_version)
              f = pending_connection.execute(auth_request)
              f.map(pending_connection)
            elsif @authenticator
              Future.failed(AuthenticationError.new('Authenticator does not support the required authentication class "%s" and/or protocol version %d' % [pending_connection.authentication_class, @protocol_version]))
            else
              Future.failed(AuthenticationError.new('Server requested authentication, but no authenticator provided'))
            end
          else
            Future.resolved(pending_connection)
          end
        end
      end

      class NodeIdentificationPhase
        def run(pending_connection)
          request = Protocol::QueryRequest.new('SELECT data_center, host_id FROM system.local', nil, :one)
          f = pending_connection.execute(request)
          f.on_value do |result|
            unless result.empty?
              pending_connection[:host_id] = result.first['host_id']
              pending_connection[:data_center] = result.first['data_center']
            end
          end
          f.map(pending_connection)
        end
      end

      class KeyspaceChangingPhase
        def run(pending_connection)
          pending_connection.use_keyspace
        end
      end

      class PendingConnection
        attr_reader :host, :connection, :authentication_class, :initial_keyspace

        def initialize(host, initial_keyspace, connection=nil, authentication_class=nil)
          @host = host
          @initial_keyspace = initial_keyspace
          @connection = connection
          @authentication_class = authentication_class
          @request_runner = RequestRunner.new
        end

        def with_connection(connection)
          self.class.new(host, @initial_keyspace, connection, @authentication_class)
        end

        def with_authentication_class(authentication_class)
          self.class.new(host, @initial_keyspace, @connection, authentication_class)
        end

        def [](key)
          @connection[key]
        end

        def []=(key, value)
          @connection[key] = value
        end

        def execute(request)
          @request_runner.execute(@connection, request)
        end

        def use_keyspace
          if @initial_keyspace
            KeyspaceChanger.new(@request_runner).use_keyspace(@connection, @initial_keyspace).map(self)
          else
            Future.resolved(self)
          end
        end
      end

      class FailedConnection
        attr_reader :error, :host, :port

        def initialize(error, host, port)
          @error = error
          @host = host
          @port = port
        end

        def connected?
          false
        end
      end
    end
  end
end
