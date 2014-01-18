# encoding: utf-8

module Cql
  module Client
    # @private
    class ConnectionHelper
      def initialize(io_reactor, port, authenticator, protocol_version, connections_per_node, connection_timeout, compressor, logger)
        @io_reactor = io_reactor
        @port = port
        @protocol_version = protocol_version
        @connections_per_node = connections_per_node
        @connection_timeout = connection_timeout
        @logger = logger
        @request_runner = RequestRunner.new
        @compressor = compressor

        @cache_options_phase = CacheOptionsPhase.new(@request_runner)
        @start_up_phase = StartUpPhase.new(@request_runner, @compressor, @logger)
        @authentication_phase = AuthenticationPhase.new(@request_runner, authenticator, @protocol_version)
        @node_identification_phase = NodeIdentificationPhase.new(@request_runner)
      end

      def connect(hosts, initial_keyspace)
        f = @io_reactor.start.flat_map do
          connect_to_hosts(hosts, initial_keyspace, true)
        end
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
        @logger.debug('Looking for additional nodes')
        connection = seed_connections.sample
        return Future.resolved([]) unless connection
        request = Protocol::QueryRequest.new('SELECT peer, data_center, host_id, rpc_address FROM system.peers', nil, :one)
        peer_info = @request_runner.execute(connection, request)
        peer_info.flat_map do |result|
          seed_dcs = seed_connections.map { |c| c[:data_center] }.uniq
          unconnected_peers = result.select do |row|
            seed_dcs.include?(row['data_center']) && seed_connections.none? { |c| c[:host_id] == row['host_id'] }
          end
          if unconnected_peers.empty?
            @logger.debug('No additional nodes found')
          else
            @logger.debug('%d additional nodes found' % unconnected_peers.size)
          end
          node_addresses = unconnected_peers.map do |row|
            rpc_address = row['rpc_address'].to_s
            if rpc_address == '0.0.0.0'
              row['peer'].to_s
            else
              rpc_address
            end
          end
          if node_addresses.any?
            connect_to_hosts(node_addresses, initial_keyspace, false)
          else
            Future.resolved([])
          end
        end
      end

      private

      def connect_to_hosts(hosts, initial_keyspace, peer_discovery)
        connection_futures = hosts.flat_map do |host|
          Array.new(@connections_per_node) do
            connect_to_host(host, initial_keyspace).recover do |error|
              FailedConnection.new(error, host, @port)
            end
          end
        end
        connection_futures.each do |cf|
          cf.on_value do |c|
            if c.is_a?(FailedConnection)
              @logger.warn('Failed connecting to node at %s:%d: %s' % [c.host, c.port, c.error.message])
            else
              @logger.info('Connected to node %s at %s:%d in data center %s' % [c[:host_id], c.host, c.port, c[:data_center]])
            end
            c.on_closed do
              @logger.warn('Connection to node %s at %s:%d in data center %s unexpectedly closed' % [c[:host_id], c.host, c.port, c[:data_center]])
            end
          end
        end
        hosts_connected_future = Future.all(*connection_futures)
        if peer_discovery
          hosts_connected_future.flat_map do |connections|
            discover_peers(connections.select(&:connected?), initial_keyspace).map do |peer_connections|
              connections + peer_connections
            end
          end
        else
          hosts_connected_future
        end
      end

      def connect_to_host(host, keyspace)
        @logger.debug('Connecting to node at %s:%d' % [host, @port])
        phases = [
          ConnectPhase.new(@io_reactor, host, @port, @connection_timeout),
          @cache_options_phase,
          @start_up_phase,
          @authentication_phase,
          @node_identification_phase,
          KeyspaceChangingPhase.new(KeyspaceChanger.new(@request_runner), keyspace),
        ]
        phases.reduce(Future.resolved) do |f, phase|
          f.flat_map do |pending_connection|
            phase.run(pending_connection)
          end
        end
      end

      class ConnectPhase
        def initialize(io_reactor, host, port, connection_timeout)
          @io_reactor = io_reactor
          @host = host
          @port = port
          @connection_timeout = connection_timeout
        end

        def run(*)
          @io_reactor.connect(@host, @port, @connection_timeout).map do |connection|
            PendingConnection.new(connection)
          end
        end
      end

      class CacheOptionsPhase
        def initialize(request_runner)
          @request_runner = request_runner
        end

        def run(pending_connection)
          f = @request_runner.execute(pending_connection.connection, Protocol::OptionsRequest.new)
          f.on_value do |supported_options|
            pending_connection.connection[:cql_version] = supported_options['CQL_VERSION']
            pending_connection.connection[:compression] = supported_options['COMPRESSION']
          end
          f.map(pending_connection)
        end
      end

      class StartUpPhase
        def initialize(request_runner, compressor, logger)
          @request_runner = request_runner
          @compressor = compressor
          @logger = logger
        end

        def run(pending_connection)
          compression = @compressor && @compressor.algorithm
          if @compressor && !pending_connection.connection[:compression].include?(@compressor.algorithm)
            @logger.warn(%[Compression algorithm "#{@compressor.algorithm}" not supported (server supports "#{pending_connection.connection[:compression].join('", "')}")])
            compression = nil
          elsif @compressor
            @logger.debug('Using "%s" compression' % @compressor.algorithm)
          end
          request = Protocol::StartupRequest.new(nil, compression)
          f = @request_runner.execute(pending_connection.connection, request)
          f.map do |startup_response|
            if startup_response.is_a?(AuthenticationRequired)
              PendingConnection.new(pending_connection.connection, startup_response.authentication_class)
            else
              pending_connection
            end
          end
        end
      end

      class AuthenticationPhase
        def initialize(request_runner, authenticator, protocol_version)
          @request_runner = request_runner
          @authenticator = authenticator
          @protocol_version = protocol_version
        end

        def run(pending_connection)
          if pending_connection.authentication_class
            if @authenticator && @authenticator.supports?(pending_connection.authentication_class, @protocol_version)
              auth_request = @authenticator.initial_request(@protocol_version)
              f = @request_runner.execute(pending_connection.connection, auth_request)
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
        def initialize(request_runner)
          @request_runner = request_runner
        end

        def run(pending_connection)
          request = Protocol::QueryRequest.new('SELECT data_center, host_id FROM system.local', nil, :one)
          f = @request_runner.execute(pending_connection.connection, request)
          f.on_value do |result|
            unless result.empty?
              pending_connection.connection[:host_id] = result.first['host_id']
              pending_connection.connection[:data_center] = result.first['data_center']
            end
          end
          f.map(pending_connection)
        end
      end

      class KeyspaceChangingPhase
        def initialize(keyspace_changer, keyspace_name)
          @keyspace_changer = keyspace_changer
          @keyspace_name = keyspace_name
        end

        def run(pending_connection)
          @keyspace_changer.use_keyspace(pending_connection.connection, @keyspace_name)
        end
      end

      class PendingConnection
        attr_reader :connection, :authentication_class

        def initialize(connection, authentication_class=nil)
          @connection = connection
          @authentication_class = authentication_class
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
