# encoding: utf-8

module Cql
  module Client
    # @private
    class ConnectionHelper
      def initialize(io_reactor, port, credentials, connection_timeout, logger)
        @io_reactor = io_reactor
        @port = port
        @credentials = credentials
        @connection_timeout = connection_timeout
        @logger = logger
        @request_runner = RequestRunner.new
        @keyspace_changer = KeyspaceChanger.new
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
        request = Protocol::QueryRequest.new('SELECT peer, data_center, host_id, rpc_address FROM system.peers', :one)
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
        connection_futures = hosts.map do |host|
          connect_to_host(host, initial_keyspace).recover do |error|
            FailedConnection.new(error, host, @port)
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
        connected = @io_reactor.connect(host, @port, @connection_timeout)
        connected.flat_map do |connection|
          initialize_connection(connection, keyspace)
        end
      end

      def initialize_connection(connection, keyspace)
        started = @request_runner.execute(connection, Protocol::StartupRequest.new)
        authenticated = started.flat_map { |response| maybe_authenticate(response, connection) }
        identified = authenticated.flat_map { identify_node(connection) }
        identified.flat_map { @keyspace_changer.use_keyspace(keyspace, connection) }
      end

      def identify_node(connection)
        request = Protocol::QueryRequest.new('SELECT data_center, host_id FROM system.local', :one)
        f = @request_runner.execute(connection, request)
        f.on_value do |result|
          unless result.empty?
            connection[:host_id] = result.first['host_id']
            connection[:data_center] = result.first['data_center']
          end
        end
        f
      end

      def maybe_authenticate(response, connection)
        case response
        when AuthenticationRequired
          if @credentials
            credentials_request = Protocol::CredentialsRequest.new(@credentials)
            @request_runner.execute(connection, credentials_request).map { connection }
          else
            Future.failed(AuthenticationError.new('Server requested authentication, but no credentials given'))
          end
        else
          Future.resolved(connection)
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
