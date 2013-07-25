# encoding: utf-8

module Cql
  module Client
    # @private
    class AsynchronousClient < Client
      def initialize(options={})
        @connection_timeout = options[:connection_timeout] || 10
        @hosts = extract_hosts(options)
        @port = options[:port] || 9042
        @io_reactor = options[:io_reactor] || Io::IoReactor.new(Protocol::CqlProtocolHandler)
        @lock = Mutex.new
        @connected = false
        @connecting = false
        @closing = false
        @initial_keyspace = options[:keyspace]
        @credentials = options[:credentials]
        @request_runner = RequestRunner.new
      end

      def connect
        @lock.synchronize do
          return @connected_future if can_execute?
          @connecting = true
          @connected_future = Future.new
          @connections = []
        end
        when_not_closing do
          setup_connections
        end
        @connected_future.on_complete do
          @lock.synchronize do
            @connecting = false
            @connected = true
          end
        end
        @connected_future.on_failure do
          @lock.synchronize do
            @connecting = false
            @connected = false
          end
        end
        @connected_future
      end

      def close
        @lock.synchronize do
          return @closed_future if @closing
          @closing = true
          @closed_future = Future.new
        end
        when_not_connecting do
          f = @io_reactor.stop
          f.on_complete { @closed_future.complete!(self) }
          f.on_failure { |e| @closed_future.fail!(e) }
        end
        @closed_future.on_complete do
          @lock.synchronize do
            @closing = false
            @connected = false
          end
        end
        @closed_future.on_failure do
          @lock.synchronize do
            @closing = false
            @connected = false
          end
        end
        @closed_future
      end

      def connected?
        @connected
      end

      def keyspace
        @lock.synchronize do
          @connections.first.keyspace
        end
      end

      def use(keyspace)
        with_failure_handler do
          connections = @lock.synchronize do
            @connections.select { |c| c.keyspace != keyspace }
          end
          if connections.any?
            futures = connections.map { |connection| use_keyspace(keyspace, connection) }
            Future.combine(*futures).map { nil }
          else
            Future.completed(nil)
          end
        end
      end

      def execute(cql, consistency=nil)
        with_failure_handler do
          consistency ||= DEFAULT_CONSISTENCY_LEVEL
          execute_request(Protocol::QueryRequest.new(cql, consistency))
        end
      end

      def prepare(cql)
        with_failure_handler do
          execute_request(Protocol::PrepareRequest.new(cql))
        end
      end

      private

      KEYSPACE_NAME_PATTERN = /^\w[\w\d_]*$|^"\w[\w\d_]*"$/
      DEFAULT_CONSISTENCY_LEVEL = :quorum

      class FailedConnection
        attr_reader :error

        def initialize(error)
          @error = error
        end

        def connected?
          false
        end
      end

      def extract_hosts(options)
        if options[:hosts]
          options[:hosts].uniq
        elsif options[:host]
          options[:host].split(',').uniq
        else
          %w[localhost]
        end
      end

      def can_execute?
        @connected || @connecting
      end

      def valid_keyspace_name?(name)
        name =~ KEYSPACE_NAME_PATTERN
      end

      def with_failure_handler
        return Future.failed(NotConnectedError.new) unless can_execute?
        yield
      rescue => e
        Future.failed(e)
      end

      def when_not_connecting(&callback)
        if @connecting
          @connected_future.on_complete(&callback)
          @connected_future.on_failure(&callback)
        else
          callback.call
        end
      end

      def when_not_closing(&callback)
        if @closing
          @closed_future.on_complete(&callback)
          @closed_future.on_failure(&callback)
        else
          callback.call
        end
      end

      def discover_peers(seed_connections, initial_keyspace)
        connected_seeds = seed_connections.select(&:connected?)
        connection = connected_seeds.sample
        return Future.completed([]) unless connection
        request = Protocol::QueryRequest.new('SELECT data_center, host_id, rpc_address FROM system.peers', :one)
        peer_info = execute_request(request, connection)
        peer_info.flat_map do |result|
          seed_dcs = connected_seeds.map { |c| c[:data_center] }.uniq
          unconnected_peers = result.select do |row|
            seed_dcs.include?(row['data_center']) && connected_seeds.none? { |c| c[:host_id] == row['host_id'] }
          end
          node_addresses = unconnected_peers.map { |row| row['rpc_address'].to_s }
          if node_addresses.any?
            connect_to_hosts(node_addresses, initial_keyspace, false)
          else
            Future.completed([])
          end
        end
      end

      def setup_connections
        f = @io_reactor.start.flat_map do
          connect_to_hosts(@hosts, @initial_keyspace, true)
        end
        f.on_failure do |e|
          fail_connecting(e)
        end
        f.on_complete do |connections|
          connected_connections = connections.select(&:connected?)
          if connected_connections.any?
            @connections = connected_connections
            @connected_future.complete!(self)
          else
            fail_connecting(connections.first.error)
          end
        end
      end

      def fail_connecting(e)
        close
        if e.is_a?(Cql::QueryError) && e.code == 0x100
          @connected_future.fail!(AuthenticationError.new(e.message))
        else
          @connected_future.fail!(e)
        end
      end

      def connect_to_hosts(hosts, initial_keyspace, peer_discovery)
        connection_futures = hosts.map do |host|
          connect_to_host(host, initial_keyspace).recover do |error|
            FailedConnection.new(error)
          end
        end
        hosts_connected_future = Future.combine(*connection_futures)
        if peer_discovery
          hosts_connected_future.flat_map do |connections|
            discover_peers(connections, initial_keyspace).map do |peer_connections|
              connections + peer_connections
            end
          end
        else
          hosts_connected_future
        end
      end

      def connect_to_host(host, keyspace)
        connected = @io_reactor.connect(host, @port, @connection_timeout)
        connected.flat_map do |connection|
          initialize_connection(connection, keyspace)
        end
      end

      def initialize_connection(connection, keyspace)
        started = execute_request(Protocol::StartupRequest.new, connection)
        authenticated = started.flat_map { |response| maybe_authenticate(response, connection) }
        identified = authenticated.flat_map { identify_node(connection) }
        identified.flat_map { use_keyspace(keyspace, connection) }
      end

      def identify_node(connection)
        request = Protocol::QueryRequest.new('SELECT data_center, host_id FROM system.local', :one)
        f = execute_request(request, connection)
        f.on_complete do |result|
          unless result.empty?
            connection[:host_id] = result.first['host_id']
            connection[:data_center] = result.first['data_center']
          end
        end
        f
      end

      def use_keyspace(keyspace, connection)
        return Future.completed(connection) unless keyspace
        return Future.failed(InvalidKeyspaceNameError.new(%("#{keyspace}" is not a valid keyspace name))) unless valid_keyspace_name?(keyspace)
        execute_request(Protocol::QueryRequest.new("USE #{keyspace}", :one), connection).map { connection }
      end

      def maybe_authenticate(response, connection)
        case response
        when AuthenticationRequired
          if @credentials
            credentials_request = Protocol::CredentialsRequest.new(@credentials)
            execute_request(credentials_request, connection).map { connection }
          else
            Future.failed(AuthenticationError.new('Server requested authentication, but no credentials given'))
          end
        else
          Future.completed(connection)
        end
      end

      def execute_request(request, connection=nil)
        f = @request_runner.execute(connection || @connections.sample, request)
        f.map do |result|
          if result.is_a?(KeyspaceChanged)
            use(result.keyspace)
            nil
          else
            result
          end
        end
      end
    end
  end
end
