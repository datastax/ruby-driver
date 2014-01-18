# encoding: utf-8

module Cql
  module Client
    # @private
    class AsynchronousClient < Client
      def initialize(options={})
        @compressor = options[:compressor]
        @logger = options[:logger] || NullLogger.new
        @protocol_version = options[:protocol_version] || 2
        @io_reactor = options[:io_reactor] || Io::IoReactor.new(protocol_handler_factory(@compressor))
        @hosts = extract_hosts(options)
        @initial_keyspace = options[:keyspace]
        @lock = Mutex.new
        @connected = false
        @connecting = false
        @closing = false
        @request_runner = RequestRunner.new
        @keyspace_changer = KeyspaceChanger.new
        @connection_manager = ConnectionManager.new
        @port = options[:port] || DEFAULT_PORT
        @authenticator = options[:authenticator]
        @connections_per_node = options[:connections_per_node] || 1
        @connection_timeout = options[:connection_timeout] || DEFAULT_CONNECTION_TIMEOUT
        default_consistency = options[:default_consistency] || DEFAULT_CONSISTENCY
        @execute_options_decoder = ExecuteOptionsDecoder.new(default_consistency)
      end

      def connect
        @lock.synchronize do
          raise ClientError, 'Cannot connect a closed client' if @closing || @closed
          return @connected_future if can_execute?
          @connecting = true
          @connected_future = begin
            f = connect_with_protocol_version_fallback
            f.on_value do |connections|
              @connection_manager.add_connections(connections)
              register_event_listener(@connection_manager.random_connection)
            end
            f.map { self }
          end
        end
        @connected_future.on_complete(&method(:connected))
        @connected_future
      end

      def close
        @lock.synchronize do
          return @closed_future if @closing
          @closing = true
          @closed_future = begin
            if @connecting
              f = @connected_future.recover
              f = f.flat_map { @io_reactor.stop }
              f = f.map { self }
              f
            else
              f = @io_reactor.stop
              f = f.map { self }
              f
            end
          end
        end
        @closed_future.on_complete(&method(:closed))
        @closed_future
      end

      def connected?
        @connected
      end

      def keyspace
        @connection_manager.random_connection.keyspace
      end

      def use(keyspace)
        with_failure_handler do
          connections = @connection_manager.select { |c| c.keyspace != keyspace }
          if connections.any?
            futures = connections.map { |connection| @keyspace_changer.use_keyspace(keyspace, connection) }
            Future.all(*futures).map { nil }
          else
            Future.resolved
          end
        end
      end

      def execute(cql, *args)
        with_failure_handler do
          options_or_consistency = nil
          if args.last.is_a?(Symbol) || args.last.is_a?(Hash)
            options_or_consistency = args.pop
          end
          consistency, timeout, trace = @execute_options_decoder.decode_options(options_or_consistency)
          execute_request(Protocol::QueryRequest.new(cql, args, consistency, trace), timeout)
        end
      end

      def prepare(cql)
        with_failure_handler do
          AsynchronousPreparedStatement.prepare(cql, @execute_options_decoder, @connection_manager, @logger)
        end
      end

      private

      DEFAULT_CONSISTENCY = :quorum
      DEFAULT_PORT = 9042
      DEFAULT_CONNECTION_TIMEOUT = 10
      MAX_RECONNECTION_ATTEMPTS = 5

      def protocol_handler_factory(compressor)
        lambda { |connection, timeout| Protocol::CqlProtocolHandler.new(connection, timeout, @protocol_version, compressor) }
      end

      def create_connection_helper
        ConnectionHelper.new(
          @io_reactor,
          @port,
          @authenticator,
          @protocol_version,
          @connections_per_node,
          @connection_timeout,
          @compressor,
          @logger
        )
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

      def connect_with_protocol_version_fallback
        connection_helper = create_connection_helper
        f = connection_helper.connect(@hosts, @initial_keyspace)
        f.fallback do |error|
          if error.is_a?(QueryError) && error.code == 0x0a && @protocol_version > 1
            @protocol_version -= 1
            connect_with_protocol_version_fallback
          else
            raise error
          end
        end
      end

      def connected(f)
        if f.resolved?
          @lock.synchronize do
            @connecting = false
            @connected = true
          end
          @logger.info('Cluster connection complete')
        else
          @lock.synchronize do
            @connecting = false
            @connected = false
          end
          f.on_failure do |e|
            @logger.error('Failed connecting to cluster: %s' % e.message)
          end
          close
        end
      end

      def closed(f)
        @lock.synchronize do
          @closing = false
          @closed = true
          @connected = false
          if f.resolved?
            @logger.info('Cluster disconnect complete')
          else
            f.on_failure do |e|
              @logger.error('Cluster disconnect failed: %s' % e.message)
            end
          end
        end
      end

      def can_execute?
        !@closing && (@connecting || (@connected && @connection_manager.connected?))
      end

      def with_failure_handler
        return Future.failed(NotConnectedError.new) unless can_execute?
        yield
      rescue => e
        Future.failed(e)
      end

      def register_event_listener(connection)
        register_request = Protocol::RegisterRequest.new(Protocol::TopologyChangeEventResponse::TYPE, Protocol::StatusChangeEventResponse::TYPE)
        execute_request(register_request, nil, connection)
        connection.on_closed do
          if connected?
            begin
              register_event_listener(@connection_manager.random_connection)
            rescue NotConnectedError
              # we had started closing down after the connection check
            end
          end
        end
        connection.on_event do |event|
          if event.change == 'UP' || event.change == 'NEW_NODE'
            @logger.debug('Received %s event' % event.change)
            unless @looking_for_nodes
              @looking_for_nodes = true
              handle_topology_change.on_complete do |f|
                @looking_for_nodes = false
              end
            end
          end
        end
      end

      def handle_topology_change(remaning_attempts=MAX_RECONNECTION_ATTEMPTS)
        with_failure_handler do
          seed_connections = @connection_manager.snapshot
          connection_helper = create_connection_helper
          f = connection_helper.discover_peers(seed_connections, keyspace)
          f.flat_map do |connections|
            connected_connections = connections.select(&:connected?)
            if connected_connections.any?
              @connection_manager.add_connections(connected_connections)
              Future.resolved
            elsif remaning_attempts > 0
              timeout = 2**(MAX_RECONNECTION_ATTEMPTS - remaning_attempts)
              @logger.debug('Scheduling new peer discovery in %ds' % timeout)
              f = @io_reactor.schedule_timer(timeout)
              f.flat_map do
                handle_topology_change(remaning_attempts - 1)
              end
            else
              @logger.warn('Giving up looking for additional nodes')
              Future.resolved
            end
          end
        end
      end

      def execute_request(request, timeout=nil, connection=nil)
        f = @request_runner.execute(connection || @connection_manager.random_connection, request, timeout)
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
