# encoding: utf-8

module Cql
  module Client
    # @private
    class AsynchronousClient < Client
      def initialize(options={})
        @connection_timeout = options[:connection_timeout] || 10
        @host = options[:host] || 'localhost'
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

      KEYSPACE_NAME_PATTERN = /^\w[\w\d_]*$/
      DEFAULT_CONSISTENCY_LEVEL = :quorum

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

      def setup_connections
        hosts_connected_future = @io_reactor.start.flat_map do
          hosts = @host.split(',')
          connection_futures = hosts.map { |host| connect_to_host(host, @initial_keyspace) }
          Future.combine(*connection_futures)
        end
        hosts_connected_future.on_failure do |e|
          close
          if e.is_a?(Cql::QueryError) && e.code == 0x100
            @connected_future.fail!(AuthenticationError.new(e.message))
          else
            @connected_future.fail!(e)
          end
        end
        hosts_connected_future.on_complete do |connections|
          @connections.concat(connections)
          @connected_future.complete!(self)
        end
      end

      def connect_to_host(host, keyspace)
        connected = @io_reactor.connect(host, @port, @connection_timeout)
        initialized = connected.flat_map do |connection|
          initialize_connection(connection, keyspace)
        end
      end

      def initialize_connection(connection, keyspace)
        started = execute_request(Protocol::StartupRequest.new, connection)
        authenticated = started.flat_map { |response| maybe_authenticate(response, connection) }
        authenticated.flat_map { |connection| use_keyspace(keyspace, connection) }
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
