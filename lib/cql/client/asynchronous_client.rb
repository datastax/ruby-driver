# encoding: utf-8

module Cql
  module Client
    # @private
    class AsynchronousClient < Client
      def initialize(options={})
        connection_timeout = options[:connection_timeout]
        @host = options[:host] || 'localhost'
        @port = options[:port] || 9042
        @io_reactor = options[:io_reactor] || Io::IoReactor.new(connection_timeout: connection_timeout)
        @lock = Mutex.new
        @connected = false
        @connecting = false
        @closing = false
        @initial_keyspace = options[:keyspace]
        @credentials = options[:credentials]
        @connection_keyspaces = {}
      end

      def connect
        @lock.synchronize do
          return @connected_future if @connected || @connecting
          @connecting = true
          @connected_future = Future.new
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
          return @connection_ids.map { |id| @connection_keyspaces[id] }.first
        end
      end

      def use(keyspace, connection_ids=nil)
        return Future.failed(NotConnectedError.new) unless @connected || @connecting
        return Future.failed(InvalidKeyspaceNameError.new(%("#{keyspace}" is not a valid keyspace name))) unless valid_keyspace_name?(keyspace)
        connection_ids ||= @connection_ids
        @lock.synchronize do
          connection_ids = connection_ids.select { |id| @connection_keyspaces[id] != keyspace }
        end
        if connection_ids.any?
          futures = connection_ids.map do |connection_id|
            execute_request(Protocol::QueryRequest.new("USE #{keyspace}", :one), connection_id)
          end
          futures.compact!
          return Future.combine(*futures).map { nil }
        else
          Future.completed(nil)
        end
      end

      def execute(cql, consistency=nil)
        consistency ||= DEFAULT_CONSISTENCY_LEVEL
        return Future.failed(NotConnectedError.new) unless @connected || @connecting
        f = execute_request(Protocol::QueryRequest.new(cql, consistency))
        f.on_complete do
          ensure_keyspace!
        end
        f
      rescue => e
        Future.failed(e)
      end

      # @private
      def execute_statement(connection_id, statement_id, metadata, values, consistency)
        return Future.failed(NotConnectedError.new) unless @connected || @connecting
        request = Protocol::ExecuteRequest.new(statement_id, metadata, values, consistency || DEFAULT_CONSISTENCY_LEVEL)
        execute_request(request, connection_id)
      rescue => e
        Future.failed(e)
      end

      def prepare(cql)
        return Future.failed(NotConnectedError.new) unless @connected || @connecting
        execute_request(Protocol::PrepareRequest.new(cql))
      rescue => e
        Future.failed(e)
      end

      private

      KEYSPACE_NAME_PATTERN = /^\w[\w\d_]*$/
      DEFAULT_CONSISTENCY_LEVEL = :quorum

      def valid_keyspace_name?(name)
        name =~ KEYSPACE_NAME_PATTERN
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
          connection_futures = hosts.map { |host| connect_to_host(host) }
          Future.combine(*connection_futures)
        end
        hosts_connected_future.on_complete do |connection_ids|
          @connection_ids = connection_ids
        end
        if @initial_keyspace
          initialized_future = hosts_connected_future.flat_map do |*args|
            use(@initial_keyspace)
          end
        else
          initialized_future = hosts_connected_future
        end
        initialized_future.on_failure do |e|
          close
          if e.is_a?(Cql::QueryError) && e.code == 0x100
            @connected_future.fail!(AuthenticationError.new(e.message))
          else
            @connected_future.fail!(e)
          end
        end
        initialized_future.on_complete do
          @connected_future.complete!(self)
        end
      end

      def connect_to_host(host)
        connected = @io_reactor.add_connection(host, @port)
        connected.flat_map do |connection_id|
          started = execute_request(Protocol::StartupRequest.new, connection_id)
          started.flat_map { |response| maybe_authenticate(response, connection_id) }
        end
      end

      def maybe_authenticate(response, connection_id)
        case response
        when AuthenticationRequired
          if @credentials
            credentials_request = Protocol::CredentialsRequest.new(@credentials)
            execute_request(credentials_request, connection_id).map { connection_id }
          else
            Future.failed(AuthenticationError.new('Server requested authentication, but no credentials given'))
          end
        else
          Future.completed(connection_id)
        end
      end

      def execute_request(request, connection_id=nil)
        @io_reactor.queue_request(request, connection_id).map do |response, connection_id|
          interpret_response!(request, response, connection_id)
        end
      end

      def interpret_response!(request, response, connection_id)
        case response
        when Protocol::ErrorResponse
          case request
          when Protocol::QueryRequest
            raise QueryError.new(response.code, response.message, request.cql)
          else
            raise QueryError.new(response.code, response.message)
          end
        when Protocol::RowsResultResponse
          QueryResult.new(response.metadata, response.rows)
        when Protocol::PreparedResultResponse
          AsynchronousPreparedStatement.new(self, connection_id, response.id, response.metadata)
        when Protocol::SetKeyspaceResultResponse
          @lock.synchronize do
            @last_keyspace_change = @connection_keyspaces[connection_id] = response.keyspace
          end
          nil
        when Protocol::AuthenticateResponse
          AuthenticationRequired.new(response.authentication_class)
        else
          nil
        end
      end

      def ensure_keyspace!
        ks = nil
        @lock.synchronize do
          ks = @last_keyspace_change
          return unless @last_keyspace_change
        end
        use(ks, @connection_ids) if ks
      end

      class AuthenticationRequired
        attr_reader :authentication_class

        def initialize(authentication_class)
          @authentication_class = authentication_class
        end
      end
    end
  end
end
