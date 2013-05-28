# encoding: utf-8

module Cql
  module Client
    # A CQL client manages connections to one or more Cassandra nodes and you use
    # it run queries, insert and update data.
    #
    # @example Connecting and changing to a keyspace
    #   # create a client and connect to two Cassandra nodes
    #   client = Cql::Client.connect(host: 'node01.cassandra.local,node02.cassandra.local')
    #   # change to a keyspace
    #   client.use('stuff')
    #
    # @example Query for data
    #   rows = client.execute('SELECT * FROM things WHERE id = 2')
    #   rows.each do |row|
    #     p row
    #   end
    #
    # @example Inserting and updating data
    #   client.execute("INSERT INTO things (id, value) VALUES (4, 'foo')")
    #   client.execute("UPDATE things SET value = 'bar' WHERE id = 5")
    #
    # @example Prepared statements
    #   statement = client.prepare('INSERT INTO things (id, value) VALUES (?, ?)')
    #   statement.execute(9, 'qux')
    #   statement.execute(8, 'baz')
    #
    # Client instances are threadsafe.
    #
    class SynchronousClient
      # Create a new client.
      #
      # Creating a client does not automatically connect to Cassandra, you need to
      # call {#connect} to connect, or use {Client.connect}. `#connect` returns
      # `self` so you can chain that call after `new`.
      #
      # @param [Hash] options
      # @option options [String] :host ('localhost') One or more (comma separated)
      #   hostnames for the Cassandra nodes you want to connect to.
      # @option options [String] :port (9042) The port to connect to
      # @option options [Integer] :connection_timeout (5) Max time to wait for a
      #   connection, in seconds
      # @option options [String] :keyspace The keyspace to change to immediately
      #   after all connections have been established, this is optional.
      def initialize(options={})
        connection_timeout = options[:connection_timeout]
        @host = options[:host] || 'localhost'
        @port = options[:port] || 9042
        @io_reactor = options[:io_reactor] || Io::IoReactor.new(connection_timeout: connection_timeout)
        @lock = Mutex.new
        @started = false
        @shut_down = false
        @initial_keyspace = options[:keyspace]
        @credentials = options[:credentials]
        @connection_keyspaces = {}
      end

      # Connect to all nodes.
      #
      # You must call this method before you call any of the other methods of a
      # client. Calling it again will have no effect.
      #
      # If `:keyspace` was specified when the client was created the current
      # keyspace will also be changed (otherwise the current keyspace will not
      # be set).
      #
      # @return self
      #
      def connect
        @lock.synchronize do
          return if @started
          @started = true
          @io_reactor.start
          hosts = @host.split(',')
          connection_futures = hosts.map { |host| connect_to_host(host) }
          @connection_ids = Future.combine(*connection_futures).get
        end
        use(@initial_keyspace) if @initial_keyspace
        self
      rescue => e
        close
        if e.is_a?(Cql::QueryError) && e.code == 0x100
          raise AuthenticationError, e.message, e.backtrace
        else
          raise
        end
      end

      # Disconnect from all nodes.
      #
      # @return self
      #
      def close
        @lock.synchronize do
          return if @shut_down || !@started
          @shut_down = true
          @started = false
        end
        @io_reactor.stop.get
        self
      end

      # Returns whether or not the client is connected.
      #
      def connected?
        @started
      end

      # Returns the name of the current keyspace, or `nil` if no keyspace has been
      # set yet.
      #
      def keyspace
        @lock.synchronize do
          return @connection_ids.map { |id| @connection_keyspaces[id] }.first
        end
      end

      # Changes keyspace by sending a `USE` statement to all connections.
      #
      # The the second parameter is meant for internal use only.
      #
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      #
      def use(keyspace, connection_ids=@connection_ids)
        raise NotConnectedError unless connected?
        if check_keyspace_name!(keyspace)
          @lock.synchronize do
            connection_ids = connection_ids.select { |id| @connection_keyspaces[id] != keyspace }
          end
          if connection_ids.any?
            futures = connection_ids.map do |connection_id|
              execute_request(Protocol::QueryRequest.new("USE #{keyspace}", :one), connection_id)
            end
            futures.compact!
            Future.combine(*futures).get
          end
          nil
        end
      end

      # Execute a CQL statement
      #
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @raise [Cql::QueryError] raised when the CQL has syntax errors or for
      #   other situations when the server complains.
      # @return [nil, Enumerable<Hash>] Most statements have no result and return
      #   `nil`, but `SELECT` statements return an `Enumerable` of rows
      #   (see {QueryResult}).
      #
      def execute(cql, consistency=DEFAULT_CONSISTENCY_LEVEL)
        raise NotConnectedError unless connected?
        result = execute_request(Protocol::QueryRequest.new(cql, consistency)).value
        ensure_keyspace!
        result
      end

      # @private
      def execute_statement(connection_id, statement_id, metadata, values, consistency)
        raise NotConnectedError unless connected?
        execute_request(Protocol::ExecuteRequest.new(statement_id, metadata, values, consistency || DEFAULT_CONSISTENCY_LEVEL), connection_id).value
      end

      # Returns a prepared statement that can be run over and over again with
      # different values.
      #
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @return [Cql::PreparedStatement] an object encapsulating the prepared statement
      #
      def prepare(cql)
        raise NotConnectedError unless connected?
        execute_request(Protocol::PrepareRequest.new(cql)).value
      end

      private

      KEYSPACE_NAME_PATTERN = /^\w[\w\d_]*$/
      DEFAULT_CONSISTENCY_LEVEL = :quorum

      def check_keyspace_name!(name)
        if name !~ KEYSPACE_NAME_PATTERN
          raise InvalidKeyspaceNameError, %("#{name}" is not a valid keyspace name)
        end
        true
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
          PreparedStatement.new(self, connection_id, response.id, response.metadata)
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
