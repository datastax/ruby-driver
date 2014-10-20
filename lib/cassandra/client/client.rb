# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

module Cassandra
  module Client
    # A CQL client manages connections to one or more Cassandra nodes and you use
    # it run queries, insert and update data, prepare statements and switch
    # keyspaces.
    #
    # To get a reference to a client you call {Cassandra::Client.connect}. When you
    # don't need the client anymore you can call {#close} to close all connections.
    #
    # Internally the client runs an IO reactor in a background thread. The reactor
    # handles all IO and manages the connections to the Cassandra nodes. This
    # makes it possible for the client to handle highly concurrent applications
    # very efficiently.
    #
    # Client instances are threadsafe and you only need a single instance for in
    # an application. Using multiple instances is more likely to lead to worse
    # performance than better.
    #
    # Because the client opens sockets, and runs threads it cannot be used by
    # the child created when forking a process. If your application forks (for
    # example applications running in the Unicorn application server or Resque
    # task queue) you _must_ connect after forking.
    #
    # @see Cassandra::Client.connect
    class Client
      # @!method close
      #
      # Disconnect from all nodes.
      #
      # @return [Cassandra::Client]

      # @!method connected?
      #
      # Returns whether or not the client is connected.
      #
      # @return [true, false]

      # @!method keyspace
      #
      # Returns the name of the current keyspace, or `nil` if no keyspace has been
      # set yet.
      #
      # @return [String]

      # @!method use(keyspace)
      #
      # Changes keyspace by sending a `USE` statement to all connections.
      #
      # The the second parameter is meant for internal use only.
      #
      # @param [String] keyspace
      # @raise [Cassandra::Errors::ClientError] raised when the client is not connected
      # @return [nil]

      # @!method execute(cql, *values, options={})
      #
      # Execute a CQL statement, optionally passing bound values.
      #
      # When passing bound values the request encoder will have to guess what
      # types to encode the values as. For most types this will be no problem,
      # but for integers and floating point numbers the larger size will be
      # chosen (e.g. `BIGINT` and `DOUBLE` and not `INT` and `FLOAT`). You can
      # override the guessing with the `:type_hint` option. Don't use on-the-fly
      # bound values when you will issue the request multiple times, prepared
      # statements are almost always a better choice.
      #
      # _Please note that on-the-fly bound values are only supported by Cassandra
      # 2.0 and above._
      #
      # @example A simple CQL query
      #   result = client.execute("SELECT * FROM users WHERE user_name = 'sue'")
      #   result.each do |row|
      #     p row
      #   end
      #
      # @example Using on-the-fly bound values
      #   client.execute('INSERT INTO users (user_name, full_name) VALUES (?, ?)', 'sue', 'Sue Smith')
      #
      # @example Using on-the-fly bound values with type hints
      #   client.execute('INSERT INTO users (user_name, age) VALUES (?, ?)', 'sue', 33, type_hints: [nil, :int])
      #
      # @example Specifying the consistency as a symbol
      #   client.execute("UPDATE users SET full_name = 'Sue S. Smith' WHERE user_name = 'sue'", consistency: :one)
      #
      # @example Specifying the consistency and other options
      #   client.execute("SELECT * FROM users", consistency: :all, timeout: 1.5)
      #
      # @example Loading a big result page by page
      #   result_page = client.execute("SELECT * FROM large_table WHERE id = 'partition_with_lots_of_data'", page_size: 100)
      #   while result_page
      #     result_page.each do |row|
      #       p row
      #     end
      #     result_page = result_page.next_page
      #   end
      #
      # @example Activating tracing for a query
      #   result = client.execute("SELECT * FROM users", tracing: true)
      #   p result.trace_id
      #
      # @param [String] cql
      # @param [Array] values Values to bind to any binding markers in the
      #   query (i.e. "?" placeholders) -- using this feature is similar to
      #   using a prepared statement, but without the type checking. The client
      #   needs to guess which data types to encode the values as, and will err
      #   on the side of caution, using types like BIGINT instead of INT for
      #   integers, and DOUBLE instead of FLOAT for floating point numbers. It
      #   is not recommended to use this feature for anything but convenience,
      #   and the algorithm used to guess types is to be considered experimental.
      # @param [Hash] options
      # @option options [Symbol] :consistency (:quorum) The
      #   consistency to use for this query.
      # @option options [Symbol] :serial_consistency (nil) The
      #   consistency to use for conditional updates (`:serial` or
      #   `:local_serial`), see the CQL documentation for the semantics of
      #   serial consistencies and conditional updates. The default is assumed
      #   to be `:serial` by the server if none is specified. Ignored for non-
      #   conditional queries.
      # @option options [Integer] :timeout (nil) How long to wait
      #   for a response. If this timeout expires a {Cassandra::Errors::TimeoutError} will
      #   be raised.
      # @option options [Boolean] :trace (false) Request tracing
      #   for this request. See {Cassandra::Client::QueryResult} and
      #   {Cassandra::Client::VoidResult} for how to retrieve the tracing data.
      # @option options [Integer] :page_size (nil) Instead of
      #   returning all rows, return the response in pages of this size. The
      #   first result will contain the first page, to load subsequent pages
      #   use {Cassandra::Client::QueryResult#next_page}.
      # @option options [Array] :type_hints (nil) When passing
      #   on-the-fly bound values the request encoder will have to guess what
      #   types to encode the values as. Using this option you can give it hints
      #   and avoid it guessing wrong. The hints must be an array that has the
      #   same number of arguments as the number of bound values, and each
      #   element should be the type of the corresponding value, or nil if you
      #   prefer the encoder to guess. The types should be provided as lower
      #   case symbols, e.g. `:int`, `:time_uuid`, etc.
      # @raise [Cassandra::Errors::ClientError] raised when the client is not connected
      # @raise [Cassandra::Errors::TimeoutError] raised when a timeout was specified and no
      #   response was received within the timeout.
      # @raise [Cassandra::Errors::ExecutionError] raised when the CQL has syntax errors or for
      #   other situations when the server complains.
      # @return [nil, Cassandra::Client::QueryResult, Cassandra::Client::VoidResult] Some
      #   queries have no result and return `nil`, but `SELECT` statements
      #   return an `Enumerable` of rows (see {Cassandra::Client::QueryResult}), and
      #   `INSERT` and `UPDATE` return a similar type
      #   (see {Cassandra::Client::VoidResult}).

      # @!method prepare(cql)
      #
      # Returns a prepared statement that can be run over and over again with
      # different bound values.
      #
      # @see Cassandra::Client::PreparedStatement
      # @param [String] cql The CQL to prepare
      # @raise [Cassandra::Errors::ClientError] raised when the client is not connected
      # @raise [Cassandra::Errors::IoError] raised when there is an IO error, for example
      #   if the server suddenly closes the connection
      # @raise [Cassandra::Errors::ExecutionError] raised when there is an error on the server
      #   side, for example when you specify a malformed CQL query
      # @return [Cassandra::Client::PreparedStatement] an object encapsulating the
      #   prepared statement

      # @!method batch(type=:logged, options={})
      #
      # Yields a batch when called with a block. The batch is automatically
      # executed at the end of the block and the result is returned.
      #
      # Returns a batch when called wihtout a block. The batch will remember
      # the options given and merge these with any additional options given
      # when {Cassandra::Client::Batch#execute} is called.
      #
      # Please note that the batch object returned by this method _is not thread
      # safe_.
      #
      # The type parameter can be ommitted and the options can then be given
      # as first parameter.
      #
      # @example Executing queries in a batch
      #   client.batch do |batch|
      #     batch.add(%(INSERT INTO metrics (id, time, value) VALUES (1234, NOW(), 23423)))
      #     batch.add(%(INSERT INTO metrics (id, time, value) VALUES (2346, NOW(), 13)))
      #     batch.add(%(INSERT INTO metrics (id, time, value) VALUES (2342, NOW(), 2367)))
      #     batch.add(%(INSERT INTO metrics (id, time, value) VALUES (4562, NOW(), 1231)))
      #   end
      #
      # @example Using the returned batch object
      #   batch = client.batch(:counter, trace: true)
      #   batch.add('UPDATE counts SET value = value + ? WHERE id = ?', 4, 87654)
      #   batch.add('UPDATE counts SET value = value + ? WHERE id = ?', 3, 6572)
      #   result = batch.execute(timeout: 10)
      #   puts result.trace_id
      #
      # @example Providing type hints for on-the-fly bound values
      #   batch = client.batch
      #   batch.add('UPDATE counts SET value = value + ? WHERE id = ?', 4, type_hints: [:int])
      #   batch.execute
      #
      # @see Cassandra::Client::Batch
      # @param [Symbol] type the type of batch, must be one of `:logged`,
      #   `:unlogged` and `:counter`. The precise meaning of these  is defined
      #   in the CQL specification.
      # @yieldparam [Cassandra::Client::Batch] batch the batch
      # @return [Cassandra::Client::VoidResult, Cassandra::Client::Batch] when no block is
      #   given the batch is returned, when a block is given the result of
      #   executing the batch is returned (see {Cassandra::Client::Batch#execute}).
    end

    # @private
    class AsynchronousClient < Client
      def initialize(options={})
        @compressor = options[:compressor]
        @cql_version = options[:cql_version]
        @logger = options[:logger] || NullLogger.new
        @protocol_version = options[:protocol_version] || 2
        @io_reactor = options[:io_reactor] || Ione::Io::IoReactor.new
        @hosts = extract_hosts(options)
        @initial_keyspace = options[:keyspace]
        @connections_per_node = options[:connections_per_node] || 1
        @lock = Mutex.new
        @request_runner = RequestRunner.new
        @connection_manager = ConnectionManager.new
        @execute_options_decoder = ExecuteOptionsDecoder.new(options[:default_consistency] || DEFAULT_CONSISTENCY)
        @port = options[:port] || DEFAULT_PORT
        @connection_timeout = options[:connection_timeout] || DEFAULT_CONNECTION_TIMEOUT
        @credentials = options[:credentials]
        @auth_provider = options[:auth_provider] || @credentials && Auth::Providers::Password.new(*@credentials.values_at(:username, :password))
        @connected = false
        @connecting = false
        @closing = false
      end

      def connect
        @lock.synchronize do
          raise Errors::ClientError, 'Cannot connect a closed client' if @closing || @closed
          return @connected_future if can_execute?
          @connecting = true
          @connected_future = begin
            f = @io_reactor.start
            f = f.flat_map { connect_with_protocol_version_fallback }
            f = f.flat_map { |connections| connect_to_all_peers(connections) }
            f = f.flat_map do |connections|
              @connection_manager.add_connections(connections)
              register_event_listener(@connection_manager.random_connection)
            end
            f = f.flat_map { use_keyspace(@connection_manager.snapshot, @initial_keyspace) }
            f.map(self)
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
              f = f.map(self)
              f
            else
              f = @io_reactor.stop
              f = f.map(self)
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
          connections = @connection_manager.reject { |c| c.keyspace == keyspace }
          return Ione::Future.resolved if connections.empty?
          use_keyspace(connections, keyspace).map(nil)
        end
      end

      def execute(cql, *args)
        with_failure_handler do
          options_or_consistency = nil
          if args.last.is_a?(Symbol) || args.last.is_a?(Hash)
            options_or_consistency = args.pop
          end
          options = @execute_options_decoder.decode_options(options_or_consistency)
          request = Protocol::QueryRequest.new(cql, args, options[:type_hints], options[:consistency], options[:serial_consistency], options[:page_size], options[:paging_state], options[:trace])
          f = execute_request(request, options[:timeout])
          if options.include?(:page_size)
            f = f.map { |result| AsynchronousQueryPagedQueryResult.new(self, request, result, options) }
          end
          f
        end
      end

      def prepare(cql)
        with_failure_handler do
          AsynchronousPreparedStatement.prepare(cql, @execute_options_decoder, @connection_manager, @logger)
        end
      end

      def batch(type=:logged, options=nil)
        if type.is_a?(Hash)
          options = type
          type = :logged
        end
        b = AsynchronousBatch.new(type, @execute_options_decoder, @connection_manager, options)
        if block_given?
          yield b
          b.execute
        else
          b
        end
      end

      private

      DEFAULT_CQL_VERSIONS = {1 => '3.0.0'}
      DEFAULT_CQL_VERSIONS.default = '3.1.0'
      DEFAULT_CQL_VERSIONS.freeze
      DEFAULT_CONSISTENCY = :quorum
      DEFAULT_PORT = 9042
      DEFAULT_CONNECTION_TIMEOUT = 10
      MAX_RECONNECTION_ATTEMPTS = 5

      def extract_hosts(options)
        if options[:hosts] && options[:hosts].any?
          options[:hosts].uniq
        elsif options[:host]
          options[:host].split(',').uniq
        else
          %w[localhost]
        end
      end

      def create_cluster_connector
        cql_version = @cql_version || DEFAULT_CQL_VERSIONS[@protocol_version]
        authentication_step = @protocol_version == 1 ? CredentialsAuthenticationStep.new(@credentials) : SaslAuthenticationStep.new(@auth_provider)
        protocol_handler_factory = lambda { |connection| Protocol::CqlProtocolHandler.new(connection, @io_reactor, @protocol_version, @compressor) }
        ClusterConnector.new(
          Connector.new([
            ConnectStep.new(@io_reactor, protocol_handler_factory, @port, @connection_timeout, @logger),
            CacheOptionsStep.new,
            InitializeStep.new(@compressor, @logger),
            authentication_step,
            CachePropertiesStep.new,
          ]),
          @logger
        )
      end

      def connect_with_protocol_version_fallback
        f = create_cluster_connector.connect_all(@hosts, @connections_per_node)
        f.fallback do |error|
          if error.is_a?(Errors::ProtocolError) && @protocol_version > 1
            @logger.warn('Could not connect using protocol version %d (will try again with %d): %s' % [@protocol_version, @protocol_version - 1, error.message])
            @protocol_version -= 1
            connect_with_protocol_version_fallback
          else
            raise(error, error.message, error.backtrace)
          end
        end
      end

      def connect_to_all_peers(seed_connections, initial_keyspace=@initial_keyspace)
        @logger.debug('Looking for additional nodes')
        peer_discovery = PeerDiscovery.new(seed_connections)
        peer_discovery.new_hosts.flat_map do |hosts|
          if hosts.empty?
            @logger.debug('No additional nodes found')
            Ione::Future.resolved(seed_connections)
          else
            @logger.debug('%d additional nodes found' % hosts.size)
            f = create_cluster_connector.connect_all(hosts, @connections_per_node)
            f = f.map do |discovered_connections|
              seed_connections + discovered_connections
            end
            f.recover(seed_connections)
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
        return Ione::Future.failed(Errors::ClientError.new) unless can_execute?
        yield
      rescue => e
        Ione::Future.failed(e)
      end

      def use_keyspace(connections, keyspace)
        return Ione::Future.resolved(connections) unless keyspace
        return Ione::Future.failed(InvalidKeyspaceNameError.new(%("#{keyspace}" is not a valid keyspace name))) unless keyspace =~ /^\w[\w\d_]*$|^"\w[\w\d_]*"$/

        futures = connections.map do |connection|
          request = Protocol::QueryRequest.new("USE #{Util.escape_name(keyspace)}", nil, nil, :one)
          @request_runner.execute(connection, request).map(connection)
        end
        Ione::Future.all(*futures)
      end

      def register_event_listener(connection)
        register_request = Protocol::RegisterRequest.new(Protocol::TopologyChangeEventResponse::TYPE, Protocol::StatusChangeEventResponse::TYPE)
        f = execute_request(register_request, nil, connection)
        f.on_value do
          connection.on_closed do
            if connected?
              begin
                register_event_listener(@connection_manager.random_connection)
              rescue Errors::IOError
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
        f
      end

      def handle_topology_change(remaning_attempts=MAX_RECONNECTION_ATTEMPTS)
        with_failure_handler do
          seed_connections = @connection_manager.snapshot
          f = connect_to_all_peers(seed_connections, keyspace)
          f.flat_map do |all_connections|
            new_connections = all_connections - seed_connections
            if new_connections.size > 0
              f = use_keyspace(new_connections, keyspace)
              f.on_value do
                @connection_manager.add_connections(new_connections)
              end
              f
            elsif remaning_attempts > 0
              timeout = 2**(MAX_RECONNECTION_ATTEMPTS - remaning_attempts)
              @logger.debug('Scheduling new peer discovery in %ds' % timeout)
              f = @io_reactor.schedule_timer(timeout)
              f.flat_map do
                handle_topology_change(remaning_attempts - 1)
              end
            else
              @logger.warn('Giving up looking for additional nodes')
              Ione::Future.resolved
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

    # @private
    class SynchronousClient < Client
      include SynchronousBacktrace

      def initialize(async_client)
        @async_client = async_client
      end

      def connect
        synchronous_backtrace { @async_client.connect.value }
        self
      end

      def close
        synchronous_backtrace { @async_client.close.value }
        self
      end

      def connected?
        @async_client.connected?
      end

      def keyspace
        @async_client.keyspace
      end

      def use(keyspace)
        synchronous_backtrace { @async_client.use(keyspace).value }
      end

      def execute(cql, *args)
        synchronous_backtrace do
          result = @async_client.execute(cql, *args).value
          result = SynchronousPagedQueryResult.new(result) if result.is_a?(PagedQueryResult)
          result
        end
      end

      def prepare(cql)
        async_statement = synchronous_backtrace { @async_client.prepare(cql).value }
        SynchronousPreparedStatement.new(async_statement)
      end

      def batch(type=:logged, options={}, &block)
        if block_given?
          synchronous_backtrace { @async_client.batch(type, options, &block).value }
        else
          SynchronousBatch.new(@async_client.batch(type, options))
        end
      end

      def async
        @async_client
      end
    end
  end
end
