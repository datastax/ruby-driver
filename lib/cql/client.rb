# encoding: utf-8

module Cql
  class QueryError < CqlError
    attr_reader :code

    def initialize(code, message)
      super(message)
      @code = code
    end
  end

  ClientError = Class.new(CqlError)

  # A CQL client manages connections to one or more Cassandra nodes and you use
  # it run queries, insert and update data.
  #
  # @example Connecting and changing to a keyspace
  #   # create a client that will connect to two Cassandra nodes
  #   client = Cql::Client.new(host: 'node01.cassandra.local,node02.cassandra.local')
  #   # establish node connections
  #   client.start!
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
  class Client
    NotConnectedError = Class.new(ClientError)
    InvalidKeyspaceNameError = Class.new(ClientError)

    # Create a new client.
    #
    # Creating a client does not automatically connect to Cassandra, you need to
    # call {#start!} to connect. `#start!` returns `self` so you can chain that
    # call after `new`.
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
    def start!
      @lock.synchronize do
        return if @started
        @io_reactor.start
        hosts = @host.split(',')
        start_request = Protocol::StartupRequest.new
        connection_futures = hosts.map do |host|
          @io_reactor.add_connection(host, @port).flat_map do |connection_id|
            execute_request(start_request, connection_id).map { connection_id }
          end
        end
        @connection_ids = Future.combine(*connection_futures).get
        @started = true
      end
      use(@initial_keyspace) if @initial_keyspace
      self
    end

    # Disconnect from all nodes.
    #
    # @return self
    #
    def shutdown!
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

    def execute_request(request, connection_id=nil)
      @io_reactor.queue_request(request, connection_id).map do |response, connection_id|
        interpret_response!(response, connection_id)
      end
    end

    def interpret_response!(response, connection_id)
      case response
      when Protocol::ErrorResponse
        raise QueryError.new(response.code, response.message)
      when Protocol::RowsResultResponse
        QueryResult.new(response.metadata, response.rows)
      when Protocol::PreparedResultResponse
        PreparedStatement.new(self, connection_id, response.id, response.metadata)
      when Protocol::SetKeyspaceResultResponse
        @lock.synchronize do
          @last_keyspace_change = @connection_keyspaces[connection_id] = response.keyspace
        end
        nil
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

    public

    # The representation of a prepared statement.
    #
    # Prepared statements are parsed once and stored on the server, allowing
    # you to execute them over and over again but only send values for the bound
    # parameters.
    #
    class PreparedStatement
      # @return [ResultMetadata]
      attr_reader :metadata

      def initialize(*args)
        @client, @connection_id, @statement_id, @raw_metadata = args
        @metadata = ResultMetadata.new(@raw_metadata)
      end

      # Execute the prepared statement with a list of values for the bound parameters.
      #
      # The number of arguments must equal the number of bound parameters.
      # To set the consistency level for the request you pass a consistency
      # level (as a symbol) as the last argument. Needless to say, if you pass
      # the value for one bound parameter too few, and then a consistency level,
      # or if you pass too many values, you will get weird errors.
      #
      # @param args [Array] the values for the bound parameters, and optionally
      #   the desired consistency level, as a symbol (defaults to :quorum)
      #
      def execute(*args)
        bound_args = args.shift(@raw_metadata.size)
        consistency_level = args.shift
        @client.execute_statement(@connection_id, @statement_id, @raw_metadata, bound_args, consistency_level)
      end
    end

    class QueryResult
      include Enumerable

      # @return [ResultMetadata]
      attr_reader :metadata

      # @private
      def initialize(metadata, rows)
        @metadata = ResultMetadata.new(metadata)
        @rows = rows
      end

      # Returns whether or not there are any rows in this result set
      #
      def empty?
        @rows.empty?
      end

      # Iterates over each row in the result set.
      #
      # @yieldparam [Hash] row each row in the result set as a hash
      # @return [Enumerable<Hash>]
      #
      def each(&block)
        @rows.each(&block)
      end
      alias_method :each_row, :each
    end

    class ResultMetadata
      include Enumerable

      # @private
      def initialize(metadata)
        @metadata = metadata.each_with_object({}) { |m, h| h[m[2]] = ColumnMetadata.new(*m) }
      end

      # Returns the column metadata
      #
      # @return [ColumnMetadata] column_metadata the metadata for the column
      #
      def [](column_name)
        @metadata[column_name]
      end

      # Iterates over the metadata for each column
      #
      # @yieldparam [ColumnMetadata] metadata the metadata for each column
      # @return [Enumerable<ColumnMetadata>]
      #
      def each(&block)
        @metadata.each_value(&block)
      end
    end

    # Represents metadata about a column in a query result set or prepared
    # statement. Apart from the keyspace, table and column names there's also
    # the type as a symbol (e.g. `:varchar`, `:int`, `:date`).
    class ColumnMetadata
      attr_reader :keyspace, :table, :column_name, :type
      
      # @private
      def initialize(*args)
        @keyspace, @table, @column_name, @type = args
      end

      # @private
      def to_ary
        [@keyspace, @table, @column_name, @type]
      end
    end
  end
end