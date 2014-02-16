# encoding: utf-8

module Cql
  # This error type represents errors sent by the server, the `code` attribute
  # can be used to find the exact type, and `cql` contains the request's CQL,
  # if any. `message` contains the human readable error message sent by the
  # server.
  class QueryError < CqlError
    attr_reader :code, :cql, :details

    def initialize(code, message, cql=nil, details=nil)
      super(message)
      @code = code
      @cql = cql
      @details = details
    end
  end

  NotConnectedError = Class.new(CqlError)
  TimeoutError = Class.new(CqlError)
  ClientError = Class.new(CqlError)
  AuthenticationError = Class.new(ClientError)
  IncompleteTraceError = Class.new(ClientError)
  UnsupportedProtocolVersionError = Class.new(ClientError)
  NotPreparedError = Class.new(ClientError)

  # A CQL client manages connections to one or more Cassandra nodes and you use
  # it run queries, insert and update data.
  #
  # Client instances are threadsafe.
  #
  # See {Cql::Client::Client} for the full client API, or {Cql::Client.connect}
  # for the options available when connecting.
  #
  # @example Connecting and changing to a keyspace
  #   # create a client and connect to two Cassandra nodes
  #   client = Cql::Client.connect(hosts: %w[node01.cassandra.local node02.cassandra.local])
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
  module Client
    InvalidKeyspaceNameError = Class.new(ClientError)

    # @private
    module SynchronousBacktrace
      def synchronous_backtrace
        yield
      rescue CqlError => e
        new_backtrace = caller
        if new_backtrace.first.include?(SYNCHRONOUS_BACKTRACE_METHOD_NAME)
          new_backtrace = new_backtrace.drop(1)
        end
        e.set_backtrace(new_backtrace)
        raise
      end

      private

      SYNCHRONOUS_BACKTRACE_METHOD_NAME = 'synchronous_backtrace'
    end

    # Create a new client and connect to Cassandra.
    #
    # By default the client will connect to localhost port 9042, which can be
    # overridden with the `:hosts` and `:port` options, respectively. Once
    # connected to the hosts given in `:hosts` the rest of the nodes in the
    # cluster will automatically be discovered and connected to.
    #
    # If you have a multi data center setup the client will connect to all nodes
    # in the data centers where the nodes you pass to `:hosts` are located. So
    # if you only want to connect to nodes in one data center, make sure that
    # you only specify nodes in that data center in `:hosts`.
    #
    # The connection will succeed if at least one node is up and accepts the
    # connection. Nodes that don't respond within the specified timeout, or
    # where the connection initialization fails for some reason, are ignored.
    #
    # @param [Hash] options
    # @option options [Array<String>] :hosts (['localhost']) One or more
    #   hostnames used as seed nodes when connecting. Duplicates will be removed.
    # @option options [String] :host ('localhost') A comma separated list of 
    #   hostnames to use as seed nodes. This is a backwards-compatible version
    #   of the :hosts option, and is deprecated.
    # @option options [String] :port (9042) The port to connect to, this port
    #   will be used for all nodes. Because the `system.peers` table does not
    #   contain the port that the nodes are listening on, the port must be the
    #   same for all nodes.
    # @option options [Integer] :connection_timeout (5) Max time to wait for a
    #   connection, in seconds.
    # @option options [String] :keyspace The keyspace to change to immediately
    #   after all connections have been established, this is optional.
    # @option options [Hash] :credentials When using Cassandra's built in
    #   authentication you can provide your username and password through this
    #   option. Example: `:credentials => {:username => 'cassandra', :password => 'cassandra'}`
    # @option options [Object] :auth_provider When using custom authentication
    #   use this option to specify the auth provider that will handle the
    #   authentication negotiation. See {Cql::Client::AuthProvider} for more info.
    # @option options [Integer] :connections_per_node (1) The number of
    #   connections to open to each node. Each connection can have 128
    #   concurrent requests, so unless you have a need for more than that (times
    #   the number of nodes in your cluster), leave this option at its default.
    # @option options [Integer] :default_consistency (:quorum) The consistency
    #   to use unless otherwise specified. Consistency can also be specified on
    #   a per-request basis.
    # @option options [Cql::Compression::Compressor] :compressor An object that
    #   can compress and decompress frames. By specifying this option frame
    #   compression will be enabled. If the server does not support compression
    #   or the specific compression algorithm specified by the compressor,
    #   compression will not be enabled and a warning will be logged.
    # @option options [String] :cql_version Specifies which CQL version the
    #   server should expect.
    # @option options [Integer] :logger If you want the client to log
    #   significant events pass an object implementing the standard Ruby logger
    #   interface (e.g. quacks like `Logger` from the standard library) with
    #   this option.
    # @raise Cql::Io::ConnectionError when a connection couldn't be established
    #   to any node
    # @raise Cql::Client::QueryError when the specified keyspace does not exist
    #   or when the specifed CQL version is not supported.
    # @return [Cql::Client::Client]
    def self.connect(options={})
      SynchronousClient.new(AsynchronousClient.new(options)).connect
    end

    class PreparedStatement
      # Metadata describing the bound values
      #
      # @return [ResultMetadata]
      attr_reader :metadata

      # Metadata about the result (i.e. rows) that is returned when executing
      # this prepared statement.
      #
      # @return [ResultMetadata]
      attr_reader :result_metadata

      # Execute the prepared statement with a list of values to be bound to the
      # statements parameters.
      #
      # The number of arguments must equal the number of bound parameters. You
      # can also specify options as the last argument, or a symbol as a shortcut
      # for just specifying the consistency.
      #
      # Because you can specify options, or not, there is an edge case where if
      # the last parameter of your prepared statement is a map, and you forget
      # to specify a value for your map, the options will end up being sent to
      # Cassandra. Most other cases when you specify the wrong number of
      # arguments should result in an `ArgumentError` or `TypeError` being
      # raised.
      #
      # @param args [Array] the values for the bound parameters. The last
      #   argument can also be an options hash or a symbol (as a shortcut for
      #   specifying the consistency), see {Cql::Client::Client#execute} for
      #   full details.
      # @raise [ArgumentError] raised when number of argument does not match
      #   the number of parameters needed to be bound to the statement.
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @raise [Cql::Io::IoError] raised when there is an IO error, for example
      #   if the server suddenly closes the connection
      # @raise [Cql::QueryError] raised when there is an error on the server side
      # @return [nil, Cql::Client::QueryResult, Cql::Client::VoidResult] Some
      #   queries have no result and return `nil`, but `SELECT` statements
      #   return an `Enumerable` of rows (see {Cql::Client::QueryResult}), and
      #   `INSERT` and `UPDATE` return a similar type
      #   (see {Cql::Client::VoidResult}).
      def execute(*args)
      end
    end

    class Batch
      # @!method add(cql_or_prepared_statement, *bound_values)
      #
      # Add a query or a prepared statement to the batch.
      #
      # @example Adding a mix of statements to a batch
      #   batch.add(%(UPDATE people SET name = 'Miriam' WHERE id = 3435))
      #   batch.add(%(UPDATE people SET name = ? WHERE id = ?), 'Miriam', 3435)
      #   batch.add(prepared_statement, 'Miriam', 3435)
      #
      # @param [String, Cql::Client::PreparedStatement] cql_or_prepared_statement
      #   a CQL string or a prepared statement object (obtained through
      #   {Cql::Client::Client#prepare})
      # @param [Array] bound_values a list of bound values -- only applies when
      #   adding prepared statements and when there are binding markers in the
      #   given CQL. If the last argument is a hash and it has the key
      #   `:type_hints` this will be passed as type hints to the request encoder
      #   (if the last argument is any other hash it will be assumed to be a
      #   bound value of type MAP). See {Cql::Client::Client#execute} for more
      #   info on type hints.
      # @return [nil]

      # @!method execute(options={})
      #
      # Execute the batch and return the result.
      #
      # @param options [Hash] an options hash or a symbol (as a shortcut for
      #   specifying the consistency), see {Cql::Client::Client#execute} for
      #   full details about how this value is interpreted.
      # @raise [Cql::QueryError] raised when there is an error on the server side
      # @raise [Cql::NotPreparedError] raised in the unlikely event that a
      #   prepared statement was not prepared on the chosen connection
      # @return [Cql::Client::VoidResult] a batch always returns a void result
    end

    class PreparedStatementBatch
      # @!method add(*bound_values)
      #
      # Add the statement to the batch with the specified bound values.
      #
      # @param [Array] bound_values the values to bind to the added statement,
      #   see {Cql::Client::PreparedStatement#execute}.
      # @return [nil]

      # @!method execute(options={})
      #
      # Execute the batch and return the result.
      #
      # @raise [Cql::QueryError] raised when there is an error on the server side
      # @raise [Cql::NotPreparedError] raised in the unlikely event that a
      #   prepared statement was not prepared on the chosen connection
      # @return [Cql::Client::VoidResult] a batch always returns a void result
    end

    # An auth provider is a factory for {Cql::Client::Authenticator} instances
    # (or objects matching that interface). Its {#create_authenticator} will be
    # called once for each connection that requires authentication.
    #
    # If the authentication requires keeping state, keep that in the
    # authenticator instances, not in the auth provider.
    #
    # @note Creating an authenticator must absolutely not block, or the whole
    #   connection process will block.
    #
    # @note Auth providers given to {Cql::Client.connect} as the `:auth_provider`
    #   option don't need to be subclasses of this class, but need to
    #   implement the same methods. This class exists only for documentation
    #   purposes.
    class AuthProvider
      # @!method create_authenticator(authentication_class, protocol_version)
      #
      # Create a new authenticator object. This method will be called once per
      # connection that requires authentication. The auth provider can create
      # different authenticators for different authentication classes, or return
      # nil if it does not support the authentication class.
      #
      # @note This method must absolutely not block.
      #
      # @param authentication_class [String] the authentication class used by
      #   the server.
      # @return [Cql::Client::Authenticator, nil] an object with an interface
      #   matching {Cql::Client::Authenticator} or nil if the authentication
      #   class is not supported.
    end

    # An authenticator handles the authentication challenge/response cycles of
    # a single connection. It can be stateful, but it must not for any reason
    # block. If any of the method calls block, the whole connection process
    # will be blocked.
    #
    # @note Authenticators created by auth providers don't need to be subclasses
    #   of this class, but need to implement the same methods. This class exists
    #   only for documentation purposes.
    class Authenticator
      # @!method initial_response
      #
      # This method must return the initial authentication token to be sent to
      # the server.
      #
      # @note This method must absolutely not block.
      #
      # @return [String] the initial authentication token

      # @!method challenge_response(token)
      #
      # If the authentication requires multiple challenge/response cycles this
      # method will be called when a challenge is returned by the server. A
      # response token must be created and will be sent back to the server.
      #
      # @note This method must absolutely not block.
      #
      # @param token [String] a challenge token sent by the server
      # @return [String] the authentication token to send back to the server

      # @!method authentication_successful(token)
      #
      # Called when the authentication is successful.
      #
      # @note This method must absolutely not block.
      #
      # @param token [String] a token sent by the server
      # @return [nil]
    end
  end
end

require 'cql/client/connection_manager'
require 'cql/client/connector'
require 'cql/client/null_logger'
require 'cql/client/column_metadata'
require 'cql/client/result_metadata'
require 'cql/client/query_trace'
require 'cql/client/execute_options_decoder'
require 'cql/client/keyspace_changer'
require 'cql/client/client'
require 'cql/client/asynchronous_prepared_statement'
require 'cql/client/synchronous_prepared_statement'
require 'cql/client/batch'
require 'cql/client/query_result'
require 'cql/client/void_result'
require 'cql/client/request_runner'
require 'cql/client/authenticators'
require 'cql/client/peer_discovery'
