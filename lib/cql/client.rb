# encoding: utf-8

module Cql
  # This error type represents errors sent by the server, the `code` attribute
  # can be used to find the exact type, and `cql` contains the request's CQL,
  # if any. `message` contains the human readable error message sent by the
  # server.
  class QueryError < CqlError
    attr_reader :code, :cql

    def initialize(code, message, cql=nil)
      super(message)
      @code = code
      @cql = cql
    end
  end

  NotConnectedError = Class.new(CqlError)
  TimeoutError = Class.new(CqlError)
  ClientError = Class.new(CqlError)
  AuthenticationError = Class.new(ClientError)
  IncompleteTraceError = Class.new(ClientError)

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
    # @option options [Integer] :connections_per_node (1) The number of
    #   connections to open to each node. Each connection can have 128
    #   concurrent requests, so unless you have a need for more than that (times
    #   the number of nodes in your cluster), leave this option at its default.
    # @option options [Integer] :default_consistency (:quorum) The consistency
    #   to use unless otherwise specified. Consistency can also be specified on
    #   a per-request basis.
    # @option options [Cql::Compression::Compressor] :compressor An object that
    #   can compress and decompress frames. By specifying this option frame
    #   compression will be enabled.
    # @option options [Integer] :logger If you want the client to log
    #   significant events pass an object implementing the standard Ruby logger
    #   interface (e.g. quacks like `Logger` from the standard library) with
    #   this option.
    # @raise Cql::Io::ConnectionError when a connection couldn't be established
    #   to any node
    # @return [Cql::Client::Client]
    def self.connect(options={})
      SynchronousClient.new(AsynchronousClient.new(options)).connect
    end

    class Client
      # @!method connect
      #
      # Connect to all nodes. See {Cql::Client.connect} for the full
      # documentation.
      #
      # This method needs to be called before any other. Calling it again will
      # have no effect.
      #
      # @see Cql::Client.connect
      # @return [Cql::Client]

      # @!method close
      #
      # Disconnect from all nodes.
      #
      # @return [Cql::Client]

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
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @return [nil]

      # @!method execute(cql, options_or_consistency=nil)
      #
      # Execute a CQL statement
      #
      # @param [String] cql
      # @param [Hash] options_or_consistency Either a consistency as a symbol
      #   (e.g. `:quorum`), or a options hash (see below). Passing a symbol is
      #   equivalent to passing the options `consistency: <symbol>`.
      # @option options_or_consistency [Symbol] :consistency (:quorum) The
      #   consistency to use for this query.
      # @option options_or_consistency [Symbol] :timeout (nil) How long to wait
      #   for a response. If this timeout expires a {Cql::TimeoutError} will
      #   be raised.
      # @option options_or_consistency [Symbol] :trace (false) Request tracing
      #   for this request. See {Cql::Client::QueryResult} for how to retrieve
      #   the tracing data.
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @raise [Cql::TimeoutError] raised when a timeout was specified and no
      #   response was received within the timeout.
      # @raise [Cql::QueryError] raised when the CQL has syntax errors or for
      #   other situations when the server complains.
      # @return [nil, Cql::Client::QueryResult] Most queries have no result and
      #   return `nil`, but `SELECT` statements return an `Enumerable` of rows
      #   (see {Cql::Client::QueryResult}).

      # @!method prepare(cql)
      #
      # Returns a prepared statement that can be run over and over again with
      # different values.
      #
      # @see Cql::Client::PreparedStatement
      # @param [String] cql The CQL to prepare
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @raise [Cql::Io::IoError] raised when there is an IO error, for example
      #   if the server suddenly closes the connection
      # @raise [Cql::QueryError] raised when there is an error on the server
      #   side, for example when you specify a malformed CQL query
      # @return [Cql::Client::PreparedStatement] an object encapsulating the
      #   prepared statement
    end

    class PreparedStatement
      # @return [ResultMetadata]
      attr_reader :metadata

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
      # @return [nil, Cql::Client::QueryResult] Most statements have no result
      #    and return `nil`, but `SELECT` statements return an `Enumerable` of
      #   rows (see {Cql::Client::QueryResult}).
      def execute(*args)
      end
    end
  end
end

require 'cql/client/connection_manager'
require 'cql/client/connection_helper'
require 'cql/client/null_logger'
require 'cql/client/column_metadata'
require 'cql/client/result_metadata'
require 'cql/client/query_result'
require 'cql/client/query_trace'
require 'cql/client/execute_options_decoder'
require 'cql/client/keyspace_changer'
require 'cql/client/asynchronous_client'
require 'cql/client/asynchronous_prepared_statement'
require 'cql/client/synchronous_client'
require 'cql/client/synchronous_prepared_statement'
require 'cql/client/request_runner'