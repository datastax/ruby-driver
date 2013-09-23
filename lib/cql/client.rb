# encoding: utf-8

module Cql
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
  # See {Cql::Client::Client} for the full client API.
  #
  module Client
    InvalidKeyspaceNameError = Class.new(ClientError)

    # Create a new client and connect to Cassandra.
    #
    # By default the client will connect to localhost port 9042, which can be
    # overridden with the `:hosts` and `:port` options, respectively. Once
    # connected to the hosts given in `:hosts` the rest of the nodes in the
    # cluster will automatically be discovered and connected to.
    #
    # The connection will succeed if at least one node is up. Nodes that don't
    # respond within the specified timeout, or where the connection initialization
    # fails for some reason, are ignored.
    #
    # @raise Cql::Io::ConnectionError when a connection couldn't be established
    #   to any node
    # @param [Hash] options
    # @option options [Array<String>] :hosts (['localhost']) One or more
    #   hostnames used as seed nodes when connecting. Duplicates will be removed.
    # @option options [String] :host ('localhost') A comma separated list of 
    #   hostnames to use as seed nodes. This is a backwards-compatible version
    #   of the :hosts option, and is deprecated.
    # @option options [String] :port (9042) The port to connect to
    # @option options [Integer] :connection_timeout (5) Max time to wait for a
    #   connection, in seconds
    # @option options [String] :keyspace The keyspace to change to immediately
    #   after all connections have been established, this is optional.
    # @return [Cql::Client::Client]
    #
    def self.connect(options={})
      SynchronousClient.new(AsynchronousClient.new(options)).connect
    end

    class Client
      # @!method connect
      #
      # Connect to all nodes.
      #
      # You must call this method before you call any of the other methods of a
      # client. Calling it again will have no effect.
      #
      # @see Cql::Client.connect
      #
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

      # @!method execute(cql, consistency=:quorum)
      #
      # Execute a CQL statement
      #
      # @param [String] cql
      # @param [Symbol] consistency
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @raise [Cql::QueryError] raised when the CQL has syntax errors or for
      #   other situations when the server complains.
      # @return [nil, Cql::Client::QueryResult] Most statements have no result and return
      #   `nil`, but `SELECT` statements return an `Enumerable` of rows
      #   (see {Cql::Client::QueryResult}).

      # @!method prepare(cql)
      #
      # Returns a prepared statement that can be run over and over again with
      # different values.
      #
      # @param [String] cql
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @return [Cql::Client::PreparedStatement] an object encapsulating the prepared statement
    end

    class PreparedStatement
      # @return [ResultMetadata]
      attr_reader :metadata

      # Execute the prepared statement with a list of values for the bound parameters.
      #
      # The number of arguments must equal the number of bound parameters.
      # To set the consistency for the request you pass a consistency (as a
      # symbol) as the last argument. Needless to say, if you pass the value for
      # one bound parameter too few, and then a consistency, or if you pass too
      # many values, you will get weird errors.
      #
      # @param args [Array] the values for the bound parameters, and optionally
      #   the desired consistency, as a symbol (defaults to :quorum)
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @raise [Cql::QueryError] raised when there is an error on the server side
      # @return [nil, Cql::Client::QueryResult] Most statements have no result and return
      #   `nil`, but `SELECT` statements return an `Enumerable` of rows
      #   (see {Cql::Client::QueryResult}).
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
require 'cql/client/keyspace_changer'
require 'cql/client/asynchronous_client'
require 'cql/client/asynchronous_prepared_statement'
require 'cql/client/synchronous_client'
require 'cql/client/synchronous_prepared_statement'
require 'cql/client/request_runner'