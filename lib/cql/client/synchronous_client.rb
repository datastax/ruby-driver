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
      def initialize(async_client)
        @async_client = async_client
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
        @async_client.connect.map { self }.get
      end

      # Disconnect from all nodes.
      #
      # @return self
      #
      def close
        @async_client.close.map { self }.get
      end

      # Returns whether or not the client is connected.
      #
      def connected?
        @async_client.connected?
      end

      # Returns the name of the current keyspace, or `nil` if no keyspace has been
      # set yet.
      #
      def keyspace
        @async_client.keyspace
      end

      # Changes keyspace by sending a `USE` statement to all connections.
      #
      # The the second parameter is meant for internal use only.
      #
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      #
      def use(keyspace, connection_ids=nil)
        @async_client.use(keyspace, connection_ids).get
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
      def execute(cql, consistency=nil)
        @async_client.execute(cql, consistency).get
      end

      # Returns a prepared statement that can be run over and over again with
      # different values.
      #
      # @raise [Cql::NotConnectedError] raised when the client is not connected
      # @return [Cql::PreparedStatement] an object encapsulating the prepared statement
      #
      def prepare(cql)
        @async_client.prepare(cql).map { |statement| SynchronousPreparedStatement.new(statement) }.get
      end
    end
  end
end
