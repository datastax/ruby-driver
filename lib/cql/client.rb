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
  end
end

require 'cql/client/connection_manager'
require 'cql/client/connector'
require 'cql/client/null_logger'
require 'cql/client/column_metadata'
require 'cql/client/result_metadata'
require 'cql/client/execute_options_decoder'
require 'cql/client/keyspace_changer'
require 'cql/client/client'
require 'cql/client/prepared_statement'
require 'cql/client/batch'
require 'cql/client/query_result'
require 'cql/client/void_result'
require 'cql/client/request_runner'
require 'cql/client/peer_discovery'
