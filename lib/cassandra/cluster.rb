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
  # Cluster represents a cassandra cluster. It serves as a {Cassandra::Session}
  # factory and a collection of metadata.
  #
  # @see Cassandra::Cluster#connect Creating a new session
  # @see Cassandra::Cluster#each_host Getting all peers in the cluster
  # @see Cassandra::Cluster#each_keyspace Getting all existing keyspaces
  class Cluster
    extend Forwardable

    # @!method host(address)
    #   Find a host by its address
    #   @param address [IPAddr, String] ip address
    #   @return [Cassandra::Host, nil] host or nil
    #
    # @!method has_host?(address)
    #   Determine if a host by a given address exists
    #   @param address [IPAddr, String] ip address
    #   @return [Boolean] true or false
    def_delegators :@registry, :host, :has_host?

    # @!method keyspace(name)
    #   Find a keyspace by name
    #   @param name [String] keyspace name
    #   @return [Cassandra::Keyspace, nil] keyspace or nil
    #
    # @!method has_keyspace?(name)
    #   Determine if a keyspace by a given name exists
    #   @param name [String] keyspace name
    #   @return [Boolean] true or false
    def_delegators :@schema, :keyspace, :has_keyspace?

    # @private
    def initialize(logger, io_reactor, control_connection, cluster_registry, cluster_schema, execution_options, connection_options, load_balancing_policy, reconnection_policy, retry_policy, connector, futures_factory)
      @logger                = logger
      @io_reactor            = io_reactor
      @control_connection    = control_connection
      @registry              = cluster_registry
      @schema                = cluster_schema
      @execution_options     = execution_options
      @connection_options    = connection_options
      @load_balancing_policy = load_balancing_policy
      @reconnection_policy   = reconnection_policy
      @retry_policy          = retry_policy
      @connector             = connector
      @futures               = futures_factory
    end

    # Register a cluster state listener. State listener will start receiving
    # notifications about topology and schema changes
    #
    # @param listener [Cassandra::Listener] cluster state listener
    # @return [self]
    def register(listener)
      @registry.add_listener(listener)
      @schema.add_listener(listener)
      self
    end

    # Unregister a cluster state listener. State listener will stop receiving
    # notifications about topology and schema changes
    #
    # @param listener [Cassandra::Listener] cluster state listener
    # @return [self]
    def unregister(listener)
      @registry.remove_listener(listener)
      @schema.remove_listener(listener)
      self
    end

    # Yield or enumerate each member of this cluster
    # @overload each_host
    #   @yieldparam host [Cassandra::Host] current host
    #   @return [Cassandra::Cluster] self
    # @overload each_host
    #   @return [Array<Cassandra::Host>] a list of hosts
    def each_host(&block)
      r = @registry.each_host(&block)
      return self if r == @registry
      r
    end
    alias :hosts :each_host

    # Yield or enumerate each keyspace defined in this cluster
    # @overload each_keyspace
    #   @yieldparam keyspace [Cassandra::Keyspace] current keyspace
    #   @return [Cassandra::Cluster] self
    # @overload each_keyspace
    #   @return [Array<Cassandra::Keyspace>] a list of keyspaces
    def each_keyspace(&block)
      r = @schema.each_keyspace(&block)
      return self if r == @schema
      r
    end
    alias :keyspaces :each_keyspace

    # Asynchronously create a new session, optionally scoped to a keyspace
    #
    # @param keyspace [String] optional keyspace to scope session to
    #
    # @return [Cassandra::Future<Cassandra::Session>] a future new session that
    #   can prepare and execute statements
    def connect_async(keyspace = nil)
      if !keyspace.nil? && !keyspace.is_a?(::String)
        return @futures.error(::ArgumentError.new("keyspace must be a string, #{keyspace.inspect} given"))
      end

      client  = Client.new(@logger, @registry, @io_reactor, @connector, @load_balancing_policy, @reconnection_policy, @retry_policy, @connection_options, @futures)
      session = Session.new(client, @execution_options, @futures)
      promise = @futures.promise

      client.connect.on_complete do |f|
        if f.resolved?
          if keyspace
            f = session.execute_async("USE #{keyspace}")

            f.on_success {promise.fulfill(session)}
            f.on_failure {|e| promise.break(e)}
          else
            promise.fulfill(session)
          end
        else
          f.on_failure {|e| promise.break(e)}
        end
      end

      promise.future
    end

    # Synchronously create a new session, optionally scoped to a keyspace
    #
    # @param keyspace [String] optional keyspace to scope session to
    # @raise  [ArgumentError] if keyspace is not a String
    # @return [Cassandra::Session] a new session that can prepare and execute
    #   statements
    #
    # @see Cassandra::Cluster#connect_async
    def connect(keyspace = nil)
      connect_async(keyspace).get
    end

    # Asynchronously closes all sessions managed by this cluster
    #
    # @return [Cassandra::Future<Cassandra::Cluster>] a future that resolves to
    #   self once closed
    def close_async
      promise = @futures.promise

      @control_connection.close_async.on_complete do |f|
        if f.resolved?
          promise.fulfill(self)
        else
          f.on_failure {|e| promise.break(e)}
        end
      end

      promise.future
    end

    # Synchronously closes all sessions managed by this cluster
    #
    # @return [self] this cluster
    #
    # @see Cassandra::Cluster#close_async
    def close
      close_async.get
    end

    # @return [String] a CLI-friendly cluster representation
    def inspect
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
    end
  end
end

require 'cassandra/cluster/client'
require 'cassandra/cluster/connector'
require 'cassandra/cluster/control_connection'
require 'cassandra/cluster/options'
require 'cassandra/cluster/registry'
require 'cassandra/cluster/schema'
