# encoding: utf-8

module Cql
  class Cluster
    def initialize(io_reactor, control_connection, cluster_registry, driver)
      @io_reactor         = io_reactor
      @control_connection = control_connection
      @registry           = cluster_registry
      @driver             = driver
    end

    def hosts
      @registry.hosts
    end

    def register(listener)
      @registry.add_listener(listener)
      self
    end

    def connect_async(keyspace = nil)
      client  = Client.new(@driver)
      session = Session.new(client, @driver.execution_options)

      f = client.connect
      f = f.flat_map { session.execute_async("USE #{keyspace}") } if keyspace
      f.map(session)
    end

    def connect(keyspace = nil)
      connect_async(keyspace).get
    end

    def close_async
      @control_connection.close_async.map(self)
    end

    def close
      close_async.get
    end
  end
end

require 'cql/cluster/client'
require 'cql/cluster/control_connection'
require 'cql/cluster/registry'
