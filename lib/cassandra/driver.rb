# encoding: utf-8

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

module Cassandra
  # @private
  class Driver
    def self.let(name, &block)
      define_method(name)        { @instances[name] ||= @defaults.fetch(name) { instance_eval(&block) } }
      define_method(:"#{name}=") { |object| @instances[name] = object }
    end

    let(:request_runner)   { Client::RequestRunner.new }
    let(:io_reactor)       { Io::IoReactor.new }
    let(:cluster_registry) { Cluster::Registry.new(logger) }

    let(:connector) { Cluster::Connector.new(logger, io_reactor, cluster_registry, connection_options) }

    let(:control_connection) { Cluster::ControlConnection.new(logger, io_reactor, request_runner, cluster_registry, load_balancing_policy, reconnection_policy, connector, connection_options) }

    let(:cluster) { Cluster.new(logger, io_reactor, control_connection, cluster_registry, execution_options, connection_options, load_balancing_policy, reconnection_policy, retry_policy, connector) }

    let(:execution_options) do
      Execution::Options.new({
        :consistency => consistency,
        :trace       => trace,
        :page_size   => page_size
      })
    end

    let(:connection_options) { Cluster::Options.new(protocol_version, credentials, auth_provider, compressor, port, connection_timeout, connections_per_local_node, connections_per_remote_node) }

    let(:port)                  { 9042 }
    let(:protocol_version)      { 2 }
    let(:connection_timeout)    { 10 }
    let(:logger)                { Client::NullLogger.new  }
    let(:compressor)            { nil }
    let(:credentials)           { nil }
    let(:auth_provider)         { nil }
    let(:load_balancing_policy) { LoadBalancing::Policies::RoundRobin.new  }
    let(:reconnection_policy)   { Reconnection::Policies::Exponential.new(0.5, 30, 2) }
    let(:retry_policy)          { Retry::Policies::Default.new }
    let(:consistency)           { :one }
    let(:trace)                 { false }
    let(:page_size)             { nil }

    let(:connections_per_local_node)  { 2 }
    let(:connections_per_remote_node) { 1 }

    let(:listeners) { [] }

    def initialize(defaults = {})
      @defaults  = defaults
      @instances = {}
    end

    def connect(addresses)
      cluster_registry.add_listener(load_balancing_policy)
      cluster_registry.add_listener(control_connection)
      listeners.each do |listener|
        cluster.register(listener)
      end

      addresses.each {|address| cluster_registry.host_found(address)}

      control_connection.connect_async.map(cluster)
    end
  end
end
