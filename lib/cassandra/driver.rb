# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
  # @private
  class Driver
    def self.let(name, &block)
      define_method(name) do
        @instances.key?(name) ?
            @instances[name] :
            @instances[name] = instance_eval(&block)
      end
      define_method(:"#{name}=") { |object| @instances[name] = object }
    end

    let(:io_reactor)       { Ione::Io::IoReactor.new }
    let(:cluster_registry) { Cluster::Registry.new(logger) }
    let(:cluster_schema)   { Cluster::Schema.new }
    let(:cluster_metadata) do
      Cluster::Metadata.new(
        cluster_registry,
        cluster_schema,
        {
          'org.apache.cassandra.dht.Murmur3Partitioner' => murmur3_partitioner,
          'org.apache.cassandra.dht.ByteOrderedPartitioner' => ordered_partitioner,
          'org.apache.cassandra.dht.RandomPartitioner' => random_partitioner
        }.freeze,
        {
          'SimpleStrategy' => simple_replication_strategy,
          'NetworkTopologyStrategy' => network_topology_replication_strategy
        }.freeze,
        no_replication_strategy
      )
    end

    let(:executor)         { Executors::ThreadPool.new(thread_pool_size) }
    let(:futures_factory)  { Future::Factory.new(executor) }

    let(:schema_fqcn_type_parser) { Cluster::Schema::FQCNTypeParser.new }
    let(:schema_cql_type_parser)  { Cluster::Schema::CQLTypeParser.new }

    let(:simple_replication_strategy) do
      Cluster::Schema::ReplicationStrategies::Simple.new
    end
    let(:network_topology_replication_strategy) do
      Cluster::Schema::ReplicationStrategies::NetworkTopology.new
    end
    let(:no_replication_strategy) do
      Cluster::Schema::ReplicationStrategies::None.new
    end

    let(:murmur3_partitioner) { Cluster::Schema::Partitioners::Murmur3.new }
    let(:ordered_partitioner) { Cluster::Schema::Partitioners::Ordered.new }
    let(:random_partitioner)  { Cluster::Schema::Partitioners::Random.new }

    let(:connector) do
      Cluster::Connector.new(logger,
                             io_reactor,
                             cluster_registry,
                             connection_options,
                             execution_options)
    end

    let(:schema_fetcher) { create_schema_fetcher_picker }

    let(:control_connection) do
      Cluster::ControlConnection.new(logger,
                                     io_reactor,
                                     cluster_registry,
                                     cluster_schema,
                                     cluster_metadata,
                                     load_balancing_policy,
                                     reconnection_policy,
                                     address_resolution_policy,
                                     connector,
                                     connection_options,
                                     schema_fetcher)
    end

    let(:cluster) do
      Cluster.new(logger,
                  io_reactor,
                  executor,
                  control_connection,
                  cluster_registry,
                  cluster_schema,
                  cluster_metadata,
                  execution_options,
                  connection_options,
                  load_balancing_policy,
                  reconnection_policy,
                  retry_policy,
                  address_resolution_policy,
                  connector,
                  futures_factory)
    end

    let(:execution_options) do
      Execution::Options.new(consistency: consistency,
                             trace: trace,
                             page_size: page_size,
                             timeout: timeout,
                             idempotent: false)
    end

    let(:connection_options) do
      Cluster::Options.new(
        logger,
        protocol_version,
        credentials,
        auth_provider,
        compressor,
        port,
        connect_timeout,
        ssl,
        connections_per_local_node,
        connections_per_remote_node,
        heartbeat_interval,
        idle_timeout,
        synchronize_schema,
        schema_refresh_delay,
        schema_refresh_timeout,
        client_timestamps,
        nodelay,
        requests_per_connection
      )
    end

    let(:port)                      { 9042 }
    let(:protocol_version)          { nil }
    let(:connect_timeout)           { 10 }
    let(:ssl)                       { false }
    let(:logger)                    { NullLogger.new }
    let(:compressor)                { nil }
    let(:credentials)               { nil }
    let(:auth_provider)             { nil }
    let(:datacenter)                { nil }
    let(:load_balancing_policy)     do
      LoadBalancing::Policies::TokenAware.new(
        LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, 0),
        shuffle_replicas)
    end
    let(:reconnection_policy) do
      Reconnection::Policies::Exponential.new(0.5, 30, 2)
    end
    let(:retry_policy)              { Retry::Policies::Default.new }
    let(:address_resolution_policy) { AddressResolution::Policies::None.new }
    let(:consistency)               { :local_one }
    let(:trace)                     { false }
    let(:page_size)                 { 10000 }
    let(:heartbeat_interval)        { 30 }
    let(:idle_timeout)              { 60 }
    let(:timeout)                   { 12 }
    let(:synchronize_schema)        { true }
    let(:schema_refresh_delay)      { 1 }
    let(:schema_refresh_timeout)    { 10 }
    let(:thread_pool_size)          { 4 }
    let(:shuffle_replicas)          { true }
    let(:client_timestamps)         { false }
    let(:nodelay)                   { true }

    let(:connections_per_local_node)  { nil }
    let(:connections_per_remote_node) { nil }
    let(:requests_per_connection) { nil }

    let(:listeners) { [] }

    def initialize(defaults = {})
      @instances = defaults
    end

    def connect(addresses)
      load_balancing_policy.setup(cluster)
      cluster_registry.add_listener(load_balancing_policy)
      cluster_registry.add_listener(control_connection)
      listeners.each do |listener|
        cluster.register(listener)
      end

      logger.debug('Populating policies and listeners with initial endpoints')
      addresses.each {|address| cluster_registry.host_found(address)}

      logger.info('Establishing control connection')

      promise = futures_factory.promise

      control_connection.connect_async.on_complete do |f|
        if f.resolved?
          promise.fulfill(cluster)
        else
          f.on_failure do |e|
            cluster.close_async
            promise.break(e)
          end
        end
      end

      promise.future
    end

    private

    def create_schema_fetcher_picker
      picker = Cluster::Schema::Fetchers::MultiVersion.new(cluster_registry)

      picker.when('1.2') do
        Cluster::Schema::Fetchers::V1_2_x.new(schema_fqcn_type_parser, cluster_schema)
      end
      picker.when('2.0') do
        Cluster::Schema::Fetchers::V2_0_x.new(schema_fqcn_type_parser, cluster_schema)
      end
      picker.when('2.1') do
        Cluster::Schema::Fetchers::V2_1_x.new(schema_fqcn_type_parser, cluster_schema)
      end
      picker.when('2.2') do
        Cluster::Schema::Fetchers::V2_2_x.new(schema_fqcn_type_parser, cluster_schema)
      end
      picker.when('3.') do
        Cluster::Schema::Fetchers::V3_0_x.new(schema_cql_type_parser, cluster_schema)
      end

      picker
    end
  end
end
