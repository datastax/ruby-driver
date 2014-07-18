# encoding: utf-8

module Cql
  class Driver
    def self.let(name, &block)
      define_method(name)        { @instances[name] ||= @defaults.fetch(name) { instance_eval(&block) } }
      define_method(:"#{name}=") { |object| @instances[name] = object }
    end

    let(:request_runner)   { Client::RequestRunner.new }
    let(:io_reactor)       { Reactor.new(Io::IoReactor.new) }
    let(:cluster_registry) { Cluster::Registry.new }

    let(:control_connection) { Cluster::ControlConnection.new(io_reactor, request_runner, cluster_registry, self) }

    let(:cluster) { Cluster.new(io_reactor, control_connection, cluster_registry, client_options) }

    let(:client_options) { {
                             :io_reactor           => io_reactor,
                             :request_runner       => request_runner,
                             :registry             => cluster_registry,
                             :compressor           => compressor,
                             :logger               => logger,
                             :protocol_version     => protocol_version,
                             :default_consistency  => default_consistency,
                             :port                 => port,
                             :connection_timeout   => connection_timeout,
                             :credentials          => credentials,
                             :auth_provider        => auth_provider,
                             :reconnect_interval   => reconnect_interval,
                             :load_balancing_policy => load_balancing_policy,
                             :connections_per_local_node  => 2,
                             :connections_per_remote_node => 1
                           } }

    let(:port)                  { 9042 }
    let(:protocol_version)      { 2 }
    let(:connection_timeout)    { 10 }
    let(:default_consistency)   { :one }
    let(:logger)                { Client::NullLogger.new  }
    let(:compressor)            { nil }
    let(:credentials)           { nil }
    let(:auth_provider)         { nil }
    let(:reconnect_interval)    { 5 }
    let(:load_balancing_policy) { LoadBalancing::Policies::RoundRobin.new  }
    let(:reconnection_policy)   { Reconnection::Policies::Exponential.new(0.5, 30, 2) }

    def initialize(defaults = {})
      @defaults  = defaults
      @instances = {}
    end

    def connect(addresses)
      cluster_registry.add_listener(load_balancing_policy)
      addresses.each {|address| cluster_registry.host_found(address)}

      control_connection.connect_async.map(cluster)
    end
  end
end
