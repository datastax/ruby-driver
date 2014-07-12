# encoding: utf-8

module Cql
  class Container
    def self.let(name, &block)
      define_method(name) { @services[name] ||= instance_eval(&block) }
      private name
    end

    let(:request_runner)   { Client::RequestRunner.new }
    let(:io_reactor)       { Io::IoReactor.new }
    let(:cluster_registry) { Cluster::Registry.new }

    let(:control_connection) { Cluster::ControlConnection.new(io_reactor, request_runner, cluster_registry, @settings) }

    let(:cluster) { Cluster.new(io_reactor, control_connection, cluster_registry, client_options) }

    let(:client_options) { {
                             :io_reactor           => io_reactor,
                             :request_runner       => request_runner,
                             :registry             => cluster_registry,
                             :compressor           => @settings.compressor,
                             :logger               => @settings.logger,
                             :protocol_version     => @settings.protocol_version,
                             :connections_per_node => 1,
                             :default_consistency  => @settings.default_consistency,
                             :port                 => @settings.port,
                             :connection_timeout   => @settings.connection_timeout,
                             :credentials          => @settings.credentials,
                             :auth_provider        => @settings.auth_provider,
                             :reconnect_interval   => @settings.reconnect_interval
                           } }

    public :cluster, :control_connection, :io_reactor

    def initialize(settings, services = {})
      @settings = settings
      @services = services
    end

    def add_address(address)
      cluster_registry.host_found(address.to_s)
      self
    end

    def add_registry_listener(listener)
      cluster_registry.add_listener(listener)

      self
    end
  end
end
