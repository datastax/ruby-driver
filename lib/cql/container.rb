# encoding: utf-8

module Cql
  class Container
    def self.let(name, &block)
      define_method(name) { @services[name] ||= instance_eval(&block) }
      private name
    end

    let(:request_runner)   { Client::RequestRunner.new }
    let(:keyspace_changer) { Client::KeyspaceChanger.new(request_runner) }
    let(:io_reactor)       { Io::IoReactor.new }
    let(:cluster_state)    { Cluster::State.new(hosts) }

    let(:control_connection) { Cluster::ControlConnection.new(io_reactor, request_runner, cluster_state, @settings) }

    let(:cluster) { Cluster.new(control_connection, cluster_state, client_options) }

    let(:client_options) { {
                             :io_reactor           => io_reactor,
                             :request_runner       => request_runner,
                             :keyspace_changer     => keyspace_changer,
                             :compressor           => @settings.compressor,
                             :logger               => @settings.logger,
                             :protocol_version     => @settings.protocol_version,
                             :connections_per_node => 1,
                             :default_consistency  => @settings.default_consistency,
                             :port                 => @settings.port,
                             :connection_timeout   => @settings.connection_timeout,
                             :credentials          => @settings.credentials,
                             :auth_provider        => @settings.auth_provider
                           } }

    public :cluster, :control_connection, :io_reactor

    def initialize(settings)
      @settings = settings
      @services = {}
    end

    private

    def hosts
      hosts = {}
      @settings.addresses.each {|ip| hosts[ip.to_s] = Cluster::Host.new(ip.to_s)}

      ThreadSafe.new(hosts)
    end
  end
end
