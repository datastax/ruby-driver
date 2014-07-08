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

    let(:cluster) { Cluster.new(control_connection, cluster_state, client_options, @settings) }

    let(:client_options) { {
                             :io_reactor       => io_reactor,
                             :request_runner   => request_runner,
                             :keyspace_changer => keyspace_changer
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

  class Builder
    Settings = Struct.new(:addresses, :port, :protocol_version, :connection_timeout, :default_consistency, :logger, :compressor, :credentials, :auth_provider)

    def initialize
      @settings = Settings.new(Set.new, 9042, 2, 10, :one, Client::NullLogger.new, nil, nil, nil)
    end

    def add_contact_point(host)
      @settings.addresses << IPAddr.new(host)

      self
    end

    def with_logger(logger)
      @settings.logger = logger

      self
    end

    def with_contact_points(hosts)
      @settings.addresses.clear
      hosts.each {|host| add_contact_point(host)}

      self
    end

    def with_credentials(username, password)
      @settings.credentials = {:username => username, :password => password}
      @settings.auth_provider = Auth::PlainTextAuthProvider.new(username, password)

      self
    end

    def with_compressor(compressor)
      @settings.compressor = compressor

      self
    end

    def with_port(port)
      @settings.port = port

      self
    end

    def build
      @settings.addresses << '127.0.0.1' if @settings.addresses.empty?

      create_cluster.get
    end

    private

    def create_cluster
      container = Container.new(@settings)

      control_connection = container.control_connection
      io_reactor         = container.io_reactor
      cluster            = container.cluster

      self.class.create_cluster(io_reactor, control_connection, cluster)
    end

    def self.create_cluster(io_reactor, control_connection, cluster)
      f = io_reactor.start
      f = f.flat_map { control_connection.connect_async }
      f.flat_map do
        Future.all(
          control_connection.register_async,
          control_connection.refresh_hosts_async
        ).map(cluster)
      end
    end
  end
end
