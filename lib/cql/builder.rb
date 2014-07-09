# encoding: utf-8

module Cql
  class Builder
    def initialize(services = {})
      @services = services
      @settings = Settings.new(Set.new, 9042, 2, 10, :one, Client::NullLogger.new, nil, nil, nil, 5)
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
      container = Container.new(@settings, @services)

      control_connection = container.control_connection
      io_reactor         = container.io_reactor
      cluster            = container.cluster

      self.class.create_cluster(io_reactor, control_connection, cluster)
    end

    def self.create_cluster(io_reactor, control_connection, cluster)
      f = io_reactor.start
      f = f.flat_map { control_connection.connect_async }
      f.map(cluster)
    end
  end
end

require 'cql/builder/settings'
