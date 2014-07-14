# encoding: utf-8

module Cql
  class Builder
    def initialize(settings = {})
      @settings  = settings
      @addresses = ::Set.new
    end

    def add_contact_point(host)
      @addresses << IPAddr.new(host)

      self
    end

    def with_logger(logger)
      @settings[:logger] = logger

      self
    end

    def with_contact_points(*hosts)
      @addresses.clear
      hosts.each {|host| add_contact_point(host)}

      self
    end

    def with_credentials(username, password)
      @settings[:credentials] = {:username => username, :password => password}
      @settings[:auth_provider] = Auth::PlainTextAuthProvider.new(username, password)

      self
    end

    def with_compressor(compressor)
      @settings[:compressor] = compressor

      self
    end

    def with_port(port)
      @settings[:port] = port

      self
    end

    def with_load_balancing_policy(policy)
      @settings[:load_balancing_policy] = policy

      self
    end

    def build
      @addresses << IPAddr.new('127.0.0.1') if @addresses.empty?

      Driver.new(@settings).connect(@addresses).get
    end
  end
end
