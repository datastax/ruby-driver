# encoding: utf-8

module Cql
  class Builder
    def initialize
      @options = {}
    end

    def add_contact_point(host)
      @options[:hosts] ||= []
      @options[:hosts] << host

      self
    end

    def with_contact_points(hosts)
      @options[:hosts] = hosts

      self
    end

    def with_credentials(username, password)
      @options[:credentials] = {:username => username, :password => password}

      self
    end

    def build
      Cluster.new(@options.merge(:io_reactor => Io::IoReactor.new))
    end
  end
end
