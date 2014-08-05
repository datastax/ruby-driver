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

    def with_compresion(compression)
      case compression
      when :snappy
        require 'cql/compression/snappy_compressor'
        @settings[:compressor] = Compression::SnappyCompressor.new
      when :lz4
        require 'cql/compression/lz4_compressor'
        @settings[:compressor] = Compression::Lz4Compressor.new
      else
        raise ::ArgumentError, "only :snappy and :lz4 compressions are supported, #{compression.inspect} given"
      end

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

    def with_reconnection_policy(policy)
      @settings[:reconnection_policy] = policy

      self
    end

    def with_retry_policy(policy)
      @settings[:retry_policy] = policy

      self
    end

    def build
      @addresses << IPAddr.new('127.0.0.1') if @addresses.empty?

      Driver.new(@settings).connect(@addresses).value
    end
  end
end
