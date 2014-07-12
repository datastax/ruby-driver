# encoding: utf-8

module Cql
  class Builder
    class Settings
      attr_reader   :addresses
      attr_accessor :port, :protocol_version, :connection_timeout,
                    :default_consistency, :logger, :compressor, :credentials,
                    :auth_provider, :reconnect_interval, :load_balancing_policy

      def initialize(options = {})
        @port                  = options.fetch(:port, 9042)
        @protocol_version      = options.fetch(:protocol_version, 2)
        @connection_timeout    = options.fetch(:connection_timeout, 10)
        @default_consistency   = options.fetch(:default_consistency, :one)
        @logger                = options.fetch(:logger) { Client::NullLogger.new }
        @compressor            = options.fetch(:compressor, nil)
        @credentials           = options.fetch(:credentials, nil)
        @auth_provider         = options.fetch(:auth_provider, nil)
        @reconnect_interval    = options.fetch(:reconnect_interval, 5)
        @load_balancing_policy = options.fetch(:load_balancing_policy) { LoadBalancing::Policies::RoundRobin.new }
        @addresses             = ::Set.new
      end
    end
  end
end
