# encoding: utf-8

module Cql
  class Connector
    def initialize(logger, io_reactor, compressor, protocol_version, port, connection_timeout, credentials, auth_provider)
      @logger             = logger
      @io_reactor         = io_reactor
      @compressor         = compressor
      @protocol_version   = protocol_version
      @port               = port
      @connection_timeout = connection_timeout
      @credentials        = credentials
      @auth_provider      = auth_provider
    end

    def connect(address, protocol_version = @protocol_version)
      connector = Client::Connector.new([
        Client::ConnectStep.new(@io_reactor, protocol_handler_factory(protocol_version), @port, @connection_timeout, @logger),
        Client::CacheOptionsStep.new,
        Client::InitializeStep.new(@compressor, @logger),
        authentication_step(protocol_version),
        Client::CachePropertiesStep.new,
      ])

      f = connector.connect(address.to_s)
      f.fallback do |error|
        if error.is_a?(QueryError) && error.code == 0x0a && protocol_version > 1
          @logger.warn('Could not connect using protocol version %d (will try again with %d): %s' % [protocol_version, protocol_version - 1, error.message])
          connect(address, protocol_version - 1)
        else
          @logger.error('Connection failed: %s: %s' % [error.class.name, error.message])

          raise error
        end
      end
    end

    private

    def protocol_handler_factory(protocol_version)
      lambda { |connection| Protocol::CqlProtocolHandler.new(connection, @io_reactor, protocol_version, @compressor) }
    end

    def authentication_step(protocol_version)
      if protocol_version == 1
        Client::CredentialsAuthenticationStep.new(@credentials)
      else
        Client::SaslAuthenticationStep.new(@auth_provider)
      end
    end
  end
end
