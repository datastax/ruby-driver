# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

module Cassandra
  class Cluster
    # @private
    class Connector
      include MonitorMixin

      def initialize(logger,
                     io_reactor,
                     cluster_registry,
                     connection_options,
                     execution_options)
        @logger             = logger
        @reactor            = io_reactor
        @registry           = cluster_registry
        @connection_options = connection_options
        @execution_options  = execution_options
        @connections        = ::Hash.new
        @open_connections   = ::Hash.new

        mon_initialize
      end

      def connect(host)
        synchronize do
          open_connections = @open_connections[host]
          if open_connections
            connection = open_connections.shift
            @open_connections.delete(host) if open_connections.empty?
            return Ione::Future.resolved(connection)
          end
        end

        f = do_connect(host)

        f.on_failure do |error|
          connection_error(host, error)
        end

        f.on_value do |connection|
          connection.on_closed do |cause|
            disconnected(host, cause)
          end

          connected(host)
        end

        f
      end

      def refresh_status(host)
        if synchronize { @connections[host] }
          @registry.host_up(host.ip)

          return Future.resolved
        end

        @logger.debug("Checking if host #{host.ip} is up")
        f = do_connect(host)

        f.on_failure do |error|
          connection_error(host, error)
        end

        f.on_value do |connection|
          connection.on_closed do |cause|
            disconnected(host, cause)
          end

          synchronize do
            @open_connections[host] ||= []
            @open_connections[host] << connection
          end

          timer = @reactor.schedule_timer(UNCLAIMED_TIMEOUT)
          timer.on_value do
            close = false

            synchronize do
              open_connections = @open_connections[host]
              if open_connections
                close = !open_connections.delete(connection).nil?
                @open_connections.delete(host) if open_connections.empty?
              end
            end

            connection.close if close
          end

          connected(host)
        end

        f
      end

      private

      NO_CONNECTIONS    = Ione::Future.resolved([])
      UNCLAIMED_TIMEOUT = 5 # close unclaimed connections in five seconds

      def do_connect(host)
        @reactor.connect(host.ip.to_s,
                         @connection_options.port,
                         timeout: @connection_options.connect_timeout,
                         ssl: @connection_options.ssl) do |connection|
          raise Errors::ClientError, 'Not connected, reactor stopped' unless connection

          connection.to_io.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY,
                                      @connection_options.nodelay? ? 1 : 0)

          Protocol::CqlProtocolHandler.new(connection,
                                           @reactor,
                                           @connection_options.protocol_version,
                                           @connection_options.compressor,
                                           @connection_options.heartbeat_interval,
                                           @connection_options.idle_timeout,
                                           @connection_options.requests_per_connection,
                                           @connection_options.custom_type_handlers)
        end.flat_map do |connection|
          # connection is a CqlProtocolHandler
          f = request_options(connection)
          f = f.flat_map do |options|
            compression = @connection_options.compression
            supported_algorithms = options['COMPRESSION']

            if compression && !supported_algorithms.include?(compression)
              @logger.warn("Compression with #{compression.inspect} is not supported " \
                "by host at #{host.ip}, supported algorithms are " \
                "#{supported_algorithms.inspect}")
              compression = nil
            end

            supported_cql_versions = options['CQL_VERSION']
            cql_version = (supported_cql_versions && !supported_cql_versions.empty?) ?
                supported_cql_versions.first :
                '3.1.0'

            startup_connection(host, connection, cql_version, compression)
          end
          f.fallback do |error|
            case error
            when Errors::ProtocolError
              synchronize do
                current_version = connection.protocol_version
                if current_version > 1 && @connection_options.protocol_negotiable?
                  @logger.info("Host #{host.ip} doesn't support protocol version " \
                    "#{current_version}, downgrading")

                  # This is tricky. We want to try with the next lower protocol version.
                  # However, the connection_options used for all connections may have
                  # already been updated due to other node connection failures. So,
                  # it may already have a lower protocol-version than our current-1. We
                  # don't want to accidentally raise it, so we update it to the min
                  # of itself and current-1.
                  @connection_options.protocol_version =
                    [@connection_options.protocol_version, current_version - 1].min
                  do_connect(host)
                else
                  Ione::Future.failed(error)
                end
              end
            when Errors::TimeoutError
              future = Ione::CompletableFuture.new
              connection.close(error).on_complete do |_|
                future.fail(error)
              end
              future
            else
              Ione::Future.failed(error)
            end
          end
        end.fallback do |error|
          case error
          when Error
            Ione::Future.failed(error)
          else
            e = Errors::IOError.new(error.message)
            e.set_backtrace(error.backtrace)
            Ione::Future.failed(e)
          end
        end
      end

      def startup_connection(host, connection, cql_version, compression)
        connection.send_request(Protocol::StartupRequest.new(cql_version, compression),
                                @execution_options.timeout).flat_map do |r|
          case r
          when Protocol::AuthenticateResponse
            if @connection_options.protocol_version == 1
              credentials = @connection_options.credentials
              if credentials
                send_credentials(connection, credentials)
              else
                Ione::Future.failed(cannot_authenticate_error)
              end
            else
              authenticator = @connection_options.create_authenticator(r.authentication_class, host)
              if authenticator
                challenge_response_cycle(connection, authenticator, authenticator.initial_response)
              else
                Ione::Future.failed(cannot_authenticate_error)
              end
            end
          when Protocol::ReadyResponse
            ::Ione::Future.resolved(connection)
          when Protocol::ErrorResponse
            ::Ione::Future.failed(
              r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
            )
          else
            ::Ione::Future.failed(
              Errors::InternalError.new("Unexpected response #{r.inspect}")
            )
          end
        end
      end

      def cannot_authenticate_error
        Errors::AuthenticationError.new(
          'Server requested authentication, but client was not configured to ' \
            'authenticate',
          nil,
          nil,
          nil,
          VOID_STATEMENT,
          VOID_OPTIONS,
          EMPTY_LIST,
          :one,
          0
        )
      end

      def request_options(connection)
        connection.send_request(Protocol::OptionsRequest.new,
                                @execution_options.timeout).map do |r|
          case r
          when Protocol::SupportedResponse
            r.options
          when Protocol::ErrorResponse
            raise r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}"
          end
        end
      end

      def send_credentials(connection, credentials)
        connection.send_request(Protocol::CredentialsRequest.new(credentials),
                                @execution_options.timeout).map do |r|
          case r
          when Protocol::ReadyResponse
            connection
          when Protocol::ErrorResponse
            raise r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}"
          end
        end
      end

      def challenge_response_cycle(connection, authenticator, token)
        connection.send_request(Protocol::AuthResponseRequest.new(token),
                                @execution_options.timeout).flat_map do |r|
          case r
          when Protocol::AuthChallengeResponse
            token = authenticator.challenge_response(r.token)
            challenge_response_cycle(connection, authenticator, token)
          when Protocol::AuthSuccessResponse
            authenticator.authentication_successful(r.token)
            ::Ione::Future.resolved(connection)
          when Protocol::ErrorResponse
            ::Ione::Future.failed(
              r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
            )
          else
            ::Ione::Future.failed(
              Errors::InternalError.new("Unexpected response #{r.inspect}")
            )
          end
        end
      end

      def connected(host)
        notify = false

        synchronize do
          connections = @connections[host]

          if connections
            @connections[host] = connections + 1
          else
            notify = true

            @connections[host] = 1
          end
        end

        @registry.host_up(host.ip) if notify

        self
      end

      def disconnected(host, error)
        notify = false

        synchronize do
          connections = @connections[host]

          return self unless connections

          connections -= 1

          if connections == 0
            notify = !error.nil?
            @connections.delete(host)
          else
            @connections[host] = connections
          end
        end

        @logger.debug("Host #{host.ip} closed connection (#{error.class.name}: " \
          "#{error.message})") if error

        if notify
          @logger.warn("Host #{host.ip} closed all connections")
          @registry.host_down(host.ip)
        end

        self
      end

      def connection_error(host, error)
        notify = false

        synchronize do
          notify = !error.nil? && !@connections.key?(host)
        end

        @logger.debug("Host #{host.ip} refused connection (#{error.class.name}: " \
          "#{error.message})")

        if notify
          @logger.warn("Host #{host.ip} refused all connections")
          @registry.host_down(host.ip)
        end

        self
      end
    end
  end
end
