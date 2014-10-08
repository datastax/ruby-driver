# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
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

      def initialize(logger, io_reactor, cluster_registry, connection_options)
        @logger           = logger
        @reactor          = io_reactor
        @registry         = cluster_registry
        @options          = connection_options
        @connections      = ::Hash.new
        @open_connections = ::Hash.new

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

        @logger.info("Connecting ip=#{host.ip}")

        f = do_connect(host)

        f.on_failure do |error|
          @logger.warn("Connection failed ip=#{host.ip} error=\"#{error.class.name}: #{error.message}\"")
          connection_error(host, error)
        end

        f.on_value do |connection|
          connection.on_closed do |cause|
            message = "Disconnected ip=#{host.ip}"
            message << " error=#{cause.message}" if cause

            @logger.info(message)
            disconnected(host, cause)
          end

          @logger.info("Connected ip=#{host.ip}")
          connected(host)
        end

        f
      end

      def refresh_status(host)
        if synchronize { @connections[host] }
          @registry.host_up(host.ip)

          return Future.resolved
        end

        @logger.info("Refreshing host status ip=#{host.ip}")
        f = do_connect(host)

        f.on_failure do |error|
          @logger.info("Refreshed host status ip=#{host.ip}")
          @logger.warn("Connection failed ip=#{host.ip} error=\"#{error.class.name}: #{error.message}\"")
          connection_error(host, error)
        end

        f.on_value do |connection|
          @logger.info("Refreshed host status ip=#{host.ip}")
          connection.on_closed do |cause|
            message = "Disconnected ip=#{host.ip}"
            message << " error=#{cause.message}" if cause

            @logger.info(message)
            disconnected(host, cause)
          end

          synchronize do
            @open_connections[host] ||= []
            @open_connections[host]  << connection
          end

          @logger.info("Connected ip=#{host.ip}")
          connected(host)
        end

        f
      end

      def connect_many(host, count)
        create_additional_connections(host, count, [])
      end

      private

      NO_CONNECTIONS = Ione::Future.resolved([])

      def do_connect(host)
        create_connector.connect(host.ip.to_s).fallback do |error|
          if error.is_a?(Errors::QueryError) && error.code == 0x0a
            synchronize do
              if @options.protocol_version > 1
                @logger.warn('Could not connect using protocol version %d (will try again with %d): %s' % [@options.protocol_version, @options.protocol_version - 1, error.message])
                @options.protocol_version -= 1
                do_connect(host)
              else
                Ione::Future.failed(error)
              end
            end
          else
            Ione::Future.failed(error)
          end
        end
      end

      def create_connector
        authentication_step = @options.protocol_version == 1 ? Cassandra::Client::CredentialsAuthenticationStep.new(@options.credentials) : Cassandra::Client::SaslAuthenticationStep.new(@options.auth_provider)
        protocol_handler_factory = lambda do |connection|
          raise "no connection given" unless connection
          Protocol::CqlProtocolHandler.new(connection, @reactor, @options.protocol_version, @options.compressor, @options.heartbeat_interval, @options.idle_timeout)
        end

        Cassandra::Client::Connector.new([
          Cassandra::Client::ConnectStep.new(
            @reactor,
            protocol_handler_factory,
            @options.port,
            {:timeout => @options.connect_timeout, :ssl => @options.ssl},
            @logger
          ),
          Cassandra::Client::CacheOptionsStep.new(@options.connect_timeout),
          Cassandra::Client::InitializeStep.new(@options.compressor, @logger),
          authentication_step,
          Cassandra::Client::CachePropertiesStep.new,
        ])
      end

      def create_additional_connections(host, count, established_connections, error = nil)
        futures = count.times.map do
          connect(host).recover do |e|
            Cassandra::Client::FailedConnection.new(e, host)
          end
        end

        Ione::Future.all(*futures).flat_map do |connections|
          established_connections.select!(&:connected?)

          connections.each do |connection|
            if connection.connected?
              established_connections << connection
            else
              error = connection.error
            end
          end

          if !established_connections.empty?
            connections_left = count - established_connections.size
            if connections_left == 0
              Ione::Future.resolved(established_connections)
            else
              create_additional_connections(host, connections_left, established_connections, error)
            end
          else
            Ione::Future.failed(error)
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
            notify = !error.nil? && !error.is_a?(Cassandra::Error)
            @connections.delete(host)
          else
            @connections[host] = connections
          end
        end

        @registry.host_down(host.ip) if notify

        self
      end

      def connection_error(host, error)
        notify = false

        synchronize do
          notify = !error.is_a?(Cassandra::Error) && !@connections.has_key?(host)
        end

        @registry.host_down(host.ip) if notify

        self
      end
    end
  end
end
