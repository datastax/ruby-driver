# encoding: utf-8

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

module Cql
  class Cluster
    # @private
    class Connector
      include MonitorMixin

      def initialize(logger, io_reactor, cluster_registry, connection_options)
        @logger               = logger
        @reactor              = io_reactor
        @registry             = cluster_registry
        @options              = connection_options
        @connections          = Hash.new
        @connections_to_close = Hash.new
        @close_futures        = Hash.new

        mon_initialize
      end

      def connect(host)
        synchronize do
          closing_connections = @connections_to_close[host]
          if closing_connections && !closing_connections.empty?
            connection = closing_connections.shift
            return Ione::Future.resolved(connection)
          end
        end

        @logger.info("Connecting ip=#{host.ip}")

        f = create_connector.connect(host.ip.to_s)

        f.on_failure do |error|
          @logger.warn("Connection failed ip=#{host.ip} error=#{error.message}")
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

      def connect_many(host, count)
        create_additional_connections(host, count, [])
      end

      def close(host, connection)
        synchronize do
          if @connections_to_close[host]
            @connections_to_close[host] << connection
            @close_futures[host]
          else
            @connections_to_close[host] = [connection]
            @close_futures[host] = @io_reactor.schedule_timer(0).flat_map do
              connections = nil

              synchronize do
                connections = @connections_to_close.delete(host)
                @close_futures.delete(host)
              end

              if connections && !connections.empty?
                connections.map!(&:close)
                Ione::Future.all(*connections).map(nil)
              else
                Ione::Future.resolved
              end
            end
          end
        end
      end

      private

      NO_CONNECTIONS = Ione::Future.resolved([])

      def create_connector
        authentication_step = @options.protocol_version == 1 ? Cql::Client::CredentialsAuthenticationStep.new(@options.credentials) : Cql::Client::SaslAuthenticationStep.new(@options.auth_provider)
        protocol_handler_factory = lambda { |connection| Protocol::CqlProtocolHandler.new(connection, @reactor, @options.protocol_version, @options.compressor) }

        Cql::Client::Connector.new([
          Cql::Client::ConnectStep.new(@reactor, protocol_handler_factory, @options.port, @options.connection_timeout, @logger),
          Cql::Client::CacheOptionsStep.new,
          Cql::Client::InitializeStep.new(@options.compressor, @logger),
          authentication_step,
          Cql::Client::CachePropertiesStep.new,
        ])
      end

      def create_additional_connections(host, count, established_connections, error = nil)
        futures = count.times.map do
          connect(host).recover do |e|
            Cql::Client::FailedConnection.new(e, host)
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
            notify = !error.nil? && !error.is_a?(Cql::Error)
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
          notify = !error.is_a?(Cql::Error) && !@connections.has_key?(host)
        end

        @registry.host_down(host.ip) if notify

        self
      end
    end
  end
end
