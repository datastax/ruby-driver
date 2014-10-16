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
    class ControlConnection
      include MonitorMixin

      def initialize(logger, io_reactor, cluster_registry, cluster_schema, cluster_metadata, load_balancing_policy, reconnection_policy, connector)
        @logger                = logger
        @io_reactor            = io_reactor
        @registry              = cluster_registry
        @schema                = cluster_schema
        @metadata              = cluster_metadata
        @load_balancing_policy = load_balancing_policy
        @reconnection_policy   = reconnection_policy
        @connector             = connector
        @refreshing_statuses   = Hash.new(false)
        @status                = :closed

        mon_initialize
      end

      def connect_async
        synchronize do
          return Ione::Future.resolved if @status == :connecting || @status == :connected
          @status = :connecting
        end

        @io_reactor.start.flat_map do
          plan = @load_balancing_policy.plan(nil, VOID_STATEMENT, VOID_OPTIONS)
          connect_to_first_available(plan)
        end
      end

      def host_found(host)
      end

      def host_lost(host)
        synchronize do
          timer = @refreshing_statuses.delete(host)
          @io_reactor.cancel_timer(timer) if timer
        end

        nil
      end

      def host_up(host)
        synchronize do
          timer = @refreshing_statuses.delete(host)
          @io_reactor.cancel_timer(timer) if timer

          return connect_async unless @connection || (@status == :closing || @status == :closed) || @load_balancing_policy.distance(host) == :ignore
        end

        Ione::Future.resolved
      end

      def host_down(host)
        schedule = nil
        timer    = nil 

        synchronize do
          return Ione::Future.resolved if @refreshing_statuses[host] || @load_balancing_policy.distance(host) == :ignore

          schedule = @reconnection_policy.schedule
          timeout  = schedule.next

          @logger.debug("Starting to continuously refresh status of #{host.ip} in #{timeout} seconds")

          @refreshing_statuses[host] = timer = @io_reactor.schedule_timer(timeout)
        end

        timer.on_value do
          refresh_host_status(host).fallback do |e|
            refresh_host_status_with_retry(host, schedule)
          end
        end

        nil
      end

      def close_async
        synchronize do
          return Ione::Future.resolved if @status == :closing || @status == :closed
          @status = :closing
        end
        @io_reactor.stop
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end

      private

      SELECT_LOCAL     = Protocol::QueryRequest.new('SELECT rack, data_center, host_id, release_version, tokens, partitioner FROM system.local', nil, nil, :one)
      SELECT_PEERS     = Protocol::QueryRequest.new('SELECT peer, rack, data_center, host_id, rpc_address, release_version, tokens FROM system.peers', nil, nil, :one)
      SELECT_KEYSPACES = Protocol::QueryRequest.new('SELECT * FROM system.schema_keyspaces', nil, nil, :one)
      SELECT_TABLES    = Protocol::QueryRequest.new('SELECT * FROM system.schema_columnfamilies', nil, nil, :one)
      SELECT_COLUMNS   = Protocol::QueryRequest.new('SELECT * FROM system.schema_columns', nil, nil, :one)
      REGISTER         = Protocol::RegisterRequest.new(
                           Protocol::TopologyChangeEventResponse::TYPE,
                           Protocol::StatusChangeEventResponse::TYPE,
                           Protocol::SchemaChangeEventResponse::TYPE
                         )

      def reconnect_async(schedule)
        timeout = schedule.next

        @logger.debug("Reestablishing control connection in #{timeout} seconds")

        f = @io_reactor.schedule_timer(timeout)
        f = f.flat_map do
          if synchronize { @status == :reconnecting }
            plan = @load_balancing_policy.plan(nil, VOID_STATEMENT, VOID_OPTIONS)
            connect_to_first_available(plan)
          else
            Ione::Future.resolved
          end
        end
        f.fallback do |e|
          @logger.error("Control connection failed (#{e.class.name}: #{e.message})")
          @logger.debug(Array(e.backtrace).join("\n"))

          if synchronize { @status == :reconnecting }
            reconnect_async(schedule)
          else
            return Ione::Future.resolved
          end
        end
      end

      def register_async
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        connection.send_request(REGISTER).map do

          connection.on_event do |event|
            @logger.debug("Event received #{event}")

            if event.type == 'SCHEMA_CHANGE'
              case event.change
              when 'CREATED'
                if event.table.empty?
                  refresh_schema_async
                else
                  refresh_keyspace_async(event.keyspace)
                end
              when 'DROPPED'
                if event.table.empty?
                  refresh_schema_async
                else
                  refresh_keyspace_async(event.keyspace)
                end
              when 'UPDATED'
                if event.table.empty?
                  refresh_keyspace_async(event.keyspace)
                else
                  refresh_table_async(event.keyspace, event.table)
                end
              end
            else
              case event.change
              when 'UP'
                address = event.address

                refresh_host_async(address) if @registry.has_host?(address)
              when 'DOWN'
                @registry.host_down(event.address)
              when 'NEW_NODE'
                address = event.address

                unless @registry.has_host?(address)
                  refresh_host_async(address)
                  refresh_schema_async
                end
              when 'REMOVED_NODE'
                @registry.host_lost(event.address)
                refresh_schema_async
              end
            end
          end

          self
        end
      end

      def refresh_schema_async
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        keyspaces = connection.send_request(SELECT_KEYSPACES)
        tables    = connection.send_request(SELECT_TABLES)
        columns   = connection.send_request(SELECT_COLUMNS)

        Ione::Future.all(keyspaces, tables, columns).map do |(keyspaces, tables, columns)|
          host = @registry.host(connection.host)

          @schema.update_keyspaces(host, keyspaces.rows, tables.rows, columns.rows)
          @metadata.rebuild_token_map
        end
      end

      def refresh_keyspace_async(keyspace)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        params    = [keyspace]
        keyspaces = connection.send_request(Protocol::QueryRequest.new("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?", params, nil, :one))
        tables    = connection.send_request(Protocol::QueryRequest.new("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ?", params, nil, :one))
        columns   = connection.send_request(Protocol::QueryRequest.new("SELECT * FROM system.schema_columns WHERE keyspace_name = ?", params, nil, :one))

        Ione::Future.all(keyspaces, tables, columns).map do |(keyspaces, tables, columns)|
          host = @registry.host(connection.host)

          @schema.update_keyspace(host, keyspaces.rows.first, tables.rows, columns.rows)
        end
      end

      def refresh_table_async(keyspace, table)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        params   = [keyspace, table]
        table    = connection.send_request(Protocol::QueryRequest.new("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ? AND columnfamily_name = ?", params, nil, :one))
        columns  = connection.send_request(Protocol::QueryRequest.new("SELECT * FROM system.schema_columns WHERE keyspace_name = ? AND columnfamily_name = ?", params, nil, :one))

        Ione::Future.all(table, columns).map do |(table, columns)|
          host = @registry.host(connection.host)

          @schema.udpate_table(host, keyspace, table.rows, columns.rows)
        end
      end

      def refresh_hosts_async
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        local = connection.send_request(SELECT_LOCAL)
        peers = connection.send_request(SELECT_PEERS)

        Ione::Future.all(local, peers).map do |(local, peers)|
          local = local.rows
          peers = peers.rows

          @logger.debug("#{peers.size} peer(s) found")

          raise NO_HOSTS if local.empty? && peers.empty?

          ips = ::Set.new

          ips << ip = IPAddr.new(connection.host)
          data = local.first
          @registry.host_found(ip, data)

          peers.each do |data|
            ip = peer_ip(data)
            next unless ip
            ips << ip
            @registry.host_found(ip, data)
          end

          @registry.each_host do |host|
            @registry.host_lost(host.ip) unless ips.include?(host.ip)
          end

          @metadata.update(data)

          nil
        end
      end

      def refresh_host_status(host)
        @connector.refresh_status(host)
      end

      def refresh_host_status_with_retry(host, schedule)
        timer = nil 

        synchronize do
          timeout = schedule.next

          @logger.debug("Checking host #{host.ip} in #{timeout} seconds")

          @refreshing_statuses[host] = timer = @io_reactor.schedule_timer(timeout)
        end

        timer.on_value do
          refresh_host_status(host).fallback do |e|
            refresh_host_status_with_retry(host, schedule)
          end
        end
      end

      def refresh_host_async(address)
        connection = @connection
        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        ip = address.to_s

        if ip == connection.host
          request = SELECT_LOCAL
        else
          request = Protocol::QueryRequest.new('SELECT rack, data_center, host_id, rpc_address, release_version, tokens FROM system.peers WHERE peer = ?', [address], nil, :one)
        end

        connection.send_request(request).map do |response|
          rows = response.rows

          unless rows.empty?
            @registry.host_found(address, rows.first)
          end

          self
        end
      end

      def connect_to_first_available(plan, errors = nil)
        unless plan.has_next?
          if errors.nil? && synchronize { @refreshing_statuses.empty? }
            @logger.fatal(<<-MSG)
Control connection failed and is unlikely to recover.

	This usually means that all hosts are ignored by current load
	balancing policy, most likely because they changed datacenters.
	Reconnections attempts will continue getting scheduled to
	repeat this message in the logs.
            MSG
          end

          return Ione::Future.failed(Errors::NoHostsAvailable.new(errors))
        end

        host = plan.next
        @logger.debug("Connecting to #{host.ip}")

        f = connect_to_host(host)
        f = f.flat_map do |connection|
          synchronize do
            @status = :connected

            @connection = connection

            connection.on_closed do
              reconnect = false

              synchronize do
                if @status == :closing
                  @status = :closed
                else
                  @status = :reconnecting
                  reconnect = true
                end

                @logger.info("Control connection closed")
                @connection = nil
              end

              reconnect_async(@reconnection_policy.schedule) if reconnect
            end
          end

          register_async
        end
        f = f.flat_map { refresh_hosts_async }
        f = f.flat_map { refresh_schema_async }
        f = f.fallback do |error|
          @logger.debug("Connection to #{host.ip} failed (#{error.class.name}: #{error.message})")

          case error
          when Errors::HostError
            errors ||= {}
            errors[host] = error
            connect_to_first_available(plan, errors)
          else
            Ione::Future.failed(error)
          end
        end

        f.on_complete do |f|
          @logger.info('Control connection established') if f.resolved?
        end

        f
      end

      def connect_to_host(host)
        @connector.connect(host)
      end

      def peer_ip(data)
        ip = data['rpc_address']
        ip = data['peer'] if ip == '0.0.0.0'
        ip
      end
    end
  end
end
