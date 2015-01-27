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

      def initialize(logger, io_reactor, cluster_registry, cluster_schema, cluster_metadata, load_balancing_policy, reconnection_policy, address_resolution_policy, connector, connection_options)
        @logger                = logger
        @io_reactor            = io_reactor
        @registry              = cluster_registry
        @schema                = cluster_schema
        @metadata              = cluster_metadata
        @load_balancing_policy = load_balancing_policy
        @reconnection_policy   = reconnection_policy
        @address_resolver      = address_resolution_policy
        @connector             = connector
        @connection_options    = connection_options
        @refreshing_statuses   = ::Hash.new(false)
        @status                = :closed
        @refreshing_hosts      = false
        @refreshing_host       = ::Hash.new(false)
        @closed_promise        = Ione::Promise.new
        @schema_changes        = ::Array.new
        @schema_refresh_timer  = nil
        @schema_refresh_window = nil

        mon_initialize
      end

      def on_close(&block)
        @closed_promise.future.on_value(&block)
        @closed_promise.future.on_failure(&block)
      end

      def connect_async
        synchronize do
          return Ione::Future.resolved if @status == :connecting || @status == :connected
          @status = :connecting
        end

        f = @io_reactor.start.flat_map do
          plan = @load_balancing_policy.plan(nil, VOID_STATEMENT, VOID_OPTIONS)
          connect_to_first_available(plan)
        end
        f.on_failure do |e|
          close_async
        end
        f
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

          unless @connection || (@status == :closing || @status == :closed) || @load_balancing_policy.distance(host) == :ignore
            return connect_to_first_available(@load_balancing_policy.plan(nil, VOID_STATEMENT, VOID_OPTIONS))
          end
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
            refresh_host_status_with_retry(timer, host, schedule)
          end
        end

        nil
      end

      def close_async
        synchronize do
          return @closed_promise.future if @status == :closing || @status == :closed
          @status = :closing
        end
        f = @io_reactor.stop

        f.on_value(&method(:connection_closed))
        f.on_failure(&method(:connection_closed))

        @closed_promise.future
      end

      def connection_closed(cause)
        @closed_promise.fulfill
      end

      def refresh_schema_async_maybe_retry
        refresh_schema_async.fallback do |e|
          case e
          when Errors::HostError
            refresh_schema_async_retry(e, @reconnection_policy.schedule)
          else
            connection = @connection
            connection && connection.close(e)

            Ione::Future.failed(e)
          end
        end
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end

      private

      SELECT_LOCAL     = Protocol::QueryRequest.new('SELECT rack, data_center, host_id, release_version, tokens, partitioner FROM system.local', EMPTY_LIST, EMPTY_LIST, :one)
      SELECT_PEERS     = Protocol::QueryRequest.new('SELECT peer, rack, data_center, host_id, rpc_address, release_version, tokens FROM system.peers', EMPTY_LIST, EMPTY_LIST, :one)
      SELECT_KEYSPACES = Protocol::QueryRequest.new('SELECT * FROM system.schema_keyspaces', EMPTY_LIST, EMPTY_LIST, :one)
      SELECT_TABLES    = Protocol::QueryRequest.new('SELECT * FROM system.schema_columnfamilies', EMPTY_LIST, EMPTY_LIST, :one)
      SELECT_COLUMNS   = Protocol::QueryRequest.new('SELECT * FROM system.schema_columns', EMPTY_LIST, EMPTY_LIST, :one)
      SELECT_TYPES     = Protocol::QueryRequest.new('SELECT * FROM system.schema_usertypes', EMPTY_LIST, EMPTY_LIST, :one)

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

        request = Protocol::RegisterRequest.new(
          Protocol::TopologyChangeEventResponse::TYPE,
          Protocol::StatusChangeEventResponse::TYPE
        )

        request.events << Protocol::SchemaChangeEventResponse::TYPE if @connection_options.synchronize_schema?

        f = connection.send_request(request)
        f = f.map do |r|
          case r
          when Protocol::ReadyResponse
            nil
          when Protocol::ErrorResponse
            raise r.to_error(VOID_STATEMENT)
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}"
          end
        end
        f = f.map do
          connection.on_event do |event|
            @logger.debug("Event received #{event}")

            if event.type == 'SCHEMA_CHANGE'
              handle_schema_change(event)
            else
              case event.change
              when 'UP'
                address = event.address

                refresh_host_async_maybe_retry(address) if @registry.has_host?(address)
              when 'DOWN'
                @registry.host_down(event.address)
              when 'NEW_NODE'
                address = event.address

                unless @registry.has_host?(address)
                  refresh_host_async_maybe_retry(address)
                  refresh_schema_async_maybe_retry
                end
              when 'REMOVED_NODE'
                @registry.host_lost(event.address)
                refresh_schema_async_maybe_retry
              end
            end
          end

          self
        end
      end

      def refresh_schema_async
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        keyspaces = send_select_request(connection, SELECT_KEYSPACES)
        tables    = send_select_request(connection, SELECT_TABLES)
        columns   = send_select_request(connection, SELECT_COLUMNS)

        if @connection_options.protocol_version > 2
          types = send_select_request(connection, SELECT_TYPES)
        else
          types = Ione::Future.resolved(EMPTY_LIST)
        end

        Ione::Future.all(keyspaces, tables, columns, types).map do |(keyspaces, tables, columns, types)|
          host = @registry.host(connection.host)

          @schema.update_keyspaces(host, keyspaces, tables, columns, types)
          @metadata.rebuild_token_map
        end
      end

      def refresh_schema_async_retry(error, schedule)
        timeout = schedule.next
        @logger.info("Failed to refresh schema (#{error.class.name}: #{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          refresh_schema_async.fallback do |e|
            case e
            when Errors::HostError
              refresh_schema_async_retry(e, schedule)
            else
              connection = @connection
              connection && connection.close(e)

              Ione::Future.failed(e)
            end
          end
        end
      end

      def refresh_keyspace_async_maybe_retry(keyspace)
        refresh_keyspace_async(keyspace).fallback do |e|
          case e
          when Errors::HostError
            refresh_keyspace_async_retry(keyspace, e, @reconnection_policy.schedule)
          else
            connection = @connection
            connection && connection.close(e)

            Ione::Future.failed(e)
          end
        end
      end

      def refresh_keyspace_async_retry(keyspace, error, schedule)
        timeout = schedule.next
        @logger.info("Failed to refresh keyspace #{keyspace} (#{error.class.name}: #{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          refresh_keyspace_async(keyspace).fallback do |e|
            case e
            when Errors::HostError
              refresh_keyspace_async_retry(keyspace, e, schedule)
            else
              connection = @connection
              connection && connection.close(e)

              Ione::Future.failed(e)
            end
          end
        end
      end

      def refresh_keyspace_async(keyspace)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        keyspaces = send_select_request(connection, Protocol::QueryRequest.new("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '%s'" % keyspace, EMPTY_LIST, EMPTY_LIST, :one))
        tables    = send_select_request(connection, Protocol::QueryRequest.new("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = '%s'" % keyspace, EMPTY_LIST, EMPTY_LIST, :one))
        columns   = send_select_request(connection, Protocol::QueryRequest.new("SELECT * FROM system.schema_columns WHERE keyspace_name = '%s'" % keyspace, EMPTY_LIST, EMPTY_LIST, :one))

        if @connection_options.protocol_version > 2
          types = send_select_request(connection, Protocol::QueryRequest.new("SELECT * FROM system.schema_usertypes WHERE keyspace_name = '%s'" % keyspace, EMPTY_LIST, EMPTY_LIST, :one))
        else
          types = Ione::Future.resolved(EMPTY_LIST)
        end

        Ione::Future.all(keyspaces, tables, columns, types).map do |(keyspaces, tables, columns, types)|
          host = @registry.host(connection.host)

          if keyspaces.empty?
            @schema.delete_keyspace(keyspace)
          else
            @schema.update_keyspace(host, keyspaces.first, tables, columns, types)
          end
        end
      end

      def refresh_table_async_maybe_retry(keyspace, table)
        refresh_table_async(keyspace, table).fallback do |e|
          case e
          when Errors::HostError
            refresh_keyspace_async_retry(keyspace, e, @reconnection_policy.schedule)
          else
            connection = @connection
            connection && connection.close(e)

            Ione::Future.failed(e)
          end
        end
      end

      def refresh_table_async_retry(keyspace, table, error, schedule)
        timeout = schedule.next
        @logger.info("Failed to refresh keyspace #{keyspace} (#{error.class.name}: #{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          refresh_keyspace_async(keyspace).fallback do |e|
            case e
            when Errors::HostError
              refresh_keyspace_async_retry(keyspace, e, schedule)
            else
              connection = @connection
              connection && connection.close(e)

              Ione::Future.failed(e)
            end
          end
        end
      end

      def refresh_table_async(keyspace, table)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        params   = [keyspace, table]
        tables   = send_select_request(connection, Protocol::QueryRequest.new("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = '%s' AND columnfamily_name = '%s'" % params, EMPTY_LIST, EMPTY_LIST, :one))
        columns  = send_select_request(connection, Protocol::QueryRequest.new("SELECT * FROM system.schema_columns WHERE keyspace_name = '%s' AND columnfamily_name = '%s'" % params, EMPTY_LIST, EMPTY_LIST, :one))

        Ione::Future.all(tables, columns).map do |(tables, columns)|
          host = @registry.host(connection.host)

          if tables.empty?
            @schema.delete_table(keyspace, table)
          else
            @schema.udpate_table(host, keyspace, tables.first, columns)
          end
        end
      end

      def refresh_type_async_maybe_retry(keyspace, type)
        refresh_type_async(keyspace, type).fallback do |e|
          case e
          when Errors::HostError
            refresh_keyspace_async_retry(keyspace, e, @reconnection_policy.schedule)
          else
            connection = @connection
            connection && connection.close(e)

            Ione::Future.failed(e)
          end
        end
      end

      def refresh_type_async_retry(keyspace, type, error, schedule)
        timeout = schedule.next
        @logger.info("Failed to refresh type #{keyspace}.#{type} (#{error.class.name}: #{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          refresh_keyspace_async(keyspace).fallback do |e|
            case e
            when Errors::HostError
              refresh_keyspace_async_retry(keyspace, e, schedule)
            else
              connection = @connection
              connection && connection.close(e)

              Ione::Future.failed(e)
            end
          end
        end
      end

      def refresh_type_async(keyspace, type)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        params   = [keyspace, type]
        types    = send_select_request(connection, Protocol::QueryRequest.new("SELECT * FROM system.schema_usertypes WHERE keyspace_name = '%s' AND type_name = '%s'" % params, EMPTY_LIST, EMPTY_LIST, :one))

        types.map do |types|
          host = @registry.host(connection.host)

          if types.empty?
            @schema.delete_type(keyspace, type)
          else
            @schema.udpate_type(host, keyspace, types.first)
          end
        end
      end

      def refresh_hosts_async_maybe_retry
        synchronize do
          return Ione::Future.resolved if @refreshing_hosts
          @refreshing_hosts = true
        end

        refresh_hosts_async.fallback do |e|
          case e
          when Errors::HostError
            refresh_hosts_async_retry(e, @reconnection_policy.schedule)
          else
            connection = @connection
            connection && connection.close(e)

            Ione::Future.failed(e)
          end
        end.map do
          synchronize do
            @refreshing_hosts = false
          end
        end
      end

      def refresh_hosts_async_retry(error, schedule)
        timeout = schedule.next
        @logger.info("Failed to refresh hosts (#{error.class.name}: #{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          refresh_hosts_async.fallback do |e|
            case e
            when Errors::HostError
              refresh_hosts_async_retry(e, schedule)
            else
              connection = @connection
              connection && connection.close(e)

              Ione::Future.failed(e)
            end
          end
        end
      end

      def refresh_hosts_async
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        local = send_select_request(connection, SELECT_LOCAL)
        peers = send_select_request(connection, SELECT_PEERS)

        Ione::Future.all(local, peers).map do |(local, peers)|
          @logger.debug("#{peers.size} peer(s) found")

          ips = ::Set.new

          unless local.empty?
            ips << ip = IPAddr.new(connection.host)
            data = local.first
            @registry.host_found(ip, data)
            @metadata.update(data)
          end

          peers.shuffle!
          peers.each do |data|
            ip = peer_ip(data)
            next unless ip
            ips << ip
            @registry.host_found(ip, data)
          end

          @registry.each_host do |host|
            @registry.host_lost(host.ip) unless ips.include?(host.ip)
          end

          nil
        end
      end

      def refresh_host_status(host)
        @connector.refresh_status(host)
      end

      def refresh_host_status_with_retry(original_timer, host, schedule)
        timer = nil

        synchronize do
          timer = @refreshing_statuses[host]

          # host must have been lost/up or timer was rescheduled
          return Ione::Future.resolved if timer.nil? || timer != original_timer

          timeout = schedule.next

          @logger.debug("Checking host #{host.ip} in #{timeout} seconds")

          @refreshing_statuses[host] = timer = @io_reactor.schedule_timer(timeout)
        end

        timer.on_value do
          refresh_host_status(host).fallback do |e|
            refresh_host_status_with_retry(timer, host, schedule)
          end
        end
      end

      def refresh_host_async_maybe_retry(host)
        synchronize do
          return Ione::Future.resolved if @refreshing_hosts || @refreshing_host[host]
          @refreshing_host[host] = true
        end

        refresh_host_async(host).fallback do |e|
          case e
          when Errors::HostError
            refresh_host_async_retry(host, e, @reconnection_policy.schedule)
          else
            connection = @connection
            connection && connection.close(e)

            Ione::Future.failed(e)
          end
        end.map do
          synchronize do
            @refreshing_host.delete(host)
          end
        end
      end

      def refresh_host_async_retry(host, error, schedule)
        timeout = schedule.next
        @logger.info("Failed to refresh host #{host.ip.to_s} (#{error.class.name}: #{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          refresh_host_async(host).fallback do |e|
            case e
            when Errors::HostError
              refresh_host_async_retry(host, e, schedule)
            else
              connection = @connection
              connection && connection.close(e)

              Ione::Future.failed(e)
            end
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
          request = Protocol::QueryRequest.new("SELECT rack, data_center, host_id, rpc_address, release_version, tokens FROM system.peers WHERE peer = '%s'" % address, EMPTY_LIST, EMPTY_LIST, :one)
        end

        send_select_request(connection, request).map do |rows|
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

            connection.on_closed do |cause|
              reconnect = false

              synchronize do
                if connection == @connection
                  if @status == :closing
                    @status = :closed
                  else
                    @status = :reconnecting
                    reconnect = true
                  end

                  if cause
                    @logger.info("Control connection closed (#{cause.class.name}: #{cause.message})")
                  else
                    @logger.info("Control connection closed")
                  end

                  @connection = nil
                end
              end

              reconnect_async(@reconnection_policy.schedule) if reconnect
            end
          end

          register_async
        end
        f = f.flat_map { refresh_hosts_async_maybe_retry }
        f = f.flat_map { refresh_schema_async_maybe_retry }
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

        @address_resolver.resolve(ip)
      end

      def process_schema_changes(schema_changes)
        refresh_keyspaces = ::Hash.new
        refresh_tables    = ::Hash.new
        refresh_types     = ::Hash.new

        schema_changes.each do |change|
          keyspace = change.keyspace

          next if refresh_keyspaces.has_key?(keyspace)

          case change.target
          when Protocol::Constants::SCHEMA_CHANGE_TARGET_KEYSPACE
            refresh_tables.delete(keyspace)
            refresh_types.delete(keyspace)
            refresh_keyspaces[keyspace] = true
          when Protocol::Constants::SCHEMA_CHANGE_TARGET_TABLE
            tables = refresh_tables[keyspace] ||= ::Hash.new
            tables[change.table] = true
          when Protocol::Constants::SCHEMA_CHANGE_TARGET_UDT
            types = refresh_types[keyspace] ||= ::Hash.new
            types[change.type_name] = true
          end
        end

        futures = ::Array.new

        refresh_keyspaces.each_key do |keyspace|
          futures << refresh_keyspace_async_maybe_retry(keyspace)
        end

        refresh_tables.each do |(keyspace, tables)|
          tables.each_key do |table|
            futures << refresh_table_async_maybe_retry(keyspace, table)
          end
        end

        refresh_types.each do |(keyspace, types)|
          types.each_key do |type|
            futures << refresh_type_async_maybe_retry(keyspace, type)
          end
        end

        Ione::Future.all(*futures)
      end

      def handle_schema_change(change)
        timer = nil
        expiration_timer = nil

        synchronize do
          @schema_changes << change

          @io_reactor.cancel_timer(@schema_refresh_timer) if @schema_refresh_timer
          timer = @schema_refresh_timer = @io_reactor.schedule_timer(@connection_options.schema_refresh_delay)

          unless @schema_refresh_window
            expiration_timer = @schema_refresh_window = @io_reactor.schedule_timer(@connection_options.schema_refresh_timeout)
          end
        end

        if expiration_timer
          expiration_timer.on_value do
            schema_changes = nil

            synchronize do
              @io_reactor.cancel_timer(@schema_refresh_timer)

              @schema_refresh_window = nil
              @schema_refresh_timer  = nil

              schema_changes  = @schema_changes
              @schema_changes = ::Array.new
            end

            process_schema_changes(schema_changes)
          end
        end

        timer.on_value do
          schema_changes = nil

          synchronize do
            @io_reactor.cancel_timer(@schema_refresh_window)

            @schema_refresh_window = nil
            @schema_refresh_timer  = nil

            schema_changes  = @schema_changes
            @schema_changes = ::Array.new
          end

          process_schema_changes(schema_changes)
        end

        nil
      end

      def send_select_request(connection, request)
        connection.send_request(request).map do |r|
          case r
          when Protocol::RowsResultResponse
            r.rows
          when Protocol::ErrorResponse
            raise r.to_error(VOID_STATEMENT)
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}"
          end
        end
      end
    end
  end
end
