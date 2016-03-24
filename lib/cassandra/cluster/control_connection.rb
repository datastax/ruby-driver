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
    class ControlConnection
      include MonitorMixin

      def initialize(logger, io_reactor, cluster_registry, cluster_schema,
                     cluster_metadata, load_balancing_policy,
                     reconnection_policy, address_resolution_policy, connector,
                     connection_options, schema_fetcher)
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
        @schema_fetcher        = schema_fetcher
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

          unless @connection ||
                 (@status == :closing || @status == :closed) ||
                 @load_balancing_policy.distance(host) == :ignore
            return connect_to_first_available(
              @load_balancing_policy.plan(nil, VOID_STATEMENT, VOID_OPTIONS))
          end
        end

        Ione::Future.resolved
      end

      def host_down(host)
        schedule = nil
        timer    = nil

        synchronize do
          if @refreshing_statuses[host] ||
             @load_balancing_policy.distance(host) == :ignore
            return Ione::Future.resolved
          end

          schedule = @reconnection_policy.schedule
          timeout  = schedule.next

          @logger.debug("Starting to continuously refresh status of #{host.ip} in " \
            "#{timeout} seconds")

          @refreshing_statuses[host] = timer = @io_reactor.schedule_timer(timeout)
        end

        timer.on_value do
          refresh_host_status(host).fallback do |_e|
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

      def inspect
        "#<#{self.class.name}:0x#{object_id.to_s(16)}>"
      end

      private

      SELECT_LOCAL  = Protocol::QueryRequest.new(
        'SELECT * ' \
          'FROM system.local',
        EMPTY_LIST,
        EMPTY_LIST,
        :one)
      SELECT_PEERS  = Protocol::QueryRequest.new(
        'SELECT * ' \
          'FROM system.peers',
        EMPTY_LIST,
        EMPTY_LIST,
        :one)

      SELECT_PEER_QUERY =
        'SELECT * ' \
        'FROM system.peers ' \
        "WHERE peer = '%s'".freeze

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

          return Ione::Future.resolved unless synchronize { @status == :reconnecting }

          # We're reconnecting...
          reconnect_async(schedule)
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
            raise r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}"
          end
        end
        f.map do
          connection.on_event do |event|
            @logger.debug("Event received #{event}")

            if event.type == 'SCHEMA_CHANGE'
              handle_schema_change(event)
            else
              case event.change
              when 'UP'
                address = event.address

                if @registry.has_host?(address)
                  @registry.host_up(address)
                else
                  refresh_host_async_maybe_retry(address)
                  refresh_schema_async_wrapper
                end
              when 'DOWN'
                @registry.host_down(event.address)
              when 'NEW_NODE'
                address = event.address

                unless @registry.has_host?(address)
                  refresh_host_async_maybe_retry(address)
                  refresh_schema_async_wrapper
                end
              when 'REMOVED_NODE'
                @registry.host_lost(event.address)
                refresh_schema_async_wrapper
              end
            end
          end

          self
        end
      end

      def refresh_schema_async
        connection = @connection

        @logger.info('Refreshing schema')

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @schema_fetcher.fetch(connection).map do |keyspaces|
          @schema.replace(keyspaces)
          @metadata.rebuild_token_map
          @logger.info('Schema refreshed')
        end
      end

      def refresh_keyspace_async(keyspace_name)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @logger.info("Refreshing keyspace \"#{keyspace_name}\"")

        @schema_fetcher.fetch_keyspace(connection, keyspace_name).map do |keyspace|
          if keyspace
            @schema.replace_keyspace(keyspace)
          else
            @schema.delete_keyspace(keyspace_name)
          end

          @logger.info("Completed refreshing keyspace \"#{keyspace_name}\"")
        end
      end

      def refresh_table_async(keyspace_name, table_name)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @logger.info("Refreshing table \"#{keyspace_name}.#{table_name}\"")

        @schema_fetcher.fetch_table(connection, keyspace_name, table_name).map do |table|
          if table
            @schema.replace_table(table)
          else
            @schema.delete_table(keyspace_name, table_name)
          end

          @logger.info("Completed refreshing table \"#{keyspace_name}.#{table_name}\"")
        end
      end

      def refresh_materialized_view_async(keyspace_name, view_name)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @logger.info("Refreshing materialized view \"#{keyspace_name}.#{view_name}\"")

        @schema_fetcher.fetch_materialized_view(connection, keyspace_name, view_name).map do |view|
          if view
            @schema.replace_materialized_view(view)
          else
            @schema.delete_materialized_view(keyspace_name, view_name)
          end

          @logger.info("Completed refreshing materialized view \"#{keyspace_name}.#{view_name}\"")
        end
      end

      def refresh_type_async(keyspace_name, type_name)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @logger.info("Refreshing user-defined type \"#{keyspace_name}.#{type_name}\"")

        @schema_fetcher.fetch_type(connection, keyspace_name, type_name).map do |type|
          if type
            @schema.replace_type(type)
          else
            @schema.delete_type(keyspace_name, type_name)
          end

          @logger.info("Completed refreshing user-defined type \"#{keyspace_name}.#{type_name}\"")
        end
      end

      def refresh_function_async(keyspace_name, function_name, function_args)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @logger.info('Refreshing user-defined function ' \
          "\"#{keyspace_name}.#{function_name}\"")

        # function_args is an array of string, and we need an array of parsed types.
        parsed_function_args =
          @schema_fetcher.parse_argument_types(connection, keyspace_name, function_args)
        @schema_fetcher.fetch_function(connection,
                                       keyspace_name,
                                       function_name,
                                       parsed_function_args).map do |function|
          if function
            @schema.replace_function(function)
          else
            @schema.delete_function(keyspace_name, function_name, parsed_function_args)
          end

          @logger.info('Completed refreshing user-defined function ' \
            "\"#{keyspace_name}.#{function_name}(#{function_args.join(',')})\"")
        end
      end

      def refresh_aggregate_async(keyspace_name, aggregate_name, aggregate_args)
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @logger.info('Refreshing user-defined aggregate ' \
          "\"#{keyspace_name}.#{aggregate_name}\"")

        # aggregate_args is an array of string, and we need an array of parsed types.
        parsed_aggregate_args = @schema_fetcher.parse_argument_types(connection,
                                                                     keyspace_name,
                                                                     aggregate_args)
        @schema_fetcher.fetch_aggregate(connection,
                                        keyspace_name,
                                        aggregate_name,
                                        parsed_aggregate_args).map do |aggregate|
          if aggregate
            @schema.replace_aggregate(aggregate)
          else
            @schema.delete_aggregate(keyspace_name, aggregate_name, parsed_aggregate_args)
          end

          @logger.info('Completed refreshing user-defined aggregate ' \
            "\"#{keyspace_name}.#{aggregate_name}(#{aggregate_args.join(',')})\"")
        end
      end

      def refresh_peers_async_maybe_retry
        synchronize do
          return Ione::Future.resolved if @refreshing_hosts
          @refreshing_hosts = true
        end

        refresh_peers_async.fallback do |e|
          case e
          when Errors::HostError, Errors::TimeoutError
            refresh_peers_async_retry(e, @reconnection_policy.schedule)
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

      def refresh_peers_async_retry(error, schedule)
        timeout = schedule.next
        @logger.info("Failed to refresh hosts (#{error.class.name}: " \
          "#{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          refresh_peers_async.fallback do |e|
            case e
            when Errors::HostError, Errors::TimeoutError
              refresh_peers_async_retry(e, schedule)
            else
              connection = @connection
              connection && connection.close(e)

              Ione::Future.failed(e)
            end
          end
        end
      end

      def refresh_peers_async
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @logger.info('Refreshing peers metadata')

        send_select_request(connection, SELECT_PEERS).map do |peers|
          @logger.debug("#{peers.size} peer(s) found")

          ips = ::Set.new

          peers.shuffle!
          peers.each do |data|
            ip = peer_ip(data)
            next unless ip
            ips << ip
            @registry.host_found(ip, data)
          end

          @registry.each_host do |host|
            next if host.ip == connection.host
            @registry.host_lost(host.ip) unless ips.include?(host.ip)
          end

          @logger.info('Completed refreshing peers metadata')

          nil
        end
      end

      def refresh_metadata_async
        connection = @connection

        return Ione::Future.failed(Errors::ClientError.new('Not connected')) if connection.nil?

        @logger.info("Refreshing connected host's metadata")

        send_select_request(connection, SELECT_LOCAL).map do |local|
          raise Errors::InternalError, "Unable to fetch connected host's metadata" if local.empty?

          data = local.first
          @registry.host_found(IPAddr.new(connection.host), data)
          @metadata.update(data)

          @logger.info("Completed refreshing connected host's metadata")

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
          refresh_host_status(host).fallback do |_e|
            refresh_host_status_with_retry(timer, host, schedule)
          end
        end
      end

      def refresh_host_async_maybe_retry(address)
        synchronize do
          return Ione::Future.resolved if @refreshing_hosts || @refreshing_host[address]
          @refreshing_host[address] = true
        end

        refresh_host_async(address).fallback do |e|
          case e
          when Errors::HostError, Errors::TimeoutError
            refresh_host_async_retry(address, e, @reconnection_policy.schedule)
          else
            connection = @connection
            connection && connection.close(e)

            Ione::Future.failed(e)
          end
        end.map do
          synchronize do
            @refreshing_host.delete(address)
          end
        end
      end

      def refresh_host_async_retry(address, error, schedule)
        timeout = schedule.next
        @logger.info("Failed to refresh host #{address} (#{error.class.name}: " \
          "#{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          refresh_host_async(address).fallback do |e|
            case e
            when Errors::HostError, Errors::TimeoutError
              refresh_host_async_retry(address, e, schedule)
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

        @logger.info("Refreshing host metadata: #{ip}")

        request = if ip == connection.host
                    SELECT_LOCAL
                  else
                    Protocol::QueryRequest.new(SELECT_PEER_QUERY % ip,
                                               EMPTY_LIST,
                                               EMPTY_LIST,
                                               :one)
                  end

        send_select_request(connection, request).map do |rows|
          raise Errors::InternalError, "Unable to find host metadata: #{ip}" if rows.empty?

          @logger.info("Completed refreshing host metadata: #{ip}")
          @registry.host_found(address, rows.first)

          self
        end
      rescue => e
        @logger.error("Refreshing host metadata failed (#{e.class.name}: #{e.message})")
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
                    @logger.info('Control connection closed ' \
                      "(#{cause.class.name}: #{cause.message})")
                  else
                    @logger.info('Control connection closed')
                  end

                  @connection = nil
                end
              end

              reconnect_async(@reconnection_policy.schedule) if reconnect
            end
          end

          refresh_maybe_retry(:metadata)
        end
        f = f.flat_map { register_async }
        f = f.flat_map { refresh_peers_async_maybe_retry }
        f = f.flat_map { refresh_maybe_retry(:schema) } if @connection_options.synchronize_schema?
        f = f.fallback do |error|
          @logger.debug("Connection to #{host.ip} failed " \
            "(#{error.class.name}: #{error.message})")

          case error
          when Errors::HostError, Errors::TimeoutError
            errors ||= {}
            errors[host] = error
            connect_to_first_available(plan, errors)
          else
            Ione::Future.failed(error)
          end
        end

        f.on_complete do |connection_future|
          @logger.info('Control connection established') if connection_future.resolved?
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
        refresh_tables_and_views = ::Hash.new
        refresh_types      = ::Hash.new

        # This hash is of the form <keyspace, [Change (for function changes)]>
        refresh_functions  = ::Hash.new

        # This hash is of the form <keyspace, [Change (for aggregate changes)]>
        refresh_aggregates = ::Hash.new

        schema_changes.each do |change|
          keyspace = change.keyspace

          next if refresh_keyspaces.key?(keyspace)

          case change.target
          when Protocol::Constants::SCHEMA_CHANGE_TARGET_KEYSPACE
            refresh_tables_and_views.delete(keyspace)
            refresh_types.delete(keyspace)
            refresh_functions.delete(keyspace)
            refresh_aggregates.delete(keyspace)
            refresh_keyspaces[keyspace] = true
          when Protocol::Constants::SCHEMA_CHANGE_TARGET_TABLE
            # We can't distinguish between table and view change events, so refresh both.
            tables_and_views = refresh_tables_and_views[keyspace] ||= ::Hash.new
            tables_and_views[change.name] = true
          when Protocol::Constants::SCHEMA_CHANGE_TARGET_UDT
            types = refresh_types[keyspace] ||= ::Hash.new
            types[change.name] = true
          when Protocol::Constants::SCHEMA_CHANGE_TARGET_FUNCTION
            functions = refresh_functions[keyspace] ||= []
            functions << change
          when Protocol::Constants::SCHEMA_CHANGE_TARGET_AGGREGATE
            aggregates = refresh_aggregates[keyspace] ||= []
            aggregates << change
          end
        end

        futures = ::Array.new

        refresh_keyspaces.each_key do |keyspace|
          futures << refresh_maybe_retry(:keyspace, keyspace)
        end

        refresh_tables_and_views.each do |(keyspace, tables_and_views)|
          tables_and_views.each_key do |table_or_view|
            futures << refresh_maybe_retry(:table, keyspace, table_or_view)
            futures << refresh_maybe_retry(:materialized_view, keyspace, table_or_view)
          end
        end

        refresh_types.each do |(keyspace, types)|
          types.each_key do |type|
            futures << refresh_maybe_retry(:type, keyspace, type)
          end
        end

        refresh_functions.each do |(keyspace, function_changes)|
          function_changes.each do |change|
            futures <<
              refresh_maybe_retry(:function, keyspace, change.name, change.arguments)
          end
        end

        refresh_aggregates.each do |(keyspace, aggregate_changes)|
          aggregate_changes.each do |change|
            futures <<
              refresh_maybe_retry(:aggregate, keyspace, change.name, change.arguments)
          end
        end

        Ione::Future.all(*futures)
      end

      def refresh_maybe_retry(what, *args)
        send(:"refresh_#{what}_async", *args).fallback do |e|
          case e
          when Errors::HostError, Errors::TimeoutError
            refresh_retry(what, e, @reconnection_policy.schedule, *args)
          else
            connection = @connection
            connection && connection.close(e)

            Ione::Future.failed(e)
          end
        end
      end

      def refresh_retry(what, error, schedule, *args)
        timeout = schedule.next
        @logger.info("Failed to refresh #{what} #{args.inspect} " \
          "(#{error.class.name}: #{error.message}), retrying in #{timeout}")

        timer = @io_reactor.schedule_timer(timeout)
        timer.flat_map do
          send(:"refresh_#{what}_async", *args).fallback do |e|
            case e
            when Errors::HostError, Errors::TimeoutError
              refresh_retry(what, e, schedule, *args)
            else
              connection = @connection
              connection && connection.close(e)

              Ione::Future.failed(e)
            end
          end
        end
      end

      def refresh_schema_async_wrapper
        # This is kinda tricky. We want to start refreshing the schema asynchronously.
        # However, if we're already in the process of doing so, return the future
        # representing that result rather than starting another schema refresh.
        #
        # A few other nuances while a refresh is in progress:
        # * if a new attempt is made to refresh, keep track of that and schedule another
        #   refresh after the current one completes.
        # * we don't want schema_change events to be processed since the full refresh
        #   may overwrite the results of handling the schema_change events with older
        #   data. That said, we don't want to lose track of schema_change events; just
        #   delay processing them until after the full refresh is done.
        #
        # Finally, when a full refresh begins, clear out any pending changes in
        # @schema_changes because the full refresh subsumes them. This has two benefits:
        # 1. avoid round trips to Cassandra to get details related to those schema
        #    changes.
        # 2. avoid race conditions where those updates may return older data than our
        #    full refresh and might win as last writer with that potentially older data.
        synchronize do
          if @refresh_schema_future
            @pending_schema_refresh = true
            return @refresh_schema_future
          end

          # Fresh refresh; prep this connection!

          # Since we're starting a new refresh, there can be no pending refresh request.
          @pending_schema_refresh = false

          # Clear outstanding schema changes and timers.
          @schema_changes = []
          @io_reactor.cancel_timer(@schema_refresh_timer) if @schema_refresh_timer
          @schema_refresh_timer = nil
          @io_reactor.cancel_timer(@schema_refresh_window) if @schema_refresh_window
          @schema_refresh_window = nil

          # Start refreshing..
          @refresh_schema_future = refresh_maybe_retry(:schema)
          @refresh_schema_future.on_complete do
            pending = false
            synchronize do
              # We're done refreshing. If we have a pending refresh, launch it now.
              @refresh_schema_future = nil
              pending = @pending_schema_refresh
              @pending_schema_refresh = false
              unless pending
                # Restore timers if there are pending schema changes.
                handle_schema_change(nil)
              end
            end

            refresh_schema_async_wrapper if pending
          end

          # Return the (now cached) future
          @refresh_schema_future
        end
      end

      def handle_schema_change(change)
        timer = nil
        expiration_timer = nil

        synchronize do
          # If change is nil, it means we want to set up timers (if there are pending
          # changes). Otherwise, we definitely have a change and want to set up timers.
          # Also, we only want to set up timers if we're not in the middle of a full
          # refresh.
          @schema_changes << change if change

          unless @schema_changes.empty? || @refresh_schema_future
            @io_reactor.cancel_timer(@schema_refresh_timer) if @schema_refresh_timer
            timer = @schema_refresh_timer =
                      @io_reactor.schedule_timer(@connection_options.schema_refresh_delay)

            unless @schema_refresh_window
              @schema_refresh_window =
                @io_reactor.schedule_timer(@connection_options.schema_refresh_timeout)
              expiration_timer = @schema_refresh_window
            end
          end
        end

        expiration_timer.on_value do
          schema_changes = nil

          synchronize do
            @io_reactor.cancel_timer(@schema_refresh_timer)

            @schema_refresh_window = nil
            @schema_refresh_timer = nil

            schema_changes = @schema_changes
            @schema_changes = ::Array.new
          end

          process_schema_changes(schema_changes)
        end if expiration_timer

        timer.on_value do
          schema_changes = nil

          synchronize do
            @io_reactor.cancel_timer(@schema_refresh_window)

            @schema_refresh_window = nil
            @schema_refresh_timer = nil

            schema_changes = @schema_changes
            @schema_changes = ::Array.new
          end

          process_schema_changes(schema_changes)
        end if timer

        nil
      end

      def send_select_request(connection, request)
        backtrace = caller
        connection.send_request(request).map do |r|
          case r
          when Protocol::RowsResultResponse
            r.rows
          when Protocol::ErrorResponse
            e = r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
            e.set_backtrace(backtrace)
            raise e
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}", caller
          end
        end
      end
    end
  end
end
