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
    class Client
      include MonitorMixin

      attr_reader :keyspace

      def initialize(logger, cluster_registry, cluster_schema, io_reactor, connector, load_balancing_policy, reconnection_policy, retry_policy, address_resolution_policy, connection_options, futures_factory)
        @logger                      = logger
        @registry                    = cluster_registry
        @schema                      = cluster_schema
        @reactor                     = io_reactor
        @connector                   = connector
        @load_balancing_policy       = load_balancing_policy
        @reconnection_policy         = reconnection_policy
        @retry_policy                = retry_policy
        @address_resolver            = address_resolution_policy
        @connection_options          = connection_options
        @futures                     = futures_factory
        @connecting_hosts            = ::Hash.new
        @connections                 = ::Hash.new
        @prepared_statements         = ::Hash.new
        @preparing_statements        = ::Hash.new
        @keyspace                    = nil
        @state                       = :idle

        mon_initialize
      end

      def connect
        synchronize do
          return CLIENT_CLOSED     if @state == :closed || @state == :closing
          return @connected_future if @state == :connecting || @state == :connected

          @state = :connecting
          @registry.each_host do |host|
            distance = @load_balancing_policy.distance(host)
            next if distance == :ignore

            @connecting_hosts[host] = distance
          end
        end

        @connected_future = begin
          @logger.info('Creating session')
          @registry.add_listener(self)

          futures = @connecting_hosts.map do |(host, distance)|
            f = connect_to_host(host, distance)
            f.recover do |error|
              Cassandra::Client::FailedConnection.new(error, host)
            end
          end

          Ione::Future.all(*futures).map do |connections|
            connections.flatten!
            raise NO_HOSTS if connections.empty?

            unless connections.any?(&:connected?)
              errors = {}
              connections.each {|c| errors[c.host] = c.error}
              raise Errors::NoHostsAvailable.new(errors)
            end

            self
          end
        end
        @connected_future.on_complete(&method(:connected))
        @connected_future
      end

      def close
        state = nil

        synchronize do
          return CLIENT_NOT_CONNECTED if @state == :idle
          return @closed_future if @state == :closed || @state == :closing

          state, @state = @state, :closing
        end

        @closed_future = begin
          @registry.remove_listener(self)

          if state == :connecting
            f = @connected_future.recover.flat_map { close_connections }
          else
            f = close_connections
          end

          f.map(self)
        end
        @closed_future.on_complete(&method(:closed))
        @closed_future
      end

      # These methods shall be called from inside reactor thread only
      def host_found(host)
        nil
      end

      def host_lost(host)
        nil
      end

      def host_up(host)
        distance = nil

        synchronize do
          return Ione::Future.resolved if @connecting_hosts.include?(host)

          distance = @load_balancing_policy.distance(host)
          return Ione::Future.resolved if distance == :ignore

          @connecting_hosts[host] = distance
        end

        connect_to_host_maybe_retry(host, distance).map(nil)
      end

      def host_down(host)
        manager = nil

        synchronize do
          return Ione::Future.resolved if !@connections.has_key?(host) && !@connecting_hosts.include?(host)

          @connecting_hosts.delete(host)
          @prepared_statements.delete(host)
          @preparing_statements.delete(host)

          manager = @connections.delete(host)
        end

        if manager
          Ione::Future.all(*manager.snapshot.map! {|c| c.close}).map(nil)
        else
          Ione::Future.resolved
        end
      end

      def query(statement, options, paging_state = nil)
        request = Protocol::QueryRequest.new(statement.cql, statement.params, nil, options.consistency, options.serial_consistency, options.page_size, paging_state, options.trace?)
        timeout = options.timeout
        promise = @futures.promise

        keyspace = @keyspace
        plan     = @load_balancing_policy.plan(keyspace, statement, options)

        send_request_by_plan(promise, keyspace, statement, options, request, plan, timeout)

        promise.future
      end

      def prepare(cql, options)
        request = Protocol::PrepareRequest.new(cql, options.trace?)
        timeout = options.timeout
        promise = @futures.promise

        keyspace  = @keyspace
        statement = VOID_STATEMENT
        plan      = @load_balancing_policy.plan(keyspace, statement, options)

        send_request_by_plan(promise, keyspace, statement, options, request, plan, timeout)

        promise.future
      end

      def execute(statement, options, paging_state = nil)
        timeout         = options.timeout
        result_metadata = statement.result_metadata
        request         = Protocol::ExecuteRequest.new(nil, statement.params_metadata, statement.params, result_metadata.nil?, options.consistency, options.serial_consistency, options.page_size, paging_state, options.trace?)
        promise         = @futures.promise

        keyspace = @keyspace
        plan     = @load_balancing_policy.plan(keyspace, statement, options)

        execute_by_plan(promise, keyspace, statement, options, request, plan, timeout)

        promise.future
      end

      def batch(statement, options)
        timeout  = options.timeout
        keyspace = @keyspace
        plan     = @load_balancing_policy.plan(keyspace, statement, options)
        promise  = @futures.promise

        batch_by_plan(promise, keyspace, statement, options, plan, timeout)

        promise.future
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end

      private

      NO_CONNECTIONS = Ione::Future.resolved([])
      BATCH_TYPES    = {
        :logged   => Protocol::BatchRequest::LOGGED_TYPE,
        :unlogged => Protocol::BatchRequest::UNLOGGED_TYPE,
        :counter  => Protocol::BatchRequest::COUNTER_TYPE,
      }.freeze
      CLIENT_CLOSED        = Ione::Future.failed(Errors::ClientError.new('Client closed'))
      NOT_CONNECTED        = Errors::ClientError.new('Client not connected')
      CLIENT_NOT_CONNECTED = Ione::Future.failed(NOT_CONNECTED)

      UNAVAILABLE_ERROR_CODE   = 0x1000
      WRITE_TIMEOUT_ERROR_CODE = 0x1100
      READ_TIMEOUT_ERROR_CODE  = 0x1200
      OVERLOADED_ERROR_CODE    = 0x1001
      SERVER_ERROR_CODE        = 0x0000
      BOOTSTRAPPING_ERROR_CODE = 0x1002
      UNPREPARED_ERROR_CODE    = 0x2500

      SELECT_SCHEMA_PEERS = Protocol::QueryRequest.new("SELECT peer, rpc_address, schema_version FROM system.peers", nil, nil, :one)
      SELECT_SCHEMA_LOCAL = Protocol::QueryRequest.new("SELECT schema_version FROM system.local WHERE key='local'", nil, nil, :one)

      def connected(f)
        if f.resolved?
          synchronize do
            @state = :connected
          end

          @logger.info('Session created')
        else
          synchronize do
            @state = :defunct
          end

          f.on_failure do |e|
            @logger.error("Session failed to connect (#{e.class.name}: #{e.message})")
          end

          close
        end
      end

      def closed(f)
        synchronize do
          @state = :closed

          if f.resolved?
            @logger.info('Session closed')
          else
            f.on_failure do |e|
              @logger.error("Session failed to close (#{e.class.name}: #{e.message})")
            end
          end
        end
      end

      def close_connections
        futures = []
        synchronize do
          @connections.each do |host, connections|
            connections.snapshot.each do |c|
              futures << c.close
            end
          end.clear
        end

        Ione::Future.all(*futures).map(self)
      end

      def connect_to_host_maybe_retry(host, distance)
        f = connect_to_host(host, distance)

        f.on_failure do |e|
          connect_to_host_with_retry(host, @reconnection_policy.schedule)
        end

        f
      end

      def connect_to_host_with_retry(host, schedule)
        interval = schedule.next

        @logger.debug("Reconnecting to #{host.ip} in #{interval} seconds")

        f = @reactor.schedule_timer(interval)
        f.flat_map do
          distance = nil

          synchronize do
            if @connecting_hosts.include?(host)
              distance = @load_balancing_policy.distance(host)
            end
          end

          if distance && distance != :ignore
            connect_to_host(host, distance).fallback do |e|
              connect_to_host_with_retry(host, schedule)
            end
          else
            NO_CONNECTIONS
          end
        end
      end

      def connect_to_host(host, distance)
        case distance
        when :ignore
          return NO_CONNECTIONS
        when :local
          pool_size = @connection_options.connections_per_local_node
        when :remote
          pool_size = @connection_options.connections_per_remote_node
        else
          @logger.error("Invalid load balancing distance, not connecting to #{host.ip}. Distance must be one of #{LoadBalancing::DISTANCES.inspect}, #{distance.inspect} given")
          return NO_CONNECTIONS
        end

        f = @connector.connect_many(host, pool_size)

        f.on_value do |connections|
          manager = nil

          synchronize do
            @connecting_hosts.delete(host)
            @prepared_statements[host] = {}
            @preparing_statements[host] = {}
            manager = @connections[host] ||= Cassandra::Client::ConnectionManager.new
          end

          manager.add_connections(connections)
        end

        f
      end

      def execute_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors = nil, hosts = [])
        unless plan.has_next?
          promise.break(Errors::NoHostsAvailable.new(errors))
          return
        end

        hosts << host = plan.next
        manager = nil
        synchronize { manager = @connections[host] }

        unless manager
          errors ||= {}
          errors[host] = NOT_CONNECTED
          return execute_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
        end

        connection = manager.random_connection

        if keyspace && connection.keyspace != keyspace
          switch = switch_keyspace(connection, keyspace, timeout)
          switch.on_complete do |s|
            if s.resolved?
              prepare_and_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
            else
              s.on_failure do |e|
                case e
                when Errors::HostError
                  errors ||= {}
                  errors[host] = e
                  execute_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
                else
                  promise.break(e)
                end
              end
            end
          end
        else
          prepare_and_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
        end
      rescue => e
        errors ||= {}
        errors[host] = e
        execute_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
      end

      def prepare_and_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
        cql = statement.cql
        id  = synchronize { @prepared_statements[host][cql] }

        if id
          request.id = id
          do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
        else
          prepare = prepare_statement(host, connection, cql, timeout)
          prepare.on_complete do |_|
            if prepare.resolved?
              request.id = prepare.value
              do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
            else
              prepare.on_failure do |e|
                case e
                when Errors::HostError
                  errors ||= {}
                  errors[host] = e
                  execute_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
                else
                  promise.break(e)
                end
              end
            end
          end
        end
      end

      def batch_by_plan(promise, keyspace, statement, options, plan, timeout, errors = nil, hosts = [])
        unless plan.has_next?
          promise.break(Errors::NoHostsAvailable.new(errors))
          return
        end

        hosts << host = plan.next
        manager = nil
        synchronize { manager = @connections[host] }

        unless manager
          errors ||= {}
          errors[host] = NOT_CONNECTED
          return batch_by_plan(promise, keyspace, statement, options, plan, timeout, errors, hosts)
        end

        connection = manager.random_connection

        if keyspace && connection.keyspace != keyspace
          switch = switch_keyspace(connection, keyspace, timeout)
          switch.on_complete do |s|
            if s.resolved?
              batch_and_send_request_by_plan(host, connection, promise, keyspace, statement, options, plan, timeout, errors, hosts)
            else
              s.on_failure do |e|
                case e
                when Errors::HostError
                  errors ||= {}
                  errors[host] = e
                  batch_by_plan(promise, keyspace, statement, options, plan, timeout, errors, hosts)
                else
                  promise.break(e)
                end
              end
            end
          end
        else
          batch_and_send_request_by_plan(host, connection, promise, keyspace, statement, options, plan, timeout, errors, hosts)
        end
      rescue => e
        errors ||= {}
        errors[host] = e
        batch_by_plan(promise, keyspace, statement, options, plan, timeout, errors, hosts)
      end

      def batch_and_send_request_by_plan(host, connection, promise, keyspace, statement, options, plan, timeout, errors, hosts)
        request    = Protocol::BatchRequest.new(BATCH_TYPES[statement.type], options.consistency, options.trace?)
        unprepared = Hash.new {|hash, cql| hash[cql] = []}

        statement.statements.each do |statement|
          cql = statement.cql

          if statement.is_a?(Statements::Bound)
            id = synchronize { @prepared_statements[host][cql] }

            if id
              request.add_prepared(id, statement.params_metadata, statement.params)
            else
              unprepared[cql] << statement
            end
          else
            request.add_query(cql, statement.params)
          end
        end

        if unprepared.empty?
          do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
        else
          to_prepare = unprepared.to_a
          futures    = to_prepare.map do |cql, _|
            prepare_statement(host, connection, cql, timeout)
          end

          Ione::Future.all(*futures).on_complete do |f|
            if f.resolved?
              prepared_ids = f.value
              to_prepare.each_with_index do |(_, statements), i|
                statements.each do |statement|
                  request.add_prepared(prepared_ids[i], statement.params_metadata, statement.params)
                end
              end

              do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
            else
              f.on_failure do |e|
                case e
                when Errors::HostError
                  errors ||= {}
                  errors[host] = e
                  batch_by_plan(promise, keyspace, statement, options, plan, timeout, errors, hosts)
                else
                  promise.break(e)
                end
              end
            end
          end
        end
      end

      def send_request_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors = nil, hosts = [])
        unless plan.has_next?
          promise.break(Errors::NoHostsAvailable.new(errors))
          return
        end

        hosts << host = plan.next
        manager = nil
        synchronize { manager = @connections[host] }

        unless manager
          errors ||= {}
          errors[host] = NOT_CONNECTED
          return send_request_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
        end

        connection = manager.random_connection

        if keyspace && connection.keyspace != keyspace
          switch = switch_keyspace(connection, keyspace, timeout)
          switch.on_complete do |s|
            if s.resolved?
              do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
            else
              s.on_failure do |e|
                case e
                when Errors::HostError
                  errors ||= {}
                  errors[host] = e
                  send_request_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
                else
                  promise.break(e)
                end
              end
            end
          end
        else
          do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
        end
      rescue => e
        errors ||= {}
        errors[host] = e
        send_request_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
      end

      def do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts, retries = 0)
        request.retries = retries

        f = connection.send_request(request, timeout)
        f.on_complete do |f|
          if f.resolved?
            r = f.value
            case r
            when Protocol::DetailedErrorResponse
              details  = r.details
              decision = begin
                case r.code
                when UNAVAILABLE_ERROR_CODE
                  @retry_policy.unavailable(statement, details[:cl], details[:required], details[:alive], retries)
                when WRITE_TIMEOUT_ERROR_CODE
                  @retry_policy.write_timeout(statement, details[:cl], details[:write_type], details[:blockfor], details[:received], retries)
                when READ_TIMEOUT_ERROR_CODE
                  @retry_policy.read_timeout(statement, details[:cl], details[:blockfor], details[:received], details[:data_present], retries)
                when UNPREPARED_ERROR_CODE
                  cql = statement.cql

                  synchronize do
                    @preparing_statements[host].delete(cql)
                    @prepared_statements[host].delete(cql)
                  end

                  prepare = prepare_statement(host, connection, cql, timeout)
                  prepare.on_complete do |_|
                    if prepare.resolved?
                      request.id = prepare.value
                      do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
                    else
                      prepare.on_failure do |e|
                        case e
                        when Errors::HostError
                          errors ||= {}
                          errors[host] = e
                          execute_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
                        else
                          promise.break(e)
                        end
                      end
                    end
                  end
                  break
                else
                  promise.break(r.to_error(statement))
                  break
                end
              rescue => e
                promise.break(e)
                break
              end

              case decision
              when Retry::Decisions::Retry
                request.consistency = decision.consistency
                do_send_request_by_plan(host, connection, promise, keyspace, statement, options, request, plan, timeout, errors, hosts, retries + 1)
              when Retry::Decisions::Ignore
                promise.fulfill(Results::Void.new(r.trace_id, keyspace, statement, options, hosts, request.consistency, retries, self, @futures))
              when Retry::Decisions::Reraise
                promise.break(r.to_error(statement))
              else
                promise.break(r.to_error(statement))
              end
            when Protocol::ErrorResponse
              case r.code
              when OVERLOADED_ERROR_CODE, SERVER_ERROR_CODE, BOOTSTRAPPING_ERROR_CODE
                errors ||= {}
                errors[host] = r.to_error(statement)

                case request
                when Protocol::QueryRequest, Protocol::PrepareRequest
                  send_request_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
                when Protocol::ExecuteRequest
                  execute_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
                when Protocol::BatchRequest
                  batch_by_plan(promise, keyspace, statement, options, plan, timeout, errors, hosts)
                end
              else
                promise.break(r.to_error(statement))
              end
            when Protocol::SetKeyspaceResultResponse
              @keyspace = r.keyspace
              promise.fulfill(Results::Void.new(r.trace_id, keyspace, statement, options, hosts, request.consistency, retries, self, @futures))
            when Protocol::PreparedResultResponse
              cql = request.cql
              synchronize do
                @prepared_statements[host][cql] = r.id
                @preparing_statements[host].delete(cql)
              end

              promise.fulfill(Statements::Prepared.new(cql, r.metadata, r.result_metadata, r.trace_id, keyspace, statement, options, hosts, request.consistency, retries, self, @futures, @schema))
            when Protocol::RawRowsResultResponse
              r.materialize(statement.result_metadata)
              promise.fulfill(Results::Paged.new(r.rows, r.paging_state, r.trace_id, keyspace, statement, options, hosts, request.consistency, retries, self, @futures))
            when Protocol::RowsResultResponse
              promise.fulfill(Results::Paged.new(r.rows, r.paging_state, r.trace_id, keyspace, statement, options, hosts, request.consistency, retries, self, @futures))
            when Protocol::SchemaChangeResultResponse
              if r.change == 'DROPPED' && r.keyspace == @keyspace && r.table.empty?
                @keyspace = nil
              end

              @logger.info('Waiting for schema to propagate to all hosts after a change')
              wait_for_schema_agreement(connection, @reconnection_policy.schedule).on_complete do |f|
                unless f.resolved?
                  f.on_failure do |e|
                    @logger.error("Schema agreement failure (#{e.class.name}: #{e.message})")
                  end
                end
                promise.fulfill(Results::Void.new(r.trace_id, keyspace, statement, options, hosts, request.consistency, retries, self, @futures))
              end
            else
              promise.fulfill(Results::Void.new(r.trace_id, keyspace, statement, options, hosts, request.consistency, retries, self, @futures))
            end
          else
            f.on_failure do |e|
              errors ||= {}
              errors[host] = e
              case request
              when Protocol::QueryRequest, Protocol::PrepareRequest
                send_request_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
              when Protocol::ExecuteRequest
                execute_by_plan(promise, keyspace, statement, options, request, plan, timeout, errors, hosts)
              when Protocol::BatchRequest
                batch_by_plan(promise, keyspace, statement, options, plan, timeout, errors, hosts)
              else
                promise.break(e)
              end
            end
          end
        end
      end

      def wait_for_schema_agreement(connection, schedule)
        peers = connection.send_request(SELECT_SCHEMA_PEERS)
        local = connection.send_request(SELECT_SCHEMA_LOCAL)

        Ione::Future.all(peers, local).flat_map do |(peers, local)|
          peers = peers.rows
          local = local.rows

          versions = ::Set.new

          unless local.empty?
            host = @registry.host(connection.host)

            if host && @load_balancing_policy.distance(host) != :ignore
              versions << version = local.first['schema_version']
              @logger.debug("Host #{host.ip} schema version is #{version}")
            end
          end

          peers.each do |row|
            host = @registry.host(peer_ip(row))
            next unless host && @load_balancing_policy.distance(host) != :ignore

            versions << version = row['schema_version']
            @logger.debug("Host #{host.ip} schema version is #{version}")
          end

          if versions.one?
            @logger.debug('All hosts have the same schema')
            Ione::Future.resolved
          else
            interval = schedule.next
            @logger.warn("Hosts have different schema versions: #{versions.to_a.inspect}, retrying in #{interval} seconds")
            @reactor.schedule_timer(interval).flat_map do
              wait_for_schema_agreement(connection, schedule)
            end
          end
        end
      end

      def peer_ip(data)
        ip = data['rpc_address']
        ip = data['peer'] if ip == '0.0.0.0'

        @address_resolver.resolve(ip)
      end

      def switch_keyspace(connection, keyspace, timeout)
        pending_keyspace = connection[:pending_keyspace]
        pending_switch   = connection[:pending_switch]

        return pending_switch || Ione::Future.resolved if pending_keyspace == keyspace

        request = Protocol::QueryRequest.new("USE #{Util.escape_name(keyspace)}", nil, nil, :one)

        f = connection.send_request(request, timeout).map do |r|
          case r
          when Protocol::SetKeyspaceResultResponse
            @keyspace = r.keyspace
            nil
          when Protocol::ErrorResponse
            raise r.to_error(Statements::Simple.new("USE #{Util.escape_name(keyspace)}"))
          else
            raise Errors::ProtocolError, "Unexpected response #{r.inspect}"
          end
        end

        connection[:pending_keyspace] = keyspace
        connection[:pending_switch]   = f

        f.on_complete do |f|
          connection[:pending_switch]   = nil
          connection[:pending_keyspace] = nil
        end

        f
      end

      def prepare_statement(host, connection, cql, timeout)
        synchronize do
          pending = @preparing_statements[host]

          return pending[cql] if pending.has_key?(cql)
        end

        request = Protocol::PrepareRequest.new(cql, false)

        f = connection.send_request(request, timeout).map do |r|
          case r
          when Protocol::PreparedResultResponse
            id = r.id
            synchronize do
              @prepared_statements[host][cql] = id
              @preparing_statements[host].delete(cql)
            end
            id
          when Protocol::ErrorResponse
            raise r.to_error(VOID_STATEMENT)
          else
            raise Errors::ProtocolError, "Unexpected response #{r.inspect}"
          end
        end

        synchronize do
          @preparing_statements[host][cql] = f
        end

        f
      end
    end
  end
end
