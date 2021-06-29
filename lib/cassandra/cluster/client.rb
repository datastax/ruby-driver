# encoding: utf-8

#--
# Copyright DataStax, Inc.
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

      def initialize(logger,
                     cluster_registry,
                     cluster_schema,
                     io_reactor,
                     connector,
                     profile_manager,
                     reconnection_policy,
                     address_resolution_policy,
                     connection_options,
                     futures_factory,
                     timestamp_generator)
        @logger                      = logger
        @registry                    = cluster_registry
        @schema                      = cluster_schema
        @reactor                     = io_reactor
        @connector                   = connector
        @profile_manager             = profile_manager
        @reconnection_policy         = reconnection_policy
        @address_resolver            = address_resolution_policy
        @connection_options          = connection_options
        @futures                     = futures_factory
        @connections                 = ::Hash.new
        @prepared_statements         = ::Hash.new
        @preparing_statements        = ::Hash.new {|hash, host| hash[host] = {}}
        @pending_connections         = ::Hash.new
        @keyspace                    = nil
        @state                       = :idle
        @timestamp_generator         = timestamp_generator

        mon_initialize
      end

      def connect
        connecting_hosts = ::Hash.new

        synchronize do
          return CLIENT_CLOSED     if @state == :closed || @state == :closing
          return @connected_future if @state == :connecting || @state == :connected

          @state = :connecting
          @registry.each_host do |host|
            distance = @profile_manager.distance(host)

            case distance
            when :ignore
              next
            when :local
              pool_size = @connection_options.connections_per_local_node
            when :remote
              pool_size = @connection_options.connections_per_remote_node
            else
              @logger.error("Not connecting to #{host.ip} - invalid load balancing " \
                'distance. Distance must be one of ' \
                "#{LoadBalancing::DISTANCES.inspect}, #{distance.inspect} given")
              next
            end

            connecting_hosts[host] = pool_size
            @pending_connections[host] = 0
            @preparing_statements[host] = {}
            @connections[host] = ConnectionPool.new
          end
        end

        @connected_future = begin
          @logger.info('Creating session')
          @registry.add_listener(self)
          @schema.add_listener(self)

          futures = connecting_hosts.map do |(host, pool_size)|
            f = connect_to_host(host, pool_size)
            f.recover do |error|
              FailedConnection.new(error, host)
            end
          end

          Ione::Future.all(*futures).map do |connections|
            connections.flatten!
            raise NO_HOSTS if connections.empty?

            failed_connections = connections.reject(&:connected?)

            @logger.debug("connection future resolved with #{failed_connections.len} failed connections out of #{connections.len} connections")

            # convert Cassandra::Protocol::CqlProtocolHandler to something with a real host
            failed_connections.map! do |c|
              if c.host.is_a?(String)
                host = @registry.each_host.detect { |h| h.ip.to_s == c.host } || raise("Unable to find host #{c.host}")
                FailedConnection.new(c.error, host)
              else
                c
              end
            end

            if failed_connections.size == connections.size
              errors = {}
              connections.each {|c| errors[c.host] = c.error unless c.error.nil?}
              raise Errors::NoHostsAvailable.new(errors)
            else
              failed_connections.each do |f|
                @logger.warn("want to reconnect to #{f.host}")
                connect_to_host_with_retry(f.host,
                                           connecting_hosts[f.host],
                                           @reconnection_policy.schedule)
              end
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

          state = @state
          @state = :closing
        end

        @closed_future = begin
          @registry.remove_listener(self)
          @schema.remove_listener(self)

          f = if state == :connecting
                @connected_future.recover.flat_map { close_connections }
              else
                close_connections
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
        pool_size = 0

        synchronize do
          distance = @profile_manager.distance(host)
          case distance
          when :ignore
            return Ione::Future.resolved
          when :local
            pool_size = @connection_options.connections_per_local_node
          when :remote
            pool_size = @connection_options.connections_per_remote_node
          else
            @logger.error("Not connecting to #{host.ip} - " \
              'invalid load balancing distance. Distance must be one of ' \
              "#{LoadBalancing::DISTANCES.inspect}, #{distance.inspect} given")
            return Ione::Future.resolved
          end

          @pending_connections[host] ||= 0
          @preparing_statements[host] = {}
          @connections[host] = ConnectionPool.new
        end

        connect_to_host_maybe_retry(host, pool_size)
      end

      def host_down(host)
        pool = nil

        synchronize do
          return Ione::Future.resolved unless @connections.key?(host)

          @pending_connections.delete(host) unless @pending_connections[host] > 0
          @preparing_statements.delete(host)
          pool = @connections.delete(host)
        end

        if pool
          Ione::Future.all(*pool.snapshot.map!(&:close)).map(nil)
        else
          Ione::Future.resolved
        end
      end

      def keyspace_created(keyspace)
      end

      def keyspace_changed(keyspace)
      end

      def keyspace_dropped(keyspace)
        @keyspace = nil if @keyspace == keyspace.name
        nil
      end

      def query(statement, options)
        if !statement.params.empty? && @connection_options.protocol_version == 1
          return @futures.error(
            Errors::ClientError.new(
              'Positional arguments are not supported by the current version of ' \
              'Apache Cassandra'
            )
          )
        end

        timestamp = @timestamp_generator.next if @timestamp_generator && @connection_options.protocol_version > 2
        payload   = nil
        payload   = options.payload if @connection_options.protocol_version >= 4
        request   = Protocol::QueryRequest.new(statement.cql,
                                               statement.params,
                                               statement.params_types,
                                               options.consistency,
                                               options.serial_consistency,
                                               options.page_size,
                                               options.paging_state,
                                               options.trace?,
                                               statement.params_names,
                                               timestamp,
                                               payload)
        timeout   = options.timeout
        promise   = @futures.promise

        keyspace = @keyspace
        plan = options.load_balancing_policy.plan(keyspace, statement, options)

        send_request_by_plan(promise,
                             keyspace,
                             statement,
                             options,
                             request,
                             plan,
                             timeout)

        promise.future
      end

      def prepare(cql, options)
        payload = nil
        payload = options.payload if @connection_options.protocol_version >= 4
        request = Protocol::PrepareRequest.new(cql, options.trace?, payload)
        timeout = options.timeout
        promise = @futures.promise

        keyspace  = @keyspace
        statement = VOID_STATEMENT
        plan      = options.load_balancing_policy.plan(keyspace, statement, options)

        send_request_by_plan(promise,
                             keyspace,
                             statement,
                             options,
                             request,
                             plan,
                             timeout)

        promise.future
      end

      def execute(statement, options)
        timestamp = @timestamp_generator.next if @timestamp_generator && @connection_options.protocol_version > 2
        payload         = nil
        payload         = options.payload if @connection_options.protocol_version >= 4
        timeout         = options.timeout
        result_metadata = statement.result_metadata
        request         = Protocol::ExecuteRequest.new(nil,
                                                       statement.params_types,
                                                       statement.params,
                                                       result_metadata.nil?,
                                                       options.consistency,
                                                       options.serial_consistency,
                                                       options.page_size,
                                                       options.paging_state,
                                                       options.trace?,
                                                       timestamp,
                                                       payload)
        promise         = @futures.promise

        keyspace = @keyspace
        plan     = options.load_balancing_policy.plan(keyspace, statement, options)

        execute_by_plan(promise, keyspace, statement, options, request, plan, timeout)

        promise.future
      end

      def batch(statement, options)
        if @connection_options.protocol_version < 2
          return @futures.error(
            Errors::ClientError.new(
              'Batch statements are not supported by the current version of ' \
              'Apache Cassandra'
            )
          )
        end

        timestamp = @timestamp_generator.next if @timestamp_generator && @connection_options.protocol_version > 2
        payload   = nil
        payload   = options.payload if @connection_options.protocol_version >= 4
        timeout   = options.timeout
        request   = Protocol::BatchRequest.new(BATCH_TYPES[statement.type],
                                               options.consistency,
                                               options.trace?,
                                               options.serial_consistency,
                                               timestamp,
                                               payload)
        keyspace  = @keyspace
        plan      = options.load_balancing_policy.plan(keyspace, statement, options)
        promise   = @futures.promise

        batch_by_plan(promise, keyspace, statement, options, request, plan, timeout)

        promise.future
      end

      def inspect
        "#<#{self.class.name}:0x#{object_id.to_s(16)}>"
      end

      private

      NO_CONNECTIONS = Ione::Future.resolved([])
      BATCH_TYPES    = {
        logged: Protocol::BatchRequest::LOGGED_TYPE,
        unlogged: Protocol::BatchRequest::UNLOGGED_TYPE,
        counter: Protocol::BatchRequest::COUNTER_TYPE
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

      SELECT_SCHEMA_PEERS =
        Protocol::QueryRequest.new(
          'SELECT peer, rpc_address, schema_version FROM system.peers',
          EMPTY_LIST,
          EMPTY_LIST,
          :one
        )
      SELECT_SCHEMA_LOCAL =
        Protocol::QueryRequest.new(
          "SELECT schema_version FROM system.local WHERE key='local'",
          EMPTY_LIST,
          EMPTY_LIST,
          :one
        )

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
          @connections.each do |_host, connections|
            connections.snapshot.each do |c|
              futures << c.close
            end
          end.clear
        end

        Ione::Future.all(*futures).map(self)
      end

      def connect_to_host_maybe_retry(host, pool_size)
        connect_to_host(host, pool_size).fallback do |e|
          @logger.error('Scheduling initial connection retry to ' \
            "#{host.ip} (#{e.class.name}: #{e.message})")
          connect_to_host_with_retry(host, pool_size, @reconnection_policy.schedule)
        end.map(nil)
      end

      def connect_to_host_with_retry(host, pool_size, schedule)
        interval = schedule.next

        @logger.debug("Reconnecting to #{host.ip} in #{interval} seconds")

        f = @reactor.schedule_timer(interval)
        f.flat_map do
          connect_to_host(host, pool_size).fallback do |e|
            @logger.error('Scheduling connection retry to ' \
              "#{host.ip} (#{e.class.name}: #{e.message})")
            connect_to_host_with_retry(host, pool_size, schedule)
          end
        end
      end

      def connect_to_host(host, pool_size)
        size = 0

        synchronize do
          unless @connections.include?(host)
            @logger.info("Not connecting to #{host.ip} - host is currently down")
            return NO_CONNECTIONS
          end

          pool = @connections[host]
          size = pool_size - pool.size

          if size <= 0
            @logger.info("Not connecting to #{host.ip} - host is already fully connected")
            return NO_CONNECTIONS
          end

          size -= @pending_connections[host]

          if size <= 0
            @logger.info("Not connecting to #{host.ip} - " \
              'host is already pending connections')
            return NO_CONNECTIONS
          end

          @pending_connections[host] += size
        end

        @logger.debug("Creating #{size} request connections to #{host.ip}")
        futures = size.times.map do
          @connector.connect(host).recover do |e|
            FailedConnection.new(e, host)
          end
        end

        Ione::Future.all(*futures).flat_map do |connections|
          error = nil

          connections.reject! do |connection|
            if connection.connected?
              false
            else
              error = connection.error
              true
            end
          end

          @logger.debug("Created #{connections.size} request connections to #{host.ip}")

          pool = nil

          synchronize do
            @pending_connections[host] -= size

            if @connections.include?(host)
              pool = @connections[host]
            else
              @pending_connections.delete(host) unless @pending_connections[host] > 0
            end
          end

          if pool
            pool.add_connections(connections)

            connections.each do |connection|
              connection.on_closed do |cause|
                if cause
                  @logger.info('Request connection closed ' \
                      "(#{cause.class.name}: #{cause.message})")
                else
                  @logger.info('Request connection closed')
                end
                connect_to_host_maybe_retry(host, pool_size) if cause
              end
            end
          else
            connections.each(&:close)
          end

          if error
            Ione::Future.failed(error)
          else
            Ione::Future.resolved(connections)
          end
        end
      end

      def execute_by_plan(promise,
                          keyspace,
                          statement,
                          options,
                          request,
                          plan,
                          timeout,
                          errors = nil,
                          hosts = [],
                          retries = -1)
        unless plan.has_next?
          promise.break(Errors::NoHostsAvailable.new(errors))
          return
        end

        hosts << host = plan.next
        retries += 1

        pool = nil
        synchronize { pool = @connections[host] }

        unless pool
          errors ||= {}
          errors[host] = NOT_CONNECTED
          return execute_by_plan(promise,
                                 keyspace,
                                 statement,
                                 options,
                                 request,
                                 plan,
                                 timeout,
                                 errors,
                                 hosts,
                                 retries)
        end

        connection = pool.random_connection

        if keyspace && connection.keyspace != keyspace
          switch = switch_keyspace(connection, keyspace, timeout)
          switch.on_complete do |s|
            if s.resolved?
              prepare_and_send_request_by_plan(host,
                                               connection,
                                               promise,
                                               keyspace,
                                               statement,
                                               options,
                                               request,
                                               plan,
                                               timeout,
                                               errors,
                                               hosts,
                                               retries)
            else
              s.on_failure do |e|
                if e.is_a?(Errors::HostError) ||
                   (e.is_a?(Errors::TimeoutError) && statement.idempotent?)
                  errors ||= {}
                  errors[host] = e
                  execute_by_plan(promise,
                                  keyspace,
                                  statement,
                                  options,
                                  request,
                                  plan,
                                  timeout,
                                  errors,
                                  hosts,
                                  retries)
                else
                  promise.break(e)
                end
              end
            end
          end
        else
          prepare_and_send_request_by_plan(host,
                                           connection,
                                           promise,
                                           keyspace,
                                           statement,
                                           options,
                                           request,
                                           plan,
                                           timeout,
                                           errors,
                                           hosts,
                                           retries)
        end
      rescue => e
        errors ||= {}
        errors[host] = e
        execute_by_plan(promise,
                        keyspace,
                        statement,
                        options,
                        request,
                        plan,
                        timeout,
                        errors,
                        hosts,
                        retries)
      end

      def prepare_and_send_request_by_plan(host,
                                           connection,
                                           promise,
                                           keyspace,
                                           statement,
                                           options,
                                           request,
                                           plan,
                                           timeout,
                                           errors,
                                           hosts,
                                           retries)
        cql = statement.cql

        # Get the prepared statement id for this statement from our cache if possible. We are optimistic
        # that the statement has previously been prepared on all hosts, so the id will be valid. However, if
        # we're in the midst of preparing the statement on the given host, we know that executing with the id
        # will fail. So, act like we don't have the prepared-statement id in that case.

        id = synchronize { @preparing_statements[host][cql] ? nil : @prepared_statements[cql] }

        if id
          request.id = id
          do_send_request_by_plan(host,
                                  connection,
                                  promise,
                                  keyspace,
                                  statement,
                                  options,
                                  request,
                                  plan,
                                  timeout,
                                  errors,
                                  hosts,
                                  retries)
        else
          prepare = prepare_statement(host, connection, cql, timeout)
          prepare.on_complete do |_|
            if prepare.resolved?
              request.id = prepare.value
              do_send_request_by_plan(host,
                                      connection,
                                      promise,
                                      keyspace,
                                      statement,
                                      options,
                                      request,
                                      plan,
                                      timeout,
                                      errors,
                                      hosts,
                                      retries)
            else
              prepare.on_failure do |e|
                if e.is_a?(Errors::HostError) ||
                   (e.is_a?(Errors::TimeoutError) && statement.idempotent?)
                  errors ||= {}
                  errors[host] = e
                  execute_by_plan(promise,
                                  keyspace,
                                  statement,
                                  options,
                                  request,
                                  plan,
                                  timeout,
                                  errors,
                                  hosts,
                                  retries)
                else
                  promise.break(e)
                end
              end
            end
          end
        end
      rescue => e
        promise.break(e)
      end

      def batch_by_plan(promise,
                        keyspace,
                        statement,
                        options,
                        request,
                        plan,
                        timeout,
                        errors = nil,
                        hosts = [],
                        retries = -1)
        unless plan.has_next?
          promise.break(Errors::NoHostsAvailable.new(errors))
          return
        end

        hosts << host = plan.next
        retries += 1
        pool = nil
        synchronize { pool = @connections[host] }

        unless pool
          errors ||= {}
          errors[host] = NOT_CONNECTED
          return batch_by_plan(promise,
                               keyspace,
                               statement,
                               options,
                               request,
                               plan,
                               timeout,
                               errors,
                               hosts,
                               retries)
        end

        connection = pool.random_connection

        if keyspace && connection.keyspace != keyspace
          switch = switch_keyspace(connection, keyspace, timeout)
          switch.on_complete do |s|
            if s.resolved?
              batch_and_send_request_by_plan(host,
                                             connection,
                                             promise,
                                             keyspace,
                                             statement,
                                             request,
                                             options,
                                             plan,
                                             timeout,
                                             errors,
                                             hosts,
                                             retries)
            else
              s.on_failure do |e|
                if e.is_a?(Errors::HostError) ||
                   (e.is_a?(Errors::TimeoutError) && statement.idempotent?)
                  errors ||= {}
                  errors[host] = e
                  batch_by_plan(promise,
                                keyspace,
                                statement,
                                options,
                                request,
                                plan,
                                timeout,
                                errors,
                                hosts,
                                retries)
                else
                  promise.break(e)
                end
              end
            end
          end
        else
          batch_and_send_request_by_plan(host,
                                         connection,
                                         promise,
                                         keyspace,
                                         statement,
                                         request,
                                         options,
                                         plan,
                                         timeout,
                                         errors,
                                         hosts,
                                         retries)
        end
      rescue => e
        errors ||= {}
        errors[host] = e
        batch_by_plan(promise,
                      keyspace,
                      statement,
                      options,
                      request,
                      plan,
                      timeout,
                      errors,
                      hosts,
                      retries)
      end

      def batch_and_send_request_by_plan(host,
                                         connection,
                                         promise,
                                         keyspace,
                                         batch_statement,
                                         request,
                                         options,
                                         plan,
                                         timeout,
                                         errors,
                                         hosts,
                                         retries)
        request.clear
        unprepared = Hash.new {|hash, cql| hash[cql] = []}

        batch_statement.statements.each do |statement|
          cql = statement.cql

          if statement.is_a?(Statements::Bound)
            # Get the prepared statement id for this statement from our cache if possible. We are optimistic
            # that the statement has previously been prepared on all hosts, so the id will be valid. However, if
            # we're in the midst of preparing the statement on the given host, we know that executing with the id
            # will fail. So, act like we don't have the prepared-statement id in that case.

            id = synchronize { @preparing_statements[host][cql] ? nil : @prepared_statements[cql] }

            if id
              request.add_prepared(id, statement.params, statement.params_types)
            else
              unprepared[cql] << statement
            end
          else
            request.add_query(cql, statement.params, statement.params_types)
          end
        end

        if unprepared.empty?
          do_send_request_by_plan(host,
                                  connection,
                                  promise,
                                  keyspace,
                                  batch_statement,
                                  options,
                                  request,
                                  plan,
                                  timeout,
                                  errors,
                                  hosts,
                                  retries)
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
                  request.add_prepared(prepared_ids[i],
                                       statement.params,
                                       statement.params_types)
                end
              end

              do_send_request_by_plan(host,
                                      connection,
                                      promise,
                                      keyspace,
                                      batch_statement,
                                      options,
                                      request,
                                      plan,
                                      timeout,
                                      errors,
                                      hosts,
                                      retries)
            else
              f.on_failure do |e|
                if e.is_a?(Errors::HostError) ||
                   (e.is_a?(Errors::TimeoutError) && batch_statement.idempotent?)
                  errors ||= {}
                  errors[host] = e
                  batch_by_plan(promise,
                                keyspace,
                                batch_statement,
                                options,
                                request,
                                plan,
                                timeout,
                                errors,
                                hosts,
                                retries)
                else
                  promise.break(e)
                end
              end
            end
          end
        end
      end

      def send_request_by_plan(promise,
                               keyspace,
                               statement,
                               options,
                               request,
                               plan,
                               timeout,
                               errors = nil,
                               hosts = [],
                               retries = -1)
        unless plan.has_next?
          promise.break(Errors::NoHostsAvailable.new(errors))
          return
        end

        hosts << host = plan.next
        retries += 1
        pool = nil
        synchronize { pool = @connections[host] }

        unless pool
          errors ||= {}
          errors[host] = NOT_CONNECTED
          return send_request_by_plan(promise,
                                      keyspace,
                                      statement,
                                      options,
                                      request,
                                      plan,
                                      timeout,
                                      errors,
                                      hosts,
                                      retries)
        end

        connection = pool.random_connection

        if keyspace && connection.keyspace != keyspace
          switch = switch_keyspace(connection, keyspace, timeout)
          switch.on_complete do |s|
            if s.resolved?
              do_send_request_by_plan(host,
                                      connection,
                                      promise,
                                      keyspace,
                                      statement,
                                      options,
                                      request,
                                      plan,
                                      timeout,
                                      errors,
                                      hosts,
                                      retries)
            else
              s.on_failure do |e|
                if e.is_a?(Errors::HostError) ||
                   (e.is_a?(Errors::TimeoutError) && statement.idempotent?)
                  errors ||= {}
                  errors[host] = e
                  send_request_by_plan(promise,
                                       keyspace,
                                       statement,
                                       options,
                                       request,
                                       plan,
                                       timeout,
                                       errors,
                                       hosts,
                                       retries)
                else
                  promise.break(e)
                end
              end
            end
          end
        else
          do_send_request_by_plan(host,
                                  connection,
                                  promise,
                                  keyspace,
                                  statement,
                                  options,
                                  request,
                                  plan,
                                  timeout,
                                  errors,
                                  hosts,
                                  retries)
        end
      rescue => e
        errors ||= {}
        errors[host] = e
        send_request_by_plan(promise,
                             keyspace,
                             statement,
                             options,
                             request,
                             plan,
                             timeout,
                             errors,
                             hosts,
                             retries)
      end

      def do_send_request_by_plan(host,
                                  connection,
                                  promise,
                                  keyspace,
                                  statement,
                                  options,
                                  request,
                                  plan,
                                  timeout,
                                  errors,
                                  hosts,
                                  retries)
        request.retries = retries

        f = connection.send_request(request, timeout)
        f.on_complete do |response_future|
          errors ||= {}
          handle_response(response_future,
                          host,
                          connection,
                          promise,
                          keyspace,
                          statement,
                          options,
                          request,
                          plan,
                          timeout,
                          errors,
                          hosts,
                          retries)
        end
      rescue => e
        promise.break(e)
      end

      def handle_response(response_future,
                          host,
                          connection,
                          promise,
                          keyspace,
                          statement,
                          options,
                          request,
                          plan,
                          timeout,
                          errors,
                          hosts,
                          retries)
        if response_future.resolved?
          r = response_future.value

          begin
            decision = nil

            case r
            when Protocol::UnavailableErrorResponse
              decision = options.retry_policy.unavailable(statement,
                                                          r.consistency,
                                                          r.required,
                                                          r.alive,
                                                          retries)
            when Protocol::WriteTimeoutErrorResponse
              decision = options.retry_policy.write_timeout(statement,
                                                            r.consistency,
                                                            r.write_type,
                                                            r.blockfor,
                                                            r.received,
                                                            retries)
            when Protocol::ReadTimeoutErrorResponse
              decision = options.retry_policy.read_timeout(statement,
                                                           r.consistency,
                                                           r.blockfor,
                                                           r.received,
                                                           r.data_present,
                                                           retries)
            when Protocol::UnpreparedErrorResponse
              cql = nil
              if statement.is_a?(Cassandra::Statements::Batch)
                # Find the prepared statement with the prepared-statement-id reported by the node.
                unprepared_child = statement.statements.select do |s|
                  (s.is_a?(Cassandra::Statements::Prepared) || s.is_a?(Cassandra::Statements::Bound)) && s.id == r.id
                end.first
                cql = unprepared_child ? unprepared_child.cql : nil
              else
                # This is a normal statement, so we have everything we need.
                cql = statement.cql
                synchronize { @preparing_statements[host].delete(cql) }
              end

              prepare = prepare_statement(host, connection, cql, timeout)
              prepare.on_complete do |_|
                if prepare.resolved?
                  request.id = prepare.value unless request.is_a?(Cassandra::Protocol::BatchRequest)
                  do_send_request_by_plan(host,
                                          connection,
                                          promise,
                                          keyspace,
                                          statement,
                                          options,
                                          request,
                                          plan,
                                          timeout,
                                          errors,
                                          hosts,
                                          retries)
                else
                  prepare.on_failure do |e|
                    if e.is_a?(Errors::HostError) ||
                       (e.is_a?(Errors::TimeoutError) && statement.idempotent?)
                      errors[host] = e
                      execute_by_plan(promise,
                                      keyspace,
                                      statement,
                                      options,
                                      request,
                                      plan,
                                      timeout,
                                      errors,
                                      hosts,
                                      retries)
                    else
                      promise.break(e)
                    end
                  end
                end
              end
            when Protocol::ErrorResponse
              error = r.to_error(keyspace,
                                 statement,
                                 options,
                                 hosts,
                                 request.consistency,
                                 retries)

              if error.is_a?(Errors::HostError) ||
                 (error.is_a?(Errors::TimeoutError) && statement.idempotent?)
                errors[host] = error

                case request
                when Protocol::QueryRequest, Protocol::PrepareRequest
                  send_request_by_plan(promise,
                                       keyspace,
                                       statement,
                                       options,
                                       request,
                                       plan,
                                       timeout,
                                       errors,
                                       hosts,
                                       retries)
                when Protocol::ExecuteRequest
                  execute_by_plan(promise,
                                  keyspace,
                                  statement,
                                  options,
                                  request,
                                  plan,
                                  timeout,
                                  errors,
                                  hosts,
                                  retries)
                when Protocol::BatchRequest
                  batch_by_plan(promise,
                                keyspace,
                                statement,
                                options,
                                request,
                                plan,
                                timeout,
                                errors,
                                hosts,
                                retries)
                end
              else
                promise.break(error)
              end
            when Protocol::SetKeyspaceResultResponse
              @keyspace = r.keyspace
              promise.fulfill(Cassandra::Results::Void.new(r.custom_payload,
                                                           r.warnings,
                                                           r.trace_id,
                                                           keyspace,
                                                           statement,
                                                           options,
                                                           hosts,
                                                           request.consistency,
                                                           retries,
                                                           self,
                                                           @futures))
            when Protocol::PreparedResultResponse
              cql = request.cql
              synchronize do
                @prepared_statements[cql] = r.id
                @preparing_statements[host].delete(cql)
              end

              metadata = r.metadata
              pk_idx = r.pk_idx
              pk_idx ||= @schema.get_pk_idx(metadata)

              promise.fulfill(
                Statements::Prepared.new(r.id,
                                         r.custom_payload,
                                         r.warnings,
                                         cql,
                                         metadata,
                                         r.result_metadata,
                                         pk_idx,
                                         r.trace_id,
                                         keyspace,
                                         statement,
                                         options,
                                         hosts,
                                         request.consistency,
                                         retries,
                                         self,
                                         @connection_options)
              )
            when Protocol::RawRowsResultResponse
              r.materialize(statement.result_metadata)
              promise.fulfill(
                Results::Paged.new(r.custom_payload,
                                   r.warnings,
                                   r.rows,
                                   r.paging_state,
                                   r.trace_id,
                                   keyspace,
                                   statement,
                                   options,
                                   hosts,
                                   request.consistency,
                                   retries,
                                   self,
                                   @futures)
              )
            when Protocol::RowsResultResponse
              promise.fulfill(
                Results::Paged.new(r.custom_payload,
                                   r.warnings,
                                   r.rows,
                                   r.paging_state,
                                   r.trace_id,
                                   keyspace,
                                   statement,
                                   options,
                                   hosts,
                                   request.consistency,
                                   retries,
                                   self,
                                   @futures)
              )
            when Protocol::SchemaChangeResultResponse
              if r.change == 'DROPPED' &&
                 r.target == Protocol::Constants::SCHEMA_CHANGE_TARGET_KEYSPACE
                @schema.delete_keyspace(r.keyspace)
              end

              @logger.debug('Waiting for schema to propagate to all hosts after a change')
              wait_for_schema_agreement(connection,
                                        @reconnection_policy.schedule).on_complete do |f|
                unless f.resolved?
                  f.on_failure do |e|
                    @logger.error(
                      "Schema agreement failure (#{e.class.name}: #{e.message})"
                    )
                  end
                end
                promise.fulfill(
                  Results::Void.new(r.custom_payload,
                                    r.warnings,
                                    r.trace_id,
                                    keyspace,
                                    statement,
                                    options,
                                    hosts,
                                    request.consistency,
                                    retries,
                                    self,
                                    @futures)
                )
              end
            else
              promise.fulfill(Results::Void.new(r.custom_payload,
                                                r.warnings,
                                                r.trace_id,
                                                keyspace,
                                                statement,
                                                options,
                                                hosts,
                                                request.consistency,
                                                retries,
                                                self,
                                                @futures))
            end

            if decision
              case decision
              when Retry::Decisions::Retry
                request.consistency = decision.consistency
                do_send_request_by_plan(host,
                                        connection,
                                        promise,
                                        keyspace,
                                        statement,
                                        options,
                                        request,
                                        plan,
                                        timeout,
                                        errors,
                                        hosts,
                                        retries + 1)
              when Retry::Decisions::TryNextHost
                errors[host] = r.to_error(keyspace,
                                          statement,
                                          options,
                                          hosts,
                                          request.consistency,
                                          retries)
                case request
                when Protocol::QueryRequest, Protocol::PrepareRequest
                  send_request_by_plan(promise,
                                       keyspace,
                                       statement,
                                       options,
                                       request,
                                       plan,
                                       timeout,
                                       errors,
                                       hosts,
                                       retries)
                when Protocol::ExecuteRequest
                  execute_by_plan(promise,
                                  keyspace,
                                  statement,
                                  options,
                                  request,
                                  plan,
                                  timeout,
                                  errors,
                                  hosts,
                                  retries)
                when Protocol::BatchRequest
                  batch_by_plan(promise,
                                keyspace,
                                statement,
                                options,
                                request,
                                plan,
                                timeout,
                                errors,
                                hosts,
                                retries)
                else
                  promise.break(e)
                end
              when Retry::Decisions::Ignore
                promise.fulfill(
                  Results::Void.new(r.custom_payload,
                                    r.warnings,
                                    nil,
                                    keyspace,
                                    statement,
                                    options,
                                    hosts,
                                    request.consistency,
                                    retries,
                                    self,
                                    @futures)
                )
              when Retry::Decisions::Reraise
                promise.break(
                  r.to_error(keyspace,
                             statement,
                             options,
                             hosts,
                             request.consistency,
                             retries)
                )
              else
                promise.break(
                  r.to_error(keyspace,
                             statement,
                             options,
                             hosts,
                             request.consistency,
                             retries)
                )
              end
            end
          rescue => e
            promise.break(e)
          end
        else
          response_future.on_failure do |ex|
            if ex.is_a?(Errors::HostError) ||
               (ex.is_a?(Errors::TimeoutError) && statement.idempotent?)

              errors[host] = ex
              case request
              when Protocol::QueryRequest, Protocol::PrepareRequest
                send_request_by_plan(promise,
                                     keyspace,
                                     statement,
                                     options,
                                     request,
                                     plan,
                                     timeout,
                                     errors,
                                     hosts,
                                     retries)
              when Protocol::ExecuteRequest
                execute_by_plan(promise,
                                keyspace,
                                statement,
                                options,
                                request,
                                plan,
                                timeout,
                                errors,
                                hosts,
                                retries)
              when Protocol::BatchRequest
                batch_by_plan(promise,
                              keyspace,
                              statement,
                              options,
                              request,
                              plan,
                              timeout,
                              errors,
                              hosts,
                              retries)
              else
                promise.break(ex)
              end
            else
              promise.break(ex)
            end
          end
        end
      end

      def wait_for_schema_agreement(connection, schedule)
        peers_future = send_select_request(connection, SELECT_SCHEMA_PEERS)
        local_future = send_select_request(connection, SELECT_SCHEMA_LOCAL)

        Ione::Future.all(peers_future, local_future).flat_map do |(peers, local)|
          versions = ::Set.new

          unless local.empty?
            host = @registry.host(connection.host)

            if host && @profile_manager.distance(host) != :ignore
              versions << version = local.first['schema_version']
              @logger.debug("Host #{host.ip} schema version is #{version}")
            end
          end

          peers.each do |row|
            host = @registry.host(peer_ip(row))
            next unless host && @profile_manager.distance(host) != :ignore

            versions << version = row['schema_version']
            @logger.debug("Host #{host.ip} schema version is #{version}")
          end

          if versions.one?
            @logger.debug('All hosts have the same schema')
            Ione::Future.resolved
          else
            interval = schedule.next
            @logger.info('Hosts have different schema versions: ' \
              "#{versions.to_a.inspect}, retrying in #{interval} seconds")
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

        request = Protocol::QueryRequest.new("USE #{Util.escape_name(keyspace)}",
                                             EMPTY_LIST,
                                             EMPTY_LIST,
                                             :one)

        f = connection.send_request(request, timeout).map do |r|
          case r
          when Protocol::SetKeyspaceResultResponse
            @keyspace = r.keyspace
            nil
          when Protocol::ErrorResponse
            raise r.to_error(nil,
                             Statements::Simple.new("USE #{Util.escape_name(keyspace)}"),
                             VOID_OPTIONS,
                             EMPTY_LIST,
                             :one,
                             0)
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}"
          end
        end

        connection[:pending_keyspace] = keyspace
        connection[:pending_switch]   = f

        f.on_complete do |_f|
          connection[:pending_switch]   = nil
          connection[:pending_keyspace] = nil
        end

        f
      end

      def prepare_statement(host, connection, cql, timeout)
        synchronize do
          pending = @preparing_statements[host]

          return pending[cql] if pending.key?(cql)
        end

        request = Protocol::PrepareRequest.new(cql, false)

        f = connection.send_request(request, timeout).map do |r|
          case r
          when Protocol::PreparedResultResponse
            id = r.id
            synchronize do
              @prepared_statements[cql] = id
              @preparing_statements[host].delete(cql)
            end
            id
          when Protocol::ErrorResponse
            raise r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}"
          end
        end

        synchronize do
          @preparing_statements[host][cql] = f
        end

        f
      end

      def send_select_request(connection, request)
        connection.send_request(request).map do |r|
          case r
          when Protocol::RowsResultResponse
            r.rows
          when Protocol::ErrorResponse
            raise r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
          else
            raise Errors::InternalError, "Unexpected response #{r.inspect}"
          end
        end
      end
    end
  end
end
