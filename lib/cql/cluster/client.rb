# encoding: utf-8

module Cql
  class Cluster
    class Client
      include MonitorMixin

      def initialize(driver)
        @driver                      = driver
        @logger                      = driver.logger
        @registry                    = driver.cluster_registry
        @reactor                     = driver.io_reactor
        @request_runner              = driver.request_runner
        @load_balancing_policy       = driver.load_balancing_policy
        @reconnection_policy         = driver.reconnection_policy
        @retry_policy                = driver.retry_policy
        @connections_per_local_node  = driver.connections_per_local_node
        @connections_per_remote_node = driver.connections_per_remote_node
        @connecting_hosts            = ::Set.new
        @connections                 = ::Hash.new
        @prepared_statements         = ::Hash.new
        @keyspace                    = nil
        @state                       = :idle

        mon_initialize
      end

      def connect
        synchronize do
          return CLIENT_CLOSED     if @state == :closed || @state == :closing
          return @connected_future if @state == :connecting || @state == :connected

          @state = :connecting

          @connected_future = begin
            futures = @registry.hosts.map do |host|
              @connecting_hosts << host
              distance = @load_balancing_policy.distance(host)

              f = connect_to_host(host, distance)
              f.recover do |error|
                @connecting_hosts.delete(host)
                Cql::Client::FailedConnection.new(error, host)
              end
            end

            Future.all(*futures).map do |connections|
              connections.flatten!
              raise NO_HOSTS if connections.empty?

              unless connections.any?(&:connected?)
                errors = {}
                connections.each {|c| errors[c.host] = c.error}
                raise NoHostsAvailable.new(errors)
              end

              self
            end
          end
          @connected_future.on_complete(&method(:connected))
          @connected_future
        end
      end

      def close
        synchronize do
          return CLIENT_NOT_CONNECTED if @state == :idle
          return @closed_future if @state == :closed || @state == :closing

          state, @state = @state, :closing

          @closed_future = begin
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
      end

      # These methods shall be called from inside reactor thread only
      def host_found(host)
        nil
      end

      def host_lost(host)
        nil
      end

      def host_up(host)
        return Future.resolved if @connecting_hosts.include?(host)

        @connecting_hosts << host

        f = connect_to_host_maybe_retry(host, @load_balancing_policy.distance(host))
        f.map(nil)
      end

      def host_down(host)
        return Future.resolved if @connecting_hosts.delete?(host) || !@connections.has_key?(host)

        prepared_statements = @prepared_statements.dup
        prepared_statements.delete(host)
        @prepared_statements = prepared_statements

        connections  = @connections.dup
        futures      = connections.delete(host).snapshot.map {|c| c.close}
        @connections = connections

        Future.all(*futures).map(nil)
      end

      def query(statement, options)
        request = Protocol::QueryRequest.new(statement.cql, statement.params, nil, options.consistency, options.serial_consistency, options.page_size, nil, options.trace?)
        timeout = options.timeout

        keyspace = @keyspace
        plan     = @load_balancing_policy.plan(keyspace, statement, options)

        send_request_by_plan(keyspace, statement, request, plan, timeout) do |keyspace, statement, request, response, hosts|
          execution_info = create_execution_info(keyspace, statement, options, request, response, hosts)

          case response
          when Protocol::RowsResultResponse
            Results::Paged.new(response.metadata, response.rows, response.paging_state, execution_info)
          else
            Results::Void.new(execution_info)
          end
        end
      end

      def prepare(cql, options)
        request   = Protocol::PrepareRequest.new(cql, options.trace?)
        timeout   = options.timeout
        statement = VOID_STATEMENT

        keyspace = @keyspace
        plan     = @load_balancing_policy.plan(keyspace, statement, options)

        send_request_by_plan(keyspace, statement, request, plan, timeout) do |keyspace, statement, request, response, hosts|
          execution_info = create_execution_info(keyspace, statement, options, request, response, hosts)

          Statements::Prepared.new(cql, response.metadata, response.result_metadata, execution_info)
        end
      end

      def execute(statement, options, paging_state = nil)
        timeout         = options.timeout
        result_metadata = statement.result_metadata
        request         = Protocol::ExecuteRequest.new(nil, statement.params_metadata, statement.params, result_metadata.nil?, options.consistency, options.serial_consistency, options.page_size, paging_state, options.trace?)

        keyspace = @keyspace
        plan     = @load_balancing_policy.plan(keyspace, statement, options)

        execute_by_plan(keyspace, statement, result_metadata, request, plan, timeout) do |keyspace, statement, request, response, hosts|
          execution_info = create_execution_info(keyspace, statement, options, request, response, hosts)

          case response
          when Protocol::RawRowsResultResponse
            response.materialize(result_metadata)
            Results::Paged.new(result_metadata, response.rows, response.paging_state, execution_info)
          when Protocol::RowsResultResponse
            Results::Paged.new(response.metadata, response.rows, response.paging_state, execution_info)
          else
            Results::Void.new(execution_info)
          end
        end
      end

      def batch(statement, options)
        keyspace = @keyspace
        plan     = @load_balancing_policy.plan(keyspace, statement, options)

        batch_by_plan(keyspace, statement, plan, options) do |keyspace, statement, request, response, hosts|
          execution_info = create_execution_info(keyspace, statement, options, request, response, hosts)

          Results::Void.new(execution_info)
        end
      end

      private

      NO_CONNECTIONS = Future.resolved([])
      BATCH_TYPES    = {
        :logged   => Protocol::BatchRequest::LOGGED_TYPE,
        :unlogged => Protocol::BatchRequest::UNLOGGED_TYPE,
        :counter  => Protocol::BatchRequest::COUNTER_TYPE,
      }.freeze
      CLIENT_CLOSED        = Future.failed(ClientError.new('Cannot connect a closed client'))
      CLIENT_NOT_CONNECTED = Future.failed(ClientError.new('Cannot close a not connected client'))

      def connected(f)
        if f.resolved?
          synchronize do
            @state = :connected
          end

          @registry.add_listener(self)
          @logger.info('Cluster connection complete')
        else
          synchronize do
            @state = :defunct
          end

          f.on_failure do |e|
            @logger.error('Failed connecting to cluster: %s' % e.message)
          end

          close
        end
      end

      def closed(f)
        synchronize do
          @state = :closed

          @registry.remove_listener(self)

          if f.resolved?
            @logger.info('Cluster disconnect complete')
          else
            f.on_failure do |e|
              @logger.error('Cluster disconnect failed: %s' % e.message)
            end
          end
        end
      end

      def close_connections
        futures = @connections.values.flat_map {|m| m.snapshot.map {|c| c.close}}
        Future.all(*futures).map(self)
      end

      def create_cluster_connector
        authentication_step = @driver.protocol_version == 1 ? Cql::Client::CredentialsAuthenticationStep.new(@driver.credentials) : Cql::Client::SaslAuthenticationStep.new(@driver.auth_provider)
        protocol_handler_factory = lambda { |connection| Protocol::CqlProtocolHandler.new(connection, @reactor, @driver.protocol_version, @driver.compressor) }
        Cql::Client::ClusterConnector.new(
          Cql::Client::Connector.new([
            Cql::Client::ConnectStep.new(@reactor, protocol_handler_factory, @driver.port, @driver.connection_timeout, @logger),
            Cql::Client::CacheOptionsStep.new,
            Cql::Client::InitializeStep.new(@driver.compressor, @logger),
            authentication_step,
            Cql::Client::CachePropertiesStep.new,
          ]),
          @logger
        )
      end

      def connect_to_host_maybe_retry(host, distance)
        f = connect_to_host(host, distance)
        f.fallback do |e|
          raise e unless e.is_a?(Io::ConnectionError)

          connect_to_host_with_retry(host, distance, @reconnection_policy.schedule)
        end
      end

      def connect_to_host_with_retry(host, distance, schedule)
        interval = schedule.next

        @logger.debug('Reconnecting in %d seconds' % interval)

        f = @reactor.schedule_timer(interval)
        f.flat_map do
          if @connecting_hosts.include?(host)
            connect_to_host(host, distance).fallback do |e|
              raise e unless e.is_a?(Io::ConnectionError)

              connect_to_host_with_retry(host, distance, schedule)
            end
          else
            NO_CONNECTIONS
          end
        end
      rescue ::StopIteration
        @connecting_hosts.delete(host)
        NO_CONNECTIONS
      end

      def connect_to_host(host, distance)
        return NO_CONNECTIONS if distance.ignore?

        if distance.local?
          pool_size = @connections_per_local_node
        else
          pool_size = @connections_per_remote_node
        end

        f = create_cluster_connector.connect_all([host.ip.to_s], pool_size)
        f.map do |connections|
          @connecting_hosts.delete(host)
          unless @connections.has_key?(host)
            @connections = @connections.merge(host => Cql::Client::ConnectionManager.new)
          end
          @connections[host].add_connections(connections)
          @prepared_statements = @prepared_statements.merge(host => {})
          connections
        end
      end

      def execute_by_plan(keyspace, statement, result_metadata, request, plan, timeout, errors = {}, hosts = [], &block)
        hosts << host = plan.next
        id = @prepared_statements[host][statement.cql]

        if id
          request.id = id
          f = send_request(keyspace, statement, request, timeout, host, result_metadata)
        else
          f = send_request(keyspace, VOID_STATEMENT, Protocol::PrepareRequest.new(statement.cql, false), timeout, host)
          f = f.flat_map do |result|
            request.id = result.id
            send_request(keyspace, statement, request, timeout, host, result_metadata)
          end
        end

        if block_given?
          f = f.map do |r|
            yield(keyspace, statement, request, r, hosts)
          end
        end

        f.fallback do |e|
          raise e if e.is_a?(QueryError)

          errors[host] = e
          execute_by_plan(keyspace, statement, result_metadata, request, plan, timeout, errors, hosts, &block)
        end
      rescue ::KeyError
        retry
      rescue ::StopIteration
        raise NoHostsAvailable.new(errors)
      end

      def batch_by_plan(keyspace, batch, plan, options, errors = {}, hosts = [], &block)
        hosts << host = plan.next
        request = Protocol::BatchRequest.new(BATCH_TYPES[batch.type], options.consistency, options.trace?)
        timeout = options.timeout

        unprepared = Hash.new {|hash, cql| hash[cql] = []}

        batch.statements.each do |statement|
          cql = statement.cql

          if statement.is_a?(Statements::Bound)
            id = @prepared_statements[host][cql]

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
          f = send_request(keyspace, batch, request, timeout, host)
        else
          to_prepare = unprepared.to_a
          futures    = to_prepare.map do |cql, _|
            send_request(keyspace, VOID_STATEMENT, Protocol::PrepareRequest.new(cql, false), timeout, host)
          end

          f = Future.all(*futures).flat_map do |responses|
            to_prepare.each_with_index do |(_, statements), i|
              id = responses[i].id
              statements.each do |statement|
                request.add_prepared(id, statement.params_metadata, statement.params)
              end
            end

            send_request(keyspace, batch, request, timeout, host)
          end
        end

        if block_given?
          f = f.map do |r|
            yield(keyspace, batch, request, r, hosts)
          end
        end

        f.fallback do |e|
          raise e if e.is_a?(QueryError)

          errors[host] = e
          batch_by_plan(keyspace, batch, plan, options, errors, hosts, &block)
        end
      rescue ::KeyError
        retry
      rescue ::StopIteration
        raise NoHostsAvailable.new(errors)
      end

      def send_request_by_plan(keyspace, statement, request, plan, timeout, errors = {}, hosts = [], &block)
        hosts << host = plan.next
        f = send_request(keyspace, statement, request, timeout, host)
        if block_given?
          f = f.map do |r|
            yield(keyspace, statement, request, r, hosts)
          end
        end
        f.fallback do |e|
          raise e if e.is_a?(QueryError)

          errors[host] = e
          send_request_by_plan(keyspace, statement, request, plan, timeout, errors, hosts, &block)
        end
      rescue ::KeyError
        retry
      rescue ::StopIteration
        raise NoHostsAvailable.new(errors)
      end

      def send_request(keyspace, statement, request, timeout, host, response_metadata = nil)
        connection = @connections[host].random_connection

        if keyspace && connection.keyspace != keyspace
          if connection[:pending_keyspace] == keyspace
            connection[:pending_switch].flat_map do
              do_send_request(host, connection, statement, request, timeout, response_metadata)
            end
          else
            f = switch_keyspace(host, connection, keyspace, timeout)

            connection[:pending_switch] = f

            f.flat_map do
              do_send_request(host, connection, statement, request, timeout, response_metadata)
            end
          end
        else
          do_send_request(host, connection, statement, request, timeout, response_metadata)
        end
      end

      def switch_keyspace(host, connection, keyspace, timeout)
        connection[:pending_keyspace] = keyspace
        request = Protocol::QueryRequest.new("USE #{keyspace}", nil, nil, :one)
        do_send_request(host, connection, VOID_STATEMENT, request, timeout, nil)
      end

      def do_send_request(host, connection, statement, request, timeout, response_metadata, retries = 0)
        request.retries = retries

        f = connection.send_request(request, timeout)
        f = f.map do |r|
          case r
          when Protocol::DetailedErrorResponse
            raise QueryError.new(r.code, r.message, statement.cql, r.details)
          when Protocol::ErrorResponse
            raise QueryError.new(r.code, r.message, statement.cql, nil)
          when Protocol::SetKeyspaceResultResponse
            connection[:pending_switch]   = nil
            connection[:pending_keyspace] = nil
            @keyspace = r.keyspace
          when Protocol::PreparedResultResponse
            prepared_statements = @prepared_statements.dup
            prepared_statements[host][request.cql] = r.id
            @prepared_statements = prepared_statements
          end

          r
        end

        f.fallback do |e|
          raise e unless e.is_a?(QueryError)

          details  = e.details
          decision = case e.code
          when 0x1000 # unavailable
            @retry_policy.unavailable(statement, details[:cl], details[:required], details[:alive], retries)
          when 0x1100 # write_timeout
            @retry_policy.write_timeout(statement, details[:cl], details[:write_type], details[:blockfor], details[:received], retries)
          when 0x1200 # read_timeout
            @retry_policy.read_timeout(statement, details[:cl], details[:blockfor], details[:received], details[:data_present], retries)
          else
            raise e
          end

          case decision
          when Retry::Decisions::Retry
            request.consistency = decision.consistency
            do_send_request(host, connection, statement, request, timeout, response_metadata, retries + 1)
          when Retry::Decisions::Ignore
            Future.resolved(Cql::Client::VoidResult::INSTANCE)
          when Retry::Decisions::Reraise
            raise e
          else
            raise e
          end
        end
      end

      def create_execution_info(keyspace, statement, options, request, response, hosts)
        trace_id = response.trace_id
        trace    = trace_id ? Execution::Trace.new(trace_id, self) : nil
        info     = Execution::Info.new(keyspace, statement, options, hosts, request.consistency, request.retries, trace)
      end
    end
  end
end
