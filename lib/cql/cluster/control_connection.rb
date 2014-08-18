# encoding: utf-8

module Cql
  class Cluster
    # @private
    class ControlConnection
      include MonitorMixin

      def initialize(logger, io_reactor, request_runner, cluster_registry, load_balancing_policy, reconnection_policy, connector, connection_options)
        @logger                = logger
        @io_reactor            = io_reactor
        @request_runner        = request_runner
        @registry              = cluster_registry
        @load_balancing_policy = load_balancing_policy
        @reconnection_policy   = reconnection_policy
        @connector             = connector
        @connection_options    = connection_options
        @refreshing_statuses   = Hash.new(false)
        @status                = :closed

        mon_initialize
      end

      def connect_async
        synchronize do
          return Ione::Future.resolved if @status == :connecting || @status == :connected
          @status = :connecting
        end

        @logger.debug('Establishing control connection')

        @io_reactor.start.flat_map do
          plan = @load_balancing_policy.plan(nil, VOID_STATEMENT, VOID_OPTIONS)
          connect_to_first_available(plan)
        end
      end

      def host_found(host)
      end

      def host_lost(host)
      end

      def host_up(host)
        synchronize do
          @refreshing_statuses.delete(host)

          return connect_async if !@connection && !(@status == :closed || @status == :closed)
        end

        Ione::Future.resolved
      end

      def host_down(host)
        synchronize do
          return Ione::Future.resolved if (@connection && @connection.connected?) || @refreshing_statuses[host]

          @logger.debug("Starting to continuously refresh status for ip=#{host.ip}")
          @refreshing_statuses[host] = true
        end

        refresh_host_status_with_retry(host, @reconnection_policy.schedule)
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

      SELECT_LOCAL  = Protocol::QueryRequest.new('SELECT rack, data_center, host_id, release_version FROM system.local', nil, nil, :one)
      SELECT_PEERS  = Protocol::QueryRequest.new('SELECT peer, rack, data_center, host_id, rpc_address, release_version FROM system.peers', nil, nil, :one)
      REGISTER      = Protocol::RegisterRequest.new(
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
            @logger.debug('Reestablishing control connection')
            plan = @load_balancing_policy.plan(nil, VOID_STATEMENT, VOID_OPTIONS)
            connect_to_first_available(plan)
          else
            @logger.debug('Stopping reconnection')
            Ione::Future.resolved
          end
        end
        f.fallback do
          if synchronize { @status == :reconnecting }
            reconnect_async(schedule)
          else
            @logger.debug('Stopping reconnection')
            return Ione::Future.resolved
          end
        end
      end

      def register_async
        connection = @connection

        return Ione::Future.failed("not connected") if connection.nil?

        @logger.debug('Registering for events')

        @request_runner.execute(connection, REGISTER).map do
          @logger.debug('Registered for events')

          connection.on_event do |event|
            @logger.debug("Event received #{event}")

            if event.type == 'SCHEMA_CHANGE'
            else
              case event.change
              when 'UP'
                address = event.address

                refresh_host_async(address) if @registry.has_host?(address)
              when 'DOWN'
                @registry.host_down(event.address)
              when 'NEW_NODE'
                address = event.address

                refresh_host_async(address) unless @registry.has_host?(address)
              when 'REMOVED_NODE'
                @registry.host_lost(event.address)
              end
            end
          end

          self
        end
      end

      def refresh_hosts_async

        connection = @connection

        return Ione::Future.failed("not connected") if connection.nil?

        @logger.debug('Fetching cluster metadata and peers')

        local = @request_runner.execute(connection, SELECT_LOCAL)
        peers = @request_runner.execute(connection, SELECT_PEERS)

        Ione::Future.all(local, peers).flat_map do |(local, peers)|
          @logger.debug('%d peers found' % peers.size)

          raise NO_HOSTS if local.empty? && peers.empty?

          local_ip = connection.host
          ips      = ::Set.new

          unless local.empty?
            ips << local_ip
            @registry.host_found(IPAddr.new(local_ip), local.first)
          end

          peers.each do |data|
            ip = peer_ip(data)
            ips << ip.to_s
            @registry.host_found(ip, data)
          end

          futures = []

          @registry.each_host do |host|
            if ips.include?(host.ip.to_s)
              futures << refresh_host_status(host) if host.down? && synchronize { !@refreshing_statuses[host] }
            else
              @registry.host_lost(host.ip)
            end
          end

          if futures.empty?
            Ione::Future.resolved(self)
          else
            Ione::Future.all(*futures)
          end
        end
      end

      def refresh_host_status(host)
        @logger.info("Refreshing host status ip=#{host.ip}")
        @connector.connect(host).map do |connection|
          @connector.close(host, connection)
          @logger.info("Refreshed host status ip=#{host.ip}")
        end
      end

      def refresh_host_status_with_retry(host, schedule)
        timeout = schedule.next

        @logger.info("Refreshing host status refresh ip=#{host.ip} in #{timeout}")

        f = @io_reactor.schedule_timer(timeout)
        f.flat_map do
          if synchronize { @refreshing_statuses[host] }
            refresh_host_status(host).fallback do |e|
              refresh_host_status_with_retry(host, schedule)
            end
          else
            Ione::Future.resolved
          end
        end
      end

      def refresh_host_async(address)
        connection = @connection
        return Ione::Future.failed("not connected") if connection.nil?

        ip = address.to_s

        @logger.debug('Fetching node information for %s' % ip)

        if ip == connection.host
          request = @request_runner.execute(
                      connection,
                      Protocol::QueryRequest.new(
                        'SELECT rack, data_center, host_id, release_version' \
                        'FROM system.local',
                        nil, nil, :one
                      )
                    )
        else
          request = @request_runner.execute(
                      connection,
                      Protocol::QueryRequest.new(
                        'SELECT rack, data_center, host_id, rpc_address,' \
                        'release_version FROM system.peers WHERE peer = ?',
                        [address], nil, :one
                      )
                    )
        end

        request.map do |result|
          @registry.host_found(address, result.first) unless result.empty?

          self
        end
      end

      def connect_to_first_available(plan, errors = nil)
        host = plan.next
        @logger.debug("Attempting connection to ip=#{host.ip}")
        f = connect_to_host(host)
        f = f.flat_map do |connection|
          synchronize do
            @status = :connected

            @logger.debug("Control connection established ip=#{connection.host}")
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

                @logger.debug("Control connection closed ip=#{connection.host}")
                @connection = nil
              end

              reconnect_async(@reconnection_policy.schedule) if reconnect
            end
          end

          register_async
        end
        f = f.flat_map { refresh_hosts_async }
        f.fallback do |error|
          if error.is_a?(Errors::AuthenticationError)
            Ione::Future.failed(error)
          elsif error.is_a?(Errors::QueryError)
            if error.code == 0x100
              Ione::Future.failed(Errors::AuthenticationError.new(error.message))
            else
              Ione::Future.failed(error)
            end
          else
            errors  ||= {}
            errors[host] = error
            connect_to_first_available(plan, errors)
          end
        end
      rescue ::StopIteration
        @logger.warn("Control connection failed")
        Ione::Future.failed(Errors::NoHostsAvailable.new(errors || {}))
      end

      def connect_to_host(host)
        @connector.connect(host).fallback do |error|
          if error.is_a?(Errors::QueryError) && error.code == 0x0a && @connection_options.protocol_version > 1
            @logger.warn('Could not connect using protocol version %d (will try again with %d): %s' % [@connection_options.protocol_version, @connection_options.protocol_version - 1, error.message])
            @connection_options.protocol_version -= 1
            connect_to_host(host)
          else
            @logger.error('Connection failed: %s: %s' % [error.class.name, error.message])

            Ione::Future.failed(error)
          end
        end
      end

      def peer_ip(data)
        ip = data['rpc_address']
        ip = data['peer'] if ip == '0.0.0.0'
        ip
      end
    end
  end
end
