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

        mon_initialize
      end

      def connect_async
        @registry.ips.each do |ip|
          @registry.host_up(ip)
        end

        plan = @load_balancing_policy.plan(nil, VOID_STATEMENT, VOID_OPTIONS)

        f = @io_reactor.start
        f = f.flat_map do
          connect_to_first_available(plan)
        end
        f.on_value do |connection|
          synchronize { @connection = connection }

          connection.on_closed do
            @logger.debug('Connection closed')
            synchronize do
              @connection = nil
              reconnect(@reconnection_policy.schedule) unless @closed
            end
          end
        end
        f = f.flat_map { register_async }
        f = f.flat_map { refresh_hosts_async }
        f
      end

      def close_async
        synchronize do
          return Ione::Future.resolved if @closed
          @closed = true
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

      def reconnect(schedule)
        synchronize do
          return Ione::Future.failed("closed") if @closed
        end

        connect_async.fallback do |e|
          @logger.error('Connection failed: %s: %s' % [e.class.name, e.message])

          timeout = schedule.next

          @logger.debug('Reconnecting in %d seconds' % timeout)

          f = @io_reactor.schedule_timer(timeout)
          f.flat_map { reconnect(schedule) }
        end
      end

      def register_async
        connection = synchronize do
          return Ione::Future.failed("not connected") if @connection.nil?
          @connection
        end

        @request_runner.execute(connection, REGISTER).map do
          connection.on_event do |event|
            @logger.debug('Received %s %s event' % [event.type, event.change])

            if event.type == 'SCHEMA_CHANGE'
            else
              case event.change
              when 'UP'
                address = event.address

                refresh_host_async(address) if @registry.host_known?(address)
              when 'DOWN'
                @registry.host_down(event.address)
              when 'NEW_NODE'
                address = event.address

                refresh_host_async(address) unless @registry.host_known?(address)
              when 'REMOVED_NODE'
                @registry.host_lost(event.address)
              end
            end
          end

          self
        end
      end

      def refresh_hosts_async
        connection = synchronize do
          return Ione::Future.failed("not connected") if @connection.nil?

          @connection
        end

        @logger.debug('Looking for additional nodes')

        local = @request_runner.execute(connection, SELECT_LOCAL)
        peers = @request_runner.execute(connection, SELECT_PEERS)

        Ione::Future.all(local, peers).map do |(local, peers)|
          @logger.debug('%d additional nodes found' % peers.size)

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

          @registry.ips.each do |ip|
            @registry.host_lost(ip) unless ips.include?(ip)
          end

          self
        end
      end

      def refresh_host_async(address)
        connection = synchronize do
          return Ione::Future.failed("not connected") if @connection.nil?

          @connection
        end

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
        connect_to_host(host).fallback do |error|
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
        Ione::Future.failed(Errors::NoHostsAvailable.new(errors || {}))
      end

      def connect_to_host(host)
        @connector.connect_to_host(host).fallback do |error|
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
