# encoding: utf-8

module Cql
  class Cluster
    class ControlConnection
      include MonitorMixin

      def initialize(logger, io_reactor, request_runner, cluster_registry, load_balancing_policy, reconnection_policy, driver)
        @logger                = logger
        @io_reactor            = io_reactor
        @request_runner        = request_runner
        @cluster               = cluster_registry
        @load_balancing_policy = load_balancing_policy
        @reconnection_policy   = reconnection_policy
        @driver                = driver

        mon_initialize
      end

      def connect_async
        plan = @load_balancing_policy.plan(nil, VOID_STATEMENT)

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
          return Future.resolved if @closed
          @closed = true
        end
        @io_reactor.stop
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end

      private

      NOT_CONNECTED = NotConnectedError.new("not connected")
      SELECT_LOCAL  = Protocol::QueryRequest.new('SELECT rack, data_center, host_id, release_version FROM system.local', nil, nil, :one)
      SELECT_PEERS  = Protocol::QueryRequest.new('SELECT peer, rack, data_center, host_id, rpc_address, release_version FROM system.peers', nil, nil, :one)
      REGISTER      = Protocol::RegisterRequest.new(
                        Protocol::TopologyChangeEventResponse::TYPE,
                        Protocol::StatusChangeEventResponse::TYPE,
                        Protocol::SchemaChangeEventResponse::TYPE
                      )

      def reconnect(schedule)
        synchronize do
          return Future.failed("closed") if @closed
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
          return Future.failed("not connected") if @connection.nil?
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

                refresh_host_async(address) if @cluster.host_known?(address)
              when 'DOWN'
                address = event.address

                @cluster.host_down(address)
              when 'NEW_NODE'
                address = event.address

                refresh_host_async(address) unless @cluster.host_known?(address)
              when 'REMOVED_NODE'
                address = event.address

                @cluster.host_lost(address)
              end
            end
          end

          self
        end
      end

      def refresh_hosts_async
        connection = synchronize do
          return Future.failed("not connected") if @connection.nil?

          @connection
        end

        @logger.debug('Looking for additional nodes')

        local = @request_runner.execute(connection, SELECT_LOCAL)
        peers = @request_runner.execute(connection, SELECT_PEERS)

        Future.all(local, peers).map do |(local, peers)|
          @logger.debug('%d additional nodes found' % peers.size)

          raise NO_HOSTS if local.empty? && peers.empty?

          local_ip = connection.host
          ips      = ::Set.new

          unless local.empty?
            ips << local_ip
            @cluster.host_found(local_ip, local.first)
          end

          peers.each do |data|
            ip = peer_ip(data)
            ips << ip.to_s
            @cluster.host_found(ip, data)
          end

          @cluster.ips.each do |ip|
            @cluster.host_lost(ip) unless ips.include?(ip)
          end

          self
        end
      end

      def refresh_host_async(address)
        connection = synchronize do
          return Future.failed("not connected") if @connection.nil?

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
          @cluster.host_found(ip, result.first) unless result.empty?

          self
        end
      end

      def connect_to_first_available(plan, errors = {})
        h = plan.next
        f = connect_to_host(h.ip.to_s)
        f.fallback do |error|
          raise error if error.is_a?(AuthenticationError)

          if error.is_a?(Cql::QueryError)
            if error.code == 0x100
              raise AuthenticationError.new(error.message)
            else
              raise error
            end
          end

          errors[h] = error
          connect_to_first_available(plan, errors)
        end
      rescue ::StopIteration
        Future.failed(NoHostsAvailable.new(errors))
      end

      def connect_to_host(host)
        connector = Cql::Client::Connector.new([
          Cql::Client::ConnectStep.new(@io_reactor, protocol_handler_factory, @driver.port, @driver.connection_timeout, @logger),
          Cql::Client::CacheOptionsStep.new,
          Cql::Client::InitializeStep.new(@driver.compressor, @logger),
          authentication_step,
          Cql::Client::CachePropertiesStep.new,
        ])

        f = connector.connect(host)
        f.fallback do |error|
          if error.is_a?(QueryError) && error.code == 0x0a && @driver.protocol_version > 1
            @logger.warn('Could not connect using protocol version %d (will try again with %d): %s' % [@driver.protocol_version, @driver.protocol_version - 1, error.message])
            @driver.protocol_version -= 1
            connect_to_host(host)
          else
            @logger.error('Connection failed: %s: %s' % [error.class.name, error.message])

            raise error
          end
        end
      end

      def peer_ip(data)
        ip = data['rpc_address']
        ip = data['peer'] if ip == '0.0.0.0'
        ip
      end

      def protocol_handler_factory
        self.class.protocol_handler_factory(@io_reactor, @driver.protocol_version, @driver.compressor)
      end

      def authentication_step
        if @driver.protocol_version == 1
          Cql::Client::CredentialsAuthenticationStep.new(@driver.credentials)
        else
          Cql::Client::SaslAuthenticationStep.new(@driver.auth_provider)
        end
      end

      def self.protocol_handler_factory(io_reactor, protocol_version, compressor)
        lambda { |connection| Protocol::CqlProtocolHandler.new(connection, io_reactor, protocol_version, compressor) }
      end
    end
  end
end
