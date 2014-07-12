# encoding: utf-8

module Cql
  class Cluster
    class ControlConnection
      def initialize(io_reactor, request_runner, cluster_registry, builder_settings)
        @io_reactor     = io_reactor
        @request_runner = request_runner
        @cluster        = cluster_registry
        @settings       = builder_settings
      end

      def connect_async
        plan = @settings.load_balancing_policy.plan(nil, VOID_STATEMENT)
        f = connect_to_first_available(plan)
        f.on_value do |connection|
          @connection = connection

          @connection.on_closed do
            @connection = nil
            @settings.logger.debug('Connection closed')
            reconnect
          end
        end
        f = f.flat_map { register_async }
        f = f.flat_map { refresh_hosts_async }
        f
      end

      def close_async
        return Future.resolved if @closed

        @closed = true

        if @connection
          @connection.close
        else
          Future.resolved
        end
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end

      private

      NOT_CONNECTED = NotConnectedError.new("not connected")
      NO_HOSTS      = NoHostsAvailable.new
      SELECT_LOCAL  = Protocol::QueryRequest.new('SELECT rack, data_center, host_id, release_version FROM system.local', nil, nil, :one)
      SELECT_PEERS  = Protocol::QueryRequest.new('SELECT peer, rack, data_center, host_id, rpc_address, release_version FROM system.peers', nil, nil, :one)
      REGISTER      = Protocol::RegisterRequest.new(
                        Protocol::TopologyChangeEventResponse::TYPE,
                        Protocol::StatusChangeEventResponse::TYPE,
                        Protocol::SchemaChangeEventResponse::TYPE
                      )

      def reconnect
        return if @closed

        connect_async.fallback do |e|
          @settings.logger.error('Connection failed: %s: %s' % [e.class.name, e.message])

          timeout = @settings.reconnect_interval

          @settings.logger.debug('Reconnecting in %d seconds' % timeout)

          f = @io_reactor.schedule_timer(timeout)
          f.flat_map { reconnect }
        end
      end

      def register_async
        @request_runner.execute(@connection, REGISTER).map do
          @connection.on_event do |event|
            @settings.logger.debug('Received %s %s event' % [event.type, event.change])

            if event.type == 'SCHEMA_CHANGE'
            else
              case event.change
              when 'UP'
                address = event.address

                refresh_host_async(address) if @cluster.host_known?(address.to_s)
              when 'DOWN'
                address = event.address

                @cluster.host_down(address.to_s)
              when 'NEW_NODE'
                address = event.address

                refresh_host_async(address) unless @cluster.host_known?(address.to_s)
              when 'REMOVED_NODE'
                address = event.address

                @cluster.host_lost(address.to_s)
              end
            end
          end

          self
        end
      end

      def refresh_hosts_async
        @settings.logger.debug('Looking for additional nodes')

        local = @request_runner.execute(@connection, SELECT_LOCAL)
        peers = @request_runner.execute(@connection, SELECT_PEERS)

        Future.all(local, peers).map do |(local, peers)|
          @settings.logger.debug('%d additional nodes found' % peers.size)

          raise NO_HOSTS if local.empty? && peers.empty?

          local_ip = @connection.host
          ips      = ::Set.new

          unless local.empty?
            ips << local_ip
            @cluster.host_found(local_ip, local.first)
          end

          peers.each do |data|
            ip = peer_ip(data)
            ips << ip
            @cluster.host_found(ip, data)
          end

          @cluster.ips.each do |ip|
            @cluster.host_lost(ip) unless ips.include?(ip)
          end

          self
        end
      end

      def refresh_host_async(address)
        ip = address.to_s

        @settings.logger.debug('Fetching node information for %s' % ip)

        if ip == @connection.host
          request = @request_runner.execute(
                      @connection,
                      Protocol::QueryRequest.new(
                        'SELECT rack, data_center, host_id, release_version' \
                        'FROM system.local',
                        nil, nil, :one
                      )
                    )
        else
          request = @request_runner.execute(
                      @connection,
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
        h = plan.next.ip
        f = connect_to_host(h)
        f.fallback do |error|
          errors[h] = error
          connect_to_first_available(plan, errors)
        end
      rescue ::StopIteration
        Future.failed(NoHostsAvailable.new(errors))
      end

      def connect_to_host(host)
        connector = Client::Connector.new([
          Client::ConnectStep.new(@io_reactor, protocol_handler_factory, @settings.port, @settings.connection_timeout, @settings.logger),
          Client::CacheOptionsStep.new,
          Client::InitializeStep.new(@settings.compressor, @settings.logger),
          authentication_step,
          Client::CachePropertiesStep.new,
        ])

        f = connector.connect(host)
        f.fallback do |error|
          if error.is_a?(QueryError) && error.code == 0x0a && @settings.protocol_version > 1
            @settings.logger.warn('Could not connect using protocol version %d (will try again with %d): %s' % [@settings.protocol_version, @settings.protocol_version - 1, error.message])
            @settings.protocol_version -= 1
            connect_to_host(host)
          else
            @settings.logger.error('Connection failed: %s: %s' % [error.class.name, error.message])

            raise error
          end
        end
      end

      def peer_ip(data)
        ip = data['rpc_address'].to_s
        ip = data['peer'].to_s if ip == '0.0.0.0'
        ip
      end

      def protocol_handler_factory
        self.class.protocol_handler_factory(@io_reactor, @settings.protocol_version, @settings.compressor)
      end

      def authentication_step
        if @settings.protocol_version == 1
          Client::CredentialsAuthenticationStep.new(@settings.credentials)
        else
          Client::SaslAuthenticationStep.new(@settings.auth_provider)
        end
      end

      def self.protocol_handler_factory(io_reactor, protocol_version, compressor)
        lambda { |connection| Protocol::CqlProtocolHandler.new(connection, io_reactor, protocol_version, compressor) }
      end
    end
  end
end
