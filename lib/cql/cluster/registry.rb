# encoding: utf-8

module Cql
  class Cluster
    class Registry
      include MonitorMixin

      LISTENER_METHODS = [:host_found, :host_lost, :host_up, :host_down].freeze

      def initialize
        @hosts     = ::Hash.new
        @listeners = ::Set.new

        mon_initialize
      end

      def add_listener(listener)
        raise ::ArgumentError, "registry listener must respond to #{LISTENER_METHODS.inspect}" unless LISTENER_METHODS.all? {|m| listener.respond_to?(m)}

        synchronize { @listeners << listener }

        self
      end

      def remove_listener(listener)
        success = synchronize { @listeners.delete?(listener) }

        raise ::ArgumentError, "unknown listener #{listener.inspect}" unless success

        self
      end

      def hosts
        synchronize { @hosts.values }
      end

      def host_known?(ip)
        synchronize { @hosts.has_key?(ip) }
      end

      def ips
        synchronize { @hosts.keys }
      end

      def host_found(ip, data = {})
        host      = nil
        listeners = nil

        synchronize do
          host      = @hosts[ip]
          listeners = @listeners.dup
        end

        if host
          if host.id              == data['host_id']         &&
             host.release_version == data['release_version'] &&
             host.rack            == data['rack']            &&
             host.datacenter      == data['data_center']

            return self if host.up?

            host = toggle_up(host, listeners)
          else
            host = toggle_down(host, listeners) if host.up?

            listeners.each do |listener|
              listener.host_lost(host)
            end

            host = create_host(ip, data)

            listeners.each do |listener|
              listener.host_found(host)
              listener.host_up(host)
            end
          end
        else
          host = create_host(ip, data)

          listeners.each do |listener|
            listener.host_found(host)
            listener.host_up(host)
          end
        end

        synchronize { @hosts[ip] = host }

        self
      end

      def host_down(ip)
        host      = nil
        listeners = nil

        synchronize do
          host = @hosts[ip]

          return self unless host && !host.down?

          listeners = @listeners.dup
        end

        host = toggle_down(host, listeners)

        synchronize { @hosts[ip] = host }

        self
      end

      def host_up(ip)
        host      = nil
        listeners = nil

        synchronize do
          host = @hosts[ip]

          return self unless host && !host.up?

          listeners = @listeners.dup
        end

        host = toggle_up(host, listeners)

        synchronize { @hosts[ip] = host }

        self
      end

      def host_lost(ip)
        host      = nil
        listeners = nil

        synchronize do
          host = @hosts.delete(ip) { return self }
          listeners = @listeners.dup
        end

        host = toggle_down(host, listeners) if host.up?

        listeners.each do |listener|
          listener.host_lost(host)
        end

        self
      end

      private

      def create_host(ip, data)
        Host.new(ip, data['host_id'], data['rack'], data['data_center'], data['release_version'], :up)
      end

      def toggle_up(host, listeners)
        host = Host.new(host.ip, host.id, host.rack, host.datacenter, host.release_version, :up)
        listeners.each do |listener|
          listener.host_up(host)
        end
        host
      end

      def toggle_down(host, listeners)
        host = Host.new(host.ip, host.id, host.rack, host.datacenter, host.release_version, :down)
        listeners.each do |listener|
          listener.host_down(host)
        end
        host
      end
    end
  end
end
