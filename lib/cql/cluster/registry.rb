# encoding: utf-8

module Cql
  class Cluster
    class Registry
      LISTENER_METHODS = [:host_found, :host_lost, :host_up, :host_down].freeze

      def initialize
        @hosts     = ThreadSafe.new(::Hash.new)
        @listeners = ThreadSafe.new(::Set.new)
      end

      def add_listener(listener)
        raise ::ArgumentError, "registry listener must respond to #{LISTENER_METHODS.inspect}" unless LISTENER_METHODS.all? {|m| listener.respond_to?(m)}

        @listeners << listener

        self
      end

      def remove_listener(listener)
        raise ::ArgumentError, "unknown listener #{listener.inspect}" unless @listeners.delete?(listener)

        self
      end

      def hosts
        @hosts.map {|_, h| Cql::Host.new(h.ip, h.id, h.rack, h.datacenter, h.release_version, h.status)}
      end

      def host_known?(ip)
        @hosts.has_key?(ip)
      end

      def ips
        @hosts.keys
      end

      def host_found(ip, data = {})
        if @hosts.has_key?(ip)
          host                 = @hosts[ip]
          host.id              = data['host_id']
          host.release_version = data['release_version']

          rack       = data['rack']
          datacenter = data['data_center']

          if rack == host.rack && datacenter == host.datacenter
            if host.down?
              host.up!

              @listeners.each do |listener|
                listener.host_up(host)
              end
            end
          else
            if host.down?
              @listeners.each do |listener|
                listener.host_lost(host)
              end
            else
              host.down!

              @listeners.each do |listener|
                listener.host_down(host)
                listener.host_lost(host)
              end
            end

            host.rack       = rack
            host.datacenter = datacenter

            host.up!

            @listeners.each do |listener|
              listener.host_found(host)
              listener.host_up(host)
            end
          end
        else
          host = @hosts[ip] = Host.new(ip, data)

          @listeners.each do |listener|
            listener.host_found(host)
            listener.host_up(host)
          end
        end

        self
      end

      def host_down(ip)
        host = @hosts[ip]

        return unless host && !host.down?

        host.down!

        @listeners.each do |listener|
          listener.host_down(host)
        end

        self
      end

      def host_up(ip)
        host = @hosts[ip]

        return unless host && !host.up?

        host.up!

        @listeners.each do |listener|
          listener.host_up(host)
        end

        self
      end

      def host_lost(ip)
        host = @hosts.delete(ip) { return self }

        unless host.down?
          host.down!

          @listeners.each do |listener|
            listener.host_down(host)
          end
        end

        @listeners.each do |listener|
          listener.host_lost(host)
        end

        self
      end
    end
  end
end
