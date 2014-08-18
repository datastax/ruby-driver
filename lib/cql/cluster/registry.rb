# encoding: utf-8

module Cql
  class Cluster
    # @private
    class Registry
      include MonitorMixin

      LISTENER_METHODS = [:host_found, :host_lost, :host_up, :host_down].freeze

      def initialize
        @hosts     = ::Hash.new
        @listeners = ::Set.new

        mon_initialize
      end

      def add_listener(listener)
        synchronize { @listeners = @listeners.dup.add(listener) }

        self
      end

      def remove_listener(listener)
        synchronize { @listeners = @listeners.dup.delete(listener) }

        self
      end

      def each_host(&block)
        @hosts.values.each(&block)
      end
      alias :hosts :each_host

      def host(address)
        @hosts[address.to_s]
      end

      def has_host?(address)
        @hosts.has_key?(address.to_s)
      end

      def host_found(address, data = {})
        ip   = address.to_s
        host = @hosts[ip]

        if host
          if host.id              == data['host_id']         &&
             host.release_version == data['release_version'] &&
             host.rack            == data['rack']            &&
             host.datacenter      == data['data_center']

            return self
          else
            host = toggle_down(host) if host.up?

            @listeners.each do |listener|
              listener.host_lost(host) rescue nil
            end

            host = create_host(address, data)

            @listeners.each do |listener|
              listener.host_found(host) rescue nil
              listener.host_up(host) rescue nil
            end
          end
        else
          host = create_host(address, data)

          @listeners.each do |listener|
            listener.host_found(host) rescue nil
            listener.host_up(host) rescue nil
          end
        end

        synchronize { @hosts = @hosts.merge(ip => host) }

        self
      end

      def host_down(address)
        ip   = address.to_s
        host = @hosts[ip]

        return self unless host && !host.down?

        host = toggle_down(host)

        synchronize { @hosts = @hosts.merge(ip => host) }

        self
      end

      def host_up(address)
        ip   = address.to_s
        host = @hosts[ip]

        return self unless host && !host.up?

        host = toggle_up(host)

        synchronize { @hosts = @hosts.merge(ip => host) }

        self
      end

      def host_lost(address)
        ip   = address.to_s
        host = nil

        return self unless @hosts.has_key?(ip)

        synchronize do
          hosts  = @hosts.dup
          host   = hosts.delete(ip)
          @hosts = hosts
        end

        host = toggle_down(host) if host.up?

        @listeners.each do |listener|
          listener.host_lost(host) rescue nil
        end

        self
      end

      private

      def create_host(ip, data)
        Host.new(ip, data['host_id'], data['rack'], data['data_center'], data['release_version'], :up)
      end

      def toggle_up(host)
        host = Host.new(host.ip, host.id, host.rack, host.datacenter, host.release_version, :up)
        @listeners.each do |listener|
          listener.host_up(host) rescue nil
        end
        host
      end

      def toggle_down(host)
        host = Host.new(host.ip, host.id, host.rack, host.datacenter, host.release_version, :down)
        @listeners.each do |listener|
          listener.host_down(host) rescue nil
        end
        host
      end
    end
  end
end
