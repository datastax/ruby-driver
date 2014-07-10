# encoding: utf-8

module Cql
  class Cluster
    class State
      def initialize(hosts, clients)
        @hosts   = hosts
        @clients = clients
      end

      def has_clients?
        !@clients.empty?
      end

      def each_client(&block)
        @clients.each(&block)
      end

      def remove_client(client)
        @clients.delete(client)

        self
      end

      def add_client(client)
        @clients << client

        self
      end

      def host_known?(ip)
        @hosts.has_key?(ip)
      end

      def host_found(ip, data)
        if @hosts.has_key?(ip)
          host                 = @hosts[ip]
          host.id              = data['host_id']
          host.release_version = data['release_version']

          rack       = data['rack']
          datacenter = data['data_center']

          if rack == host.rack && datacenter == host.datacenter
            if host.down?
              host.up!

              @clients.each do |client|
                client.host_up(host)
              end
            end
          else
            if host.down?
              @clients.each do |client|
                client.host_lost(host)
              end
            else
              host.down!

              @clients.each do |client|
                client.host_down(host)
                client.host_lost(host)
              end
            end

            host.rack       = rack
            host.datacenter = datacenter

            host.up!

            @clients.each do |client|
              client.host_found(host)
              client.host_up(host)
            end
          end
        else
          host = @hosts[ip] = Host.new(ip, data)

          @clients.each do |client|
            client.host_found(host)
            client.host_up(host)
          end
        end

        self
      end

      def host_down(ip)
        host = @hosts[ip]

        return unless host && !host.down?

        host.down!

        @clients.each do |client|
          client.host_down(host)
        end

        self
      end

      def host_up(ip)
        host = @hosts[ip]

        return unless host && !host.up?

        host.up!

        @clients.each do |client|
          client.host_up(host)
        end

        self
      end

      def host_lost(ip)
        host = @hosts.delete(ip) { return self }

        unless host.down?
          host.down!

          @clients.each do |client|
            client.host_down(host)
          end
        end

        @clients.each do |client|
          client.host_lost(host)
        end

        self
      end

      def ips
        @hosts.keys
      end

      def hosts
        @hosts.map {|_, h| Cql::Host.new(h.ip, h.id, h.rack, h.datacenter, h.release_version, h.status)}
      end
    end
  end
end
