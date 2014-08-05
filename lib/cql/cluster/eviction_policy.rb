# encoding: utf-8

module Cql
  class Cluster
    class EvictionPolicy
      include MonitorMixin

      def initialize(cluster_registry)
        @registry    = cluster_registry
        @connections = Hash.new

        mon_initialize
      end

      def connected(host)
        count = synchronize { @connections[host] }

        if count
          count += 1
        else
          count = 1
          @registry.host_up(host.ip)
        end

        synchronize { @connections[host] = count }

        self
      end

      def disconnected(host, cause)
        count = synchronize { @connections[host] }

        return self unless count

        count -= 1

        if count == 0
          synchronize { @connections.delete(host) }
        else
          synchronize { @connections[host] = count }
        end

        self
      end

      def connection_error(host, error)
        @registry.host_down(host.ip) unless synchronize { @connections[host] }

        self
      end
    end
  end
end
