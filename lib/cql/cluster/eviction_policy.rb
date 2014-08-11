# encoding: utf-8

module Cql
  class Cluster
    # @private
    class EvictionPolicy
      include MonitorMixin

      MAX_RETRIES = 10

      def initialize(cluster_registry)
        @registry    = cluster_registry
        @connections = Hash.new
        @retries     = Hash.new

        mon_initialize
      end

      def connected(host)
        notify = false

        synchronize do
          count = @connections[host]

          if count
            count += 1

            @connections[host] = count
          else
            count  = 1
            notify = true

            @retries.delete(host)
            @connections[host] = count
          end
        end

        @registry.host_up(host.ip) if notify

        self
      end

      def disconnected(host, cause)
        synchronize do
          count = @connections[host]

          return self unless count

          count -= 1

          if count == 0
            @connections.delete(host)
          else
            @connections[host] = count
          end
        end

        self
      end

      def connection_error(host, error)
        notify = false

        synchronize do
          unless @connections[host]
            count = @retries[host]

            if count
              if count >= MAX_RETRIES
                @retries.delete(host)

                notify = true
              else
                @retries[host] = count + 1
              end
            else
              @retries[host] = 1
            end
          end
        end

        @registry.host_down(host.ip) if notify

        self
      end
    end
  end
end
