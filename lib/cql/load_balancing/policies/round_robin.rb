# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class RoundRobin
        # @private
        class Plan
          def initialize(hosts, index)
            @hosts = hosts
            @index = index

            @total = @remaining = hosts.size
          end

          def next
            raise ::StopIteration if @remaining == 0

            @remaining -= 1
            index, @index = @index, (@index + 1) % @total

            @hosts[index]
          end
        end

        include Policy, MonitorMixin

        def initialize
          @hosts    = ::Array.new
          @position = 0

          mon_initialize
        end

        def host_up(host)
          @hosts = synchronize { @hosts.dup.push(host) }

          self
        end

        def host_down(host)
          @hosts = synchronize do
            hosts = @hosts.dup
            hosts.delete(host)
            hosts
          end

          self
        end

        def host_found(host)
          self
        end

        def host_lost(host)
          self
        end

        def distance(host)
          @hosts.include?(host) ? local : ignore
        end

        def plan(keyspace, statement, options)
          hosts    = @hosts
          position = @position
          total    = hosts.size
          return EMPTY_PLAN if total == 0

          @position = (position + 1) % total

          Plan.new(hosts, position)
        end
      end
    end
  end
end
