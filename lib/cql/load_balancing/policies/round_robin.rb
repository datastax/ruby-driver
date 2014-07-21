# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class RoundRobin
        class Plan
          def initialize(hosts, index)
            @hosts = hosts
            @index = index

            @remaining = hosts.size
            @total     = hosts.size
          end

          def next
            raise ::StopIteration if @remaining == 0

            @remaining -= 1
            index, @index = @index, (@index + 1) % @mod

            @hosts[index]
          end
        end

        include Policy

        def initialize
          @hosts    = ::Set.new
          @position = 0
        end

        def host_up(host)
          @hosts = @hosts.dup.add(host)
          self
        end

        def host_down(host)
          @hosts = @hosts.dup.delete(host)
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

          Plan.new(hosts.to_a, position)
        end
      end
    end
  end
end
