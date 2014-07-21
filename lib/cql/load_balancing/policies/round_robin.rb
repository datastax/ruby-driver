# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class RoundRobin
        class Plan
          def initialize(hosts)
            @hosts  = hosts
            @index  = 0
            @max    = hosts.size
          end

          def next
            if @index == @max
              raise ::StopIteration
            else
              index, @index = @index, @index + 1

              @hosts[index]
            end
          end
        end

        class EmptyPlan
          def next
            raise ::StopIteration
          end
        end

        NO_HOSTS = EmptyPlan.new
        include Policy

        def initialize
          @hosts    = ::Set.new
          @position = 0
        end

        def host_up(host)
          @hosts.add(host)
          self
        end

        def host_down(host)
          @hosts.delete(host)
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
          return NO_HOSTS if @hosts.empty?
          position, @position = @position, (@position + 1) % @hosts.size
          Plan.new(@hosts.to_a.rotate!(position))
        end
      end
    end
  end
end
