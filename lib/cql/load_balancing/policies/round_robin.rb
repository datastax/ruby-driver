# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class RoundRobin
        NO_HOSTS = [].to_enum.freeze
        include Policy

        def initialize
          @hosts    = ::Array.new
          @position = 0
        end

        def host_up(host)
          @hosts << host
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
          local
        end

        def plan(keyspace, statement)
          return NO_HOSTS if @hosts.empty?
          plan = @hosts.rotate(@position)
          @position = (@position + 1) % @hosts.size
          plan.to_enum
        end
      end
    end
  end
end
