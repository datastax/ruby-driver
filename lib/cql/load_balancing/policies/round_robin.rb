# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class RoundRobin
        NO_HOSTS = [].to_enum.freeze
        include Policy

        def initialize
          @hosts    = ::Hash.new
          @position = 0
        end

        def host_up(host)
          @hosts[host] = true
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
          @hosts.has_key?(host) ? local : ignore
        end

        def plan(keyspace, request)
          return NO_HOSTS if @hosts.empty?
          plan = @hosts.keys
          plan.rotate!(@position)
          @position = (@position + 1) % @hosts.size
          plan.to_enum
        end
      end
    end
  end
end
