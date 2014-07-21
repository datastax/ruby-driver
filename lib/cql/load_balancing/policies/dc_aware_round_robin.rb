# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class DCAwareRoundRobin
        class Plan
          def initialize(local, remote, index)
            @local  = local
            @remote = remote
            @index  = index

            @local_total      = local.size
            @local_remaining  = local.size
            @remote_total     = remote.size
            @remote_remaining = remote.size
          end

          def next
            if @local_remaining > 0
              @local_remaining -= 1
              i, @index = (@index % @local_total), @index + 1

              return @local[i]
            end

            if @remote_remaining > 0
              @remote_remaining -= 1
              i, @index = (@index % @remote_total), @index + 1

              return @remote[i]
            end

            raise ::StopIteration
          end
        end

        include Policy

        def initialize(datacenter, max_remote_hosts_to_use = 0)
          datacenter              = String(datacenter)
          max_remote_hosts_to_use = Integer(max_remote_hosts_to_use)

          raise ::ArgumentError, "datacenter cannot be nil" if datacenter.nil?
          raise ::ArgumentError, "max_remote_hosts_to_use must be >= 0" if max_remote_hosts_to_use < 0

          @datacenter = datacenter
          @max_remote = max_remote_hosts_to_use
          @local      = ::Set.new
          @remote     = ::Set.new
          @position   = 0
        end

        def host_up(host)
          if host.datacenter.nil? || host.datacenter == @datacenter
            @local  = @local.dup.add(host)
          else
            @remote = @remote.dup.add(host) if @remote.size < @max_remote
          end

          self
        end

        def host_down(host)
          if host.datacenter == @datacenter
            @local  = @local.dup.delete(host)
          else
            @remote = @remote.dup.delete(host)
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
          if host.datacenter == @datacenter
            @local.include?(host) ? local : ignore
          else
            @remote.include?(host) ? remote : ignore
          end
        end

        def plan(keyspace, statement, options)
          local    = @local
          remote   = @remote
          position = @position
          total    = local.size + remote.size

          return EMPTY_PLAN if total == 0

          @position = (@position + 1) % total

          Plan.new(local.to_a, remote.to_a, position)
        end
      end
    end
  end
end
