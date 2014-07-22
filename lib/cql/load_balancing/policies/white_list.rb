# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class WhiteList
        class Plan
          def initialize(ips, wrapped_plan)
            @ips  = ips
            @plan = wrapped_plan
          end

          def next
            begin
              host = @plan.next
            end until @ips.include?(host.ip)

            host
          end
        end

        include Policy

        extend Forwardable

        def_delegators :@policy, :host_up, :host_down, :host_found, :host_lost

        def initialize(ips, wrapped_policy)
          raise ::ArgumentError, "ips must be enumerable" unless ips.respond_to?(:each)
          raise ::ArgumentError, "supplied policy must respond to #plan(keyspace, statement, options)" unless wrapped_policy.respond_to?(:plan)

          @ips    = ::Set.new
          @policy = wrapped_policy

          ips.each do |ip|
            case ip
            when ::IPAddr
              @ips << ip
            when ::String
              @ips << IPAddr.new(ip)
            else
              raise ::ArgumentError, "ips must contain only instance of String or IPAddr, #{ip.inspect} given"
            end
          end
        end

        def distance(host)
          @ips.include?(host.ip) ? @policy.distance(host) : ignore
        end

        def plan(keyspace, statement, options)
          Plan.new(@ips, @policy.plan(keyspace, statement, options))
        end
      end
    end
  end
end
