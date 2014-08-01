# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class WhiteList
        include Policy

        extend Forwardable

        def_delegators :@policy, :plan, :distance

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
              @ips << ::IPAddr.new(ip)
            else
              raise ::ArgumentError, "ips must contain only instance of String or IPAddr, #{ip.inspect} given"
            end
          end
        end

        def host_found(host)
          @policy.host_found(host) if @ips.include?(host.ip)
        end

        def host_lost(host)
          @policy.host_lost(host) if @ips.include?(host.ip)
        end

        def host_up(host)
          @policy.host_up(host) if @ips.include?(host.ip)
        end

        def host_down(host)
          @policy.host_down(host) if @ips.include?(host.ip)
        end
      end
    end
  end
end
