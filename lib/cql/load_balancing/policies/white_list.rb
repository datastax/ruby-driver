# encoding: utf-8

module Cql
  module LoadBalancing
    module Policies
      class WhiteList
        include Policy

        extend Forwardable

        # @!method plan(keyspace, statement, options)
        #   Delegates to wrapped policy
        #   @see Cql::LoadBalancing::Policy#plan
        #
        # @!method distance(host)
        #   Delegates to wrapped policy
        #   @see Cql::LoadBalancing::Policy#distance
        def_delegators :@policy, :plan, :distance

        # @param ips [Enumerable<String, IPAddr>] a list of ips to whitelist
        # @param wrapped_policy [Cql::LoadBalancing::Policy] actual policy to filter
        # @raise [ArgumentError] if arguments are of unexpected types
        def initialize(ips, wrapped_policy)
          raise ::ArgumentError, "ips must be enumerable" unless ips.respond_to?(:each)
          raise ::ArgumentError, "supplied policy must be a Cql::LoadBalancing::Policy, #{wrapped_policy.inspect} given" unless wrapped_policy.is_a?(Policy)

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

        # Delegates to wrapped policy if host's ip is whitelisted
        # @param host [Cql::Host] a host instance
        # @see Cql::LoadBalancing::Policy#host_found
        def host_found(host)
          @policy.host_found(host) if @ips.include?(host.ip)
        end

        # Delegates to wrapped policy if host's ip is whitelisted
        # @param host [Cql::Host] a host instance
        # @see Cql::LoadBalancing::Policy#host_lost
        def host_lost(host)
          @policy.host_lost(host) if @ips.include?(host.ip)
        end

        # Delegates to wrapped policy if host's ip is whitelisted
        # @param host [Cql::Host] a host instance
        # @see Cql::LoadBalancing::Policy#host_up
        def host_up(host)
          @policy.host_up(host) if @ips.include?(host.ip)
        end

        # Delegates to wrapped policy if host's ip is whitelisted
        # @param host [Cql::Host] a host instance
        # @see Cql::LoadBalancing::Policy#host_down
        def host_down(host)
          @policy.host_down(host) if @ips.include?(host.ip)
        end
      end
    end
  end
end
