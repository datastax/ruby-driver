# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Cassandra
  module LoadBalancing
    module Policies
      class WhiteList < Policy
        extend Forwardable

        # @!method plan(keyspace, statement, options)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#plan
        #
        # @!method distance(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#distance
        def_delegators :@policy, :plan, :distance

        # @param ips [Enumerable<String, IPAddr>] a list of ips to whitelist
        # @param wrapped_policy [Cassandra::LoadBalancing::Policy] actual policy to filter
        # @raise [ArgumentError] if arguments are of unexpected types
        def initialize(ips, wrapped_policy)
          raise ::ArgumentError, "ips must be enumerable" unless ips.respond_to?(:each)
          methods = [:host_up, :host_down, :host_found, :host_lost, :distance, :plan]

          unless methods.all? {|method| wrapped_policy.respond_to?(method)}
            raise ::ArgumentError, "supplied policy must be a Cassandra::LoadBalancing::Policy, #{wrapped_policy.inspect} given"
          end

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
        # @param host [Cassandra::Host] a host instance
        # @see Cassandra::LoadBalancing::Policy#host_found
        def host_found(host)
          @policy.host_found(host) if @ips.include?(host.ip)
        end

        # Delegates to wrapped policy if host's ip is whitelisted
        # @param host [Cassandra::Host] a host instance
        # @see Cassandra::LoadBalancing::Policy#host_lost
        def host_lost(host)
          @policy.host_lost(host) if @ips.include?(host.ip)
        end

        # Delegates to wrapped policy if host's ip is whitelisted
        # @param host [Cassandra::Host] a host instance
        # @see Cassandra::LoadBalancing::Policy#host_up
        def host_up(host)
          @policy.host_up(host) if @ips.include?(host.ip)
        end

        # Delegates to wrapped policy if host's ip is whitelisted
        # @param host [Cassandra::Host] a host instance
        # @see Cassandra::LoadBalancing::Policy#host_down
        def host_down(host)
          @policy.host_down(host) if @ips.include?(host.ip)
        end
      end
    end
  end
end
