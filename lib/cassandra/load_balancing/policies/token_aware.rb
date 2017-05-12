# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
#++

module Cassandra
  module LoadBalancing
    module Policies
      class TokenAware < Policy
        # @private
        class Plan
          def initialize(hosts, policy, keyspace, statement, options)
            @hosts     = hosts
            @policy    = policy
            @keyspace  = keyspace
            @statement = statement
            @options   = options
            @seen      = ::Hash.new
          end

          def has_next?
            until @hosts.empty?
              host = @hosts.shift

              next unless @policy.distance(host) == :local
              @seen[host] = true
              @next = host
              break
            end

            return true if @next

            @plan ||= @policy.plan(@keyspace, @statement, @options)

            while @plan.has_next?
              host = @plan.next

              unless @seen[host]
                @next = host
                return true
              end
            end

            false
          end

          def next
            host  = @next
            @next = nil
            host
          end
        end

        extend Forwardable

        # @!method distance(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#distance
        #
        # @!method host_found(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#host_found
        #
        # @!method host_up(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#host_up
        #
        # @!method host_down(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#host_down
        #
        # @!method host_lost(host)
        #   Delegates to wrapped policy
        #   @see Cassandra::LoadBalancing::Policy#host_lost
        def_delegators :@policy, :distance, :host_found, :host_up, :host_down, :host_lost

        # @param wrapped_policy [Cassandra::LoadBalancing::Policy] actual
        #   policy to filter
        # @param shuffle [Boolean] (true) whether or not to shuffle the replicas
        #
        # @note If replicas are not shuffled (`shuffle = false`), then it is
        #   possibile to create hotspots in a write-heavy scenario, where most
        #   of the write requests will be handled by the same node(s). The
        #   default behavior of shuffling replicas helps mitigate this by
        #   universally distributing write load between replicas. However, it
        #   under-utilizes read caching and forces multiple replicas to cache
        #   the same read statements.
        def initialize(wrapped_policy, shuffle = true)
          methods = [:host_up, :host_down, :host_found, :host_lost, :setup, :teardown,
                     :distance, :plan]

          Util.assert_responds_to_all(methods, wrapped_policy) do
            "supplied policy must respond to #{methods.inspect}, but doesn't"
          end

          @policy  = wrapped_policy
          @shuffle = !!shuffle
        end

        def setup(cluster)
          @cluster = cluster
          @policy.setup(cluster)
          nil
        end

        def teardown(cluster)
          @cluster = nil
          @policy.teardown(cluster)
          nil
        end

        def plan(keyspace, statement, options)
          return @policy.plan(keyspace, statement, options) unless @cluster

          replicas = @cluster.find_replicas(keyspace, statement)
          return @policy.plan(keyspace, statement, options) if replicas.empty?

          replicas = if @shuffle
                       replicas.shuffle
                     else
                       replicas.dup
                     end

          Plan.new(replicas, @policy, keyspace, statement, options)
        end

        # @private
        def inspect
          "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "policy=#{@policy.inspect}, " \
          "shuffle=#{@shuffle.inspect}>"
        end
      end
    end
  end
end
