# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
      class RoundRobin < Policy
        # @private
        class Plan
          def initialize(hosts, index)
            @hosts = hosts
            @index = index

            @total = @remaining = hosts.size
          end

          def has_next?
            @remaining > 0
          end

          def next
            return if @remaining == 0

            @remaining -= 1
            index  = @index
            @index = (index + 1) % @total

            @hosts[index]
          end
        end

        include MonitorMixin

        def initialize
          @hosts    = ::Array.new
          @position = 0

          mon_initialize
        end

        # Adds this host to rotation
        #
        # @param host [Cassandra::Host] a host instance
        # @return [Cassandra::LoadBalancing::Policies::RoundRobin] self
        # @see Cassandra::Listener#host_up
        def host_up(host)
          synchronize { @hosts = @hosts.dup.push(host) }

          self
        end

        # Removes this host from rotation
        #
        # @param host [Cassandra::Host] a host instance
        # @return [Cassandra::LoadBalancing::Policies::RoundRobin] self
        # @see Cassandra::Listener#host_down
        def host_down(host)
          synchronize do
            @hosts = @hosts.dup
            @hosts.delete(host)
          end

          self
        end

        # Noop
        #
        # @param host [Cassandra::Host] a host instance
        # @return [Cassandra::LoadBalancing::Policies::RoundRobin] self
        # @see Cassandra::Listener#host_found
        def host_found(host)
          self
        end

        # Noop
        #
        # @param host [Cassandra::Host] a host instance
        # @return [Cassandra::LoadBalancing::Policies::RoundRobin] self
        # @see Cassandra::Listener#host_lost
        def host_lost(host)
          self
        end

        # Returns distance to host. All hosts in rotation are considered
        # `:local`, all other hosts - `:ignore`.
        #
        # @param host [Cassandra::Host] a host instance
        # @return [Symbol] `:local` for all hosts in rotation and `:ignore` for
        #   all other hosts.
        # @see Cassandra::LoadBalancing::Policy#distance
        def distance(host)
          @hosts.include?(host) ? :local : :ignore
        end

        # Returns a load balancing plan that rotates hosts by 1 each time a
        # plan is requested.
        #
        # @param keyspace [String] current keyspace of the {Cassandra::Session}
        # @param statement [Cassandra::Statement] actual statement to be
        #   executed
        # @param options [Cassandra::Execution::Options] execution options to
        #   be used
        # @return [Cassandra::LoadBalancing::Plan] a rotated load balancing plan
        # @see Cassandra::LoadBalancing::Policy#plan
        def plan(keyspace, statement, options)
          hosts = @hosts
          total = hosts.size

          return EMPTY_PLAN if total == 0

          position  = @position % total
          @position = position + 1

          Plan.new(hosts, position)
        end
      end
    end
  end
end
