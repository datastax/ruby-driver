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
      class RoundRobin
        # @private
        class Plan
          def initialize(hosts, index)
            @hosts = hosts
            @index = index

            @total = @remaining = hosts.size
          end

          def next
            raise ::StopIteration if @remaining == 0

            @remaining -= 1
            index, @index = @index, (@index + 1) % @total

            @hosts[index]
          end
        end

        include Policy, MonitorMixin

        def initialize
          @hosts    = ::Array.new
          @position = 0

          mon_initialize
        end

        def host_up(host)
          synchronize { @hosts = @hosts.dup.push(host) }

          self
        end

        def host_down(host)
          synchronize do
            hosts = @hosts.dup
            hosts.delete(host)
            @hosts = hosts
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
          @hosts.include?(host) ? local : ignore
        end

        def plan(keyspace, statement, options)
          hosts    = @hosts
          position = @position
          total    = hosts.size
          return EMPTY_PLAN if total == 0

          @position = (position + 1) % total

          Plan.new(hosts, position)
        end
      end
    end
  end
end
