# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
      class DCAwareRoundRobin < Policy
        # @private
        class Plan
          def initialize(local, remote, index)
            @local  = local
            @remote = remote
            @index  = index

            @local_remaining  = @local_total  = local.size
            @remote_remaining = @remote_total = remote.size
          end

          def has_next?
            @local_remaining > 0 || @remote_remaining > 0
          end

          def next
            if @local_remaining > 0
              @local_remaining -= 1
              i = (@index % @local_total)
              @index += 1

              return @local[i]
            end

            if @remote_remaining > 0
              @remote_remaining -= 1
              i = (@index % @remote_total)
              @index += 1

              return @remote[i]
            end
          end
        end

        include MonitorMixin

        def initialize(datacenter = nil,
                       max_remote_hosts_to_use = nil,
                       use_remote_hosts_for_local_consistency = false)
          datacenter              &&= String(datacenter)
          max_remote_hosts_to_use &&= Integer(max_remote_hosts_to_use)

          unless datacenter.nil?
            Util.assert_not_empty(datacenter) { 'datacenter cannot be empty' }
          end

          unless max_remote_hosts_to_use.nil?
            Util.assert(max_remote_hosts_to_use >= 0) do
              'max_remote_hosts_to_use must be nil or >= 0'
            end
          end

          @datacenter = datacenter
          @max_remote = max_remote_hosts_to_use
          @local      = ::Array.new
          @remote     = ::Array.new
          @position   = 0

          @use_remote = !!use_remote_hosts_for_local_consistency

          mon_initialize
        end

        def host_up(host)
          @datacenter = host.datacenter if !@datacenter && host.datacenter

          if host.datacenter.nil? || host.datacenter == @datacenter
            synchronize { @local = @local.dup.push(host) }
          else
            if @max_remote.nil? || @remote.size < @max_remote
              synchronize { @remote = @remote.dup.push(host) }
            end
          end

          self
        end

        def host_down(host)
          if host.datacenter.nil? || host.datacenter == @datacenter
            synchronize do
              @local = @local.dup
              @local.delete(host)
            end
          else
            synchronize do
              @remote = @remote.dup
              @remote.delete(host)
            end
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
          if host.datacenter.nil? || host.datacenter == @datacenter
            @local.include?(host) ? :local : :ignore
          else
            @remote.include?(host) ? :remote : :ignore
          end
        end

        def plan(keyspace, statement, options)
          local = @local

          remote = if LOCAL_CONSISTENCIES.include?(options.consistency) && !@use_remote
                     EMPTY_ARRAY
                   else
                     @remote
                   end

          total = local.size + remote.size

          return EMPTY_PLAN if total == 0

          position  = @position % total
          @position = position + 1

          Plan.new(local, remote, position)
        end

        private

        # @private
        LOCAL_CONSISTENCIES = [:local_quorum, :local_one].freeze
        # @private
        EMPTY_ARRAY         = [].freeze
      end
    end
  end
end
