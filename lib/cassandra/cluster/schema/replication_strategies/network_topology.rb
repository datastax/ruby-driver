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
  class Cluster
    class Schema
      # @private
      module ReplicationStrategies
        # @private
        class NetworkTopology
          def replication_map(token_hosts, token_ring, replication_options)
            racks                  = ::Hash.new
            datacenter_token_rings = ::Hash.new
            size                   = token_ring.size

            token_ring.each_with_index do |token, i|
              host = token_hosts[token]

              racks[host.datacenter] ||= ::Set.new
              racks[host.datacenter].add(host.rack)

              datacenter_token_rings[host.datacenter]  ||= Hash.new
              datacenter_token_rings[host.datacenter][i] = token
            end

            replication_map = ::Hash.new

            token_ring.each_with_index do |token, i|
              replicas = ::Set.new
              visited  = ::Hash.new
              skipped  = ::Hash.new

              replication_options.each do |datacenter, factor|
                ring = datacenter_token_rings[datacenter]
                next unless ring
                factor = [Integer(factor), ring.size].min rescue next


                total_racks    = racks[datacenter].size
                visited_racks  = visited[datacenter] ||= ::Set.new
                skipped_hosts  = skipped[datacenter] ||= ::Set.new
                added_replicas = ::Set.new

                size.times do |j|
                  break if added_replicas.size >= factor

                  tk = ring[(i + j) % size]
                  next unless tk
                  host = token_hosts[tk]
                  rack = host.rack

                  # unknown rack or seen all racks
                  if rack.nil? || visited_racks.size == total_racks
                    replicas << host
                    added_replicas << host
                  else
                    if visited_racks.include?(rack)
                      skipped_hosts << host
                    else
                      replicas << host
                      visited_racks << rack
                      added_replicas << host

                      if visited_racks.size == total_racks
                        skipped_hosts.each do |skipped_host|
                          break if added_replicas.size >= factor

                          replicas << skipped_host
                          added_replicas << host
                        end
                      end
                    end
                  end
                end
              end

              replication_map[token] = replicas.to_a.freeze
            end

            replication_map
          end
        end
      end
    end
  end
end
