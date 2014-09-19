# encoding: utf-8

#--
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
#++

module Cassandra
  class Cluster
    class Schema
      module ReplicationStrategies
        class NetworkTopology
          def replication_map(token_hosts, token_ring, replication_options)
            size  = token_ring.size
            racks = ::Hash.new
            token_hosts.each_value do |host|
              racks[host.datacenter] ||= ::Set.new
              racks[host.datacenter].add(host.rack)
            end

            replication_map = ::Hash.new

            token_ring.each_with_index do |token, i|
              all_replicas = ::Hash.new
              visited = ::Hash.new
              skipped_datacenter_hosts = ::Hash.new
              replicas = ::Set.new

              size.times do |j|
                break if all_replicas.size == racks.size && !all_replicas.any? {|(datacenter, r)| r.size < Integer(replication_options[datacenter])}

                host       = token_hosts[token_ring[(i + j) % size]]
                datacenter = host.datacenter
                next if datacenter.nil?

                factor = Integer(replication_options[datacenter])
                next unless factor

                replicas_in_datacenter = all_replicas[datacenter] ||= ::Set.new
                next if replicas_in_datacenter.size >= factor

                rack = host.rack
                visited_racks = visited[datacenter] ||= ::Set.new

                if rack.nil? || visited_racks.size == racks[datacenter].size
                  replicas << host
                  replicas_in_datacenter << host
                else
                  if visited_racks.include?(rack)
                    skipped_hosts = skipped_datacenter_hosts[datacenter] ||= ::Set.new
                    skipped_hosts << host
                  else
                    replicas << host
                    replicas_in_datacenter << host
                    visited_racks << rack

                    if visited_racks.size == racks[datacenter].size && (skipped_hosts = skipped_datacenter_hosts[datacenter]) && replicas_in_datacenter.size < factor
                      skipped_hosts.each do |skipped_host|
                        replicas << skipped_host
                        replicas_in_datacenter << skipped_host
                        break if replicas_in_datacenter.size >= factor
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
