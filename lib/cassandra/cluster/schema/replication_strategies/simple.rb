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
  class Cluster
    class Schema
      # @private
      module ReplicationStrategies
        # @private
        class Simple
          def replication_map(token_hosts, token_ring, replication_options)
            factor = Integer(replication_options['replication_factor'])
            size   = token_ring.size
            factor = size if size < factor
            replication_map = ::Hash.new

            token_ring.each_with_index do |token, i|
              replication_map[token] = factor.times.map do |j|
                token_hosts[token_ring[(i + j) % size]]
              end.freeze
            end

            replication_map
          end
        end
      end
    end
  end
end
