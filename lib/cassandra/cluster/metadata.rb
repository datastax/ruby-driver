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
    # @private
    class Metadata
      include MonitorMixin

      attr_reader :name

      def initialize(cluster_registry,
                     cluster_schema,
                     schema_partitioners,
                     replication_strategies,
                     default_replication_strategy)
        @registry         = cluster_registry
        @schema           = cluster_schema
        @partitioners     = schema_partitioners
        @strategies       = replication_strategies
        @default_strategy = default_replication_strategy
        @token_replicas   = ::Hash.new
        @token_ring       = ::Array.new
      end

      def find_replicas(keyspace, statement)
        unless statement.respond_to?(:partition_key) && statement.respond_to?(:keyspace)
          return EMPTY_LIST
        end

        keyspace      = String(statement.keyspace || keyspace)
        partition_key = statement.partition_key
        return EMPTY_LIST unless keyspace && partition_key

        partitioner = @partitioner
        return EMPTY_LIST unless partitioner

        keyspace_hosts = @token_replicas[keyspace]
        return EMPTY_LIST if keyspace_hosts.nil? || keyspace_hosts.empty?

        token = partitioner.create_token(partition_key)
        index = insertion_point(@token_ring, token)
        index = 0 if index >= @token_ring.size
        hosts = keyspace_hosts[@token_ring[index]]
        return EMPTY_LIST unless hosts

        hosts
      end

      def update(data)
        @name        = data['name']
        @partitioner = @partitioners[data['partitioner']]

        self
      end

      def rebuild_token_map
        partitioner = @partitioner
        return self unless partitioner

        tokens        = ::SortedSet.new
        token_to_host = ::Hash.new

        @registry.each_host do |host|
          host.tokens.each do |token|
            token = begin
                      partitioner.parse_token(token)
                    rescue
                      next
                    end
            tokens.add(token)
            token_to_host[token] = host
          end
        end

        token_ring     = tokens.to_a
        token_replicas = ::Hash.new
        token_maps     = ::Hash.new

        @schema.each_keyspace do |keyspace|
          replication = keyspace.replication
          key         = replication_key(replication.klass, replication.options)

          unless token_maps.include?(key)
            strategy        = @strategies[replication.klass] || @default_strategy
            token_maps[key] = strategy.replication_map(
              token_to_host,
              token_ring,
              replication.options
            )
          end

          token_replicas[keyspace.name] = token_maps[key]
        end

        @token_replicas = token_replicas
        @token_ring     = token_ring

        self
      end

      private

      def replication_key(klass, options)
        (klass + ':' + options.keys.sort.map {|k| "#{k}=#{options[k]}"}.join(',')).hash
      end

      def insertion_point(list, item)
        min = 0
        max = list.size - 1

        while min <= max
          idx = (min + max) / 2
          val = list[idx]

          if val < item
            min = idx + 1
          elsif val > item
            max = idx - 1
          else
            return idx # item found
          end
        end

        min # item not found.
      end
    end
  end
end
