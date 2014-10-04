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

require 'spec_helper'

module Cassandra
  module CQL
    describe(TypeGuesser) do
      let(:schema) { Cluster::Schema.new(Cluster::Schema::TypeParser.new) }

      let(:host)      { Cassandra::Host.new('127.0.0.1', nil, nil, nil, '2.0.10') }
      let(:keyspaces) {
        [
          {
            "keyspace_name"    => "simplex",
            "durable_writes"   => true,
            "strategy_class"   =>"org.apache.cassandra.locator.SimpleStrategy",
            "strategy_options" => "{\"replication_factor\":\"3\"}"
          }
        ]
      }
      let(:tables) {
        [
          {
            "keyspace_name"               => "simplex",
            "columnfamily_name"           => "users",
            "bloom_filter_fp_chance"      => 0.01,
            "caching"                     => "KEYS_ONLY",
            "column_aliases"              => "[]",
            "comment"                     => "",
            "compaction_strategy_class"   => "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
            "compaction_strategy_options" => "{}",
            "comparator"                  => "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type)",
            "compression_parameters"      => "{\"sstable_compression\":\"org.apache.cassandra.io.compress.LZ4Compressor\"}",
            "default_time_to_live"        => 0, "default_validator"=>"org.apache.cassandra.db.marshal.BytesType",
            "dropped_columns"             => nil,
            "gc_grace_seconds"            => 864000,
            "index_interval"              => 128,
            "is_dense"                    => false,
            "key_aliases"                 => "[\"id\"]",
            "key_validator"               => "org.apache.cassandra.db.marshal.UUIDType",
            "local_read_repair_chance"    => 0.1,
            "max_compaction_threshold"    => 32,
            "memtable_flush_period_in_ms" => 0,
            "min_compaction_threshold"    => 4,
            "populate_io_cache_on_flush"  => false,
            "read_repair_chance"          => 0.0,
            "replicate_on_write"          => true,
            "speculative_retry"           => "99.0PERCENTILE",
            "subcomparator"               => nil,
            "type"                        => "Standard",
            "value_alias"                 => nil
          }
        ]
      }

      let(:columns) {
        [
          {
            "keyspace_name"     => "simplex",
            "columnfamily_name" => "users",
            "column_name"       => "age",
            "component_index"   => 0,
            "index_name"        => nil,
            "index_options"     => nil,
            "index_type"        => nil,
            "type"              => "regular",
            "validator"         => "org.apache.cassandra.db.marshal.Int32Type"
          },
          {
            "keyspace_name"     => "simplex",
            "columnfamily_name" => "users",
            "column_name"       => "email",
            "component_index"   => 0,
            "index_name"        => nil,
            "index_options"     => nil,
            "index_type"        => nil,
            "type"              => "regular",
            "validator"         => "org.apache.cassandra.db.marshal.UTF8Type"
          },
          {
            "keyspace_name"     => "simplex",
            "columnfamily_name" => "users",
            "column_name"       => "id",
            "component_index"   => nil,
            "index_name"        => nil,
            "index_options"     => nil,
            "index_type"        => nil,
            "type"              => "partition_key",
            "validator"         => "org.apache.cassandra.db.marshal.UUIDType"
          },
          {
            "keyspace_name"     => "simplex",
            "columnfamily_name" => "users",
            "column_name"       => "username",
            "component_index"   => 0,
            "index_name"        => nil,
            "index_options"     => nil,
            "index_type"        => nil,
            "type"              => "regular",
            "validator"         => "org.apache.cassandra.db.marshal.UTF8Type"
          }
        ]
      }

      subject { TypeGuesser.new(schema) }

      before do
        schema.update_keyspaces(host, keyspaces, tables, columns)
      end

      describe('#guess') do
        [
          [
            "SELECT * FROM users",
            []
          ],
          [
            "UPDATE users SET username='bulat', email='bulat.shakirzyanov@datastax.com', age = 28 WHERE id = ?",
            [:uuid]
          ],
          [
            "SELECT username, email FROM users WHERE id = ?",
            [:uuid]
          ],
          [
            "DELETE username, email FROM simplex.users WHERE id = ?",
            [:uuid]
          ],
          [
            "INSERT INTO users (username, email, id, age) VALUES (?, ?, ?, ?) USING TTL 86400",
            [:text, :text, :uuid, :int]
          ]
        ].each do |(cql, typehints)|
          it "works on #{cql.inspect}" do
            expect(subject.guess(cql, 'simplex')).to eq(typehints)
          end
        end
      end
    end
  end
end
