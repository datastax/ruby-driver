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
    # @private
    class Schema
      include MonitorMixin

      def initialize
        @keyspaces = ::Hash.new
        @listeners = ::Set.new

        mon_initialize
      end

      def get_pk_idx(metadata)
        return EMPTY_LIST unless metadata

        # metadata is an array of column-specs; each column-spec is an array
        # of keyspace_name, tablename, other stuff. We only care about the first two.
        # See read_prepared_metadata_v4 in coder.rb for more details.
        # NB: sandman: I think all of the column specs have the same keyspace and
        # table name in this context, so we can safely grab the first.

        keyspace_name, table_name = metadata.first
        return EMPTY_LIST unless keyspace_name && table_name

        keyspace = @keyspaces[keyspace_name]
        return EMPTY_LIST unless keyspace

        table = keyspace.table(table_name)
        return EMPTY_LIST unless table

        partition_key = table.partition_key
        return EMPTY_LIST unless partition_key && partition_key.size <= metadata.size

        partition_key.map do |column|
          i = metadata.index {|(_, _, name, _)| name == column.name}
          return EMPTY_LIST if i.nil?
          i
        end
      end

      def add_listener(listener)
        synchronize do
          @listeners = @listeners.dup.add(listener)
        end

        self
      end

      def remove_listener(listener)
        synchronize do
          @listeners = @listeners.dup.delete(listener)
        end

        self
      end

      def replace(keyspaces)
        current_keyspaces = ::Set.new

        keyspaces.each do |keyspace|
          current_keyspaces << keyspace.name

          replace_keyspace(keyspace)
        end

        @keyspaces.each do |name, _keyspace|
          delete_keyspace(name) unless current_keyspaces.include?(name)
        end

        self
      end

      def replace_keyspace(keyspace)
        old_keyspace = @keyspaces[keyspace.name]

        return self if old_keyspace == keyspace

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace.name] = keyspace
          @keyspaces = keyspaces
        end

        if old_keyspace
          keyspace_changed(keyspace)
        else
          keyspace_created(keyspace)
        end

        self
      end

      def delete_keyspace(keyspace_name)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces.delete(keyspace_name)
          @keyspaces = keyspaces
        end

        keyspace_dropped(keyspace)

        self
      end

      def replace_table(table)
        keyspace = table.keyspace

        return self unless keyspace

        old_table = keyspace.table(table.name)

        return self if old_table == table

        keyspace = keyspace.update_table(table)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace.name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def delete_table(keyspace_name, table_name)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        table = keyspace.table(table_name)

        return self unless table

        keyspace = keyspace.delete_table(table_name)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def replace_materialized_view(view)
        keyspace = view.keyspace

        return self unless keyspace

        old_view = keyspace.materialized_view(view.name)

        return self if old_view == view

        keyspace = keyspace.update_materialized_view(view)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace.name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def delete_materialized_view(keyspace_name, view_name)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        view = keyspace.materialized_view(view_name)

        return self unless view

        keyspace = keyspace.delete_materialized_view(view_name)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def replace_type(type)
        keyspace = @keyspaces[type.keyspace]

        return self unless keyspace

        old_type = keyspace.type(type.name)

        return self if old_type == type

        keyspace = keyspace.update_type(type)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace.name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def delete_type(keyspace_name, type_name)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        type = keyspace.type(type_name)

        return self unless type

        keyspace = keyspace.delete_type(type_name)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def replace_function(function)
        keyspace = @keyspaces[function.keyspace]

        return self unless keyspace

        old_function = keyspace.function(function.name, *function.argument_types)

        return self if old_function == function

        keyspace = keyspace.update_function(function)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace.name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def delete_function(keyspace_name, function_name, function_arg_types)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        function = keyspace.function(function_name, *function_arg_types)

        return self unless function

        keyspace = keyspace.delete_function(function_name, function_arg_types)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def replace_aggregate(aggregate)
        keyspace = @keyspaces[aggregate.keyspace]

        return self unless keyspace

        old_aggregate = keyspace.aggregate(aggregate.name, *aggregate.argument_types)

        return self if old_aggregate == aggregate

        keyspace = keyspace.update_aggregate(aggregate)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace.name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def delete_aggregate(keyspace_name, aggregate_name, aggregate_arg_types)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        aggregate = keyspace.aggregate(aggregate_name, *aggregate_arg_types)

        return self unless aggregate

        keyspace = keyspace.delete_aggregate(aggregate_name, aggregate_arg_types)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def has_keyspace?(name)
        @keyspaces.include?(name)
      end

      def keyspace(name)
        @keyspaces[name]
      end

      def each_keyspace(&block)
        if block_given?
          @keyspaces.each_value(&block)
          self
        else
          @keyspaces.values
        end
      end
      alias keyspaces each_keyspace

      private

      def keyspace_created(keyspace)
        @listeners.each do |listener|
          begin
            listener.keyspace_created(keyspace)
          rescue
            nil
          end
        end
      end

      def keyspace_changed(keyspace)
        @listeners.each do |listener|
          begin
            listener.keyspace_changed(keyspace)
          rescue
            nil
          end
        end
      end

      def keyspace_dropped(keyspace)
        @listeners.each do |listener|
          begin
            listener.keyspace_dropped(keyspace)
          rescue
            nil
          end
        end
      end
    end
  end
end

require 'cassandra/cluster/schema/cql_type_parser'
require 'cassandra/cluster/schema/fetchers'
require 'cassandra/cluster/schema/partitioners'
require 'cassandra/cluster/schema/replication_strategies'
require 'cassandra/cluster/schema/fqcn_type_parser'
