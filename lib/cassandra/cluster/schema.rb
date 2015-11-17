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
    class Schema
      include MonitorMixin

      def initialize
        @keyspaces = ::Hash.new
        @listeners = ::Set.new

        mon_initialize
      end

      def get_pk_idx(metadata)
        return EMPTY_LIST unless metadata

        keyspace_name, table_name, _ = metadata.first
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

        @keyspaces.each do |name, keyspace|
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
        keyspace = @keyspaces[table.keyspace]

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

      def replace_type(type)
        keyspace = @keyspaces[type.keyspace]

        return self unless keyspace

        old_type = keyspace.type(type.name)

        return self if old_type == type

        keyspace = keyspace.update_type(type)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspaces.name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def delete_type(keyspace_name, type_name)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        type = keyspace.table(type_name)

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
      alias :keyspaces :each_keyspace

      private

      def keyspace_created(keyspace)
        @listeners.each do |listener|
          listener.keyspace_created(keyspace) rescue nil
        end
      end

      def keyspace_changed(keyspace)
        @listeners.each do |listener|
          listener.keyspace_changed(keyspace) rescue nil
        end
      end

      def keyspace_dropped(keyspace)
        @listeners.each do |listener|
          listener.keyspace_dropped(keyspace) rescue nil
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
