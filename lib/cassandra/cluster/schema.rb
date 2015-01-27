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
    # @private
    class Schema
      include MonitorMixin

      def initialize(schema_type_parser)
        @type_parser = schema_type_parser
        @keyspaces   = ::Hash.new
        @listeners   = ::Set.new

        mon_initialize
      end

      def create_partition_key(keyspace, table, values)
        keyspace = @keyspaces[keyspace]
        keyspace && keyspace.create_partition_key(table, values)
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

      def update_keyspaces(host, keyspaces, tables, columns, types)
        columns = columns.each_with_object(deephash { [] }) do |row, index|
          index[row['keyspace_name']] << row
        end

        tables = tables.each_with_object(deephash { [] }) do |row, index|
          index[row['keyspace_name']] << row
        end

        types = types.each_with_object(deephash { [] }) do |row, index|
          index[row['keyspace_name']] << row
        end

        current_keyspaces = ::Set.new

        keyspaces.each do |row|
          current_keyspaces << keyspace = row['keyspace_name']

          update_keyspace(host, row, tables[keyspace], columns[keyspace], types[keyspace])
        end

        @keyspaces.each do |name, keyspace|
          delete_keyspace(name) unless current_keyspaces.include?(name)
        end

        self
      end

      def update_keyspace(host, keyspace, tables, columns, types)
        keyspace_name = keyspace['keyspace_name']

        columns = columns.each_with_object(deephash { ::Hash.new }) do |row, index|
          index[row['columnfamily_name']][row['column_name']] = row
        end

        tables = tables.each_with_object(Hash.new) do |row, index|
          name = row['columnfamily_name']
          index[name] = create_table(row, columns[name], host.release_version)
        end

        types = types.each_with_object(Hash.new) do |row, index|
          name = row['type_name']
          index[name] = create_type(row, host.release_version)
        end

        replication = Keyspace::Replication.new(keyspace['strategy_class'], ::JSON.load(keyspace['strategy_options']))
        keyspace = Keyspace.new(keyspace_name, keyspace['durable_writes'], replication, tables, types)

        return self if keyspace == @keyspaces[keyspace_name]

        created = !@keyspaces.include?(keyspace_name)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        if created
          keyspace_created(keyspace)
        else
          keyspace_changed(keyspace)
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

      def udpate_table(host, keyspace_name, table, columns)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        columns  = columns.each_with_object(::Hash.new) do |row, index|
          index[row['column_name']] = row
        end
        table    = create_table(table, columns, host.release_version)
        keyspace = keyspace.update_table(table)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def delete_table(keyspace_name, table_name)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        keyspace = keyspace.delete_table(table_name)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def udpate_type(host, keyspace_name, type)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

        type     = create_type(type, host.release_version)
        keyspace = keyspace.update_type(type)

        synchronize do
          keyspaces = @keyspaces.dup
          keyspaces[keyspace_name] = keyspace
          @keyspaces = keyspaces
        end

        keyspace_changed(keyspace)

        self
      end

      def delete_type(keyspace_name, type_name)
        keyspace = @keyspaces[keyspace_name]

        return self unless keyspace

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

      def create_type(type, version)
        keyspace        = type['keyspace_name']
        name            = type['type_name']
        fields          = ::Hash.new

        type['field_names'].zip(type['field_types']) do |(field_name, field_type)|
          field_type = @type_parser.parse(field_type).results.first.first

          fields[field_name] = UserType::Field.new(field_name, field_type)
        end

        UserType.new(keyspace, name, fields)
      end

      def create_table(table, columns, version)
        keyspace        = table['keyspace_name']
        name            = table['columnfamily_name']
        key_validator   = @type_parser.parse(table['key_validator'])
        comparator      = @type_parser.parse(table['comparator'])
        column_aliases  = ::JSON.load(table['column_aliases'])

        clustering_size = find_clustering_size(comparator, columns.values,
                                               column_aliases, version)

        is_dense           = clustering_size != comparator.results.size - 1
        is_compact         = is_dense || !comparator.collections
        partition_key      = []
        clustering_columns = []
        clustering_order   = []

        compaction_strategy = Table::Compaction.new(
          table['compaction_strategy_class'],
          ::JSON.load(table['compaction_strategy_options'])
        )
        compression_parameters = ::JSON.load(table['compression_parameters'])

        options = Table::Options.new(table, compaction_strategy, compression_parameters, is_compact, version)
        columns = create_columns(key_validator, comparator, column_aliases, is_dense, clustering_size, table, columns, version, partition_key, clustering_columns, clustering_order)

        Table.new(keyspace, name, partition_key, clustering_columns, columns, options, clustering_order, version)
      end

      def find_clustering_size(comparator, columns, aliases, cassandra_version)
        if cassandra_version.start_with?('1')
          if comparator.collections
            size = comparator.results.size
            (!comparator.collections.empty? || aliases.size == size - 1 && comparator.results.last.first == :text) ? size - 1 : size
          else
            (!aliases.empty? || columns.empty?) ? 1 : 0
          end
        else
          max_index = nil

          columns.each do |cl|
            if cl['type'].to_s.upcase == 'CLUSTERING_KEY'
              index = cl['component_index'] || 0

              if max_index.nil? || index > max_index
                max_index = index
              end
            end
          end

          return 0 if max_index.nil?

          max_index + 1
        end
      end

      def create_columns(key_validator, comparator, column_aliases, is_dense, clustering_size, table, columns, cassandra_version, partition_key, clustering_columns, clustering_order)
        table_columns      = {}
        other_columns      = []

        if cassandra_version.start_with?('1')
          key_aliases = ::JSON.load(table['key_aliases'])

          key_validator.results.each_with_index do |(type, order), i|
            key_alias = key_aliases.fetch(i) { i.zero? ? "key" : "key#{i + 1}" }

            partition_key[i] = Column.new(key_alias, type, order)
          end

          if comparator.results.size > 1
            clustering_size.times do |i|
              column_alias = column_aliases.fetch(i) { "column#{i + 1}" }
              type, order  = comparator.results.fetch(i)

              clustering_columns[i] = Column.new(column_alias, type, order)
              clustering_order[i]   = order
            end
          else
            column_alias = column_aliases.first || "column1"
            type, order  = comparator.results.first

            clustering_columns[0] = Column.new(column_alias, type, order)
            clustering_order[0]   = order
          end

          if is_dense
            value_alias = table['value_alias']
            value_alias = 'value' if value_alias.nil? || value_alias.empty?
            type, order = @type_parser.parse(table['default_validator']).results.first
            other_columns << Column.new(value_alias, type, order)
          end

          columns.each do |name, row|
            other_columns << create_column(row)
          end
        else
          columns.each do |name, row|
            next if row['column_name'].empty?

            column = create_column(row)
            type   = row['type'].to_s
            index  = row['component_index'] || 0

            case type.upcase
            when 'PARTITION_KEY'
              partition_key[index] = column
            when 'CLUSTERING_KEY'
              clustering_columns[index] = column
              clustering_order[index]   = column.order
            else
              other_columns << column
            end
          end
        end

        partition_key.each do |column|
          table_columns[column.name] = column
        end

        clustering_columns.each do |column|
          table_columns[column.name] = column
        end

        other_columns.each do |column|
          table_columns[column.name] = column
        end

        table_columns
      end

      def create_column(column)
        name        = column['column_name']
        type, order = @type_parser.parse(column['validator']).results.first
        is_static   = (column['type'] == 'STATIC')

        if column['index_type'].nil?
          index   = nil
        elsif column['index_type'].to_s.upcase == 'CUSTOM' || !column['index_options']
          index   = Column::Index.new(column['index_name'])
        else
          options = ::JSON.load(column['index_options'])
          index   = Column::Index.new(column['index_name'], options['class_name'])
        end

        Column.new(name, type, order, index, is_static)
      end

      def deephash
        ::Hash.new {|hash, key| hash[key] = yield}
      end

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

require 'cassandra/cluster/schema/partitioners'
require 'cassandra/cluster/schema/replication_strategies'
require 'cassandra/cluster/schema/type_parser'
