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
      module Fetcher
        FUTURE_EMPTY_LIST = Ione::Future.resolved(EMPTY_LIST)
        REPLICATION_PACKAGE_PREFIX = 'org.apache.cassandra.locator.'.freeze
        COMPRESSION_PACKAGE_PREFIX = 'org.apache.cassandra.io.compress.'.freeze

        def fetch(connection)
          # rubocop:disable Metrics/LineLength
          Ione::Future.all(select_keyspaces(connection),
                           select_tables(connection),
                           select_columns(connection),
                           select_types(connection),
                           select_functions(connection),
                           select_aggregates(connection),
                           select_materialized_views(connection),
                           select_indexes(connection),
                           select_triggers(connection))
                      .map do |rows_keyspaces, rows_tables, rows_columns, rows_types, rows_functions, rows_aggregates, rows_views, rows_indexes, rows_triggers|
                        lookup_tables     = map_rows_by(rows_tables, 'keyspace_name')
                        lookup_columns    = map_rows_by(rows_columns, 'keyspace_name')
                        lookup_types      = map_rows_by(rows_types, 'keyspace_name')
                        lookup_functions  = map_rows_by(rows_functions, 'keyspace_name')
                        lookup_aggregates = map_rows_by(rows_aggregates, 'keyspace_name')
                        lookup_views      = map_rows_by(rows_views, 'keyspace_name')
                        lookup_indexes    = map_rows_by(rows_indexes, 'keyspace_name')
                        lookup_triggers   = map_rows_by(rows_triggers, 'keyspace_name')

                        rows_keyspaces.map do |keyspace_data|
                          name = keyspace_data['keyspace_name']

                          create_keyspace(keyspace_data,
                                          lookup_tables[name],
                                          lookup_columns[name],
                                          lookup_types[name],
                                          lookup_functions[name],
                                          lookup_aggregates[name],
                                          lookup_views[name],
                                          lookup_indexes[name],
                                          lookup_triggers[name])
                        end
                      end
        end

        def fetch_keyspace(connection, keyspace_name)
          Ione::Future.all(select_keyspace(connection, keyspace_name),
                           select_keyspace_tables(connection, keyspace_name),
                           select_keyspace_columns(connection, keyspace_name),
                           select_keyspace_types(connection, keyspace_name),
                           select_keyspace_functions(connection, keyspace_name),
                           select_keyspace_aggregates(connection, keyspace_name),
                           select_keyspace_materialized_views(connection, keyspace_name),
                           select_keyspace_indexes(connection, keyspace_name),
                           select_keyspace_triggers(connection, keyspace_name))
                      .map do |rows_keyspaces, rows_tables, rows_columns, rows_types, rows_functions, rows_aggregates, rows_views, rows_indexes, rows_triggers|
                        if rows_keyspaces.empty?
                          nil
                        else
                          create_keyspace(rows_keyspaces.first,
                                          rows_tables,
                                          rows_columns,
                                          rows_types,
                                          rows_functions,
                                          rows_aggregates,
                                          rows_views,
                                          rows_indexes,
                                          rows_triggers)
                        end
                      end
        end

        def fetch_table(connection, keyspace_name, table_name)
          Ione::Future.all(select_table(connection, keyspace_name, table_name),
                           select_table_columns(connection, keyspace_name, table_name),
                           select_table_indexes(connection, keyspace_name, table_name),
                           select_table_triggers(connection, keyspace_name, table_name))
                      .map do |(rows_tables, rows_columns, rows_indexes, rows_triggers)|
            if rows_tables.empty?
              nil
            else
              create_table(rows_tables.first,
                           rows_columns,
                           rows_indexes,
                           rows_triggers)
            end
          end
        end

        def fetch_materialized_view(connection, keyspace_name, view_name)
          Ione::Future.all(select_materialized_view(connection, keyspace_name, view_name),
                           select_table_columns(connection, keyspace_name, view_name))
                      .map do |rows_views, rows_columns|
            if rows_views.empty?
              nil
            else
              view_row = rows_views.first
              create_materialized_view(view_row,
                                       rows_columns)
            end
          end
        end

        def fetch_type(connection, keyspace_name, type_name)
          select_type(connection, keyspace_name, type_name).map do |rows_types|
            if rows_types.empty?
              nil
            else
              create_type(rows_types.first)
            end
          end
        end

        def fetch_function(connection, keyspace_name, function_name, function_args)
          select_function(connection, keyspace_name, function_name, function_args)
            .map do |rows_functions|
            if rows_functions.empty?
              nil
            else
              create_function(rows_functions.first)
            end
          end
        end

        def fetch_aggregate(connection, keyspace_name, aggregate_name, aggregate_args)
          select_aggregate(connection, keyspace_name, aggregate_name, aggregate_args)
            .map do |rows_aggregates|
            if rows_aggregates.empty?
              nil
            else
              create_aggregate(rows_aggregates.first, @schema.keyspace(keyspace_name)
                  .send(:raw_functions))
            end
          end
        end

        private

        def select_keyspaces(connection)
          FUTURE_EMPTY_LIST
        end

        def select_tables(connection)
          FUTURE_EMPTY_LIST
        end

        def select_materialized_views(connection)
          FUTURE_EMPTY_LIST
        end

        def select_columns(connection)
          FUTURE_EMPTY_LIST
        end

        def select_indexes(connection)
          FUTURE_EMPTY_LIST
        end

        def select_triggers(connection)
          FUTURE_EMPTY_LIST
        end

        def select_types(connection)
          FUTURE_EMPTY_LIST
        end

        def select_functions(connection)
          FUTURE_EMPTY_LIST
        end

        def select_aggregates(connection)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace_tables(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace_materialized_views(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace_columns(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace_indexes(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace_triggers(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace_types(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace_functions(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_keyspace_aggregates(connection, keyspace_name)
          FUTURE_EMPTY_LIST
        end

        def select_table(connection, keyspace_name, table_name)
          FUTURE_EMPTY_LIST
        end

        def select_materialized_view(connection, keyspace_name, view_name)
          FUTURE_EMPTY_LIST
        end

        def select_table_columns(connection, keyspace_name, table_name)
          FUTURE_EMPTY_LIST
        end

        def select_table_indexes(connection, keyspace_name, table_name)
          FUTURE_EMPTY_LIST
        end

        def select_table_triggers(connection, keyspace_name, table_name)
          FUTURE_EMPTY_LIST
        end

        def select_type(connection, keyspace_name, type_name)
          FUTURE_EMPTY_LIST
        end

        def select_function(connection, keyspace_name, function_name, function_args)
          FUTURE_EMPTY_LIST
        end

        def select_aggregate(connection, keyspace_name, aggregate_name, aggregate_args)
          FUTURE_EMPTY_LIST
        end

        def send_select_request(connection, cql, params = EMPTY_LIST, types = EMPTY_LIST)
          backtrace = caller
          connection.send_request(
            Protocol::QueryRequest.new(cql, params, types, :one)
          ).map do |r|
            case r
            when Protocol::RowsResultResponse
              r.rows
            when Protocol::ErrorResponse
              e = r.to_error(nil, VOID_STATEMENT, VOID_OPTIONS, EMPTY_LIST, :one, 0)
              e.set_backtrace(backtrace)
              raise e
            else
              raise Errors::InternalError, "Unexpected response #{r.inspect}", caller
            end
          end
        end

        def map_rows_by(rows, key_name, &block)
          rows.each_with_object(::Hash.new { EMPTY_LIST }) do |row, map|
            key = row[key_name]
            map[key] = [] unless map.key?(key)

            map[key] << if block
                          yield(row)
                        else
                          row
                        end
          end
        end
      end

      # @private
      module Fetchers
        # rubocop:disable Style/ClassAndModuleCamelCase
        class V1_2_x
          SELECT_KEYSPACES        = 'SELECT * FROM system.schema_keyspaces'.freeze
          SELECT_TABLES           = 'SELECT * FROM system.schema_columnfamilies'.freeze
          SELECT_COLUMNS          = 'SELECT * FROM system.schema_columns'.freeze
          SELECT_KEYSPACE         =
            'SELECT * ' \
            'FROM system.schema_keyspaces ' \
            'WHERE keyspace_name = \'%s\''.freeze
          SELECT_KEYSPACE_TABLES  =
            'SELECT * ' \
            'FROM system.schema_columnfamilies ' \
            'WHERE keyspace_name = \'%s\''.freeze
          SELECT_KEYSPACE_COLUMNS =
            'SELECT * FROM system.schema_columns WHERE keyspace_name = \'%s\''.freeze
          SELECT_TABLE            =
            'SELECT * ' \
            'FROM system.schema_columnfamilies ' \
            'WHERE keyspace_name = \'%s\' AND columnfamily_name = \'%s\''.freeze
          SELECT_TABLE_COLUMNS    =
            'SELECT * ' \
            'FROM system.schema_columns ' \
            'WHERE keyspace_name = \'%s\' AND columnfamily_name = \'%s\''.freeze

          include Cassandra::Cluster::Schema::Fetcher

          def initialize(type_parser, schema)
            @type_parser = type_parser
            @schema      = schema
          end

          private

          def select_keyspaces(connection)
            send_select_request(connection, SELECT_KEYSPACES)
          end

          def select_tables(connection)
            send_select_request(connection, SELECT_TABLES)
          end

          def select_columns(connection)
            send_select_request(connection, SELECT_COLUMNS)
          end

          def select_keyspace(connection, keyspace_name)
            send_select_request(connection, SELECT_KEYSPACE % keyspace_name)
          end

          def select_keyspace_tables(connection, keyspace_name)
            send_select_request(connection, format(SELECT_KEYSPACE_TABLES, keyspace_name))
          end

          def select_keyspace_columns(connection, keyspace_name)
            send_select_request(connection, format(SELECT_KEYSPACE_COLUMNS, keyspace_name))
          end

          def select_table(connection, keyspace_name, table_name)
            send_select_request(connection, format(SELECT_TABLE, keyspace_name, table_name))
          end

          def select_table_columns(connection, keyspace_name, table_name)
            send_select_request(connection,
                                format(SELECT_TABLE_COLUMNS, keyspace_name, table_name))
          end

          def create_replication(keyspace_data)
            klass = keyspace_data['strategy_class']
            klass.slice!(REPLICATION_PACKAGE_PREFIX)
            options = ::JSON.load(keyspace_data['strategy_options'])
            Keyspace::Replication.new(klass, options)
          end

          def create_keyspace(keyspace_data, rows_tables, rows_columns,
                              rows_types, rows_functions, rows_aggregates,
                              rows_views, rows_indexes, rows_triggers)
            keyspace_name = keyspace_data['keyspace_name']
            replication   = create_replication(keyspace_data)
            types = rows_types.each_with_object({}) do |row, h|
              h[row['type_name']] = create_type(row)
            end

            # Create a FunctionCollection for the functions and aggregates.
            functions = Cassandra::FunctionCollection.new
            rows_functions.each do |row|
              functions.add_or_update(create_function(row))
            end

            aggregates = Cassandra::FunctionCollection.new
            rows_aggregates.each do |row|
              aggregates.add_or_update(create_aggregate(row, functions))
            end

            lookup_columns = map_rows_by(rows_columns, 'columnfamily_name')
            lookup_indexes = map_rows_by(rows_indexes, 'columnfamily_name')
            lookup_triggers = map_rows_by(rows_triggers, 'columnfamily_name')
            tables = rows_tables.each_with_object({}) do |row, h|
              table_name = row['columnfamily_name']
              h[table_name] = create_table(row,
                                           lookup_columns[table_name],
                                           lookup_indexes[table_name],
                                           lookup_triggers[table_name])
            end

            Keyspace.new(keyspace_name,
                         keyspace_data['durable_writes'],
                         replication,
                         tables,
                         types,
                         functions,
                         aggregates,
                         {})
          end

          def create_table(table_data, rows_columns, rows_indexes, rows_triggers)
            keyspace_name   = table_data['keyspace_name']
            table_name      = table_data['columnfamily_name']
            key_validator   = @type_parser.parse(table_data['key_validator'])
            comparator      = @type_parser.parse(table_data['comparator'])
            column_aliases  = ::JSON.load(table_data['column_aliases'])

            if comparator.collections.nil?
              is_compact = true
              if !column_aliases.empty? || rows_columns.empty?
                has_value = true
                clustering_size = comparator.results.size
              else
                has_value = false
                clustering_size = 0
              end
            else
              size = comparator.results.size
              if !comparator.collections.empty?
                is_compact = false
                has_value  = false
                clustering_size = size - 2
              elsif column_aliases.size == size - 1 &&
                    comparator.results.last.first == Cassandra::Types.varchar
                is_compact = false
                has_value  = false
                clustering_size = size - 1
              else
                is_compact = true
                has_value  = (!column_aliases.empty? || rows_columns.empty?)
                clustering_size = size
              end
            end

            # Separate out the partition-key, clustering-columns, and other-columns
            partition_key      = []
            clustering_columns = []
            clustering_order   = []
            other_columns = []

            compaction_strategy = create_compaction_strategy(table_data)
            table_options =
              create_table_options(table_data, compaction_strategy, is_compact)

            key_aliases = ::JSON.load(table_data['key_aliases'])

            key_validator.results.each_with_index do |(type, order, is_frozen), i|
              key_alias = key_aliases.fetch(i) { i.zero? ? 'key' : "key#{i + 1}" }

              partition_key[i] = Column.new(key_alias, type, order, false, is_frozen)
            end

            clustering_size.times do |i|
              column_alias = column_aliases.fetch(i) { "column#{i + 1}" }
              type, order, is_frozen = comparator.results.fetch(i)

              clustering_columns[i] =
                Column.new(column_alias, type, order, false, is_frozen)
              clustering_order[i]   = order
            end

            if has_value
              value_alias   = table_data['value_alias']
              value_alias ||= 'value'

              unless value_alias.empty?
                type, order, is_frozen =
                  @type_parser.parse(table_data['default_validator']).results.first
                other_columns <<
                  Column.new(value_alias, type, order, false, is_frozen)
              end
            end

            index_rows = []
            rows_columns.each do |row|
              column = create_column(row)
              other_columns << column

              # In C* 1.2.x, index info is in the column metadata; rows_indexes is [].
              index_rows << [column, row] unless row['index_type'].nil?
            end

            table = Cassandra::Table.new(@schema.keyspace(keyspace_name),
                                         table_name,
                                         partition_key,
                                         clustering_columns,
                                         other_columns,
                                         table_options,
                                         clustering_order,
                                         table_data['id'])

            # Create Index objects and add them to the table.
            index_rows.each do |column, row|
              create_index(table, column, row)
            end
            table
          end

          def create_index(table, column, row_column)
            # Most of this logic was taken from the Java driver.
            options = {}
            # For some versions of C*, this field could have a literal string 'null' value.
            if !row_column['index_options'].nil? && row_column['index_options'] != 'null' &&
               !row_column['index_options'].empty?
              options = ::JSON.load(row_column['index_options'])
            end
            column_name = Util.escape_name(column.name)
            target = if options.key?('index_keys')
                       "keys(#{column_name})"
                     elsif options.key?('index_keys_and_values')
                       "entries(#{column_name})"
                     elsif column.frozen? && (column.type == Cassandra::Types::Set ||
                         column.type == Cassandra::Types::List ||
                         column.type == Cassandra::Types::Map)
                       "full(#{column_name})"
                     else
                       column_name
                     end

            table.add_index(Cassandra::Index.new(table,
                                                 row_column['index_name'],
                                                 row_column['index_type'].downcase.to_sym,
                                                 target,
                                                 options))
          end

          def create_column(column_data)
            name      = column_data['column_name']
            is_static = (column_data['type'] == 'STATIC')
            type, order, is_frozen =
              @type_parser.parse(column_data['validator']).results.first
            Column.new(name, type, order, is_static, is_frozen)
          end

          def create_compaction_strategy(table_data)
            klass = table_data['compaction_strategy_class']
            klass.slice!('org.apache.cassandra.db.compaction.')
            options = ::JSON.load(table_data['compaction_strategy_options'])
            ColumnContainer::Compaction.new(klass, options)
          end

          def create_table_options(table_data, compaction_strategy, is_compact)
            compression_parameters = ::JSON.load(table_data['compression_parameters'])
            if compression_parameters['sstable_compression']
              compression_parameters['sstable_compression']
                .slice!(COMPRESSION_PACKAGE_PREFIX)
            end
            Cassandra::ColumnContainer::Options.new(
              table_data['comment'],
              table_data['read_repair_chance'],
              table_data['local_read_repair_chance'],
              table_data['gc_grace_seconds'],
              table_data['caching'],
              table_data['bloom_filter_fp_chance'] || 0.01,
              table_data['populate_io_cache_on_flush'],
              table_data['memtable_flush_period_in_ms'],
              table_data['default_time_to_live'],
              nil,
              nil,
              table_data['replicate_on_write'],
              nil,
              nil,
              compaction_strategy,
              compression_parameters,
              is_compact,
              table_data['crc_check_chance'],
              table_data['extensions'],
              nil
            )
          end
        end

        class V2_0_x < V1_2_x
          SELECT_TRIGGERS = 'SELECT * FROM system.schema_triggers'.freeze

          SELECT_KEYSPACE           =
            'SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_TABLES    =
            'SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_COLUMNS   =
            'SELECT * FROM system.schema_columns WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_TRIGGERS =
            'SELECT * FROM system.schema_triggers WHERE keyspace_name = ?'.freeze

          SELECT_TABLE              =
            'SELECT * ' \
            'FROM system.schema_columnfamilies ' \
            'WHERE keyspace_name = ? AND columnfamily_name = ?'.freeze
          SELECT_TABLE_COLUMNS      =
            'SELECT * ' \
            'FROM system.schema_columns ' \
            'WHERE keyspace_name = ? AND columnfamily_name = ?'.freeze
          SELECT_TABLE_TRIGGERS =
            'SELECT * ' \
            'FROM system.schema_triggers ' \
            'WHERE keyspace_name = ? AND columnfamily_name = ?'.freeze

          private

          def create_table(table_data, rows_columns, rows_indexes, rows_triggers)
            keyspace_name   = table_data['keyspace_name']
            table_name      = table_data['columnfamily_name']
            comparator      = @type_parser.parse(table_data['comparator'])
            clustering_size = 0

            # Separate out partition-key, clustering columns, other columns.
            partition_key      = []
            clustering_columns = []
            clustering_order   = []
            other_columns = []

            index_rows = []
            rows_columns.each do |row|
              next if row['column_name'].empty?

              column = create_column(row)
              type   = row['type'].to_s
              ind = row['component_index'] || 0

              case type.upcase
              when 'PARTITION_KEY'
                partition_key[ind] = column
              when 'CLUSTERING_KEY'
                clustering_columns[ind] = column
                clustering_order[ind]   = column.order

                clustering_size += 1
              else
                other_columns << column
              end

              # In C* 2.0.x, index info is in the column metadata; rows_indexes is nil.
              index_rows << [column, row] unless row['index_type'].nil?
            end

            compaction_strategy = create_compaction_strategy(table_data)
            is_compact    = (clustering_size != comparator.results.size - 1) ||
                            !comparator.collections
            table_options =
              create_table_options(table_data, compaction_strategy, is_compact)

            table = Cassandra::Table.new(@schema.keyspace(keyspace_name),
                                         table_name,
                                         partition_key,
                                         clustering_columns,
                                         other_columns,
                                         table_options,
                                         clustering_order,
                                         table_data['id'])

            # Create Index objects and add them to the table.
            index_rows.each do |column, row|
              create_index(table, column, row)
            end

            # Create Trigger objects and add them to the table.
            rows_triggers.each do |row_trigger|
              table.add_trigger(Cassandra::Trigger.new(table,
                                                       row_trigger['trigger_name'],
                                                       row_trigger['trigger_options']))
            end

            table
          end

          def select_triggers(connection)
            send_select_request(connection, SELECT_TRIGGERS)
          end

          def select_keyspace(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE, params, hints)
          end

          def select_keyspace_tables(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_TABLES, params, hints)
          end

          def select_keyspace_columns(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_COLUMNS, params, hints)
          end

          def select_keyspace_triggers(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_TRIGGERS, params, hints)
          end

          def select_table(connection, keyspace_name, table_name)
            params         = [keyspace_name, table_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TABLE, params, hints)
          end

          def select_table_columns(connection, keyspace_name, table_name)
            params         = [keyspace_name, table_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TABLE_COLUMNS, params, hints)
          end

          def select_table_triggers(connection, keyspace_name, table_name)
            params         = [keyspace_name, table_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TABLE_TRIGGERS, params, hints)
          end

          def create_table_options(table_data, compaction_strategy, is_compact)
            compression_parameters = ::JSON.load(table_data['compression_parameters'])
            if compression_parameters['sstable_compression']
              compression_parameters['sstable_compression']
                .slice!(COMPRESSION_PACKAGE_PREFIX)
            end
            Cassandra::ColumnContainer::Options.new(
              table_data['comment'],
              table_data['read_repair_chance'],
              table_data['local_read_repair_chance'],
              table_data['gc_grace_seconds'],
              table_data['caching'],
              table_data['bloom_filter_fp_chance'],
              table_data['populate_io_cache_on_flush'],
              table_data['memtable_flush_period_in_ms'],
              table_data['default_time_to_live'],
              table_data['speculative_retry'],
              table_data['index_interval'],
              table_data['replicate_on_write'],
              nil,
              nil,
              compaction_strategy,
              compression_parameters,
              is_compact,
              table_data['crc_check_chance'],
              table_data['extensions'],
              nil
            )
          end
        end

        class V2_1_x < V2_0_x
          SELECT_TYPES          = 'SELECT * FROM system.schema_usertypes'.freeze
          SELECT_KEYSPACE_TYPES =
            'SELECT * FROM system.schema_usertypes WHERE keyspace_name = ?'.freeze
          SELECT_TYPE           =
            'SELECT * ' \
            'FROM system.schema_usertypes ' \
            'WHERE keyspace_name = ? AND type_name = ?'.freeze

          private

          def create_type(type_data)
            keyspace_name = type_data['keyspace_name']
            type_name     = type_data['type_name']
            type_fields   = ::Array.new

            field_names = type_data['field_names']
            field_types = type_data['field_types']

            field_names.zip(field_types) do |(field_name, fqcn)|
              field_type = @type_parser.parse(fqcn).results.first.first

              type_fields << [field_name, field_type]
            end

            Types.udt(keyspace_name, type_name, type_fields)
          end

          def select_types(connection)
            send_select_request(connection, SELECT_TYPES)
          end

          def select_keyspace_types(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_TYPES, params, hints)
          end

          def select_type(connection, keyspace_name, type_name)
            params = [keyspace_name, type_name]
            hints  = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TYPE, params, hints)
          end

          def create_table_options(table_data, compaction_strategy, is_compact)
            compression_parameters = ::JSON.load(table_data['compression_parameters'])
            if compression_parameters['sstable_compression']
              compression_parameters['sstable_compression']
                .slice!(COMPRESSION_PACKAGE_PREFIX)
            end
            Cassandra::ColumnContainer::Options.new(
              table_data['comment'],
              table_data['read_repair_chance'],
              table_data['local_read_repair_chance'],
              table_data['gc_grace_seconds'],
              table_data['caching'],
              table_data['bloom_filter_fp_chance'],
              table_data['populate_io_cache_on_flush'],
              table_data['memtable_flush_period_in_ms'],
              table_data['default_time_to_live'],
              table_data['speculative_retry'],
              table_data['index_interval'],
              table_data['replicate_on_write'],
              table_data['min_index_interval'],
              table_data['max_index_interval'],
              compaction_strategy,
              compression_parameters,
              is_compact,
              table_data['crc_check_chance'],
              table_data['extensions'],
              nil
            )
          end
        end

        class V2_2_x < V2_1_x
          SELECT_FUNCTIONS           = 'SELECT * FROM system.schema_functions'.freeze
          SELECT_AGGREGATES          = 'SELECT * FROM system.schema_aggregates'.freeze
          SELECT_KEYSPACE_FUNCTIONS  =
            'SELECT * FROM system.schema_functions WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_AGGREGATES =
            'SELECT * FROM system.schema_aggregates WHERE keyspace_name = ?'.freeze
          SELECT_FUNCTION            =
            'SELECT * ' \
            'FROM system.schema_functions ' \
            'WHERE keyspace_name = ? AND function_name = ? ' \
            'AND argument_types = ?'.freeze
          SELECT_AGGREGATE           =
            'SELECT * ' \
            'FROM system.schema_aggregates ' \
            'WHERE keyspace_name = ? AND aggregate_name = ? ' \
            'AND argument_types = ?'.freeze

          # parse an array of string argument types and return an array of
          # [Cassandra::Type]s.
          # @param connection a connection to a Cassandra node.
          # @param keyspace_name [String] name of the keyspace.
          # @param argument_types [Array<String>] array of argument types.
          # @return [Array<Cassandra::Type>] array of parsed types.
          def parse_argument_types(connection, keyspace_name, argument_types)
            argument_types.map do |argument_type|
              @type_parser.parse(argument_type).results.first.first
            end
          end

          private

          def create_function(function_data)
            keyspace_name  = function_data['keyspace_name']
            function_name  = function_data['function_name']
            function_lang  = function_data['language']
            function_type  =
              @type_parser.parse(function_data['return_type']).results.first.first
            function_body  = function_data['body']
            called_on_null = function_data['called_on_null_input']

            arguments = []

            Array(function_data['argument_names'])
              .zip(Array(function_data['argument_types'])) do |argument_name, fqcn|
              argument_type = @type_parser.parse(fqcn).results.first.first
              arguments << Argument.new(argument_name, argument_type)
            end

            Cassandra::Function.new(keyspace_name,
                                    function_name,
                                    function_lang,
                                    function_type,
                                    arguments,
                                    function_body,
                                    called_on_null)
          end

          def create_aggregate(aggregate_data, functions)
            keyspace_name  = aggregate_data['keyspace_name']
            aggregate_name = aggregate_data['aggregate_name']
            aggregate_type =
              @type_parser.parse(aggregate_data['return_type']).results.first.first
            argument_types = aggregate_data['argument_types'].map do |fqcn|
              @type_parser.parse(fqcn).results.first.first
            end.freeze
            state_type     =
              @type_parser.parse(aggregate_data['state_type']).results.first.first
            initial_state  = Util.encode_object(
              Protocol::Coder.read_value_v4(
                Protocol::CqlByteBuffer.new.append_bytes(aggregate_data['initcond']),
                state_type, nil
              )
            )

            # The state-function takes arguments: first the stype, then the args of the aggregate.
            state_function = functions.get(aggregate_data['state_func'],
                                           [state_type].concat(argument_types))

            # The final-function takes an stype argument.
            final_function = functions.get(aggregate_data['final_func'],
                                           [state_type])

            Aggregate.new(keyspace_name,
                          aggregate_name,
                          aggregate_type,
                          argument_types,
                          state_type,
                          initial_state,
                          state_function,
                          final_function)
          end

          def select_functions(connection)
            send_select_request(connection, SELECT_FUNCTIONS)
          end

          def select_aggregates(connection)
            send_select_request(connection, SELECT_AGGREGATES)
          end

          def select_keyspace_functions(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_FUNCTIONS, params, hints)
          end

          def select_keyspace_aggregates(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_AGGREGATES, params, hints)
          end

          def select_function(connection, keyspace_name, function_name, function_args)
            params = [keyspace_name, function_name, function_args.map(&:to_s)]
            hints  = [Types.varchar, Types.varchar, Types.list(Types.varchar)]
            send_select_request(connection, SELECT_FUNCTION, params, hints)
          end

          def select_aggregate(connection, keyspace_name, aggregate_name, aggregate_args)
            params = [keyspace_name, aggregate_name, aggregate_args.map(&:to_s)]
            hints  = [Types.varchar, Types.varchar, Types.list(Types.varchar)]
            send_select_request(connection, SELECT_AGGREGATE, params, hints)
          end
        end

        class V3_0_x < V2_2_x
          SELECT_KEYSPACES  = 'SELECT * FROM system_schema.keyspaces'.freeze
          SELECT_TABLES     = 'SELECT * FROM system_schema.tables'.freeze
          SELECT_COLUMNS    = 'SELECT * FROM system_schema.columns'.freeze
          SELECT_TYPES      = 'SELECT * FROM system_schema.types'.freeze
          SELECT_FUNCTIONS  = 'SELECT * FROM system_schema.functions'.freeze
          SELECT_AGGREGATES = 'SELECT * FROM system_schema.aggregates'.freeze
          SELECT_INDEXES    = 'SELECT * FROM system_schema.indexes'.freeze
          SELECT_VIEWS      = 'SELECT * FROM system_schema.views'.freeze
          SELECT_TRIGGERS   = 'SELECT * FROM system_schema.triggers'.freeze

          SELECT_KEYSPACE            =
            'SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_TABLES     =
            'SELECT * FROM system_schema.tables WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_INDEXES    =
            'SELECT * FROM system_schema.indexes WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_COLUMNS    =
            'SELECT * FROM system_schema.columns WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_VIEWS =
            'SELECT * FROM system_schema.views WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_TYPES      =
            'SELECT * FROM system_schema.types WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_FUNCTIONS  =
            'SELECT * FROM system_schema.functions WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_AGGREGATES =
            'SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_TRIGGERS   =
            'SELECT * FROM system_schema.triggers WHERE keyspace_name = ?'.freeze

          SELECT_TABLE         =
            'SELECT * ' \
            'FROM system_schema.tables ' \
            'WHERE keyspace_name = ? AND table_name = ?'.freeze
          SELECT_TABLE_COLUMNS =
            'SELECT * ' \
            'FROM system_schema.columns ' \
            'WHERE keyspace_name = ? AND table_name = ?'.freeze
          SELECT_TABLE_INDEXES =
            'SELECT * ' \
            'FROM system_schema.indexes ' \
            'WHERE keyspace_name = ? AND table_name = ?'.freeze
          SELECT_TABLE_TRIGGERS =
            'SELECT * ' \
            'FROM system_schema.triggers ' \
            'WHERE keyspace_name = ? AND table_name = ?'.freeze

          SELECT_VIEW =
            'SELECT * ' \
            'FROM system_schema.views ' \
            'WHERE keyspace_name = ? AND view_name = ?'.freeze

          SELECT_TYPE =
            'SELECT * ' \
            'FROM system_schema.types ' \
            'WHERE keyspace_name = ? AND type_name = ?'.freeze

          SELECT_FUNCTION =
            'SELECT * ' \
            'FROM system_schema.functions ' \
            'WHERE keyspace_name = ? AND function_name = ? ' \
            'AND argument_types = ?'.freeze

          SELECT_AGGREGATE =
            'SELECT * ' \
            'FROM system_schema.aggregates ' \
            'WHERE keyspace_name = ? AND aggregate_name = ? ' \
            'AND argument_types = ?'.freeze

          # parse an array of string argument types and return an array of
          # [Cassandra::Type]s.
          # @param connection a connection to a Cassandra node.
          # @param keyspace_name [String] name of the keyspace.
          # @param argument_types [Array<String>] array of argument types.
          # @return [Array<Cassandra::Type>] array of parsed types.
          def parse_argument_types(connection, keyspace_name, argument_types)
            types = @schema.keyspace(keyspace_name).send(:raw_types)
            argument_types.map do |argument_type|
              @type_parser.parse(argument_type, types).first
            end
          end

          private

          def select_keyspaces(connection)
            send_select_request(connection, SELECT_KEYSPACES)
          end

          def select_tables(connection)
            send_select_request(connection, SELECT_TABLES)
          end

          def select_indexes(connection)
            send_select_request(connection, SELECT_INDEXES)
          end

          def select_materialized_views(connection)
            send_select_request(connection, SELECT_VIEWS)
          end

          def select_columns(connection)
            send_select_request(connection, SELECT_COLUMNS)
          end

          def select_triggers(connection)
            send_select_request(connection, SELECT_TRIGGERS)
          end

          def select_types(connection)
            send_select_request(connection, SELECT_TYPES)
          end

          def select_functions(connection)
            send_select_request(connection, SELECT_FUNCTIONS)
          end

          def select_aggregates(connection)
            send_select_request(connection, SELECT_AGGREGATES)
          end

          def select_keyspace(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE, params, hints)
          end

          def select_keyspace_tables(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_TABLES, params, hints)
          end

          def select_keyspace_columns(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_COLUMNS, params, hints)
          end

          def select_keyspace_indexes(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_INDEXES, params, hints)
          end

          def select_keyspace_materialized_views(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_VIEWS, params, hints)
          end

          def select_keyspace_triggers(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_TRIGGERS, params, hints)
          end

          def select_keyspace_types(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_TYPES, params, hints)
          end

          def select_keyspace_functions(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_FUNCTIONS, params, hints)
          end

          def select_keyspace_aggregates(connection, keyspace_name)
            params = [keyspace_name]
            hints  = [Types.varchar]
            send_select_request(connection, SELECT_KEYSPACE_AGGREGATES, params, hints)
          end

          def select_table(connection, keyspace_name, table_name)
            params         = [keyspace_name, table_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TABLE, params, hints)
          end

          def select_table_columns(connection, keyspace_name, table_name)
            # This is identical to the 2.0 impl, but the SELECT_TABLE_COLUMNS query
            # is different between the two, so we need two impls. :(
            # Also, this method works fine for finding view columns as well.
            params         = [keyspace_name, table_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TABLE_COLUMNS, params, hints)
          end

          def select_table_indexes(connection, keyspace_name, table_name)
            params         = [keyspace_name, table_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TABLE_INDEXES, params, hints)
          end

          def select_materialized_view(connection, keyspace_name, view_name)
            params         = [keyspace_name, view_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_VIEW, params, hints)
          end

          def select_table_triggers(connection, keyspace_name, table_name)
            params         = [keyspace_name, table_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TABLE_TRIGGERS, params, hints)
          end

          def select_type(connection, keyspace_name, type_name)
            params = [keyspace_name, type_name]
            hints  = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TYPE, params, hints)
          end

          def select_function(connection, keyspace_name, function_name, function_args)
            params = [keyspace_name, function_name, function_args.map(&:to_s)]
            hints  = [Types.varchar, Types.varchar, Types.list(Types.varchar)]
            send_select_request(connection, SELECT_FUNCTION, params, hints)
          end

          def select_aggregate(connection, keyspace_name, aggregate_name, aggregate_args)
            params = [keyspace_name, aggregate_name, aggregate_args.map(&:to_s)]
            hints  = [Types.varchar, Types.varchar, Types.list(Types.varchar)]
            send_select_request(connection, SELECT_AGGREGATE, params, hints)
          end

          def create_function(function_data, types = nil)
            keyspace_name  = function_data['keyspace_name']
            function_name  = function_data['function_name']
            function_lang  = function_data['language']
            types        ||= @schema.keyspace(keyspace_name).send(:raw_types)
            function_type  = @type_parser.parse(function_data['return_type'], types).first
            function_body  = function_data['body']
            called_on_null = function_data['called_on_null_input']

            arguments = []

            function_data['argument_names']
              .zip(function_data['argument_types']) do |argument_name, argument_type|
              arguments << Argument.new(argument_name,
                                        @type_parser.parse(argument_type, types).first)
            end

            Cassandra::Function.new(keyspace_name,
                                    function_name,
                                    function_lang,
                                    function_type,
                                    arguments,
                                    function_body,
                                    called_on_null)
          end

          def create_aggregate(aggregate_data, functions, types = nil)
            keyspace_name  = aggregate_data['keyspace_name']
            aggregate_name = aggregate_data['aggregate_name']
            types        ||= @schema.keyspace(keyspace_name).send(:raw_types)
            aggregate_type =
              @type_parser.parse(aggregate_data['return_type'], types).first
            argument_types = aggregate_data['argument_types'].map do |argument_type|
              @type_parser.parse(argument_type, types).first
            end.freeze
            state_type     = @type_parser.parse(aggregate_data['state_type'], types).first
            initial_state  = aggregate_data['initcond'] || 'null'

            # The state-function takes arguments: first the stype, then the args of the
            # aggregate.
            state_function = functions.get(aggregate_data['state_func'],
                                           [state_type].concat(argument_types))

            # The final-function takes an stype argument.
            final_function = functions.get(aggregate_data['final_func'],
                                           [state_type])

            Aggregate.new(keyspace_name,
                          aggregate_name,
                          aggregate_type,
                          argument_types,
                          state_type,
                          initial_state,
                          state_function,
                          final_function)
          end

          def create_types(rows_types, types)
            skipped_rows = ::Array.new

            loop do
              rows_size = rows_types.size

              until rows_types.empty?
                type_data     = rows_types.shift
                type_name     = type_data['type_name']
                type_keyspace = type_data['keyspace_name']
                type_fields   = ::Array.new

                begin
                  field_names = type_data['field_names']
                  field_types = type_data['field_types']
                  field_names.each_with_index do |field_name, i|
                    field_type = @type_parser.parse(field_types[i], types).first
                    type_fields << [field_name, field_type]
                  end

                  types[type_name] = Types.udt(type_keyspace, type_name, type_fields)
                rescue CQLTypeParser::IncompleteTypeError
                  skipped_rows << type_data
                  next
                end
              end

              break if skipped_rows.empty?

              raise 'Unable to resolve circular references among UDTs when parsing' if rows_size == skipped_rows.size

              rows_types, skipped_rows = skipped_rows, rows_types
            end
          end

          def create_keyspace(keyspace_data, rows_tables, rows_columns, rows_types,
                              rows_functions, rows_aggregates, rows_views, rows_indexes, rows_triggers)
            keyspace_name = keyspace_data['keyspace_name']
            replication   = create_replication(keyspace_data)

            types = ::Hash.new
            create_types(rows_types, types)

            # Create a FunctionCollection for the functions and aggregates.
            functions = Cassandra::FunctionCollection.new
            rows_functions.each do |row|
              functions.add_or_update(create_function(row, types))
            end

            aggregates = Cassandra::FunctionCollection.new
            rows_aggregates.each do |row|
              aggregates.add_or_update(create_aggregate(row, functions, types))
            end

            # lookup_columns is a hash of <table-name, rows_columns for that table>.
            # However, views are analogous to tables in this context, so we get
            # view columns organized by view-name also.

            lookup_columns = map_rows_by(rows_columns, 'table_name')
            lookup_indexes = map_rows_by(rows_indexes, 'table_name')
            lookup_triggers = map_rows_by(rows_triggers, 'table_name')
            tables = rows_tables.each_with_object({}) do |row, h|
              table_name = row['table_name']
              h[table_name] = create_table(row, lookup_columns[table_name],
                                           lookup_indexes[table_name], lookup_triggers[table_name], types)
            end

            views = rows_views.each_with_object({}) do |row, h|
              view_name = row['view_name']
              h[view_name] = create_materialized_view(row,
                                                      lookup_columns[view_name],
                                                      types)
            end

            Keyspace.new(keyspace_name,
                         keyspace_data['durable_writes'],
                         replication,
                         tables,
                         types,
                         functions,
                         aggregates,
                         views)
          end

          def create_replication(keyspace_data)
            options = keyspace_data['replication']
            klass   = options.delete('class')
            klass.slice!(REPLICATION_PACKAGE_PREFIX)
            Keyspace::Replication.new(klass, options)
          end

          def create_compaction_strategy(table_data)
            options = table_data['compaction']
            klass   = options.delete('class')
            klass.slice!('org.apache.cassandra.db.compaction.')
            ColumnContainer::Compaction.new(klass, options)
          end

          def create_table_options(table_data, compaction_strategy, is_compact)
            compression = table_data['compression']
            compression['class'].slice!(COMPRESSION_PACKAGE_PREFIX) if compression['class']

            Cassandra::ColumnContainer::Options.new(
              table_data['comment'],
              table_data['read_repair_chance'],
              table_data['dclocal_read_repair_chance'],
              table_data['gc_grace_seconds'],
              table_data['caching'],
              table_data['bloom_filter_fp_chance'],
              nil,
              table_data['memtable_flush_period_in_ms'],
              table_data['default_time_to_live'],
              table_data['speculative_retry'],
              nil,
              nil,
              table_data['min_index_interval'],
              table_data['max_index_interval'],
              compaction_strategy,
              compression,
              is_compact,
              table_data['crc_check_chance'],
              table_data['extensions'],
              table_data['cdc']
            )
          end

          def create_column(column_data, types)
            name      = column_data['column_name']
            is_static = column_data['kind'].to_s.casecmp('STATIC').zero?
            order     = column_data['clustering_order'] == 'desc' ? :desc : :asc
            if column_data['type'][0] == "'"
              # This is a custom column type.
              type = Types.custom(column_data['type'].slice(1, column_data['type'].length - 2))
              is_frozen = false
            else
              type, is_frozen = @type_parser.parse(column_data['type'], types)
            end

            Column.new(name, type, order, is_static, is_frozen)
          end

          def create_table(table_data, rows_columns, rows_indexes, rows_triggers, types = nil)
            keyspace_name   = table_data['keyspace_name']
            table_name      = table_data['table_name']
            table_flags     = table_data['flags']

            is_dense    = table_flags.include?('dense')
            is_super    = table_flags.include?('super')
            is_compound = table_flags.include?('compound')
            is_compact  = is_super || is_dense || !is_compound
            is_static_compact = !is_super && !is_dense && !is_compound

            # Separate out partition-key, clustering columns, other columns.
            partition_key      = []
            clustering_columns = []
            clustering_order   = []
            other_columns = []
            types ||= @schema.keyspace(keyspace_name).send(:raw_types)

            rows_columns.each do |row|
              next if row['column_name'].empty?

              kind   = row['kind'].to_s
              index  = row['position'] || 0

              if is_static_compact
                if kind.casecmp('CLUSTERING').zero? || kind.casecmp('REGULAR').zero?
                  # Skip clustering columns in static-compact tables; they are internal to C*.
                  # Oddly so are regular columns.
                  next
                end
                if kind.casecmp('STATIC').zero?
                  # Coerce static type to regular.
                  kind = 'REGULAR'
                  row['kind'] = 'regular'
                end
              end

              column = create_column(row, types)
              case kind.upcase
              when 'PARTITION_KEY'
                partition_key[index] = column
              when 'CLUSTERING'
                clustering_columns[index] = column
                clustering_order[index]   = column.order
              else
                other_columns << column
              end
            end

            # Default the crc_check_chance to 1.0 (Java driver does this, so we
            # should, too).
            table_data['crc_check_chance'] ||= 1.0
            compaction_strategy = create_compaction_strategy(table_data)
            table_options =
              create_table_options(table_data, compaction_strategy, is_compact)

            table = Cassandra::Table.new(@schema.keyspace(keyspace_name),
                                         table_name,
                                         partition_key,
                                         clustering_columns,
                                         other_columns,
                                         table_options,
                                         clustering_order,
                                         table_data['id'])
            rows_indexes.each do |row|
              create_index(table, row)
            end

            # Create Trigger objects and add them to the table.
            rows_triggers.each do |row_trigger|
              table.add_trigger(Cassandra::Trigger.new(table,
                                                       row_trigger['trigger_name'],
                                                       row_trigger['options']))
            end

            table
          end

          def create_index(table, row_index)
            options = row_index['options']
            table.add_index(Cassandra::Index.new(table, row_index['index_name'],
                                                 row_index['kind'].downcase.to_sym,
                                                 options['target'], options))
          end

          def create_materialized_view(view_data, rows_columns, types = nil)
            keyspace_name   = view_data['keyspace_name']
            view_name       = view_data['view_name']
            base_table_name = view_data['base_table_name']
            include_all_columns = view_data['include_all_columns']
            where_clause = view_data['where_clause']

            # Separate out partition key, clustering columns, other columns
            partition_key      = []
            clustering_columns = []
            other_columns = []
            types ||= @schema.keyspace(keyspace_name).send(:raw_types)

            rows_columns.each do |row|
              next if row['column_name'].empty?

              column = create_column(row, types)
              kind   = row['kind'].to_s
              index  = row['position'] || 0

              case kind.upcase
              when 'PARTITION_KEY'
                partition_key[index] = column
              when 'CLUSTERING'
                clustering_columns[index] = column
              else
                other_columns << column
              end
            end

            compaction_strategy = create_compaction_strategy(view_data)
            view_options = create_table_options(view_data, compaction_strategy, false)

            MaterializedView.new(@schema.keyspace(keyspace_name),
                                 view_name,
                                 partition_key,
                                 clustering_columns,
                                 other_columns,
                                 view_options,
                                 include_all_columns,
                                 where_clause,
                                 base_table_name,
                                 view_data['id'])
          end
        end

        class MultiVersion
          class Version
            def initialize(version, constructor)
              @version     = version
              @constructor = constructor
              @fetcher     = nil
            end

            def matches?(version)
              version.start_with?(@version)
            end

            def fetcher
              @fetcher ||= @constructor.call
            end
          end

          def initialize(registry)
            @registry = registry
            @versions = []
            @fetchers = {}
          end

          def when(version, &block)
            @versions << Version.new(version, block)
          end

          def fetch(connection)
            find_fetcher(connection)
              .fetch(connection)
          rescue => e
            return Ione::Future.failed(e)
          end

          def fetch_keyspace(connection, keyspace_name)
            find_fetcher(connection)
              .fetch_keyspace(connection, keyspace_name)
          rescue => e
            return Ione::Future.failed(e)
          end

          def fetch_table(connection, keyspace_name, table_name)
            find_fetcher(connection)
              .fetch_table(connection, keyspace_name, table_name)
          rescue => e
            return Ione::Future.failed(e)
          end

          def fetch_materialized_view(connection, keyspace_name, view_name)
            find_fetcher(connection)
              .fetch_materialized_view(connection, keyspace_name, view_name)
          rescue => e
            return Ione::Future.failed(e)
          end

          def fetch_type(connection, keyspace_name, type_name)
            find_fetcher(connection)
              .fetch_type(connection, keyspace_name, type_name)
          rescue => e
            return Ione::Future.failed(e)
          end

          def fetch_function(connection, keyspace_name, function_name, function_args)
            find_fetcher(connection)
              .fetch_function(connection, keyspace_name, function_name, function_args)
          rescue => e
            return Ione::Future.failed(e)
          end

          def fetch_aggregate(connection, keyspace_name, aggregate_name, aggregate_args)
            find_fetcher(connection)
              .fetch_aggregate(connection, keyspace_name, aggregate_name, aggregate_args)
          rescue => e
            return Ione::Future.failed(e)
          end

          # parse an array of string argument types and return an array of
          # [Cassandra::Type]s.
          # @param connection a connection to a Cassandra node.
          # @param keyspace_name [String] name of the keyspace.
          # @param argument_types [Array<String>] array of argument types.
          # @return [Array<Cassandra::Type>] array of parsed types.
          def parse_argument_types(connection, keyspace_name, argument_types)
            find_fetcher(connection).parse_argument_types(connection,
                                                          keyspace_name,
                                                          argument_types)
          end

          private

          def find_fetcher(connection)
            host = @registry.host(connection.host)

            unless host
              ips = @registry.hosts.map(&:ip)
              raise Errors::ClientError,
                    'unable to find release version for current host, ' \
                    "connected to #{connection.host}, but cluster contains " \
                    "#{ips}."
            end

            version = host.release_version
            unless version
              raise Errors::ClientError, 'unable to determine release ' \
                                         "version for host: #{host.inspect}"
            end

            @fetchers[version] ||= begin
              current = @versions.find {|v| v.matches?(version)}
              unless current
                raise Errors::ClientError, 'unsupported release version ' \
                                           "#{version.inspect}."
              end
              current.fetcher
            end
          end
        end
      end
    end
  end
end
