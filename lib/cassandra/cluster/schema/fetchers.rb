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
      module Fetcher
        FUTURE_EMPTY_LIST = Ione::Future.resolved(EMPTY_LIST)
        REPLICATION_PACKAGE_PREFIX = 'org.apache.cassandra.locator.'.freeze
        COMPRESSION_PACKAGE_PREFIX = 'org.apache.cassandra.io.compress.'.freeze

        def fetch(connection)
          Ione::Future.all(select_keyspaces(connection),
                           select_tables(connection),
                           select_columns(connection),
                           select_types(connection),
                           select_functions(connection),
                           select_aggregates(connection))
                      .map do |(rows_keyspaces, rows_tables, rows_columns,
                                rows_types, rows_functions, rows_aggregates)|

                        lookup_tables     = map_rows_by(rows_tables, 'keyspace_name')
                        lookup_columns    = map_rows_by(rows_columns, 'keyspace_name')
                        lookup_types      = map_rows_by(rows_types, 'keyspace_name')
                        lookup_functions  = map_rows_by(rows_functions, 'keyspace_name')
                        lookup_aggregates = map_rows_by(rows_aggregates, 'keyspace_name')

                        rows_keyspaces.map do |keyspace_data|
                          name = keyspace_data['keyspace_name']

                          create_keyspace(keyspace_data,
                                          lookup_tables[name],
                                          lookup_columns[name],
                                          lookup_types[name],
                                          lookup_functions[name],
                                          lookup_aggregates[name])
                        end
                      end
        end

        def fetch_keyspace(connection, keyspace_name)
          Ione::Future.all(select_keyspace(connection, keyspace_name),
                           select_keyspace_tables(connection, keyspace_name),
                           select_keyspace_columns(connection, keyspace_name),
                           select_keyspace_types(connection, keyspace_name),
                           select_keyspace_functions(connection, keyspace_name),
                           select_keyspace_aggregates(connection, keyspace_name))
                      .map do |(rows_keyspaces, rows_tables, rows_columns,
                                rows_types, rows_functions, rows_aggregates)|
                        if rows_keyspaces.empty?
                          nil
                        else
                          create_keyspace(rows_keyspaces.first,
                                          rows_tables,
                                          rows_columns,
                                          rows_types,
                                          rows_functions,
                                          rows_aggregates)
                        end
                      end
        end

        def fetch_table(connection, keyspace_name, table_name)
          Ione::Future.all(select_table(connection, keyspace_name, table_name),
                           select_table_columns(connection, keyspace_name, table_name))
                      .map do |(rows_tables, rows_columns)|
            if rows_tables.empty?
              nil
            else
              create_table(rows_tables.first,
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

        def fetch_function(connection, keyspace_name, function_name)
          select_function(connection, keyspace_name, function_name).map do |rows_functions|
            if rows_functions.empty?
              nil
            else
              create_function(rows_functions.first)
            end
          end
        end

        def fetch_aggregate(connection, keyspace_name, aggregate_name)
          select_aggregate(connection, keyspace_name, aggregate_name).map do |rows_aggregates|
            if rows_aggregates.empty?
              nil
            else
              create_aggregate(rows_aggregates.first, @schema.keyspace(keyspace_name).send(:raw_functions))
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

        def select_columns(connection)
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

        def select_keyspace_columns(connection, keyspace_name)
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

        def select_table_columns(connection, keyspace_name, table_name)
          FUTURE_EMPTY_LIST
        end

        def select_type(connection, keyspace_name, type_name)
          FUTURE_EMPTY_LIST
        end

        def select_function(connection, keyspace_name, function_name)
          FUTURE_EMPTY_LIST
        end

        def select_aggregate(connection, keyspace_name, aggregate_name)
          FUTURE_EMPTY_LIST
        end

        def send_select_request(connection, cql, params = EMPTY_LIST, types = EMPTY_LIST)
          backtrace = caller
          connection.send_request(Protocol::QueryRequest.new(cql, params, types, :one)).map do |r|
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
            map[key] = [] unless map.has_key?(key)

            if block
              map[key] << yield(row)
            else
              map[key] << row
            end
          end
        end
      end

      module Fetchers
        class V1_2_x
          SELECT_KEYSPACES        = 'SELECT * FROM system.schema_keyspaces'.freeze
          SELECT_TABLES           = 'SELECT * FROM system.schema_columnfamilies'.freeze
          SELECT_COLUMNS          = 'SELECT * FROM system.schema_columns'.freeze
          SELECT_KEYSPACE         = 'SELECT * FROM system.schema_keyspaces WHERE keyspace_name = \'%s\''.freeze
          SELECT_KEYSPACE_TABLES  = 'SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = \'%s\''.freeze
          SELECT_KEYSPACE_COLUMNS = 'SELECT * FROM system.schema_columns WHERE keyspace_name = \'%s\''.freeze
          SELECT_TABLE            = 'SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = \'%s\' AND columnfamily_name = \'%s\''.freeze
          SELECT_TABLE_COLUMNS    = 'SELECT * FROM system.schema_columns WHERE keyspace_name = \'%s\' AND columnfamily_name = \'%s\''.freeze

          include Fetcher

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
            send_select_request(connection, SELECT_KEYSPACE_TABLES % keyspace_name)
          end

          def select_keyspace_columns(connection, keyspace_name)
            send_select_request(connection, SELECT_KEYSPACE_COLUMNS % keyspace_name)
          end

          def select_table(connection, keyspace_name, table_name)
            send_select_request(connection, SELECT_TABLE % [keyspace_name, table_name])
          end

          def select_table_columns(connection, keyspace_name, table_name)
            send_select_request(connection, SELECT_TABLE_COLUMNS % [keyspace_name, table_name])
          end

          def create_replication(keyspace_data)
            klass = keyspace_data['strategy_class']
            klass.slice!(REPLICATION_PACKAGE_PREFIX)
            options = ::JSON.load(keyspace_data['strategy_options'])
            Keyspace::Replication.new(klass, options)
          end

          def create_keyspace(keyspace_data, rows_tables, rows_columns,
                              rows_types, rows_functions, rows_aggregates)
            keyspace_name = keyspace_data['keyspace_name']
            replication   = create_replication(keyspace_data)
            types = rows_types.each_with_object({}) do |row, types|
                      types[row['type_name']] = create_type(row)
                    end
            functions = rows_functions.each_with_object({}) do |row, functions|
                          functions[row['function_name']] = create_function(row)
                        end
            aggregates = rows_aggregates.each_with_object({}) do |row, aggregates|
                           aggregates[row['aggregate_name']] = create_aggregate(row, functions)
                         end

            lookup_columns = map_rows_by(rows_columns, 'columnfamily_name')
            tables = rows_tables.each_with_object({}) do |row, tables|
              table_name = row['columnfamily_name']
              tables[table_name] = create_table(row, lookup_columns[table_name])
            end

            Keyspace.new(keyspace_name, keyspace_data['durable_writes'],
                         replication, tables, types, functions, aggregates)
          end

          def create_table(table_data, rows_columns)
            keyspace_name   = table_data['keyspace_name']
            table_name      = table_data['columnfamily_name']
            key_validator   = @type_parser.parse(table_data['key_validator'])
            comparator      = @type_parser.parse(table_data['comparator'])
            column_aliases  = ::JSON.load(table_data['column_aliases'])

            if !comparator.collections.nil?
              size = comparator.results.size
              if !comparator.collections.empty?
                is_compact = false
                has_value  = false
                clustering_size = size - 2
              elsif column_aliases.size == size - 1 && comparator.results.last.first == Cassandra::Types.varchar
                is_compact = false
                has_value  = false
                clustering_size = size - 1
              else
                is_compact = true
                has_value  = (!column_aliases.empty? || rows_columns.empty?)
                clustering_size = size
              end
            else
              is_compact = true
              if (!column_aliases.empty? || rows_columns.empty?)
                has_value = true
                clustering_size = comparator.results.size
              else
                has_value = false
                clustering_size = 0
              end
            end

            partition_key      = []
            clustering_columns = []
            clustering_order   = []

            compaction_strategy = create_compaction_strategy(table_data)
            table_options = create_table_options(table_data, compaction_strategy, is_compact)
            table_columns = {}
            other_columns = []

            key_aliases = ::JSON.load(table_data['key_aliases'])

            key_validator.results.each_with_index do |(type, order, is_frozen), i|
              key_alias = key_aliases.fetch(i) { i.zero? ? "key" : "key#{i + 1}" }

              partition_key[i] = Column.new(key_alias, type, order, nil, false, is_frozen)
            end

            clustering_size.times do |i|
              column_alias = column_aliases.fetch(i) { "column#{i + 1}" }
              type, order, is_frozen = comparator.results.fetch(i)

              clustering_columns[i] = Column.new(column_alias, type, order, nil, false, is_frozen)
              clustering_order[i]   = order
            end

            if has_value
              value_alias   = table_data['value_alias']
              value_alias ||= 'value'

              unless value_alias.empty?
                type, order, is_frozen = @type_parser.parse(table_data['default_validator']).results.first
                other_columns << Column.new(value_alias, type, order, nil, false, is_frozen)
              end
            end

            rows_columns.each do |row|
              other_columns << create_column(row)
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

            Table.new(keyspace_name, table_name, partition_key, clustering_columns,
                      table_columns, table_options, clustering_order)
          end

          def create_column(column_data)
            name      = column_data['column_name']
            is_static = (column_data['type'] == 'STATIC')
            type, order, is_frozen = @type_parser.parse(column_data['validator']).results.first

            if column_data['index_type'].nil?
              index = nil
            elsif column_data['index_type'].to_s.upcase == 'CUSTOM' || !column_data['index_options']
              index = Column::Index.new(column_data['index_name'])
            else
              options = ::JSON.load(column_data['index_options'])
              index   = Column::Index.new(column_data['index_name'], options && options['class_name'])
            end

            Column.new(name, type, order, index, is_static, is_frozen)
          end

          def create_compaction_strategy(table_data)
            klass = table_data['compaction_strategy_class']
            klass.slice!('org.apache.cassandra.db.compaction.')
            options = ::JSON.load(table_data['compaction_strategy_options'])
            Table::Compaction.new(klass, options)
          end

          def create_table_options(table_data, compaction_strategy, is_compact)
            compression_parameters = ::JSON.load(table_data['compression_parameters'])
            compression_parameters['sstable_compression'].slice!(COMPRESSION_PACKAGE_PREFIX) if compression_parameters['sstable_compression']
            Table::Options.new(
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
              is_compact
            )
          end
        end

        class V2_0_x < V1_2_x
          SELECT_KEYSPACE           = 'SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_TABLES    = 'SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_COLUMNS   = 'SELECT * FROM system.schema_columns WHERE keyspace_name = ?'.freeze
          SELECT_TABLE              = 'SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ? AND columnfamily_name = ?'.freeze
          SELECT_TABLE_COLUMNS      = 'SELECT * FROM system.schema_columns WHERE keyspace_name = ? AND columnfamily_name = ?'.freeze

          private

          def create_table(table_data, rows_columns)
            keyspace_name   = table_data['keyspace_name']
            table_name      = table_data['columnfamily_name']
            comparator      = @type_parser.parse(table_data['comparator'])
            table_columns   = {}
            other_columns   = []
            clustering_size = 0

            partition_key      = []
            clustering_columns = []
            clustering_order   = []

            rows_columns.each do |row|
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

                if clustering_size.zero? || index == clustering_size
                  clustering_size = index + 1
                end
              else
                other_columns << column
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

            compaction_strategy = create_compaction_strategy(table_data)
            is_compact    = (clustering_size != comparator.results.size - 1) || !comparator.collections
            table_options = create_table_options(table_data, compaction_strategy, is_compact)

            Table.new(keyspace_name, table_name, partition_key, clustering_columns,
                      table_columns, table_options, clustering_order)
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

          def create_table_options(table_data, compaction_strategy, is_compact)
            compression_parameters = ::JSON.load(table_data['compression_parameters'])
            compression_parameters['sstable_compression'].slice!(COMPRESSION_PACKAGE_PREFIX) if compression_parameters['sstable_compression']
            Table::Options.new(
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
              is_compact
            )
          end
        end

        class V2_1_x < V2_0_x
          SELECT_TYPES          = 'SELECT * FROM system.schema_usertypes'.freeze
          SELECT_KEYSPACE_TYPES = 'SELECT * FROM system.schema_usertypes WHERE keyspace_name = ?'.freeze
          SELECT_TYPE           = 'SELECT * FROM system.schema_usertypes WHERE keyspace_name = ? AND type_name = ?'.freeze

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
            compression_parameters['sstable_compression'].slice!(COMPRESSION_PACKAGE_PREFIX) if compression_parameters['sstable_compression']
            Table::Options.new(
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
              is_compact
            )
          end
        end

        class V2_2_x < V2_1_x
          SELECT_FUNCTIONS           = 'SELECT * FROM system.schema_functions'.freeze
          SELECT_AGGREGATES          = 'SELECT * FROM system.schema_aggregates'.freeze
          SELECT_KEYSPACE_FUNCTIONS  = 'SELECT * FROM system.schema_functions WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_AGGREGATES = 'SELECT * FROM system.schema_aggregates WHERE keyspace_name = ?'.freeze
          SELECT_FUNCTION            = 'SELECT * FROM system.schema_functions WHERE keyspace_name = ? AND function_name = ?'.freeze
          SELECT_AGGREGATE           = 'SELECT * FROM system.schema_aggregates WHERE keyspace_name = ? AND aggregate_name = ?'.freeze

          private

          def create_function(function_data)
            keyspace_name  = function_data['keyspace_name']
            function_name  = function_data['function_name']
            function_lang  = function_data['language']
            function_type  = @type_parser.parse(function_data['return_type']).results.first.first
            function_body  = function_data['body']
            called_on_null = function_data['called_on_null_input']

            arguments = ::Hash.new

            function_data['argument_names'].zip(function_data['argument_types']) do |argument_name, fqcn|
              argument_type = @type_parser.parse(fqcn).results.first.first
              arguments[argument_name] = Argument.new(argument_name, argument_type)
            end

            Function.new(keyspace_name, function_name, function_lang, function_type, arguments, function_body, called_on_null)
          end

          def create_aggregate(aggregate_data, functions)
            keyspace_name  = aggregate_data['keyspace_name']
            aggregate_name = aggregate_data['aggregate_name']
            aggregate_type = @type_parser.parse(aggregate_data['return_type']).results.first.first
            argument_types = aggregate_data['argument_types'].map {|fqcn| @type_parser.parse(fqcn).results.first.first}.freeze
            state_type     = @type_parser.parse(aggregate_data['state_type']).results.first.first
            initial_state  = Util.encode_object(Protocol::Coder.read_value_v3(Protocol::CqlByteBuffer.new.append_bytes(aggregate_data['initcond']), state_type))
            state_function = functions[aggregate_data['state_func']]
            final_function = functions[aggregate_data['final_func']]

            Aggregate.new(keyspace_name, aggregate_name, aggregate_type, argument_types, state_type, initial_state, state_function, final_function)
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

          def select_function(connection, keyspace_name, function_name)
            params = [keyspace_name, function_name]
            hints  = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_FUNCTION, params, hints)
          end

          def select_aggregate(connection, keyspace_name, aggregate_name)
            params = [keyspace_name, aggregate_name]
            hints  = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_AGGREGATE, params, hints)
          end
        end

        class V3_0_x < V2_2_x
          SELECT_KEYSPACES  = "SELECT * FROM system_schema.keyspaces".freeze
          SELECT_TABLES     = "SELECT * FROM system_schema.tables".freeze
          SELECT_COLUMNS    = "SELECT * FROM system_schema.columns".freeze
          SELECT_TYPES      = "SELECT * FROM system_schema.types".freeze
          SELECT_FUNCTIONS  = "SELECT * FROM system_schema.functions".freeze
          SELECT_AGGREGATES = "SELECT * FROM system_schema.aggregates".freeze
          SELECT_INDEXES    = "SELECT * FROM system_schema.indexes".freeze
          SELECT_VIEWS      = "SELECT * FROM system_schema.materialized_views".freeze

          SELECT_KEYSPACE            = 'SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_TABLES     = 'SELECT * FROM system_schema.tables WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_COLUMNS    = 'SELECT * FROM system_schema.columns WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_TYPES      = "SELECT * FROM system_schema.types WHERE keyspace_name = ?".freeze
          SELECT_KEYSPACE_FUNCTIONS  = 'SELECT * FROM system_schema.functions WHERE keyspace_name = ?'.freeze
          SELECT_KEYSPACE_AGGREGATES = 'SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?'.freeze

          SELECT_TABLE         = 'SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?'.freeze
          SELECT_TABLE_COLUMNS = 'SELECT * FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?'.freeze

          SELECT_TYPE = 'SELECT * FROM system_schema.types WHERE keyspace_name = ? AND type_name = ?'.freeze

          SELECT_FUNCTION = 'SELECT * FROM system_schema.functions WHERE keyspace_name = ? AND function_name = ?'.freeze

          SELECT_AGGREGATE = 'SELECT * FROM system_schema.aggregates WHERE keyspace_name = ? AND aggregate_name = ?'.freeze

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
            params         = [keyspace_name, table_name]
            hints          = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TABLE_COLUMNS, params, hints)
          end

          def select_type(connection, keyspace_name, type_name)
            params = [keyspace_name, type_name]
            hints  = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_TYPE, params, hints)
          end

          def select_function(connection, keyspace_name, function_name)
            params = [keyspace_name, function_name]
            hints  = [Types.varchar, Types.varchar]
            send_select_request(connection, SELECT_FUNCTION, params, hints)
          end

          def select_aggregate(connection, keyspace_name, aggregate_name)
            params = [keyspace_name, aggregate_name]
            hints  = [Types.varchar, Types.varchar]
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

            arguments = ::Hash.new

            function_data['argument_names'].zip(function_data['argument_types']) do |argument_name, argument_type|
              arguments[argument_name] = Argument.new(argument_name, @type_parser.parse(argument_type, types).first)
            end

            Function.new(keyspace_name, function_name, function_lang, function_type, arguments, function_body, called_on_null)
          end

          def create_aggregate(aggregate_data, functions, types = nil)
            keyspace_name  = aggregate_data['keyspace_name']
            aggregate_name = aggregate_data['aggregate_name']
            types        ||= @schema.keyspace(keyspace_name).send(:raw_types)
            aggregate_type = @type_parser.parse(aggregate_data['return_type'], types).first
            argument_types = aggregate_data['argument_types'].map {|argument_type| @type_parser.parse(argument_type, types).first}.freeze
            state_type     = @type_parser.parse(aggregate_data['state_type'], types).first
            initial_state  = aggregate_data['initcond']
            state_function = functions[aggregate_data['state_func']]
            final_function = functions[aggregate_data['final_func']]

            Aggregate.new(keyspace_name, aggregate_name, aggregate_type, argument_types, state_type, initial_state, state_function, final_function)
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

              if rows_size == skipped_rows.size
                raise "Unable to resolve circular references among UDTs when parsing"
              else
                rows_types, skipped_rows = skipped_rows, rows_types
              end
            end
          end

          def create_keyspace(keyspace_data, rows_tables, rows_columns,
                              rows_types, rows_functions, rows_aggregates)
            keyspace_name = keyspace_data['keyspace_name']
            replication   = create_replication(keyspace_data)

            types = ::Hash.new
            create_types(rows_types, types)

            functions = rows_functions.each_with_object({}) do |row, functions|
                          functions[row['function_name']] = create_function(row, types)
                        end
            aggregates = rows_aggregates.each_with_object({}) do |row, aggregates|
                           aggregates[row['aggregate_name']] = create_aggregate(row, functions, types)
                         end

            lookup_columns = map_rows_by(rows_columns, 'table_name')
            tables = rows_tables.each_with_object({}) do |row, tables|
              table_name = row['table_name']
              tables[table_name] = create_table(row, lookup_columns[table_name], types)
            end

            Keyspace.new(keyspace_name, keyspace_data['durable_writes'],
                         replication, tables, types, functions, aggregates)
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
            Table::Compaction.new(klass, options)
          end

          def create_table_options(table_data, compaction_strategy, is_compact)
            compression = table_data['compression']
            compression['class'].slice!(COMPRESSION_PACKAGE_PREFIX) if compression['class']

            Table::Options.new(
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
              is_compact
            )
          end

          def create_column(column_data, types)
            name      = column_data['column_name']
            is_static = (column_data['kind'].to_s.upcase == 'STATIC')
            order     = column_data['clustering_order'] == 'desc' ? :desc : :asc
            type, is_frozen = @type_parser.parse(column_data['type'], types)

            Column.new(name, type, order, nil, is_static, is_frozen)
          end

          def create_table(table_data, rows_columns, types = nil)
            keyspace_name   = table_data['keyspace_name']
            table_name      = table_data['table_name']
            table_flags     = table_data['flags']
            table_columns   = {}
            other_columns   = []
            clustering_size = 0

            is_dense    = table_flags.include?('dense')
            is_super    = table_flags.include?('super')
            is_compound = table_flags.include?('compound')
            is_compact  = is_super || is_dense || !is_compound;

            partition_key      = []
            clustering_columns = []
            clustering_order   = []
            types            ||= @schema.keyspace(keyspace_name).send(:raw_types)

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
                clustering_order[index]   = column.order

                if clustering_size.zero? || index == clustering_size
                  clustering_size = index + 1
                end
              else
                other_columns << column
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

            compaction_strategy = create_compaction_strategy(table_data)
            table_options = create_table_options(table_data, compaction_strategy, is_compact)

            Table.new(keyspace_name, table_name, partition_key, clustering_columns,
                      table_columns, table_options, clustering_order)
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

          def fetch_type(connection, keyspace_name, type_name)
            find_fetcher(connection)
              .fetch_type(connection, keyspace_name, type_name)
          rescue => e
            return Ione::Future.failed(e)
          end

          def fetch_function(connection, keyspace_name, function_name)
            find_fetcher(connection)
              .fetch_function(connection, keyspace_name, function_name)
          rescue => e
            return Ione::Future.failed(e)
          end

          def fetch_aggregate(connection, keyspace_name, aggregate_name)
            find_fetcher(connection)
              .fetch_aggregate(connection, keyspace_name, aggregate_name)
          rescue => e
            return Ione::Future.failed(e)
          end

          private

          def find_fetcher(connection)
            host = @registry.host(connection.host)

            unless host
              ips = @registry.hosts.map(&:ip)
              raise Errors::ClientError,
                    "unable to find release version for current host, " \
                    "connected to #{connection.host}, but cluster contains " \
                    "#{ips}."
            end

            version = host.release_version
            unless version
              raise Errors::ClientError, "unable to determine release " \
                                         "version for host: #{host.inspect}"
            end

            @fetchers[version] ||= begin
              current = @versions.find {|v| v.matches?(version)}
              unless current
                raise Errors::ClientError, "unsupported release version " \
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
