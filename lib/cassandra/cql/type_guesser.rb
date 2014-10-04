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
  module CQL
    class TypeGuesser
      def initialize(cluster_schema)
        @schema  = cluster_schema
        @scanner = Scanner.new(self)
        reset
      end

      def guess(cql, current_keyspace)
        @keyspace = @schema.keyspace(current_keyspace)

        @scanner.scan(cql)

        (@state == :done) ? @typehints : nil
      ensure
        reset
      end

      def mark_keyspace(value)
        @keyspace = @schema.keyspace(value)
      end

      def mark_table(value)
        if @keyspace
          @table = @keyspace.table(value)
        end
        @state = :columns
      end

      def mark_limit(value)
        @state = :limit if @table
      end

      def mark_column(value)
        if @state == :columns
          @columns << value
        end
      end

      def mark_literal(value)
        @columns.shift if @state == :columns
      end

      def mark_positional_argument(value)
        case @state
        when :columns
          name   = @columns.shift
          column = nil
          column = @table.column(name) if @table

          if column
            @typehints << column.type
          else
            @typehints << nil
          end
        when :limit
          @typehints << :int
        end
      end

      def mark_named_argument(value)
        case @state
        when :columns
          column = @table.column(@columns.shift)

          if column
            @typehints << column.type
          else
            @typehints << nil
          end
        when :limit
          @typehints << :int
        end
      end

      def mark_select(value)
      end

      def mark_delete(value)
      end

      def mark_from(value)
      end

      def mark_update(value)
      end

      def mark_insert(value)
      end

      def mark_order(value)
      end

      def mark_in(value)
      end

      def mark_and(value)
      end

      def mark_writetime(value)
      end

      def mark_ttl(value)
      end

      def mark_now(value)
      end

      def mark_date_of(value)
      end

      def mark_min_timeuuid(value)
      end

      def mark_max_timeuuid(value)
      end

      def mark_unix_timestamp_of(value)
      end

      def mark_token(value)
      end

      def mark_function(value)
      end

      def mark_alias(value)
      end

      def mark_all(value)
      end

      def mark_count(value)
      end

      def mark_where(value)
      end

      def mark_order_by(value)
      end

      def mark_allow_filtering(value)
      end

      def mark_exists(value)
      end

      def mark_timestamp(value)
      end

      def mark_if(value)
      end

      def mark_operator(value)
      end

      def mark_property(value)
      end

      def mark_star(value)
      end

      def mark_values(value)
      end

      def mark_eof
        @state = :done
      end

      private

      def reset
        @state     = :init
        @table     = nil
        @keyspace  = nil
        @columns   = []
        @typehints = []
      end
    end
  end
end
