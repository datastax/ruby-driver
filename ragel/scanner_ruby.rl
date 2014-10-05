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
    class Scanner
      %%{
        machine scanner;

        action literal_start             { literal_s = p; }
        action literal_end               { @listener.mark_literal(cql[literal_s...p]); }
        action positional_argument_start { positional_argument_s = p; }
        action positional_argument_end   { @listener.mark_positional_argument(cql[positional_argument_s...p]); }
        action named_argument_start      { named_argument_s = p; }
        action named_argument_end        { @listener.mark_named_argument(cql[named_argument_s...p]); }
        action column_start              { column_s = p; }
        action column_end                { @listener.mark_column(cql[column_s...p]); }
        action keyspace_start            { keyspace_s = p; }
        action keyspace_end              { @listener.mark_keyspace(cql[keyspace_s...p]); }
        action table_start               { table_s = p; }
        action table_end                 { @listener.mark_table(cql[table_s...p]); }
        action order_start               { order_s = p; }
        action order_end                 { @listener.mark_order(cql[order_s...p]); }
        action in_start                  { in_s = p; }
        action in_end                    { @listener.mark_in(cql[in_s...p]); }
        action and_start                 { and_s = p; }
        action and_end                   { @listener.mark_and(cql[and_s...p]); }
        action writetime_start           { writetime_s = p; }
        action writetime_end             { @listener.mark_writetime(cql[writetime_s...p]); }
        action ttl_start                 { ttl_s = p; }
        action ttl_end                   { @listener.mark_ttl(cql[ttl_s...p]); }
        action now_start                 { now_s = p; }
        action now_end                   { @listener.mark_now(cql[now_s...p]); }
        action date_of_start             { date_of_s = p; }
        action date_of_end               { @listener.mark_date_of(cql[date_of_s...p]); }
        action min_timeuuid_start        { min_timeuuid_s = p; }
        action min_timeuuid_end          { @listener.mark_min_timeuuid(cql[min_timeuuid_s...p]); }
        action max_timeuuid_start        { max_timeuuid_s = p; }
        action max_timeuuid_end          { @listener.mark_max_timeuuid(cql[max_timeuuid_s...p]); }
        action unix_timestamp_of_start   { unix_timestamp_of_s = p; }
        action unix_timestamp_of_end     { @listener.mark_unix_timestamp_of(cql[unix_timestamp_of_s...p]); }
        action token_start               { token_s = p; }
        action token_end                 { @listener.mark_token(cql[token_s...p]); }
        action function_start            { function_s = p; }
        action function_end              { @listener.mark_function(cql[function_s...p]); }
        action alias_start               { alias_s = p; }
        action alias_end                 { @listener.mark_alias(cql[alias_s...p]); }
        action all_start                 { all_s = p; }
        action all_end                   { @listener.mark_all(cql[all_s...p]); }
        action count_start               { count_s = p; }
        action count_end                 { @listener.mark_count(cql[count_s...p]); }
        action select_start              { select_s = p; }
        action select_end                { @listener.mark_select(cql[select_s...p]); }
        action from_start                { from_s = p; }
        action from_end                  { @listener.mark_from(cql[from_s...p]); }
        action where_start               { where_s = p; }
        action where_end                 { @listener.mark_where(cql[where_s...p]); }
        action order_by_start            { order_by_s = p; }
        action order_by_end              { @listener.mark_order_by(cql[order_by_s...p]); }
        action limit_start               { limit_s = p; }
        action limit_end                 { @listener.mark_limit(cql[limit_s...p]); }
        action allow_filtering_start     { allow_filtering_s = p; }
        action allow_filtering_end       { @listener.mark_allow_filtering(cql[allow_filtering_s...p]); }
        action exists_start              { exists_s = p; }
        action exists_end                { @listener.mark_exists(cql[exists_s...p]); }
        action delete_start              { delete_s = p; }
        action delete_end                { @listener.mark_delete(cql[delete_s...p]); }
        action timestamp_start           { timestamp_s = p; }
        action timestamp_end             { @listener.mark_timestamp(cql[timestamp_s...p]); }
        action if_start                  { if_s = p; }
        action if_end                    { @listener.mark_if(cql[if_s...p]); }
        action operator_start            { operator_s = p; }
        action operator_end              { @listener.mark_operator(cql[operator_s...p]); }
        action property_start            { property_s = p; }
        action property_end              { @listener.mark_property(cql[property_s...p]); }
        action update_start              { update_s = p; }
        action update_end                { @listener.mark_update(cql[update_s...p]); }
        action star_start                { star_s = p; }
        action star_end                  { @listener.mark_star(cql[star_s...p]); }
        action insert_start              { insert_s = p; }
        action insert_end                { @listener.mark_insert(cql[insert_s...p]); }
        action values_start              { values_s = p; }
        action values_end                { @listener.mark_values(cql[values_s...p]); }
        action not_exists_start          { not_exists_s = p; }
        action not_exists_end            { @listener.mark_not_exists(cql[not_exists_s...p]); }

        include cql "cql.rl";
      }%%

      def initialize(listener)
        @listener = listener

        %% write data;
      end

      def scan(cql)
        data  = cql.bytes.to_a
        eof   = cql.bytesize
        stack = []

        %% write init;
        %% write exec;

        @listener.mark_eof if p == eof

        self
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end
    end
  end
end
