// Copyright 2013-2014 DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

%%{
  machine scanner;

  action literal_start             { literal_start = p; }
  action literal_end               { listener.callMethod(context, "mark_literal", RubyString.newString(context.runtime, data, literal_start, p - literal_start)); }
  action positional_argument_start { positional_argument_start = p; }
  action positional_argument_end   { listener.callMethod(context, "mark_positional_argument", RubyString.newString(context.runtime, data, positional_argument_start, p - positional_argument_start)); }
  action named_argument_start      { named_argument_start = p; }
  action named_argument_end        { listener.callMethod(context, "mark_named_argument", RubyString.newString(context.runtime, data, named_argument_start, p - named_argument_start)); }
  action column_start              { column_start = p; }
  action column_end                { listener.callMethod(context, "mark_column", RubyString.newString(context.runtime, data, column_start, p - column_start)); }
  action keyspace_start            { keyspace_start = p; }
  action keyspace_end              { listener.callMethod(context, "mark_keyspace", RubyString.newString(context.runtime, data, keyspace_start, p - keyspace_start)); }
  action table_start               { table_start = p; }
  action table_end                 { listener.callMethod(context, "mark_table", RubyString.newString(context.runtime, data, table_start, p - table_start)); }
  action order_start               { order_start = p; }
  action order_end                 { listener.callMethod(context, "mark_order", RubyString.newString(context.runtime, data, order_start, p - order_start)); }
  action in_start                  { in_start = p; }
  action in_end                    { listener.callMethod(context, "mark_in", RubyString.newString(context.runtime, data, in_start, p - in_start)); }
  action and_start                 { and_start = p; }
  action and_end                   { listener.callMethod(context, "mark_and", RubyString.newString(context.runtime, data, and_start, p - and_start)); }
  action writetime_start           { writetime_start = p; }
  action writetime_end             { listener.callMethod(context, "mark_writetime", RubyString.newString(context.runtime, data, writetime_start, p - writetime_start)); }
  action ttl_start                 { ttl_start = p; }
  action ttl_end                   { listener.callMethod(context, "mark_ttl", RubyString.newString(context.runtime, data, ttl_start, p - ttl_start)); }
  action now_start                 { now_start = p; }
  action now_end                   { listener.callMethod(context, "mark_now", RubyString.newString(context.runtime, data, now_start, p - now_start)); }
  action date_of_start             { date_of_start = p; }
  action date_of_end               { listener.callMethod(context, "mark_date_of", RubyString.newString(context.runtime, data, date_of_start, p - date_of_start)); }
  action min_timeuuid_start        { min_timeuuid_start = p; }
  action min_timeuuid_end          { listener.callMethod(context, "mark_min_timeuuid", RubyString.newString(context.runtime, data, min_timeuuid_start, p - min_timeuuid_start)); }
  action max_timeuuid_start        { max_timeuuid_start = p; }
  action max_timeuuid_end          { listener.callMethod(context, "mark_max_timeuuid", RubyString.newString(context.runtime, data, max_timeuuid_start, p - max_timeuuid_start)); }
  action unix_timestamp_of_start   { unix_timestamp_of_start = p; }
  action unix_timestamp_of_end     { listener.callMethod(context, "mark_unix_timestamp_of", RubyString.newString(context.runtime, data, unix_timestamp_of_start, p - unix_timestamp_of_start)); }
  action token_start               { token_start = p; }
  action token_end                 { listener.callMethod(context, "mark_token", RubyString.newString(context.runtime, data, token_start, p - token_start)); }
  action function_start            { function_start = p; }
  action function_end              { listener.callMethod(context, "mark_function", RubyString.newString(context.runtime, data, function_start, p - function_start)); }
  action alias_start               { alias_start = p; }
  action alias_end                 { listener.callMethod(context, "mark_alias", RubyString.newString(context.runtime, data, alias_start, p - alias_start)); }
  action all_start                 { all_start = p; }
  action all_end                   { listener.callMethod(context, "mark_all", RubyString.newString(context.runtime, data, all_start, p - all_start)); }
  action count_start               { count_start = p; }
  action count_end                 { listener.callMethod(context, "mark_count", RubyString.newString(context.runtime, data, count_start, p - count_start)); }
  action select_start              { select_start = p; }
  action select_end                { listener.callMethod(context, "mark_select", RubyString.newString(context.runtime, data, select_start, p - select_start)); }
  action from_start                { from_start = p; }
  action from_end                  { listener.callMethod(context, "mark_from", RubyString.newString(context.runtime, data, from_start, p - from_start)); }
  action where_start               { where_start = p; }
  action where_end                 { listener.callMethod(context, "mark_where", RubyString.newString(context.runtime, data, where_start, p - where_start)); }
  action order_by_start            { order_by_start = p; }
  action order_by_end              { listener.callMethod(context, "mark_order_by", RubyString.newString(context.runtime, data, order_by_start, p - order_by_start)); }
  action limit_start               { limit_start = p; }
  action limit_end                 { listener.callMethod(context, "mark_limit", RubyString.newString(context.runtime, data, limit_start, p - limit_start)); }
  action allow_filtering_start     { allow_filtering_start = p; }
  action allow_filtering_end       { listener.callMethod(context, "mark_allow_filtering", RubyString.newString(context.runtime, data, allow_filtering_start, p - allow_filtering_start)); }
  action exists_start              { exists_start = p; }
  action exists_end                { listener.callMethod(context, "mark_exists", RubyString.newString(context.runtime, data, exists_start, p - exists_start)); }
  action delete_start              { delete_start = p; }
  action delete_end                { listener.callMethod(context, "mark_delete", RubyString.newString(context.runtime, data, delete_start, p - delete_start)); }
  action timestamp_start           { timestamp_start = p; }
  action timestamp_end             { listener.callMethod(context, "mark_timestamp", RubyString.newString(context.runtime, data, timestamp_start, p - timestamp_start)); }
  action if_start                  { if_start = p; }
  action if_end                    { listener.callMethod(context, "mark_if", RubyString.newString(context.runtime, data, if_start, p - if_start)); }
  action operator_start            { operator_start = p; }
  action operator_end              { listener.callMethod(context, "mark_operator", RubyString.newString(context.runtime, data, operator_start, p - operator_start)); }
  action property_start            { property_start = p; }
  action property_end              { listener.callMethod(context, "mark_property", RubyString.newString(context.runtime, data, property_start, p - property_start)); }
  action update_start              { update_start = p; }
  action update_end                { listener.callMethod(context, "mark_update", RubyString.newString(context.runtime, data, update_start, p - update_start)); }
  action star_start                { star_start = p; }
  action star_end                  { listener.callMethod(context, "mark_star", RubyString.newString(context.runtime, data, star_start, p - star_start)); }
  action insert_start              { insert_start = p; }
  action insert_end                { listener.callMethod(context, "mark_insert", RubyString.newString(context.runtime, data, insert_start, p - insert_start)); }
  action values_start              { values_start = p; }
  action values_end                { listener.callMethod(context, "mark_values", RubyString.newString(context.runtime, data, values_start, p - values_start)); }
  action not_exists_start          { not_exists_start = p; }
  action not_exists_end            { listener.callMethod(context, "mark_not_exists", RubyString.newString(context.runtime, data, not_exists_start, p - not_exists_start)); }

  prepush {
    if (top >= ssize) {
      ssize = ssize + 10;
      stack = Arrays.copyOf(stack, ssize);
    }
  }

  postpop {
    if (ssize - top >= 10) {
      ssize = ssize - 10;
      stack = Arrays.copyOf(stack, ssize);
    }
  }

  include cql "cql.rl";
}%%

import java.io.IOException;
import java.util.Arrays;

import org.jruby.Ruby;
import org.jruby.RubyString;
import org.jruby.RubyFixnum;
import org.jruby.RubyModule;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

public class CqlScannerService implements BasicLibraryService
{
    public boolean basicLoad(final Ruby runtime) throws IOException
    {
        RubyModule cassandra = runtime.defineModule("Cassandra");
        RubyModule cql       = runtime.defineModuleUnder("CQL", cassandra);
        RubyClass  scanner   = cql.defineClassUnder("Scanner", runtime.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby runtime, RubyClass rubyClass) {
                return new Scanner(runtime, rubyClass);
            }
        });

        scanner.defineAnnotatedMethods(Scanner.class);

        return true;
    }

    @JRubyClass(name="Scanner")
    public static class Scanner extends RubyObject
    {
        %% write data;

        private IRubyObject listener;

        public Scanner(Ruby runtime, RubyClass metaClass)
        {
            super(runtime, metaClass);
        }

        @JRubyMethod(name="initialize", required = 1)
        protected IRubyObject initialize(ThreadContext context, IRubyObject listener)
        {
            this.listener = listener;

            return this;
        }

        @JRubyMethod(name="scan", required = 1, argTypes = {RubyString.class})
        public IRubyObject scan(ThreadContext context, IRubyObject object)
        {
            byte[] data = object.convertToString().getBytes();
            int cs, p = 0, pe = data.length, top = 0;
            int eof = pe;
            int   ssize = 5;
            int[] stack = new int[ssize];

            int literal_start = 0;
            int positional_argument_start = 0;
            int named_argument_start = 0;
            int column_start = 0;
            int keyspace_start = 0;
            int table_start = 0;
            int order_start = 0;
            int in_start = 0;
            int and_start = 0;
            int writetime_start = 0;
            int ttl_start = 0;
            int now_start = 0;
            int date_of_start = 0;
            int min_timeuuid_start = 0;
            int max_timeuuid_start = 0;
            int unix_timestamp_of_start = 0;
            int token_start = 0;
            int function_start = 0;
            int alias_start = 0;
            int all_start = 0;
            int count_start = 0;
            int select_start = 0;
            int from_start = 0;
            int where_start = 0;
            int order_by_start = 0;
            int limit_start = 0;
            int allow_filtering_start = 0;
            int exists_start = 0;
            int delete_start = 0;
            int timestamp_start = 0;
            int if_start = 0;
            int operator_start = 0;
            int property_start = 0;
            int update_start = 0;
            int star_start = 0;
            int insert_start = 0;
            int values_start = 0;
            int not_exists_start = 0;

            %% write init;
            %% write exec;

            if (p == eof) {
                listener.callMethod(context, "mark_eof");
            }

            return this;
        }
    }
}
