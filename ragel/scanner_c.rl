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

  action literal_start             { MARK(literal); }
  action literal_end               { EMIT(literal); }
  action positional_argument_start { MARK(positional_argument); }
  action positional_argument_end   { EMIT(positional_argument); }
  action named_argument_start      { MARK(named_argument); }
  action named_argument_end        { EMIT(named_argument); }
  action column_start              { MARK(column); }
  action column_end                { EMIT(column); }
  action keyspace_start            { MARK(keyspace); }
  action keyspace_end              { EMIT(keyspace); }
  action table_start               { MARK(table); }
  action table_end                 { EMIT(table); }
  action order_start               { MARK(order); }
  action order_end                 { EMIT(order); }
  action in_start                  { MARK(in); }
  action in_end                    { EMIT(in); }
  action and_start                 { MARK(and); }
  action and_end                   { EMIT(and); }
  action writetime_start           { MARK(writetime); }
  action writetime_end             { EMIT(writetime); }
  action ttl_start                 { MARK(ttl); }
  action ttl_end                   { EMIT(ttl); }
  action now_start                 { MARK(now); }
  action now_end                   { EMIT(now); }
  action date_of_start             { MARK(date_of); }
  action date_of_end               { EMIT(date_of); }
  action min_timeuuid_start        { MARK(min_timeuuid); }
  action min_timeuuid_end          { EMIT(min_timeuuid); }
  action max_timeuuid_start        { MARK(max_timeuuid); }
  action max_timeuuid_end          { EMIT(max_timeuuid); }
  action unix_timestamp_of_start   { MARK(unix_timestamp_of); }
  action unix_timestamp_of_end     { EMIT(unix_timestamp_of); }
  action token_start               { MARK(token); }
  action token_end                 { EMIT(token); }
  action function_start            { MARK(function); }
  action function_end              { EMIT(function); }
  action alias_start               { MARK(alias); }
  action alias_end                 { EMIT(alias); }
  action all_start                 { MARK(all); }
  action all_end                   { EMIT(all); }
  action count_start               { MARK(count); }
  action count_end                 { EMIT(count); }
  action select_start              { MARK(select); }
  action select_end                { EMIT(select); }
  action from_start                { MARK(from); }
  action from_end                  { EMIT(from); }
  action where_start               { MARK(where); }
  action where_end                 { EMIT(where); }
  action order_by_start            { MARK(order_by); }
  action order_by_end              { EMIT(order_by); }
  action limit_start               { MARK(limit); }
  action limit_end                 { EMIT(limit); }
  action allow_filtering_start     { MARK(allow_filtering); }
  action allow_filtering_end       { EMIT(allow_filtering); }
  action exists_start              { MARK(exists); }
  action exists_end                { EMIT(exists); }
  action delete_start              { MARK(delete); }
  action delete_end                { EMIT(delete); }
  action timestamp_start           { MARK(timestamp); }
  action timestamp_end             { EMIT(timestamp); }
  action if_start                  { MARK(if); }
  action if_end                    { EMIT(if); }
  action operator_start            { MARK(operator); }
  action operator_end              { EMIT(operator); }
  action property_start            { MARK(property); }
  action property_end              { EMIT(property); }
  action update_start              { MARK(update); }
  action update_end                { EMIT(update); }
  action star_start                { MARK(star); }
  action star_end                  { EMIT(star); }
  action insert_start              { MARK(insert); }
  action insert_end                { EMIT(insert); }
  action values_start              { MARK(values); }
  action values_end                { EMIT(values); }
  action not_exists_start          { MARK(not_exists); }
  action not_exists_end            { EMIT(not_exists); }

  prepush {
    if (top >= ssize) {
      ssize = ssize + RAGEL_STACK_INCR;
      stack = realloc(stack, sizeof(int) * ssize);
    }
  }

  postpop {
    if (ssize - top >= RAGEL_STACK_INCR) {
      ssize = ssize - RAGEL_STACK_INCR;
      stack = realloc(stack, sizeof(int) * ssize);
    }
  }

  include cql "cql.rl";
}%%

#include <assert.h>
#include <ruby.h>

#if defined(_WIN32)
#include <stddef.h>
#endif

#ifdef HAVE_RUBY_RE_H
#include <ruby/re.h>
#else
#include <re.h>
#endif

#define DATA_GET(FROM, TYPE, NAME) \
  Data_Get_Struct(FROM, TYPE, NAME); \
  if (NAME == NULL) { \
    rb_raise(rb_eArgError, "NULL found for " # NAME " when it shouldn't be."); \
  }

/* The initial size of the Ragel stack. */
#define RAGEL_STACK_SIZE 5

/* The amount by which the Ragel stack will be increased or decreased. */
#define RAGEL_STACK_INCR 10

typedef struct scanner_state_s {
  const char* start_literal;
  const char* start_positional_argument;
  const char* start_named_argument;
  const char* start_column;
  const char* start_keyspace;
  const char* start_table;
  const char* start_order;
  const char* start_in;
  const char* start_and;
  const char* start_writetime;
  const char* start_ttl;
  const char* start_now;
  const char* start_date_of;
  const char* start_min_timeuuid;
  const char* start_max_timeuuid;
  const char* start_unix_timestamp_of;
  const char* start_token;
  const char* start_function;
  const char* start_alias;
  const char* start_all;
  const char* start_count;
  const char* start_select;
  const char* start_from;
  const char* start_where;
  const char* start_order_by;
  const char* start_limit;
  const char* start_allow_filtering;
  const char* start_exists;
  const char* start_delete;
  const char* start_timestamp;
  const char* start_if;
  const char* start_operator;
  const char* start_property;
  const char* start_update;
  const char* start_star;
  const char* start_insert;
  const char* start_values;
  const char* start_not_exists;
} scanner_state;

#define MARK(M) (scanner->start_##M = p)
#define EMIT(M) (rb_funcall(listener, rb_intern("mark_" #M), 1, rb_str_new(scanner->start_##M, (p - scanner->start_##M))))

%% write data;

static void
scanner_init(scanner_state *scanner) {
  assert(scanner);

  scanner->start_literal = NULL;
  scanner->start_positional_argument = NULL;
  scanner->start_named_argument = NULL;
  scanner->start_column = NULL;
  scanner->start_keyspace = NULL;
  scanner->start_table = NULL;
  scanner->start_order = NULL;
  scanner->start_in = NULL;
  scanner->start_and = NULL;
  scanner->start_writetime = NULL;
  scanner->start_ttl = NULL;
  scanner->start_now = NULL;
  scanner->start_date_of = NULL;
  scanner->start_min_timeuuid = NULL;
  scanner->start_max_timeuuid = NULL;
  scanner->start_unix_timestamp_of = NULL;
  scanner->start_token = NULL;
  scanner->start_function = NULL;
  scanner->start_alias = NULL;
  scanner->start_all = NULL;
  scanner->start_count = NULL;
  scanner->start_select = NULL;
  scanner->start_from = NULL;
  scanner->start_where = NULL;
  scanner->start_order_by = NULL;
  scanner->start_limit = NULL;
  scanner->start_allow_filtering = NULL;
  scanner->start_exists = NULL;
  scanner->start_delete = NULL;
  scanner->start_timestamp = NULL;
  scanner->start_if = NULL;
  scanner->start_operator = NULL;
  scanner->start_property = NULL;
  scanner->start_update = NULL;
  scanner->start_star = NULL;
  scanner->start_insert = NULL;
  scanner->start_values = NULL;
  scanner->start_not_exists = NULL;
}

static VALUE
cScanner_scan(VALUE self, VALUE input)
{
  char *data;
  size_t len;
  const char *p, *pe, *eof;
  int cs, top;
  unsigned int ssize;
  int *stack;
  VALUE listener;
  scanner_state *scanner;

  scanner = NULL;
  DATA_GET(self, scanner_state, scanner);

  /* Reset scanner by re-initializing the whole thing */
  scanner_init(scanner);

  ssize    = RAGEL_STACK_SIZE;
  stack    = malloc(sizeof(int) * ssize);
  data     = RSTRING_PTR(input);
  len      = RSTRING_LEN(input);
  listener = rb_iv_get(self, "@listener");
  cs       = 0;
  top      = 0;
  p        = data;
  pe       = data + len;
  eof      = pe;

  assert(*pe == '\0' && "pointer does not end on NULL");

  %% write init;
  %% write exec;

  assert(p <= pe && "data overflow after parsing execute");

  if (p == eof) {
    rb_funcall(listener, rb_intern("mark_eof"), 0);
  }

  return self;
}

static VALUE
cScanner_alloc(VALUE klass)
{
  VALUE obj;
  scanner_state *state = ALLOC(scanner_state);

  obj = Data_Wrap_Struct(klass, NULL, -1, state);

  return obj;
}

static VALUE
cScanner_init(VALUE self, VALUE listener)
{
  rb_iv_set(self, "@listener", listener);

  return self;
}

void
Init_cql_scanner()
{
  VALUE mCassandra, mCQL, cScanner;

  mCassandra = rb_define_module_under(rb_cObject, "Cassandra");
  mCQL       = rb_define_module_under(mCassandra, "CQL");
  cScanner   = rb_define_class_under(mCQL, "Scanner", rb_cObject);

  rb_define_alloc_func(cScanner, cScanner_alloc);
  rb_define_method(cScanner, "initialize", cScanner_init, 1);
  rb_define_method(cScanner, "scan", cScanner_scan, 1);
}
