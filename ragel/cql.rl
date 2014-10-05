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
  machine cql;

  # Tokens
  T_QUESTION_MARK = '?';
  T_PAREN_OPEN    = '(';
  T_PAREN_CLOSE   = ')';
  T_STAR          = '*';
  T_COMA          = ',';
  T_COLON         = ':';
  T_DOT           = '.';
  T_BRACKET_OPEN  = '[';
  T_BRACKET_CLOSE = ']';
  T_BRACE_OPEN    = '{';
  T_BRACE_CLOSE   = '}';
  T_QUOTE         = "'";
  T_DOUBLE_QUOTE  = '"';
  T_DASH          = '-';
  T_UNDERSCORE    = '_';
  T_TRUE          = 'true'i;
  T_FALSE         = 'false'i;
  T_NULL          = 'null'i;
  T_HEX           = '0x';
  T_EQUAL         = '=';
  T_LESS          = '<';
  T_GREATER       = '>';
  T_LESS_EQUAL    = '<=';
  T_GREATER_EQUAL = '>=';
  T_ONE           = '1';
  T_PLUS          = '+';
  T_MINUS         = '-';

  # Keywords
  K_SELECT          = 'select'i;
  K_FROM            = 'from'i;
  K_WHERE           = 'where'i;
  K_IN              = 'in'i;
  K_AND             = 'and'i;
  K_ORDER_BY        = 'order'i space+ 'by'i;
  K_ASC             = 'asc'i;
  K_DESC            = 'desc'i;
  K_LIMIT           = 'limit'i;
  K_ALLOW           = 'allow'i;
  K_ALLOW_FILTERING = K_ALLOW space+ 'filtering'i;
  K_DISTINCT        = 'distinct'i;
  K_COUNT           = 'count'i;
  K_AS              = 'as'i;
  K_WRITETIME       = 'writetime'i;
  K_TTL             = 'ttl'i;
  K_NOW             = 'now'i;
  K_DELETE          = 'delete'i;
  K_USING           = 'using'i;
  K_TIMESTAMP       = 'timestamp'i;
  K_USING_TIMESTAMP = K_USING space+ K_TIMESTAMP;
  K_SET             = 'set'i;
  K_IF              = 'if'i;
  K_EXISTS          = 'exists'i;
  K_UPDATE          = 'update'i;
  K_INSERT          = 'insert'i;
  K_INTO            = 'into'i;
  K_VALUES          = 'values'i;
  K_NOT             = 'not'i;
  K_NOT_EXISTS      = K_NOT space* K_EXISTS;
  K_DATE_OF         = 'dateOf';
  K_MIN_TIMEUUID    = 'minTimeuuid';
  K_MAX_TIMEUUID    = 'maxTimeuuid';
  K_UNIX_TIMESTAMP  = 'unixTimestampOf';
  K_TOKEN           = 'token';

  # Reserved keywords
  keyword           = K_SELECT
                    | K_FROM
                    | K_WHERE
                    | K_IN
                    | K_AND
                    | K_ORDER_BY
                    | K_ASC
                    | K_DESC
                    | K_LIMIT
                    | K_ALLOW
                    | K_DELETE
                    | K_USING
                    | K_IF
                    ;

  # Identifier
  identifier        = (alpha (alpha | digit | T_UNDERSCORE)**)
                    | T_DOUBLE_QUOTE (any -- T_DOUBLE_QUOTE | T_DOUBLE_QUOTE{2})** T_DOUBLE_QUOTE
                    ;

  # Appears in various lists
  enumerator = space* T_COMA space*;

  # Values
  string            = T_QUOTE (any -- T_QUOTE | '\\\'')** T_QUOTE;
  integer           = T_DASH? digit+;
  float             = T_DASH? digit+ T_DOT digit+;
  uuid              = xdigit{8} T_DASH xdigit{4} T_DASH xdigit{4} T_DASH xdigit{4} T_DASH xdigit{12};
  boolean           = T_TRUE | T_FALSE;
  blob              = T_HEX xdigit+;
  nil               = T_NULL;
  list              = T_BRACKET_OPEN >{ fcall skip_list; };
  map_set           = T_BRACE_OPEN >{ fcall skip_map_set; };

  # Function args
  function_args     = T_PAREN_OPEN >{ fcall skip_args; };

  # Function calls
  count             = K_COUNT           space* function_args;
  writetime         = K_WRITETIME       space* function_args;
  ttl               = K_TTL             space* function_args;
  now               = K_NOW             space* function_args;
  date_of           = K_DATE_OF         space* function_args;
  min_timeuuid      = K_MIN_TIMEUUID    space* function_args;
  max_timeuuid      = K_MAX_TIMEUUID    space* function_args;
  unix_timestamp_of = K_UNIX_TIMESTAMP  space* function_args;
  token             = K_TOKEN           space* function_args;
  custom_function   = (identifier T_COLON{2})? (identifier - (K_COUNT | K_WRITETIME | K_TTL | K_NOW | K_DATE_OF | K_MIN_TIMEUUID | K_MAX_TIMEUUID | K_UNIX_TIMESTAMP | K_TOKEN)) space* function_args;

  # Simple values
  simple   = string
           | integer
           | float
           | uuid
           | boolean
           | blob
           | nil
           ;

  # Complex values
  complex  = list
           | map_set
           ;

  # Function calls
  function = writetime
           | ttl
           | now
           | date_of
           | min_timeuuid
           | max_timeuuid
           | unix_timestamp_of
           | token
           | custom_function
           ;

  # Literal values and function calls
  literal  = simple
           | complex
           | function
           ;

  # Literals or argument markers
  value      = literal            >literal_start %literal_end
             | T_QUESTION_MARK    >positional_argument_start %positional_argument_end
             | T_COLON identifier >named_argument_start %named_argument_end
             ;
  value_list = T_PAREN_OPEN value (enumerator value)* T_PAREN_CLOSE;

  column   = (identifier - keyword) >column_start %column_end;

  operator = (T_EQUAL | T_LESS | T_GREATER | T_LESS_EQUAL | T_GREATER_EQUAL) >operator_start %operator_end;

  skip_args    := (function_args | ^T_PAREN_CLOSE)* T_PAREN_CLOSE >{ fret; };
  skip_list    := (list | map_set | ^T_BRACKET_CLOSE)* T_BRACKET_CLOSE >{ fret; };
  skip_map_set := (list | map_set | ^T_BRACE_CLOSE)* T_BRACE_CLOSE >{ fret; };

  keyspace_name = identifier >keyspace_start %keyspace_end T_DOT;
  table_name    = keyspace_name? identifier >table_start %table_end;

  # SELECT
  order            = (K_ASC | K_DESC);
  ordering         = (identifier - keyword) (space+ order)?;
  order_clause     = ordering (enumerator ordering)*;
  column_list      = T_PAREN_OPEN column (enumerator column)* T_PAREN_CLOSE;
  select_condition = column space* operator space* value
                   | column space+ K_IN >in_start %in_end space+ T_PAREN_OPEN (value (enumerator value)*)? T_PAREN_CLOSE
                   | column_list space* operator space* value_list
                   | column_list space+ K_IN >in_start %in_end space+ value_list
                   | token space* operator space* value
                   ;
  select_where     = select_condition (space+ K_AND >and_start %and_end space+ select_condition)*;
  selector         = column
                   | writetime         >writetime_start %writetime_end
                   | ttl               >ttl_start %ttl_end
                   | now               >now_start %now_end
                   | date_of           >date_of_start %date_of_end
                   | min_timeuuid      >min_timeuuid_start %min_timeuuid_end
                   | max_timeuuid      >max_timeuuid_start %max_timeuuid_end
                   | unix_timestamp_of >unix_timestamp_of_start %unix_timestamp_of_end
                   | token             >token_start %token_end
                   | custom_function   >function_start %function_end
                   ;
  alias            = space+ K_AS space+ identifier;
  selector_list    = (selector alias? (enumerator selector alias?)*)
                   | T_STAR >star_start %star_end
                   ;
  select_clause    = count >count_start %count_end alias?
                   | K_DISTINCT? selector_list
                   ;
  limit            = integer            >literal_start %literal_end
                   | T_QUESTION_MARK    >positional_argument_start %positional_argument_end
                   | T_COLON identifier >named_argument_start %named_argument_end
                   ;
  select_stmt      = K_SELECT >select_start %select_end space+ select_clause space+ K_FROM >from_start %from_end space+ table_name (space+ K_WHERE >where_start %where_end space+ select_where)? (space+ K_ORDER_BY space+ order_clause)? (space+ K_LIMIT >limit_start %limit_end space+ limit)? (space+ K_ALLOW_FILTERING)?;

  # DELETE
  deletor          = (identifier - keyword) (T_BRACKET_OPEN simple T_BRACKET_CLOSE)?;
  delete_if        = deletor space* T_EQUAL >operator_start %operator_end space* value;
  delete_if_clause = K_EXISTS >exists_start %exists_end
                   | delete_if (space+ K_AND >and_start %and_end space+ delete_if)*
                   ;
  delete_condition = column space* T_EQUAL >operator_start %operator_end space* value
                   | column space+ K_IN >in_start %in_end space+ value_list
                   ;
  delete_where     = delete_condition (space+ K_AND >and_start %and_end space+ delete_condition)*;
  deletor_list     = deletor (enumerator deletor)*;
  delete_stmt      = K_DELETE >delete_start %delete_end (space+ deletor_list)? space+ K_FROM >from_start %from_end space+ table_name (space+ K_USING_TIMESTAMP space+ integer >timestamp_start %timestamp_end)? space+ K_WHERE >where_start %where_end space+ delete_where (space+ K_IF >if_start %if_end space+ delete_if_clause)?;

  # UPDATE
  assigner         = column (T_BRACKET_OPEN simple >property_start %property_end T_BRACKET_CLOSE)?;
  update_assign    = assigner space* T_EQUAL >operator_start %operator_end space* value
                   | (identifier - keyword) space* T_EQUAL space* (identifier - keyword) space* (T_PLUS | T_MINUS) space* (integer | list | map_set)
                   | (identifier - keyword) space* T_EQUAL space* (identifier - keyword) space* T_PLUS space* map_set
                   ;
  option           = K_TIMESTAMP space+ integer
                   | K_TTL space+ integer
                   ;
  using_clause     = K_USING space+ option (space+ K_AND space+ option)*;
  update_stmt      = K_UPDATE >update_start %update_end space+ table_name (space+ using_clause)? space+ K_SET space+ update_assign (enumerator update_assign)* space+ K_WHERE >where_start %where_end space+ delete_where (space+ K_IF >if_start %if_end space+ delete_if_clause)?;

  # INSERT
  insert_stmt      = K_INSERT >insert_start %insert_end space+ K_INTO space+ table_name space+ column_list space+ K_VALUES >values_start %values_end space+ value_list (space+ K_IF >if_start %if_end space+ K_NOT_EXISTS >not_exists_start %not_exists_end)? (space+ using_clause)?;

  main := select_stmt | delete_stmt | update_stmt | insert_stmt;
}%%
