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

require 'spec_helper'

class Listener
  attr_reader :tokens

  def initialize
    @tokens = []
  end

  def mark_literal(value)
    @tokens << [:literal, value]
  end

  def mark_positional_argument(value)
    @tokens << [:positional_argument, value]
  end

  def mark_named_argument(value)
    @tokens << [:named_argument, value]
  end

  def mark_column(value)
    @tokens << [:column, value]
  end

  def mark_keyspace(value)
    @tokens << [:keyspace, value]
  end

  def mark_table(value)
    @tokens << [:table, value]
  end

  def mark_order(value)
    @tokens << [:order, value]
  end

  def mark_in(value)
    @tokens << [:in, value]
  end

  def mark_and(value)
    @tokens << [:and, value]
  end

  def mark_writetime(value)
    @tokens << [:writetime, value]
  end

  def mark_ttl(value)
    @tokens << [:ttl, value]
  end

  def mark_now(value)
    @tokens << [:now, value]
  end

  def mark_date_of(value)
    @tokens << [:date_of, value]
  end

  def mark_min_timeuuid(value)
    @tokens << [:min_timeuuid, value]
  end

  def mark_max_timeuuid(value)
    @tokens << [:max_timeuuid, value]
  end

  def mark_unix_timestamp_of(value)
    @tokens << [:unix_timestamp_of, value]
  end

  def mark_token(value)
    @tokens << [:token, value]
  end

  def mark_function(value)
    @tokens << [:function, value]
  end

  def mark_alias(value)
    @tokens << [:alias, value]
  end

  def mark_all(value)
    @tokens << [:all, value]
  end

  def mark_count(value)
    @tokens << [:count, value]
  end

  def mark_select(value)
    @tokens << [:select, value]
  end

  def mark_from(value)
    @tokens << [:from, value]
  end

  def mark_where(value)
    @tokens << [:where, value]
  end

  def mark_order_by(value)
    @tokens << [:order_by, value]
  end

  def mark_limit(value)
    @tokens << [:limit, value]
  end

  def mark_allow_filtering(value)
    @tokens << [:allow_filtering, value]
  end

  def mark_exists(value)
    @tokens << [:exists, value]
  end

  def mark_delete(value)
    @tokens << [:delete, value]
  end

  def mark_timestamp(value)
    @tokens << [:timestamp, value]
  end

  def mark_if(value)
    @tokens << [:if, value]
  end

  def mark_operator(value)
    @tokens << [:operator, value]
  end

  def mark_property(value)
    @tokens << [:property, value]
  end

  def mark_update(value)
    @tokens << [:update, value]
  end

  def mark_star(value)
    @tokens << [:star, value]
  end

  def mark_insert(value)
    @tokens << [:insert, value]
  end

  def mark_values(value)
    @tokens << [:values, value]
  end

  def mark_eof
    @tokens << [:eof]
  end
end

module Cassandra
  module CQL
    describe(Scanner) do
      let(:listener) { Listener.new }
      subject { Scanner.new(listener) }

      describe('#scan') do
        [
          [
            "SELECT * FROM users", [
              [:select, 'SELECT'],
              [:star, '*'],
              [:from, 'FROM'],
              [:table, 'users'],
              [:eof]
            ]
          ],
          [
            "UPDATE users SET username='bulat', email='bulat.shakirzyanov@datastax.com', age = 28 WHERE id = ?", [
              [:update, 'UPDATE'],
              [:table, 'users'],
              [:column, 'username'],
              [:operator, '='],
              [:literal, "'bulat'"],
              [:column, 'email'],
              [:operator, '='],
              [:literal, "'bulat.shakirzyanov@datastax.com'"],
              [:column, 'age'],
              [:operator, '='],
              [:literal, '28'],
              [:where, 'WHERE'],
              [:column, 'id'],
              [:operator, '='],
              [:positional_argument, '?'],
              [:eof]
            ]
          ],
          [
            "SELECT username, email FROM users WHERE id = ?", [
              [:select, 'SELECT'],
              [:column, 'username'],
              [:column, 'email'],
              [:from, 'FROM'],
              [:table, 'users'],
              [:where, 'WHERE'],
              [:column, 'id'],
              [:operator, '='],
              [:positional_argument, '?'],
              [:eof]
            ]
          ],
          [
            "DELETE username, email FROM simplex.users WHERE id = ?", [
              [:delete, 'DELETE'],
              [:from, 'FROM'],
              [:keyspace, 'simplex'],
              [:table, 'users'],
              [:where, 'WHERE'],
              [:column, 'id'],
              [:operator, '='],
              [:positional_argument, '?'],
              [:eof]
            ]
          ],
          [
            "INSERT INTO NerdMovies (movie, director, main_actor, year) VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion', 2005) USING TTL 86400", [
              [:insert, 'INSERT'],
              [:table, 'NerdMovies'],
              [:column, 'movie'],
              [:column, 'director'],
              [:column, 'main_actor'],
              [:column, 'year'],
              [:values, 'VALUES'],
              [:literal, "'Serenity'"],
              [:literal, "'Joss Whedon'"],
              [:literal, "'Nathan Fillion'"],
              [:literal, "2005"],
              [:eof]
            ]
          ]
        ].each do |(statement, tokens)|
          it "correctly scans #{statement.inspect}" do
            subject.scan(statement)
            expect(listener.tokens).to eq(tokens)
          end
        end
      end
    end
  end
end
