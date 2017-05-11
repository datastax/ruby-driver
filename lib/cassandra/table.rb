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
  # Represents a cassandra table
  # @see Cassandra::Keyspace#each_table
  # @see Cassandra::Keyspace#table
  class Table < ColumnContainer
    # @return [Array<Symbol>] an array of order values (`:asc` or `:desc`) that apply to the
    #   `clustering_columns` array.
    attr_reader :clustering_order

    # @private
    def initialize(keyspace,
                   name,
                   partition_key,
                   clustering_columns,
                   other_columns,
                   options,
                   clustering_order,
                   id)
      super(keyspace, name, partition_key, clustering_columns, other_columns, options, id)
      @clustering_order = clustering_order.freeze
      @indexes = []
      @indexes_hash = {}
      @materialized_views = []
      @materialized_views_hash = {}
      @triggers = []
      @triggers_hash = {}
    end

    # @param name [String] index name
    # @return [Boolean] whether this table has a given index
    def has_index?(name)
      @indexes_hash.key?(name)
    end

    # @param name [String] index name
    # @return [Cassandra::Index, nil] an index or nil
    def index(name)
      @indexes_hash[name]
    end

    # Yield or enumerate each index bound to this table
    # @overload each_index
    #   @yieldparam index [Cassandra::Index] current index
    #   @return [Cassandra::Table] self
    # @overload each_index
    #   @return [Array<Cassandra::Index>] a list of indexes
    def each_index(&block)
      if block_given?
        @indexes.each(&block)
        self
      else
        @indexes.freeze
      end
    end
    alias indexes each_index

    # @param name [String] trigger name
    # @return [Boolean] whether this table has a given trigger
    def has_trigger?(name)
      @triggers_hash.key?(name)
    end

    # @param name [String] trigger name
    # @return [Cassandra::Trigger, nil] a trigger or nil
    def trigger(name)
      @triggers_hash[name]
    end

    # Yield or enumerate each trigger bound to this table
    # @overload each_trigger
    #   @yieldparam trigger [Cassandra::Index] current trigger
    #   @return [Cassandra::Table] self
    # @overload each_trigger
    #   @return [Array<Cassandra::Trigger>] a list of triggers
    def each_trigger(&block)
      if block_given?
        @triggers.each(&block)
        self
      else
        @triggers.freeze
      end
    end
    alias triggers each_trigger

    # @param name [String] materialized view name
    # @return [Boolean] whether this table has a given materialized view
    def has_materialized_view?(name)
      @materialized_views_hash.key?(name)
    end

    # @param name [String] materialized view name
    # @return [Cassandra::MaterializedView, nil] a materialized view or nil
    def materialized_view(name)
      @materialized_views_hash[name]
    end

    # Yield or enumerate each materialized view bound to this table
    # @overload each_materialized_view
    #   @yieldparam materialized_view [Cassandra::MaterializedView] current materialized view
    #   @return [Cassandra::Table] self
    # @overload each_materialized_view
    #   @return [Array<Cassandra::MaterializedView>] a list of materialized views
    def each_materialized_view(&block)
      if block_given?
        @materialized_views.each(&block)
        self
      else
        @materialized_views.freeze
      end
    end
    alias materialized_views each_materialized_view

    # @return [String] a cql representation of this table
    def to_cql
      cql = "CREATE TABLE #{Util.escape_name(@keyspace.name)}.#{Util.escape_name(@name)} (\n"
      primary_key = @partition_key.first.name if @partition_key.one? && @clustering_columns.empty?

      first = true
      @columns.each do |column|
        if first
          first = false
        else
          cql << ",\n"
        end
        cql << "  #{Util.escape_name(column.name)} #{type_to_cql(column.type, column.frozen?)}"
        cql << ' PRIMARY KEY' if primary_key && column.name == primary_key
      end

      unless primary_key
        cql << ",\n  PRIMARY KEY ("
        if @partition_key.one?
          cql << Util.escape_name(@partition_key.first.name)
        else
          cql << '('
          first = true
          @partition_key.each do |column|
            if first
              first = false
            else
              cql << ', '
            end
            cql << Util.escape_name(column.name)
          end
          cql << ')'
        end
        @clustering_columns.each do |column|
          cql << ", #{Util.escape_name(column.name)}"
        end
        cql << ')'
      end

      cql << "\n)\nWITH "

      if @clustering_order.any? {|o| o != :asc}
        cql << 'CLUSTERING ORDER BY ('
        first = true
        @clustering_columns.zip(@clustering_order) do |column, order|
          if first
            first = false
          else
            cql << ', '
          end
          cql << "#{Util.escape_name(column.name)} #{order.to_s.upcase}"
        end
        cql << ")\n AND "
      end

      cql << @options.to_cql.split("\n").join("\n ")

      cql << ';'
    end

    # @private
    def add_index(index)
      @indexes << index
      @indexes_hash[index.name] = index
    end

    # @private
    def add_view(view)
      @materialized_views << view
      @materialized_views_hash[view.name] = view
    end

    # @private
    def add_trigger(trigger)
      @triggers << trigger
      @triggers_hash[trigger.name] = trigger
    end

    # @private
    def eql?(other)
      other.is_a?(Table) &&
        super.eql?(other) &&
        @clustering_order == other.clustering_order &&
        @indexes == other.indexes
    end
    alias == eql?

    private

    # @private
    def type_to_cql(type, is_frozen)
      case type.kind
      when :tuple
        "frozen <#{type}>"
      when :udt
        if @keyspace.name == type.keyspace
          "frozen <#{Util.escape_name(type.name)}>"
        else
          "frozen <#{Util.escape_name(type.keyspace)}.#{Util.escape_name(type.name)}>"
        end
      else
        if is_frozen
          "frozen <#{type}>"
        else
          type.to_s
        end
      end
    end
  end
end
