# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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

require 'forwardable'

module Cassandra
  # Represents a cassandra materialized view
  # @see Cassandra::Keyspace#each_materialized_view
  # @see Cassandra::Keyspace#materialized_view
  class MaterializedView
    extend Forwardable

    # @return [Table] the table that this materialized view applies to.
    attr_reader :base_table

    # @private
    def initialize(keyspace,
                   name,
                   partition_key,
                   clustering_columns,
                   other_columns,
                   options,
                   include_all_columns,
                   where_clause,
                   base_table)
      @column_container = ColumnContainer.new(keyspace, name, partition_key, clustering_columns, other_columns, options)
      @include_all_columns = include_all_columns
      @where_clause = where_clause
      @base_table = base_table
    end

    # @!method name
    #   @return [String] view name
    #
    # @!method has_column?(name)
    #   @param name [String] column name
    #   @return [Boolean] whether this view has a given column
    #
    # @!method column(name)
    #   @param name [String] column name
    #   @return [Cassandra::Column, nil] a column or nil
    #
    # @!method each_column(&block)
    #   Yield or enumerate each column defined in this view
    #   @overload each_column
    #     @yieldparam column [Cassandra::Column] current column
    #     @return [Cassandra::Table] self
    #   @overload each_column
    #     @return [Array<Cassandra::Column>] a list of columns
    #
    # @!method columns
    #   @return [Array<Cassandra::Column>] a list of columns
    def_delegators :@column_container, :name, 'has_column?', :column, :each_column, :columns

    # @return [String] a cql representation of this materialized view
    def to_cql
      keyspace_name = Util.escape_name(keyspace)
      cql = "CREATE MATERIALIZED VIEW #{keyspace_name}.#{Util.escape_name(name)} AS\nSELECT "
      if @include_all_columns
        cql << '*'
      else
        cql << @column_container.raw_columns.map do |column|
          Util.escape_name(column.name)
        end.join(', ')
      end
      cql << "\nFROM #{keyspace_name}.#{Util.escape_name(@base_table.name)}"
      cql << "\nWHERE #{@where_clause}" if @where_clause
      cql << "\nPRIMARY KEY(("
      cql << partition_key.map do |column|
        Util.escape_name(column.name)
      end.join(', ')
      cql << ")"
      unless clustering_columns.empty?
        cql << ","
        cql << clustering_columns.map do |column|
          Util.escape_name(column.name)
        end.join(', ')
      end
      cql << ")\nWITH #{options.to_cql};"
    end

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "@keyspace=#{keyspace} @name=#{name}>"
    end

    # @private
    def eql?(other)
      other.is_a?(MaterializedView) &&
          @column_container == other.column_container &&
          @include_all_columns == other.include_all_columns &&
          @where_clause == other.where_clause &&
          @base_table == other.base_table
    end
    alias == eql?

    private

    # Delegators to easily get to other attributes for use within our class (for to_cql)
    def_delegators :@column_container, :keyspace, :partition_key, :clustering_columns, :options

    # We need these accessors for eql? to work, but we don't want random users to
    # get these.

    # @private
    attr_reader :column_container, :include_all_columns, :where_clause
    protected :column_container, :include_all_columns, :where_clause
  end
end
