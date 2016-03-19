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
  # Represents a cassandra table
  # @see Cassandra::Keyspace#each_table
  # @see Cassandra::Keyspace#table
  class Table
    extend Forwardable

    # @private
    def initialize(keyspace,
                   name,
                   partition_key,
                   clustering_columns,
                   other_columns,
                   options,
                   clustering_order)
      @column_container = ColumnContainer.new(keyspace, name, partition_key, clustering_columns, other_columns, options)
      @clustering_order   = clustering_order
    end

    # @!method name
    #   @return [String] table name
    #
    # @!method has_column?(name)
    #   @param name [String] column name
    #   @return [Boolean] whether this table has a given column
    #
    # @!method column(name)
    #   @param name [String] column name
    #   @return [Cassandra::Column, nil] a column or nil
    #
    # @!method each_column(&block)
    #   Yield or enumerate each column defined in this table
    #   @overload each_column
    #     @yieldparam column [Cassandra::Column] current column
    #     @return [Cassandra::Table] self
    #   @overload each_column
    #     @return [Array<Cassandra::Column>] a list of columns
    #
    # @!method columns
    #   @return [Array<Cassandra::Column>] a list of columns
    def_delegators :@column_container, :name, 'has_column?', :column, :each_column, :columns

    # @return [String] a cql representation of this table
    def to_cql
      cql = "CREATE TABLE #{Util.escape_name(keyspace)}.#{Util.escape_name(name)} (\n"
      primary_key = nil
      if partition_key.one? && clustering_columns.empty?
        primary_key = partition_key.first.name
      end

      first = true
      @column_container.raw_columns.each do |column|
        if first
          first = false
        else
          cql << ",\n" unless first
        end
        cql << "  #{column.name} #{type_to_cql(column.type, column.frozen?)}"
        cql << ' PRIMARY KEY' if primary_key && column.name == primary_key
      end

      unless primary_key
        cql << ",\n  PRIMARY KEY ("
        if partition_key.one?
          cql << partition_key.first.name
        else
          cql << '('
          first = true
          partition_key.each do |column|
            if first
              first = false
            else
              cql << ', '
            end
            cql << column.name
          end
          cql << ')'
        end
        clustering_columns.each do |column|
          cql << ", #{column.name}"
        end
        cql << ')'
      end

      cql << "\n)\nWITH "

      if @clustering_order.any? {|o| o != :asc}
        cql << 'CLUSTERING ORDER BY ('
        first = true
        clustering_columns.zip(@clustering_order) do |column, order|
          if first
            first = false
          else
            cql << ', '
          end
          cql << "#{column.name} #{order.to_s.upcase}"
        end
        cql << ")\n AND "
      end

      cql << options.to_cql.split("\n").join("\n ")

      cql << ';'
    end

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "@keyspace=#{keyspace} @name=#{name}>"
    end

    # @private
    def eql?(other)
      other.is_a?(Table) &&
        @column_container == other.column_container &&
        @clustering_order == other.clustering_order
    end
    alias == eql?

    private

    # Delegators to easily get to other attributes for use within our class (for to_cql).
    def_delegators :@column_container, :keyspace, :partition_key, :clustering_columns, :options

    # @private
    def type_to_cql(type, is_frozen)
      case type.kind
      when :tuple
        "frozen <#{type}>"
      when :udt
        if keyspace == type.keyspace
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

    # We need these accessors for eql? to work, but we don't want random users to
    # get these.

    # @private
    attr_reader :column_container, :clustering_order
    protected :column_container, :clustering_order
  end
end
