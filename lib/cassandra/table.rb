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
      @clustering_order   = clustering_order
    end

    # @return [String] a cql representation of this table
    def to_cql
      cql = "CREATE TABLE #{Util.escape_name(@keyspace.name)}.#{Util.escape_name(@name)} (\n"
      primary_key = nil
      if @partition_key.one? && @clustering_columns.empty?
        primary_key = @partition_key.first.name
      end

      first = true
      @columns.each do |column|
        if first
          first = false
        else
          cql << ",\n"
        end
        cql << "  #{column.name} #{type_to_cql(column.type, column.frozen?)}"
        cql << ' PRIMARY KEY' if primary_key && column.name == primary_key
      end

      unless primary_key
        cql << ",\n  PRIMARY KEY ("
        if @partition_key.one?
          cql << @partition_key.first.name
        else
          cql << '('
          first = true
          @partition_key.each do |column|
            if first
              first = false
            else
              cql << ', '
            end
            cql << column.name
          end
          cql << ')'
        end
        @clustering_columns.each do |column|
          cql << ", #{column.name}"
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
          cql << "#{column.name} #{order.to_s.upcase}"
        end
        cql << ")\n AND "
      end

      cql << @options.to_cql.split("\n").join("\n ")

      cql << ';'
    end

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "@keyspace=#{@keyspace.name} @name=#{@name}>"
    end

    # @private
    def eql?(other)
      other.is_a?(Table) &&
        super.eql?(other) &&
        @clustering_order == other.clustering_order
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
