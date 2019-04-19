# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
  # Represents a cassandra materialized view
  # @see Cassandra::Keyspace#each_materialized_view
  # @see Cassandra::Keyspace#materialized_view
  class MaterializedView < ColumnContainer
    # @private
    def initialize(keyspace,
                   name,
                   partition_key,
                   clustering_columns,
                   other_columns,
                   options,
                   include_all_columns,
                   where_clause,
                   base_table_name,
                   id)
      super(keyspace, name, partition_key, clustering_columns, other_columns, options, id)
      @include_all_columns = include_all_columns
      @where_clause = where_clause
      @base_table_name = base_table_name
    end

    # @return [Table] the table that this materialized view applies to.
    def base_table
      @keyspace.table(@base_table_name)
    end

    # @return [String] a cql representation of this materialized view
    def to_cql
      keyspace_name = Util.escape_name(@keyspace.name)
      cql = "CREATE MATERIALIZED VIEW #{keyspace_name}.#{Util.escape_name(@name)} AS\nSELECT "
      cql << if @include_all_columns
               '*'
             else
               @columns.map do |column|
                 Util.escape_name(column.name)
               end.join(', ')
             end
      cql << "\nFROM #{keyspace_name}.#{Util.escape_name(@base_table_name)}"
      cql << "\nWHERE #{@where_clause}" if @where_clause
      cql << "\nPRIMARY KEY (("
      cql << @partition_key.map do |column|
        Util.escape_name(column.name)
      end.join(', ')
      cql << ')'
      unless @clustering_columns.empty?
        cql << ', '
        cql << @clustering_columns.map do |column|
          Util.escape_name(column.name)
        end.join(', ')
      end
      cql << ")\nWITH #{@options.to_cql.split("\n").join("\n ")};"
    end

    # @private
    def eql?(other)
      other.is_a?(MaterializedView) &&
        super.eql?(other) &&
        @include_all_columns == other.include_all_columns &&
        @where_clause == other.where_clause &&
        @base_table_name == other.base_table.name
    end
    alias == eql?

    private

    # We need these accessors for eql? to work, but we don't want random users to
    # get these.

    # @private
    attr_reader :include_all_columns, :where_clause
    protected :include_all_columns, :where_clause
  end
end
