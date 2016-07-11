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
  # Represents a cassandra keyspace
  # @see Cassandra::Cluster#each_keyspace
  # @see Cassandra::Cluster#keyspace
  class Keyspace
    # @private
    class Replication
      attr_reader :klass, :options

      def initialize(klass, options)
        @klass   = klass
        @options = options
      end

      def to_cql
        replication = {'class' => @klass}
        replication.merge!(@options)

        Util.encode_hash(replication)
      end

      def eql?(other)
        other.is_a?(Replication) &&
          @klass == other.klass &&
          @options == other.options
      end
      alias == eql?
    end

    # @return [String] this keyspace name
    attr_reader :name
    # @private
    attr_reader :replication

    # @private
    def initialize(name,
                   durable_writes,
                   replication,
                   tables,
                   types,
                   functions,
                   aggregates,
                   views)
      @name           = name
      @durable_writes = durable_writes
      @replication    = replication
      @tables         = tables
      @types          = types
      @functions      = functions
      @aggregates     = aggregates
      @views          = views

      # Set the keyspace attribute on the tables and views.
      @tables.each_value do |t|
        t.set_keyspace(self)
      end
      @views.each_value do |v|
        v.set_keyspace(self)
      end
    end

    # @return [Boolean] whether durables writes are enabled for this keyspace
    def durable_writes?
      @durable_writes
    end

    # @return [Boolean] whether this keyspace has a table with the given name
    # @param name [String] table name
    def has_table?(name)
      @tables.key?(name)
    end

    # @return [Cassandra::Table, nil] a table or nil
    # @param name [String] table name
    def table(name)
      @tables[name]
    end

    # Yield or enumerate each table defined in this keyspace
    # @overload each_table
    #   @yieldparam table [Cassandra::Table] current table
    #   @return [Cassandra::Keyspace] self
    # @overload each_table
    #   @return [Array<Cassandra::Table>] a list of tables
    def each_table(&block)
      if block_given?
        @tables.each_value(&block)
        self
      else
        @tables.values
      end
    end
    alias tables each_table

    # @return [Boolean] whether this keyspace has a materialized view with the given name
    # @param name [String] materialized view name
    def has_materialized_view?(name)
      # We check if the view exists *and* that its base-table is set. If base-table isn't available,
      # it will be soon, so the user can poll on this method until we return a fully-baked materialized view.
      @views.key?(name) && @views[name].base_table
    end

    # @return [Cassandra::MaterializedView, nil] a materialized view or nil
    # @param name [String] materialized view name
    def materialized_view(name)
      @views[name] if has_materialized_view?(name)
    end

    # Yield or enumerate each materialized view defined in this keyspace
    # @overload each_materialized_view
    #   @yieldparam view [Cassandra::MaterializedView] current materialized view
    #   @return [Cassandra::Keyspace] self
    # @overload each_materialized_view
    #   @return [Array<Cassandra::MaterializedView>] a list of materialized views
    def each_materialized_view(&block)
      if block_given?
        @views.each_value do |v|
          block.call(v) if v.base_table
        end
        self
      else
        result = []
        @views.each_value do |v|
          result << v if v.base_table
        end
        result
      end
    end
    alias materialized_views each_materialized_view

    # @return [Boolean] whether this keyspace has a user-defined type with the
    #   given name
    # @param name [String] user-defined type name
    def has_type?(name)
      @types.key?(name)
    end

    # @return [Cassandra::Types::UserDefined, nil] a type or nil
    # @param name [String] user-defined type name
    def type(name)
      @types[name]
    end

    # Yield or enumerate each user-defined type present in this keyspace
    # @overload each_type
    #   @yieldparam type [Cassandra::Types::UserDefined] current type
    #   @return [Cassandra::Keyspace] self
    # @overload each_type
    #   @return [Array<Cassandra::Types::UserDefined>] a list of user-defined types
    def each_type(&block)
      if block_given?
        @types.each_value(&block)
        self
      else
        @types.values
      end
    end
    alias types each_type

    # @return [Boolean] whether this keyspace has a function with the given name and
    #   arguments
    # @param name [String] function name
    # @param args [Array<String>] (var-args style) function argument types
    def has_function?(name, *args)
      !@functions.get(name.downcase, args).nil?
    end

    # @return [Cassandra::Function, nil] a function or nil
    # @param name [String] function name
    # @param args [Array<String>] (var-args style) function argument types
    def function(name, *args)
      # The functions_hash datastructure is a hash <[func-name, args], Function>.
      # So construct the array-key we're looking for.
      @functions.get(name.downcase, args)
    end

    # Yield or enumerate each function defined in this keyspace
    # @overload each_function
    #   @yieldparam function [Cassandra::Function] current function
    #   @return [Cassandra::Keyspace] self
    # @overload each_function
    #   @return [Array<Cassandra::Function>] a list of functions
    def each_function(&block)
      if block_given?
        @functions.each_function(&block)
        self
      else
        @functions.functions
      end
    end
    alias functions each_function

    # @return [Boolean] whether this keyspace has an aggregate with the given
    #                   name and arguments
    # @param name [String] aggregate name
    # @param args [Array<String>] (var-args style) aggregate function argument types
    def has_aggregate?(name, *args)
      !@aggregates.get(name.downcase, args).nil?
    end

    # @return [Cassandra::Aggregate, nil] an aggregate or nil
    # @param name [String] aggregate name
    # @param args [Array<String>] (var-args style) aggregate function argument types
    def aggregate(name, *args)
      @aggregates.get(name.downcase, args)
    end

    # Yield or enumerate each aggregate defined in this keyspace
    # @overload each_aggregate
    #   @yieldparam aggregate [Cassandra::Aggregate] current aggregate
    #   @return [Cassandra::Keyspace] self
    # @overload each_aggregate
    #   @return [Array<Cassandra::Aggregate>] a list of aggregates
    def each_aggregate(&block)
      if block_given?
        @aggregates.each_function(&block)
        self
      else
        @aggregates.functions
      end
    end
    alias aggregates each_aggregate

    # @return [String] a cql representation of this keyspace
    def to_cql
      "CREATE KEYSPACE #{Util.escape_name(@name)} " \
          "WITH replication = #{@replication.to_cql} AND " \
          "durable_writes = #{@durable_writes};"
    end

    # @private
    def eql?(other)
      other.is_a?(Keyspace) &&
        @name == other.name &&
        @durable_writes == other.durable_writes &&
        @replication == other.replication &&
        @tables == other.raw_tables &&
        @types == other.raw_types &&
        @functions == other.raw_functions &&
        @aggregates == other.raw_aggregates
    end
    alias == eql?

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} @name=#{@name}>"
    end

    # @private
    def update_table(table)
      tables = @tables.dup
      tables[table.name] = table
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   tables,
                   @types,
                   @functions,
                   @aggregates,
                   @views)
    end

    # @private
    def delete_table(table_name)
      tables = @tables.dup
      tables.delete(table_name)
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   tables,
                   @types,
                   @functions,
                   @aggregates,
                   @views)
    end

    # @private
    def update_materialized_view(view)
      views = @views.dup
      views[view.name] = view
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   @tables,
                   @types,
                   @functions,
                   @aggregates,
                   views)
    end

    # @private
    def delete_materialized_view(view_name)
      views = @views.dup
      views.delete(view_name)
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   @tables,
                   @types,
                   @functions,
                   @aggregates,
                   views)
    end

    # @private
    def update_type(type)
      types = @types.dup
      types[type.name] = type
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   @tables,
                   types,
                   @functions,
                   @aggregates,
                   @views)
    end

    # @private
    def delete_type(type_name)
      types = @types.dup
      types.delete(type_name)
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   @tables,
                   types,
                   @functions,
                   @aggregates,
                   @views)
    end

    # @private
    def update_function(function)
      functions = @functions.dup
      functions.add_or_update(function)
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   @tables,
                   @types,
                   functions,
                   @aggregates,
                   @views)
    end

    # @private
    def delete_function(function_name, function_args)
      functions = @functions.dup
      functions.delete(function_name, function_args)
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   @tables,
                   @types,
                   functions,
                   @aggregates,
                   @views)
    end

    # @private
    def update_aggregate(aggregate)
      aggregates = @aggregates.dup
      aggregates.add_or_update(aggregate)
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   @tables,
                   @types,
                   @functions,
                   aggregates,
                   @views)
    end

    # @private
    def delete_aggregate(aggregate_name, aggregate_args)
      aggregates = @aggregates.dup
      aggregates.delete(aggregate_name, aggregate_args)
      Keyspace.new(@name,
                   @durable_writes,
                   @replication,
                   @tables,
                   @types,
                   @functions,
                   aggregates,
                   @views)
    end

    # @private
    attr_reader :durable_writes
    protected :durable_writes

    protected

    # @private
    def raw_tables
      @tables
    end

    # @private
    def raw_materialized_views
      @views
    end

    # @private
    def raw_types
      @types
    end

    # @private
    def raw_functions
      @functions
    end

    # @private
    def raw_aggregates
      @aggregates
    end
  end
end
