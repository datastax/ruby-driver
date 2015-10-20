# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
      alias :== :eql?
    end

    # @return [String] this keyspace name
    attr_reader :name
    # @private
    attr_reader :replication

    # @private
    def initialize(name, durable_writes, replication, tables, types)
      @name           = name
      @durable_writes = durable_writes
      @replication    = replication
      @tables         = tables
      @types          = types
    end

    # @return [Boolean] whether durables writes are enabled for this keyspace
    def durable_writes?
      @durable_writes
    end

    # @return [Boolean] whether this keyspace has a table with the given name
    # @param name [String] table name
    def has_table?(name)
      @tables.has_key?(name)
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
    alias :tables :each_table

    # @return [Boolean] whether this keyspace has a user-defined type with the
    #   given name
    # @param name [String] user-defined type name
    def has_type?(name)
      @types.has_key?(name)
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
    alias :types :each_type

    # @return [String] a cql representation of this table
    def to_cql
      "CREATE KEYSPACE #{Util.escape_name(@name)} WITH replication = #{@replication.to_cql} AND durable_writes = #{@durable_writes};"
    end

    # @return [Boolean] whether this keyspace is equal to the other
    def eql?(other)
      other.is_a?(Keyspace) &&
        @name == other.name &&
        @durable_writes == other.durable_writes &&
        @replication == other.replication &&
        @tables == other.raw_tables &&
        @types == other.raw_types
    end
    alias :== :eql?

    # @return [String] a CLI-friendly keyspace representation
    def inspect
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @name=#{@name}>"
    end

    # @private
    def update_table(table)
      tables = @tables.dup
      tables[table.name] = table
      Keyspace.new(@name, @durable_writes, @replication, tables, @types)
    end

    # @private
    def delete_table(table_name)
      tables = @tables.dup
      tables.delete(table_name)
      Keyspace.new(@name, @durable_writes, @replication, tables, @types)
    end

    # @private
    def update_type(type)
      types = @types.dup
      types[type.name] = type
      Keyspace.new(@name, @durable_writes, @replication, @tables, types)
    end

    # @private
    def delete_type(type_name)
      types = @types.dup
      types.delete(type_name)
      Keyspace.new(@name, @durable_writes, @replication, @tables, types)
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
    def raw_types
      @types
    end
  end
end
