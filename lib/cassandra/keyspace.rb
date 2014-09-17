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
    def initialize(name, durable_writes, replication, tables)
      @name           = name
      @durable_writes = durable_writes
      @replication    = replication
      @tables         = tables
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
    #   @return [Array<Cassandra::Table>] a list of tables
    # @overload each_table
    #   @return [Enumerator<Cassandra::Table>] an enumerator
    def each_table(&block)
      @tables.each_value(&block)
    end
    alias :tables :each_table

    # @return [String] a cql representation of this table
    def to_cql
      "CREATE KEYSPACE #{Util.escape_name(@name)} WITH REPLICATION = #{@replication.to_cql} AND DURABLE_WRITES = #{@durable_writes};"
    end

    # @return [Boolean] whether this keyspace is equal to the other
    def eql?(other)
      other.is_a?(Keyspace) &&
        @name == other.name &&
        @durable_writes == other.durable_writes &&
        @replication == other.replication &&
        @tables == other.raw_tables
    end
    alias :== :eql?

    # @private
    def update_table(table)
      Keyspace.new(@name, @durable_writes, @replication, @tables.merge(table.name => table))
    end

    # @private
    attr_reader :durable_writes
    protected :durable_writes

    protected

    # @private
    def raw_tables
      @tables
    end
  end
end
