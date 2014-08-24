# encoding: utf-8

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

module Cassandra
  class Keyspace
    class Replication
      attr_reader :klass, :options

      def initialize(klass, options)
        @klass   = klass
        @options = options
      end

      def to_cql
        JSON.dump({'class' => @klass}.merge(@options))
      end

      def eql?(other)
        other.is_a?(Replication) &&
          @klass == other.klass &&
          @options == other.options
      end
      alias :== :eql?
    end

    attr_reader :name, :replication

    def initialize(name, durable_writes, replication, tables)
      @name           = name
      @durable_writes = durable_writes
      @replication    = replication
      @tables         = tables
    end

    def durable_writes?
      @durable_writes
    end

    def has_table?(name)
      @tables.has_key?(name)
    end

    def table(name)
      @tables[name]
    end

    def each_table(&block)
      @tables.values.each(&block)
    end
    alias :tables :each_table

    def to_cql
      "CREATE KEYSPACE #{@name} WITH REPLICATION = #{@replication.to_cql} AND DURABLE_WRITES = #{@durable_writes};"
    end

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

    attr_reader :durable_writes
    protected :durable_writes

    protected

    def raw_tables
      @tables
    end
  end
end
