# encoding: utf-8

module Cql
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
