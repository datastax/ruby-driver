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
  # Represents a cassandra table
  # @see Cassandra::Keyspace#each_table
  # @see Cassandra::Keyspace#table
  class Table
    # @private
    class Options
      attr_reader :comment, :read_repair_chance, :local_read_repair_chance,
                  :gc_grace_seconds, :caching, :bloom_filter_fp_chance,
                  :populate_io_cache_on_flush, :memtable_flush_period_in_ms,
                  :default_time_to_live, :speculative_retry, :index_interval,
                  :replicate_on_write, :compaction_strategy, :compact_storage,
                  :compression_parameters

      def initialize(comment, read_repair_chance, local_read_repair_chance,
                     gc_grace_seconds, caching, bloom_filter_fp_chance,
                     populate_io_cache_on_flush, memtable_flush_period_in_ms,
                     default_time_to_live, speculative_retry, index_interval,
                     replicate_on_write, min_index_interval, max_index_interval,
                     compaction_strategy, compression_parameters, compact_storage)
        @comment                     = comment
        @read_repair_chance          = read_repair_chance
        @local_read_repair_chance    = local_read_repair_chance
        @gc_grace_seconds            = gc_grace_seconds
        @caching                     = caching
        @bloom_filter_fp_chance      = bloom_filter_fp_chance
        @populate_io_cache_on_flush  = populate_io_cache_on_flush
        @memtable_flush_period_in_ms = memtable_flush_period_in_ms
        @default_time_to_live        = default_time_to_live
        @speculative_retry           = speculative_retry
        @index_interval              = index_interval
        @replicate_on_write          = replicate_on_write
        @min_index_interval          = min_index_interval
        @max_index_interval          = max_index_interval
        @compaction_strategy         = compaction_strategy
        @compression_parameters      = compression_parameters
        @compact_storage             = compact_storage
      end

      def replicate_on_write?
        @replicate_on_write
      end

      def populate_io_cache_on_flush?
        @populate_io_cache_on_flush
      end

      def compact_storage?
        @compact_storage
      end

      def to_cql
        options = []

        options << 'COMPACT STORAGE' if @compact_storage
        options << "bloom_filter_fp_chance = #{Util.encode_object(@bloom_filter_fp_chance)}" unless @bloom_filter_fp_chance.nil?
        options << "caching = #{Util.encode_object(@caching)}" unless @caching.nil?
        options << "comment = #{Util.encode_object(@comment)}" unless @comment.nil?
        options << "compaction = #{@compaction_strategy.to_cql}" unless @compaction_strategy.nil?
        options << "compression = #{Util.encode_object(@compression_parameters)}" unless @compression_parameters.nil?
        options << "dclocal_read_repair_chance = #{Util.encode_object(@local_read_repair_chance)}" unless @local_read_repair_chance.nil?
        options << "default_time_to_live = #{Util.encode_object(@default_time_to_live)}" unless @default_time_to_live.nil?
        options << "gc_grace_seconds = #{Util.encode_object(@gc_grace_seconds)}" unless @gc_grace_seconds.nil?
        options << "index_interval = #{Util.encode_object(@index_interval)}" unless @index_interval.nil?
        options << "max_index_interval = #{Util.encode_object(@max_index_interval)}" unless @max_index_interval.nil?
        options << "memtable_flush_period_in_ms = #{Util.encode_object(@memtable_flush_period_in_ms)}" unless @memtable_flush_period_in_ms.nil?
        options << "min_index_interval = #{Util.encode_object(@min_index_interval)}" unless @min_index_interval.nil?
        options << "populate_io_cache_on_flush = '#{@populate_io_cache_on_flush}'" unless @populate_io_cache_on_flush.nil?
        options << "read_repair_chance = #{Util.encode_object(@read_repair_chance)}" unless @read_repair_chance.nil?
        options << "replicate_on_write = '#{@replicate_on_write}'" unless @replicate_on_write.nil?
        options << "speculative_retry = #{Util.encode_object(@speculative_retry)}" unless @speculative_retry.nil?

        options.join("\nAND ")
      end

      def eql?(other)
        other.is_a?(Options) &&
          @comment == other.comment &&
          @read_repair_chance == other.read_repair_chance &&
          @local_read_repair_chance == other.local_read_repair_chance &&
          @gc_grace_seconds == other.gc_grace_seconds &&
          @caching == other.caching &&
          @bloom_filter_fp_chance == other.bloom_filter_fp_chance &&
          @populate_io_cache_on_flush == other.populate_io_cache_on_flush &&
          @memtable_flush_period_in_ms == other.memtable_flush_period_in_ms &&
          @default_time_to_live == other.default_time_to_live &&
          @speculative_retry == other.speculative_retry &&
          @index_interval == other.index_interval &&
          @replicate_on_write == other.replicate_on_write &&
          @compaction_strategy == other.compaction_strategy &&
          @compression_parameters == other.compression_parameters &&
          @compact_storage == other.compact_storage
      end
      alias :== :eql?
    end

    # @private
    class Compaction
      attr_reader :klass, :options

      def initialize(klass, options)
        @klass   = klass
        @options = options
      end

      def to_cql
        compaction = {'class' => @klass}
        compaction.merge!(@options)

        Util.encode_hash(compaction)
      end

      def eql?(other)
        other.is_a?(Compaction) &&
          @klass == other.klass &&
          @options == other.options
      end
      alias :== :eql?
    end

    # @private
    attr_reader :keyspace
    # @return [String] table name
    attr_reader :name
    # @private
    attr_reader :options
    # @private
    attr_reader :partition_key

    # @private
    def initialize(keyspace, name, partition_key, clustering_columns, columns, options, clustering_order)
      @keyspace           = keyspace
      @name               = name
      @partition_key      = partition_key
      @clustering_columns = clustering_columns
      @columns            = columns
      @options            = options
      @clustering_order   = clustering_order
    end

    # @param name [String] column name
    # @return [Boolean] whether this table has a given column
    def has_column?(name)
      @columns.has_key?(name)
    end

    # @param name [String] column name
    # @return [Cassandra::Column, nil] a column or nil
    def column(name)
      @columns[name]
    end

    # Yield or enumerate each column defined in this table
    # @overload each_column
    #   @yieldparam column [Cassandra::Column] current column
    #   @return [Cassandra::Table] self
    # @overload each_column
    #   @return [Array<Cassandra::Column>] a list of columns
    def each_column(&block)
      if block_given?
        @columns.each_value(&block)
        self
      else
        @columns.values
      end
    end
    alias :columns :each_column

    # @return [String] a cql representation of this table
    def to_cql
      cql = "CREATE TABLE #{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)} (\n"
      primary_key = nil
      if @partition_key.one? && @clustering_columns.empty?
        primary_key = @partition_key.first.name
      end

      first = true
      @columns.each do |(_, column)|
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
        cql << "CLUSTERING ORDER BY ("
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
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @keyspace=#{@keyspace} @name=#{@name}>"
    end

    # @private
    def eql?(other)
      other.is_a?(Table) &&
        @keyspace == other.keyspace &&
        @name == other.name &&
        @partition_key == other.partition_key &&
        @clustering_columns == other.clustering_columns &&
        @columns == other.raw_columns &&
        @options == other.options &&
        @clustering_order == other.clustering_order
    end
    alias :== :eql?

    private

    # @private
    def type_to_cql(type, is_frozen)
      case type.kind
      when :tuple
        "frozen <#{type}>"
      when :udt
        if @keyspace == type.keyspace
          "frozen <#{Util.escape_name(type.name)}>"
        else
          "frozen <#{Util.escape_name(type.keyspace)}.#{Util.escape_name(type.name)}>"
        end
      else
        if is_frozen
          "frozen <#{type}>"
        else
          "#{type}"
        end
      end
    end

    attr_reader :clustering_columns, :clustering_order
    protected :clustering_columns, :clustering_order

    protected

    # @private
    def raw_columns
      @columns
    end
  end
end
