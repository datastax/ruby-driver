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

      def initialize(data, compaction_strategy, compression_parameters, compact_storage, cassandra_version)
        @comment                     = data['comment']
        @read_repair_chance          = data['read_repair_chance']
        @local_read_repair_chance    = data['local_read_repair_chance']
        @gc_grace_seconds            = data['gc_grace_seconds']
        @caching                     = data['caching']
        @bloom_filter_fp_chance      = data['bloom_filter_fp_chance']      || 0.01
        @populate_io_cache_on_flush  = data['populate_io_cache_on_flush']  || false
        @memtable_flush_period_in_ms = data['memtable_flush_period_in_ms'] || 0
        @default_time_to_live        = data['default_time_to_live']        || 0
        @speculative_retry           = data['speculative_retry']           || 'NONE'
        @index_interval              = data['index_interval']              || 128
        @replicate_on_write          = data['replicate_on_write']          || true
        @compaction_strategy         = compaction_strategy
        @compression_parameters      = compression_parameters
        @compact_storage             = compact_storage
        @cassandra_version           = cassandra_version
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
        options << "bloom_filter_fp_chance = #{@bloom_filter_fp_chance}"
        options << "caching = '#{@caching}'"
        options << "comment = '#{@comment}'" if @comment
        options << "compaction = #{@compaction_strategy.to_cql}"
        options << "compression = #{Util.encode_hash(@compression_parameters)}"
        options << "dclocal_read_repair_chance = #{@local_read_repair_chance}"
        options << "default_time_to_live = #{@default_time_to_live}" if !@cassandra_version.start_with?('1')
        options << "gc_grace_seconds = #{@gc_grace_seconds}"
        options << "index_interval = #{@index_interval}" if !@cassandra_version.start_with?('1')
        options << "populate_io_cache_on_flush = '#{@populate_io_cache_on_flush}'"
        options << "read_repair_chance = #{@read_repair_chance}"
        options << "replicate_on_write = '#{@replicate_on_write}'" if @cassandra_version.start_with?('1') || @cassandra_version.start_with?('2.0')
        options << "speculative_retry = '#{@speculative_retry}'" if !@cassandra_version.start_with?('1')

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
          @compact_storage == other.compact_storage &&
          @cassandra_version == other.cassandra_version
      end
      alias :== :eql?

      attr_reader :cassandra_version
      protected :cassandra_version
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
    def initialize(keyspace, name, partition_key, clustering_columns, columns, options, clustering_order, release_version)
      @keyspace           = keyspace
      @name               = name
      @partition_key      = partition_key
      @clustering_columns = clustering_columns
      @columns            = columns
      @options            = options
      @clustering_order   = clustering_order
      @release_version    = release_version
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
      cql   = "CREATE TABLE #{Util.escape_name(@keyspace)}.#{Util.escape_name(@name)} (\n"
      first = true
      @columns.each do |(_, column)|
        if first
          first = false
        else
          cql << ",\n" unless first
        end
        cql << "  #{column.to_cql}"
      end
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

    # @return [String] a CLI-friendly table representation
    def inspect
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @keyspace=#{@keyspace} @name=#{@name}>"
    end

    # @return [Boolean] whether this table is equal to the other
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

    # @private
    def create_partition_key(values)
      partition_key = @partition_key
      return nil if partition_key.size > values.size

      if partition_key.one?
        column      = partition_key.first
        column_name = column.name
        return nil unless values.has_key?(column_name)

        buffer = Protocol::CqlByteBuffer.new

        if @release_version > '2.1'
          Protocol::Coder.write_value_v3(buffer, values[column_name], column.type)
        else
          Protocol::Coder.write_value_v1(buffer, values[column_name], column.type)
        end

        buffer.discard(4)
      else
        buf    = nil
        buffer = nil

        partition_key.each do |column|
          column_name = column.name
          return nil unless values.has_key?(column_name)

          buf    ||= Protocol::CqlByteBuffer.new
          buffer ||= Protocol::CqlByteBuffer.new

          if @protocol_version > 2
            Protocol::Coder.write_value_v3(buf, values[column_name], column.type)
          else
            Protocol::Coder.write_value_v1(buf, values[column_name], column.type)
          end

          buf.discard(4) # discard size

          size = buf.length
          buffer.append_short(size)
          buffer << buf.read(size) << NULL_BYTE
        end
      end

      buffer.to_str
    end

    private

    NULL_BYTE = "\x00".freeze

    attr_reader :partition_key, :clustering_columns, :clustering_order
    protected :partition_key, :clustering_columns, :clustering_order

    protected

    # @private
    def raw_columns
      @columns
    end
  end
end
