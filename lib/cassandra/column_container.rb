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
  # This class contains all the logic needed for manipulating columns of an object
  # (e.g. table or materialized view). It's hidden from the user.

  # @private
  class ColumnContainer
    class Options
      attr_reader :comment, :read_repair_chance, :local_read_repair_chance,
                  :gc_grace_seconds, :caching, :bloom_filter_fp_chance,
                  :populate_io_cache_on_flush, :memtable_flush_period_in_ms,
                  :default_time_to_live, :speculative_retry, :index_interval,
                  :replicate_on_write, :compaction_strategy, :compact_storage,
                  :compression_parameters, :crc_check_chance

      def initialize(comment,
                     read_repair_chance,
                     local_read_repair_chance,
                     gc_grace_seconds,
                     caching,
                     bloom_filter_fp_chance,
                     populate_io_cache_on_flush,
                     memtable_flush_period_in_ms,
                     default_time_to_live,
                     speculative_retry,
                     index_interval,
                     replicate_on_write,
                     min_index_interval,
                     max_index_interval,
                     compaction_strategy,
                     compression_parameters,
                     compact_storage,
                     crc_check_chance)
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
        @crc_check_chance            = crc_check_chance
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
        unless @bloom_filter_fp_chance.nil?
          options <<
            "bloom_filter_fp_chance = #{Util.encode_object(@bloom_filter_fp_chance)}"
        end
        options << "caching = #{Util.encode_object(@caching)}" unless @caching.nil?
        options << "comment = #{Util.encode_object(@comment)}" unless @comment.nil?
        unless @compaction_strategy.nil?
          options << "compaction = #{@compaction_strategy.to_cql}"
        end
        unless @compression_parameters.nil?
          options << "compression = #{Util.encode_object(@compression_parameters)}"
        end
        unless @local_read_repair_chance.nil?
          options << 'dclocal_read_repair_chance = ' \
              "#{Util.encode_object(@local_read_repair_chance)}"
        end
        unless @default_time_to_live.nil?
          options << "default_time_to_live = #{Util.encode_object(@default_time_to_live)}"
        end
        unless @gc_grace_seconds.nil?
          options << "gc_grace_seconds = #{Util.encode_object(@gc_grace_seconds)}"
        end
        unless @index_interval.nil?
          options << "index_interval = #{Util.encode_object(@index_interval)}"
        end
        unless @max_index_interval.nil?
          options << "max_index_interval = #{Util.encode_object(@max_index_interval)}"
        end
        unless @memtable_flush_period_in_ms.nil?
          options << 'memtable_flush_period_in_ms = ' \
              "#{Util.encode_object(@memtable_flush_period_in_ms)}"
        end
        unless @min_index_interval.nil?
          options << "min_index_interval = #{Util.encode_object(@min_index_interval)}"
        end
        unless @populate_io_cache_on_flush.nil?
          options << "populate_io_cache_on_flush = '#{@populate_io_cache_on_flush}'"
        end
        unless @read_repair_chance.nil?
          options << "read_repair_chance = #{Util.encode_object(@read_repair_chance)}"
        end
        unless @replicate_on_write.nil?
          options << "replicate_on_write = '#{@replicate_on_write}'"
        end
        unless @speculative_retry.nil?
          options << "speculative_retry = #{Util.encode_object(@speculative_retry)}"
        end
        unless @crc_check_chance.nil?
          options << 'crc_check_chance = ' \
              "#{Util.encode_object(@crc_check_chance)}"
        end

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
          @crc_check_chance == other.crc_check_chance
      end
      alias == eql?
    end

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
      alias == eql?
    end

    attr_reader :keyspace
    attr_reader :name
    attr_reader :options
    attr_reader :partition_key

    attr_reader :clustering_columns

    def initialize(keyspace,
                   name,
                   partition_key,
                   clustering_columns,
                   other_columns,
                   options)
      @keyspace           = keyspace
      @name               = name
      @partition_key      = partition_key
      @clustering_columns = clustering_columns
      @options            = options

      # Make one array of all the columns, ordered with partition key, clustering
      # columns, then other columns. Make a hash as well, to support random access
      # to column metadata for a given column name.

      @columns = @partition_key.dup
      @columns.concat(@clustering_columns).concat(other_columns)
      @columns_hash = @columns.each_with_object({}) do |col, h|
        h[col.name] = col
      end
    end

    def has_column?(name)
      @columns_hash.key?(name)
    end

    def column(name)
      @columns_hash[name]
    end

    def each_column(&block)
      if block_given?
        @columns.each(&block)
        self
      else
        # We return a dup of the columns so that the caller can manipulate
        # the array however they want without affecting the source.
        @columns.dup
      end
    end
    alias columns each_column

    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "@keyspace=#{@keyspace} @name=#{@name}>"
    end

    def eql?(other)
      other.is_a?(ColumnContainer) &&
        @keyspace == other.keyspace &&
        @name == other.name &&
        @partition_key == other.partition_key &&
        @clustering_columns == other.clustering_columns &&
        @columns == other.raw_columns &&
        @options == other.options
    end
    alias == eql?

    def raw_columns
      @columns
    end
  end
end
