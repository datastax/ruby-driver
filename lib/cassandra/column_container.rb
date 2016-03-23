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
  # This class contains all the logic needed for manipulating columns of an object.
  class ColumnContainer
    # Encapsulates all of the configuration options of a column-container.
    class Options
      # @return [String] the comment attribute of this column-container.
      attr_reader :comment
      # @return [Float] the chance with which a read repair is triggered for this column-container.
      attr_reader :read_repair_chance
      # @return [Float] the cluster local read repair chance for this column-container.
      attr_reader :local_read_repair_chance
      # @return [Integer] the tombstone garbage collection grace time in seconds for this column-container.
      attr_reader :gc_grace_seconds
      # @return [Hash] the caching options for this column-container.
      attr_reader :caching
      # @return [Float] the false positive chance for the Bloom filter of this column-container.
      attr_reader :bloom_filter_fp_chance
      # @return [Integer] how often (in milliseconds) to flush the memtable of this column-container.
      attr_reader :memtable_flush_period_in_ms
      # @return [Integer] the default TTL for this column-container.
      attr_reader :default_time_to_live
      # Return the speculative retry setting of this column-container, which determines how much
      # response delay the coordinator node will tolerate from the chosen replica before
      # retrying the request on other replicas. This setting can be expressed as a fixed
      # delay in ms (e.g. 10ms) or as a percentile indicating "when the response time has
      # exceeded the Nth percentile of read response times for this object" (e.g. 99percentile).
      # @return [String] the speculative retry setting of this column-container.
      attr_reader :speculative_retry
      # Return the index interval of this column-container; Cassandra will hold `1/index_interval` of row keys in memory.
      # @return [Integer] the index interval of this column-container. May be nil, indicating a default value of 128.
      attr_reader :index_interval
      # @return [Hash] compression settings
      attr_reader :compression
      # When compression is enabled, this option defines the probability
      # with which checksums for compressed blocks are checked during reads.
      # @return [Float] the probability of checking checksums on compressed blocks.
      attr_reader :crc_check_chance
      # @return [Hash] the extension options of this column-container.
      attr_reader :extensions

      # @return [ColumnContainer::Compaction] the compaction strategy of this column-container.
      attr_reader :compaction_strategy

      # @private
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
                     compression,
                     compact_storage,
                     crc_check_chance,
                     extensions)
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
        @compression                 = compression
        @compact_storage             = compact_storage
        @crc_check_chance            = crc_check_chance
        @extensions                  = extensions
      end

      # Return whether to replicate counter updates to other replicas. It is *strongly* recommended
      # that this setting be `true`. Otherwise, counter updates are only written to one replica
      # and fault tolerance is sacrificed.
      # @return [Boolean] whether to replicate counter updates to other replicas.
      def replicate_on_write?
        @replicate_on_write
      end

      # @return [Boolean] whether to populate the I/O cache on flush of this
      #   column-container. May be nil, indicating a default value of `false`.
      def populate_io_cache_on_flush?
        @populate_io_cache_on_flush
      end

      # @return [Boolean] whether this column-container uses compact storage.
      def compact_storage?
        @compact_storage
      end

      # @private
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
        unless @compression.nil?
          options << "compression = #{Util.encode_object(@compression)}"
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

      # @private
      def eql?(other)
        other.is_a?(Options) &&
          @comment == other.comment &&
          @read_repair_chance == other.read_repair_chance &&
          @local_read_repair_chance == other.local_read_repair_chance &&
          @gc_grace_seconds == other.gc_grace_seconds &&
          @caching == other.caching &&
          @bloom_filter_fp_chance == other.bloom_filter_fp_chance &&
          @populate_io_cache_on_flush == other.populate_io_cache_on_flush? &&
          @memtable_flush_period_in_ms == other.memtable_flush_period_in_ms &&
          @default_time_to_live == other.default_time_to_live &&
          @speculative_retry == other.speculative_retry &&
          @index_interval == other.index_interval &&
          @replicate_on_write == other.replicate_on_write? &&
          @compaction_strategy == other.compaction_strategy &&
          @compression == other.compression &&
          @compact_storage == other.compact_storage? &&
          @crc_check_chance == other.crc_check_chance &&
          @extensions == other.extensions
      end
      alias == eql?
    end

    # Encapsulates the compaction strategy of a column-container.
    class Compaction
      # @return [String] the name of the Cassandra class that performs compaction.
      attr_reader :class_name
      # @return [Hash] compaction strategy options
      attr_reader :options

      # @private
      def initialize(class_name, options)
        @class_name   = class_name
        @options = options
      end

      # @private
      def to_cql
        compaction = {'class' => @class_name}
        compaction.merge!(@options)

        Util.encode_hash(compaction)
      end

      # @private
      def eql?(other)
        other.is_a?(Compaction) &&
          @class_name == other.class_name &&
          @options == other.options
      end
      alias == eql?
    end

    # @return [String] name of this column-container
    attr_reader :name
    # @return [Cassandra::Uuid] the id of this object in Cassandra.
    attr_reader :id
    # @return [Cassandra::Keyspace] the keyspace that this column-container belongs to.
    attr_reader :keyspace
    # @return [ColumnContainer::Options] collection of configuration options of this column-container.
    attr_reader :options
    # @return [Array<Cassandra::Column>] ordered list of column-names that make up the partition-key.
    attr_reader :partition_key
    # @return [Array<Cassandra::Column>] ordered list of column-names that make up the clustering-columns.
    attr_reader :clustering_columns
    # @return [Array<Cassandra::Column>] primary key of this column-container. It's the combination of `partition_key` and `clustering_columns`.
    # @note This composition produces a flat list, so it will not be possible for the caller to distinguish partition-key columns from clustering-columns.
    attr_reader :primary_key

    # @private
    def initialize(keyspace,
                   name,
                   partition_key,
                   clustering_columns,
                   other_columns,
                   options,
                   id)
      @keyspace           = keyspace
      @name               = name.freeze
      @partition_key      = partition_key.freeze
      @clustering_columns = clustering_columns.freeze
      @options            = options
      @id                 = id

      # Make one array of all the columns, ordered with partition key, clustering
      # columns, then other columns. Make a hash as well, to support random access
      # to column metadata for a given column name. Save off the primary key (which
      # is partition-key + clustering-columns) while we're at it.

      @primary_key = @partition_key.dup.concat(@clustering_columns).freeze
      @columns = @primary_key.dup.concat(other_columns).freeze
      @columns_hash = @columns.each_with_object({}) do |col, h|
        h[col.name] = col
      end
    end

    # @param name [String] column name
    # @return [Boolean] whether this column-container has a given column
    def has_column?(name)
      @columns_hash.key?(name)
    end

    # @param name [String] column name
    # @return [Cassandra::Column, nil] a column or nil
    def column(name)
      @columns_hash[name]
    end

    # Yield or enumerate each column defined in this column-container
    # @overload each_column
    #   @yieldparam column [Cassandra::Column] current column
    #   @return [Cassandra::ColumnContainer] self
    # @overload each_column
    #   @return [Array<Cassandra::Column>] a list of columns
    def each_column(&block)
      if block_given?
        @columns.each(&block)
        self
      else
        @columns
      end
    end
    alias columns each_column

    # @private
    # keyspace attribute may be nil because when this object was constructed, we didn't have
    # its keyspace constructed yet. So allow updating @keyspace if it's nil, thus
    # allowing fetchers to create keyspace, table/view, and hook them together without
    # worrying about chickens and eggs.
    # NOTE: Ignore the set request if the @keyspace is already set.
    def set_keyspace(keyspace)
      @keyspace = keyspace unless @keyspace
    end

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "@keyspace=#{@keyspace.name} @name=#{@name}>"
    end

    # @private
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

    # @private
    def raw_columns
      @columns
    end
    protected :raw_columns
  end
end
