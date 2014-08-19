# encoding: utf-8

module Cql
  class Table
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
        options << "compression = #{JSON.dump(@compression_parameters)}"
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

    class Compaction
      attr_reader :klass, :options

      def initialize(klass, options)
        @klass   = klass
        @options = options
      end

      def to_cql
        JSON.dump({'class' => @klass}.merge(@options))
      end

      def eql?(other)
        other.is_a?(Compaction) &&
          @klass == other.klass &&
          @options == other.options
      end
      alias :== :eql?
    end

    attr_reader :keyspace, :name, :options

    def initialize(keyspace, name, partition_key, clustering_columns, columns, options, clustering_order)
      @keyspace           = keyspace
      @name               = name
      @partition_key      = partition_key
      @clustering_columns = clustering_columns
      @columns            = columns
      @options            = options
      @clustering_order   = clustering_order
    end

    def has_column?(name)
      @columns.has_key?(name)
    end

    def column(name)
      @columns[name]
    end

    def each_column(&block)
      @columns.values.each(&block)
    end
    alias :columns :each_column

    def to_cql
      cql  = "CREATE TABLE #{@keyspace}.#{@name} (\n"
      cql << @columns.map do |(_, column)|
        "  #{column.to_cql}"
      end.join(",\n")
      cql << ",\n"
      cql << "  PRIMARY KEY ("
      if @partition_key.one?
        cql << @partition_key.first.name
      else
        cql << '(' + @partition_key.map(&:name).join(', ') + ')'
      end
      @clustering_columns.each do |column|
        cql << ", #{column.name}"
      end
      cql << ')'

      cql << "\n)\nWITH "

      if @clustering_order.any? {|o| o != :asc}
        cql << "CLUSTERING ORDER BY ("
        cql << @clustering_columns.zip(@clustering_order).map do |column, order|
          "#{column.name} #{order.to_s.upcase}"
        end.join(', ')
        cql << ")\n AND "
      end

      cql << @options.to_cql.split("\n").join("\n ")

      cql << ';'
    end

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

    attr_reader :partition_key, :clustering_columns, :clustering_order
    protected :partition_key, :clustering_columns, :clustering_order

    protected

    def raw_columns
      @columns
    end
  end
end
