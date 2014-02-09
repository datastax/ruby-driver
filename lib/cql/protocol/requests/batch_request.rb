# encoding: utf-8

module Cql
  module Protocol
    class BatchRequest < Request
      LOGGED_TYPE = 0
      UNLOGGED_TYPE = 1
      COUNTER_TYPE = 2

      attr_reader :type, :consistency, :part_count

      def initialize(type, consistency, trace=false)
        super(0x0D, trace)
        @type = type
        @part_count = 0
        @encoded_queries = ByteBuffer.new
        @consistency = consistency
      end

      def add_query(cql, values=nil, type_hints=nil)
        @encoded_queries << QUERY_KIND
        write_long_string(@encoded_queries, cql)
        QueryRequest.encode_values(@encoded_queries, values, type_hints)
        @part_count += 1
        nil
      end

      def add_prepared(id, metadata, values)
        @encoded_queries << PREPARED_KIND
        write_short_bytes(@encoded_queries, id)
        ExecuteRequest.encode_values(@encoded_queries, metadata, values)
        @part_count += 1
        nil
      end

      def write(protocol_version, io)
        io << @type.chr
        write_short(io, @part_count)
        io << @encoded_queries
        write_consistency(io, @consistency)
      end

      def to_s
        type_str = case @type
          when LOGGED_TYPE then 'LOGGED'
          when UNLOGGED_TYPE then 'UNLOGGED'
          when COUNTER_TYPE then 'COUNTER'
        end
        %(BATCH #{type_str} #{@part_count} #{@consistency.to_s.upcase})
      end

      private

      TYPE_CONVERTER = TypeConverter.new
      QUERY_KIND = "\x00".freeze
      PREPARED_KIND = "\x01".freeze
    end
  end
end
