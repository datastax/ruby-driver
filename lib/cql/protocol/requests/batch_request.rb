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
        @encoded_queries = CqlByteBuffer.new
        @consistency = consistency
      end

      def add_query(cql, values=nil, type_hints=nil)
        @encoded_queries.append(QUERY_KIND)
        @encoded_queries.append_long_string(cql)
        QueryRequest.encode_values(@encoded_queries, values, type_hints)
        @part_count += 1
        nil
      end

      def add_prepared(id, metadata, values)
        @encoded_queries.append(PREPARED_KIND)
        @encoded_queries.append_short_bytes(id)
        ExecuteRequest.encode_values(@encoded_queries, metadata, values)
        @part_count += 1
        nil
      end

      def write(protocol_version, buffer)
        buffer.append(@type.chr)
        buffer.append_short(@part_count)
        buffer.append(@encoded_queries)
        buffer.append_consistency(@consistency)
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
