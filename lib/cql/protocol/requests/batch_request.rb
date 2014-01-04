# encoding: utf-8

module Cql
  module Protocol
    class BatchRequest < Request
      LOGGED_TYPE = 0
      UNLOGGED_TYPE = 1
      COUNTER_TYPE = 2

      def initialize(type, consistency, trace=false)
        super(0x0D, trace)
        @type = type
        @num_queries = 0
        @encoded_queries = ByteBuffer.new
        @consistency = consistency
      end

      def add_query(cql, values=nil)
        @encoded_queries << QUERY_KIND
        write_long_string(@encoded_queries, cql)
        QueryRequest.encode_values(@encoded_queries, values)
        @num_queries += 1
        nil
      end

      def add_prepared(id, metadata, values)
        @encoded_queries << PREPARED_KIND
        write_short_bytes(@encoded_queries, id)
        ExecuteRequest.encode_values(@encoded_queries, metadata, values)
        @num_queries += 1
        nil
      end

      def write(protocol_version, io)
        io << @type.ord
        write_short(io, @num_queries.ord)
        io << @encoded_queries
        write_consistency(io, @consistency)
      end

      def to_s
        type_str = case @type
          when LOGGED_TYPE then 'LOGGED'
          when UNLOGGED_TYPE then 'UNLOGGED'
          when COUNTER_TYPE then 'COUNTER'
        end
        %(BATCH #{type_str} #{@num_queries} #{@consistency.to_s.upcase})
      end

      private

      TYPE_CONVERTER = TypeConverter.new
      QUERY_KIND = "\x00".freeze
      PREPARED_KIND = "\x01".freeze
    end
  end
end
