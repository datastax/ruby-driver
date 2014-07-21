# encoding: utf-8

module Cql
  module Statements
    class Prepared
      include Statement

      attr_reader :cql, :params_metadata, :result_metadata, :trace_id

      def initialize(cql, params_metadata, result_metadata, trace_id = nil)
        @cql             = cql
        @params_metadata = params_metadata
        @result_metadata = result_metadata
        @trace_id        = trace_id
      end

      def bind(*args)
        raise ::ArgumentError, "expecting exactly #{@params_metadata.size} bind parameters, #{args.size} given" if args.size != @params_metadata.size

        Bound.new(@cql, @params_metadata, @result_metadata, args)
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect}>"
      end
    end
  end
end
