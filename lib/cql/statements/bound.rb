# encoding: utf-8

module Cql
  module Statements
    class Bound
      include Statement

      attr_reader :cql, :params_metadata, :result_metadata, :params

      def initialize(cql, params_metadata, result_metadata, params)
        @cql             = cql
        @params_metadata = params_metadata
        @result_metadata = result_metadata
        @params          = params
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect} @params=#{@params}>"
      end
    end
  end
end
