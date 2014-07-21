# encoding: utf-8

module Cql
  module Statements
    class Simple
      include Statement

      attr_reader :cql, :params

      def initialize(cql, *params)
        @cql    = cql
        @params = params
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect} @params=#{@params.inspect}>"
      end
    end
  end
end
