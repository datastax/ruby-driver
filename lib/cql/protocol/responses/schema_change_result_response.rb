# encoding: utf-8

module Cql
  module Protocol
    class SchemaChangeResultResponse < ResultResponse
      attr_reader :change, :keyspace, :table

      def initialize(*args)
        @change, @keyspace, @table = args
      end

      def self.decode!(buffer)
        new(read_string!(buffer), read_string!(buffer), read_string!(buffer))
      end

      def to_s
        %(RESULT SCHEMA_CHANGE #@change "#@keyspace" "#@table")
      end
    end
  end
end
