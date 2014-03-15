# encoding: utf-8

module Cql
  module Protocol
    class SchemaChangeResultResponse < ResultResponse
      attr_reader :change, :keyspace, :table

      def initialize(change, keyspace, table, trace_id)
        super(trace_id)
        @change, @keyspace, @table = change, keyspace, table
      end

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        new(buffer.read_string, buffer.read_string, buffer.read_string, trace_id)
      end

      def eql?(other)
        self.change == other.change && self.keyspace == other.keyspace && self.table == other.table
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 0
          h = ((h & 0xffffffff) * 31) ^ @change.hash
          h = ((h & 0xffffffff) * 31) ^ @keyspace.hash
          h = ((h & 0xffffffff) * 31) ^ @table.hash
          h
        end
      end

      def to_s
        %(RESULT SCHEMA_CHANGE #@change "#@keyspace" "#@table")
      end

      private

      RESULT_TYPES[0x05] = self
    end
  end
end
