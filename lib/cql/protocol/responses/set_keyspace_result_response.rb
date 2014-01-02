# encoding: utf-8

module Cql
  module Protocol
    class SetKeyspaceResultResponse < ResultResponse
      attr_reader :keyspace

      def initialize(keyspace, trace_id)
        super(trace_id)
        @keyspace = keyspace
      end

      def self.decode!(protocol_version, buffer, trace_id=nil)
        new(read_string!(buffer), trace_id)
      end

      def to_s
        %(RESULT SET_KEYSPACE "#@keyspace")
      end

      private

      RESULT_TYPES[0x03] = self
    end
  end
end
