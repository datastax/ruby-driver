# encoding: utf-8

module Cql
  module Protocol
    class SetKeyspaceResultResponse < ResultResponse
      attr_reader :keyspace

      def initialize(keyspace)
        @keyspace = keyspace
      end

      def self.decode!(buffer)
        new(read_string!(buffer))
      end

      def to_s
        %(RESULT SET_KEYSPACE "#@keyspace")
      end
    end
  end
end
