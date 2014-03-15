# encoding: utf-8

module Cql
  module Protocol
    class VoidResultResponse < ResultResponse
      def self.decode(protocol_version, buffer, length, trace_id=nil)
        new(trace_id)
      end

      def to_s
        %(RESULT VOID)
      end

      def void?
        true
      end

      private

      RESULT_TYPES[0x01] = self
    end
  end
end
