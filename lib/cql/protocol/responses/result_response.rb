# encoding: utf-8

module Cql
  module Protocol
    class ResultResponse < Response
      attr_reader :trace_id

      def initialize(trace_id)
        @trace_id = trace_id
      end

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        kind = buffer.read_int
        impl = RESULT_TYPES[kind]
        raise UnsupportedResultKindError, %(Unsupported result kind: #{kind}) unless impl
        impl.decode(protocol_version, buffer, length - 4, trace_id)
      end

      def void?
        false
      end

      private

      RESPONSE_TYPES[0x08] = self

      RESULT_TYPES = [
        # populated by subclasses
      ]
    end
  end
end
