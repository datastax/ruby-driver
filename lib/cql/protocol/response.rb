# encoding: utf-8

module Cql
  module Protocol
    class Response
      extend Decoding

      def self.decode!(opcode, buffer, trace_id)
        response_class = RESPONSE_TYPES[opcode]
        if response_class
          response_class.decode!(buffer, trace_id)
        else
          raise UnsupportedOperationError, "The operation #{opcode} is not supported"
        end
      end

      private

      RESPONSE_TYPES = [
        # populated by subclasses
      ]
    end
  end
end