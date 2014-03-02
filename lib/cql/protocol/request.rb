# encoding: utf-8

module Cql
  module Protocol
    class Request
      attr_reader :opcode, :trace

      def initialize(opcode, trace=false)
        @opcode = opcode
        @trace = trace
      end

      def trace?
        @trace
      end

      def compressable?
        true
      end
    end
  end
end
