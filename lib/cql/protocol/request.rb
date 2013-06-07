# encoding: utf-8

module Cql
  module Protocol
    class Request
      include Encoding

      attr_reader :opcode

      def initialize(opcode)
        @opcode = opcode
      end
    end
  end
end
