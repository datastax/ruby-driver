# encoding: utf-8

module Cql
  module Protocol
    class OptionsRequest < Request
      def initialize
        super(5)
      end

      def compressable?
        false
      end

      def write(protocol_version, io)
        io
      end

      def to_s
        %(OPTIONS)
      end
    end
  end
end
