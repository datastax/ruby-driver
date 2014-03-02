# encoding: utf-8

module Cql
  module Protocol
    class SupportedResponse < Response
      attr_reader :options

      def initialize(options)
        @options = options
      end

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        new(buffer.read_string_multimap)
      end

      def to_s
        %(SUPPORTED #{options})
      end

      private

      RESPONSE_TYPES[0x06] = self
    end
  end
end
