# encoding: utf-8

module Cql
  module Protocol
    class AuthenticateResponse < Response
      attr_reader :authentication_class

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        new(buffer.read_string)
      end

      def initialize(authentication_class)
        @authentication_class = authentication_class
      end

      def to_s
        %(AUTHENTICATE #{authentication_class})
      end

      private

      RESPONSE_TYPES[0x03] = self
    end
  end
end
