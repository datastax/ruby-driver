# encoding: utf-8

module Cql
  module Protocol
    class AuthSuccessResponse < Response
      attr_reader :token

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        new(buffer.read_bytes)
      end

      def initialize(token)
        @token = token
      end

      def to_s
        %(AUTH_SUCCESS #{@token.bytesize})
      end

      private

      RESPONSE_TYPES[0x10] = self
    end
  end
end
