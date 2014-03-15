# encoding: utf-8

module Cql
  module Protocol
    class AuthChallengeResponse < Response
      attr_reader :token

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        new(buffer.read_bytes)
      end

      def initialize(token)
        @token = token
      end

      def to_s
        %(AUTH_CHALLENGE #{@token.bytesize})
      end

      private

      RESPONSE_TYPES[0x0e] = self
    end
  end
end
