# encoding: utf-8

module Cql
  module Protocol
    class AuthResponseRequest < Request
      attr_reader :token

      def initialize(token)
        super(0x0f)
        @token = token
      end

      def write(protocol_version, buffer)
        buffer.append_bytes(@token)
      end

      def to_s
        %(AUTH_RESPONSE #{@token.bytesize})
      end

      def eql?(other)
        self.token == other.token
      end
      alias_method :==, :eql?

      def hash
        @token.hash
      end
    end
  end
end
