# encoding: utf-8

module Cql
  module Protocol
    class AuthResponseRequest < Request
      def initialize(token)
        super(0x0f)
        @token = token
      end

      def write(protocol_version, io)
        write_bytes(io, @token)
      end

      def to_s
        %(AUTH_RESPONSE #{@token.bytesize})
      end
    end
  end
end
