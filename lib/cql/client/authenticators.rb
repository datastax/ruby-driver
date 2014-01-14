# encoding: utf-8

module Cql
  module Client
    class PasswordAuthenticator
      def initialize(username, password)
        @username = username
        @password = password
      end

      def supports?(authenticator, protocol_version)
        authenticator == PASSWORD_AUTHENTICATOR_FQCN && protocol_version <= 2
      end

      def initial_request(protocol_version)
        if protocol_version == 2
          Protocol::AuthResponseRequest.new("\x00#{@username}\x00#{@password}")
        elsif protocol_version == 1
          Protocol::CredentialsRequest.new(username: @username, password: @password)
        else
          raise UnsupportedProtocolVersionError, "Protocol v#{protocol_version} not supported by #{self.class.name}"
        end
      end

      private

      PASSWORD_AUTHENTICATOR_FQCN = 'org.apache.cassandra.auth.PasswordAuthenticator'.freeze
    end
  end
end