# encoding: utf-8

module Cql
  module Client
    class PlainTextAuthProvider
      def initialize(username, password)
        @username = username
        @password = password
      end

      def create_authenticator(authentication_class, protocol_version)
        if authentication_class == PASSWORD_AUTHENTICATOR_FQCN
          if protocol_version == 1
            CredentialsAuthenticator.new('username' => @username, 'password' => @password)
          else
            PlainTextAuthenticator.new(@username, @password)
          end
        end
      end

      private

      PASSWORD_AUTHENTICATOR_FQCN = 'org.apache.cassandra.auth.PasswordAuthenticator'.freeze
    end

    class PlainTextAuthenticator
      def initialize(username, password)
        @username = username
        @password = password
      end

      def initial_response
        "\x00#{@username}\x00#{@password}"
      end
    end

    class CredentialsAuthenticator
      def initialize(credentials)
        @credentials = credentials
      end

      def initial_response
        @credentials
      end
    end
  end
end