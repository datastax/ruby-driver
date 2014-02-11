# encoding: utf-8

module Cql
  module Client
    # Auth provider used for Cassandra's built in authentication.
    #
    # There is no need to create instances of this class to pass as `:auth_provider`
    # to {Cql::Client.connect}, instead use the `:credentials` option and one
    # will be created automatically for you.
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

    # Authenticator used for Cassandra's built in authentication,
    # see {Cql::Client::PlainTextAuthProvider}
    class PlainTextAuthenticator
      def initialize(username, password)
        @username = username
        @password = password
      end

      def initial_response
        "\x00#{@username}\x00#{@password}"
      end

      def challenge_response(token)
      end

      def authentication_successful(token)
      end
    end

    # @private
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