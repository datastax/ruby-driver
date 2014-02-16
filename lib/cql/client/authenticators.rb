# encoding: utf-8

module Cql
  module Client
    # An auth provider is a factory for {Cql::Client::Authenticator} instances
    # (or objects matching that interface). Its {#create_authenticator} will be
    # called once for each connection that requires authentication.
    #
    # If the authentication requires keeping state, keep that in the
    # authenticator instances, not in the auth provider.
    #
    # @note Creating an authenticator must absolutely not block, or the whole
    #   connection process will block.
    #
    # @note Auth providers given to {Cql::Client.connect} as the `:auth_provider`
    #   option don't need to be subclasses of this class, but need to
    #   implement the same methods. This class exists only for documentation
    #   purposes.
    class AuthProvider
      # @!method create_authenticator(authentication_class, protocol_version)
      #
      # Create a new authenticator object. This method will be called once per
      # connection that requires authentication. The auth provider can create
      # different authenticators for different authentication classes, or return
      # nil if it does not support the authentication class.
      #
      # @note This method must absolutely not block.
      #
      # @param authentication_class [String] the authentication class used by
      #   the server.
      # @return [Cql::Client::Authenticator, nil] an object with an interface
      #   matching {Cql::Client::Authenticator} or nil if the authentication
      #   class is not supported.
    end

    # An authenticator handles the authentication challenge/response cycles of
    # a single connection. It can be stateful, but it must not for any reason
    # block. If any of the method calls block, the whole connection process
    # will be blocked.
    #
    # @note Authenticators created by auth providers don't need to be subclasses
    #   of this class, but need to implement the same methods. This class exists
    #   only for documentation purposes.
    class Authenticator
      # @!method initial_response
      #
      # This method must return the initial authentication token to be sent to
      # the server.
      #
      # @note This method must absolutely not block.
      #
      # @return [String] the initial authentication token

      # @!method challenge_response(token)
      #
      # If the authentication requires multiple challenge/response cycles this
      # method will be called when a challenge is returned by the server. A
      # response token must be created and will be sent back to the server.
      #
      # @note This method must absolutely not block.
      #
      # @param token [String] a challenge token sent by the server
      # @return [String] the authentication token to send back to the server

      # @!method authentication_successful(token)
      #
      # Called when the authentication is successful.
      #
      # @note This method must absolutely not block.
      #
      # @param token [String] a token sent by the server
      # @return [nil]
    end

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

      def create_authenticator(authentication_class)
        if authentication_class == PASSWORD_AUTHENTICATOR_FQCN
          PlainTextAuthenticator.new(@username, @password)
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
  end
end