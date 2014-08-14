# encoding: utf-8

module Cql
  # Base class for all Errors raised by the driver 
  # @see Cql::Errors
  class Error < StandardError
  end

  module Errors
    # @!parse class IoError < StandardError; end
    # @private
    IoError = Ione::IoError

    # This error type represents errors sent by the server, the `code` attribute
    # can be used to find the exact type, and `cql` contains the request's CQL,
    # if any. `message` contains the human readable error message sent by the
    # server.
    class QueryError < Error
      # @return [Integer] error code
      attr_reader :code
      # @return [String] original CQL used
      attr_reader :cql
      # @return [Hash{Symbol => String, Integer}] various error details
      attr_reader :details

      # @private
      def initialize(code, message, cql=nil, details=nil)
        super(message)
        @code = code
        @cql = cql
        @details = details
      end
    end

    # This error is thrown when not hosts could be reached during connection or query execution.
    class NoHostsAvailable < Error
      # @return [Hash{Cql::Host => Exception}] a map of hosts to underlying exceptions
      attr_reader :errors

      # @private
      def initialize(errors = {})
        super("no hosts available, check #errors property for details")

        @errors = errors
      end
    end

    # Client error represents bad driver state or configuration
    #
    # @see Cql::Errors::AuthenticationError
    class ClientError < Error
    end

    # Raised when cannot authenticate to Cassandra
    class AuthenticationError < ClientError
    end

    # @private
    NotConnectedError = Class.new(Error)
    # @private
    NotPreparedError = Class.new(Error)
  end
end
