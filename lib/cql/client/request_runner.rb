# encoding: utf-8

module Cql
  module Client
    # @private
    class RequestRunner
      def execute(connection, request)
        connection.send_request(request).map do |response|
          case response
          when Protocol::RowsResultResponse
            QueryResult.new(response.metadata, response.rows)
          when Protocol::ErrorResponse
            cql = request.is_a?(Protocol::QueryRequest) ? request.cql : nil
            raise QueryError.new(response.code, response.message, cql)
          when Protocol::SetKeyspaceResultResponse
            KeyspaceChanged.new(response.keyspace)
          when Protocol::AuthenticateResponse
            AuthenticationRequired.new(response.authentication_class)
          else
            if block_given?
              yield response
            else
              nil
            end
          end
        end
      end
    end

    # @private
    class AuthenticationRequired
      attr_reader :authentication_class

      def initialize(authentication_class)
        @authentication_class = authentication_class
      end
    end

    # @private
    class KeyspaceChanged
      attr_reader :keyspace

      def initialize(keyspace)
        @keyspace = keyspace
      end
    end
  end
end
