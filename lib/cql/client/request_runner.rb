# encoding: utf-8

module Cql
  module Client
    # @private
    class RequestRunner
      def execute(connection, request, timeout=nil, raw_metadata=nil)
        connection.send_request(request, timeout).map do |response|
          case response
          when Protocol::RawRowsResultResponse
            LazyQueryResult.new(raw_metadata, response, response.trace_id)
          when Protocol::RowsResultResponse
            QueryResult.new(response.metadata, response.rows, response.trace_id)
          when Protocol::VoidResultResponse
            response.trace_id ? VoidResult.new(response.trace_id) : VoidResult::INSTANCE
          when Protocol::ErrorResponse
            cql = request.is_a?(Protocol::QueryRequest) ? request.cql : nil
            details = response.respond_to?(:details) ? response.details : nil
            raise QueryError.new(response.code, response.message, cql, details)
          when Protocol::SetKeyspaceResultResponse
            KeyspaceChanged.new(response.keyspace)
          when Protocol::AuthenticateResponse
            AuthenticationRequired.new(response.authentication_class)
          when Protocol::SupportedResponse
            response.options
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
