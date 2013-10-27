# encoding: utf-8

module Cql
  module Client
    # @private
    class RequestRunner
      def execute(connection, request, timeout=nil)
        connection.send_request(request, timeout).flat_map do |response|
          case response
          when Protocol::RowsResultResponse
            if response.trace_id
              session_request = Protocol::QueryRequest.new(SELECT_SESSION_CQL % response.trace_id.to_s, :quorum)
              events_request = Protocol::QueryRequest.new(SELECT_EVENTS_CQL % response.trace_id.to_s, :quorum)
              session_future = execute(connection, session_request)
              events_future = execute(connection, events_request)
              Future.all(session_future, events_future).map do |session_rows, event_rows|
                QueryResult.new(response.metadata, response.rows, QueryTrace.new(session_rows.first, event_rows))
              end
            else
              Future.resolved(QueryResult.new(response.metadata, response.rows))
            end
          when Protocol::ErrorResponse
            cql = request.is_a?(Protocol::QueryRequest) ? request.cql : nil
            raise QueryError.new(response.code, response.message, cql)
          when Protocol::SetKeyspaceResultResponse
            Future.resolved(KeyspaceChanged.new(response.keyspace))
          when Protocol::AuthenticateResponse
            Future.resolved(AuthenticationRequired.new(response.authentication_class))
          else
            if block_given?
              Future.resolved(yield(response))
            else
              Future.resolved(nil)
            end
          end
        end
      end

      private

      SELECT_SESSION_CQL = 'SELECT * FROM system_traces.sessions WHERE session_id = %s'.freeze
      SELECT_EVENTS_CQL = 'SELECT * FROM system_traces.events WHERE session_id = %s'.freeze
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
