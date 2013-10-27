# encoding: utf-8

module Cql
  module Client
    # @private
    class RequestRunner
      def execute(connection, request, timeout=nil)
        connection.send_request(request, timeout).map do |response|
          case response
          when Protocol::RowsResultResponse
            if response.trace_id
              trace_loader = TraceLoader.new(self, connection, response.trace_id)
            else
              trace_loader = TraceLoader::NULL
            end
            QueryResult.new(response.metadata, response.rows, trace_loader)
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
    class NullTraceLoader
      def load
        Future.resolved(NullQueryTrace.new)
      end
    end

    # @private
    class TraceLoader
      def initialize(request_runner, connection, trace_id)
        @request_runner = request_runner
        @connection = connection
        @trace_id = trace_id
      end

      def load
        session_request = Protocol::QueryRequest.new(SELECT_SESSION_CQL % @trace_id.to_s, :quorum)
        events_request = Protocol::QueryRequest.new(SELECT_EVENTS_CQL % @trace_id.to_s, :quorum)
        sessions_future = @request_runner.execute(@connection, session_request)
        events_future = @request_runner.execute(@connection, events_request)
        Future.all(sessions_future, events_future).map do |session_rows, event_rows|
          QueryTrace.new(session_rows.first, event_rows)
        end
      end

      NULL = NullTraceLoader.new

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
