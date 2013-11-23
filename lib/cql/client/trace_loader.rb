# encoding: utf-8

module Cql
  module Client
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
  end
end
