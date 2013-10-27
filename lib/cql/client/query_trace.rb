# encoding: utf-8

module Cql
  module Client
    class QueryTrace
      attr_reader :coordinator, :cql, :started_at, :duration, :events

      def initialize(session, events)
        if session
          @coordinator = session['coordinator']
          @cql = (parameters = session['parameters']) && parameters['query']
          @started_at = session['started_at']
          @duration = session['duration']/1_000_000.0
          if events
            @events = events.map { |e| TraceEvent.new(e) }.freeze
          end
        else
          @events = [].freeze
        end
      end
    end

    class TraceEvent
      attr_reader :activity, :source, :source_elapsed, :time

      def initialize(event)
        @activity = event['activity']
        @source = event['source']
        @source_elapsed = event['source_elapsed']/1_000_000.0
        @time = event['event_id'].to_time
      end
    end
  end
end