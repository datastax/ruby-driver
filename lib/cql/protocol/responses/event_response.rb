# encoding: utf-8

module Cql
  module Protocol
    class EventResponse < ResultResponse
      def self.decode!(buffer, trace_id=nil)
        type = read_string!(buffer)
        impl = EVENT_TYPES[type]
        raise UnsupportedEventTypeError, %(Unsupported event type: "#{type}") unless impl
        impl.decode!(buffer, trace_id)
      end

      private

      EVENT_TYPES = {
        # populated by subclasses
      }
    end
  end
end
