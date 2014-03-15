# encoding: utf-8

module Cql
  module Protocol
    class EventResponse < ResultResponse
      def self.decode(protocol_version, buffer, length, trace_id=nil)
        type = buffer.read_string
        impl = EVENT_TYPES[type]
        raise UnsupportedEventTypeError, %(Unsupported event type: "#{type}") unless impl
        new_length = length - 4 - type.bytesize
        impl.decode(protocol_version, buffer, new_length, trace_id)
      end

      private

      RESPONSE_TYPES[0x0c] = self

      EVENT_TYPES = {
        # populated by subclasses
      }
    end
  end
end
