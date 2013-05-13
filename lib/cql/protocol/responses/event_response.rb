# encoding: utf-8

module Cql
  module Protocol
    class EventResponse < ResultResponse
      def self.decode!(buffer)
        type = read_string!(buffer)
        case type
        when SchemaChangeEventResponse::TYPE
          SchemaChangeEventResponse.decode!(buffer)
        when StatusChangeEventResponse::TYPE
          StatusChangeEventResponse.decode!(buffer)
        when TopologyChangeEventResponse::TYPE
          TopologyChangeEventResponse.decode!(buffer)
        else
          raise UnsupportedEventTypeError, %(Unsupported event type: "#{type}")
        end
      end
    end
  end
end
