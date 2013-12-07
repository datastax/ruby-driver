# encoding: utf-8

module Cql
  module Protocol
    class TopologyChangeEventResponse < StatusChangeEventResponse
      TYPE = 'TOPOLOGY_CHANGE'.freeze

      def initialize(*args)
        super
        @type = TYPE
      end

      private

      EVENT_TYPES[TYPE] = self
    end
  end
end
