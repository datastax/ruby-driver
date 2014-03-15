# encoding: utf-8

module Cql
  module Protocol
    class StatusChangeEventResponse < EventResponse
      TYPE = 'STATUS_CHANGE'.freeze

      attr_reader :type, :change, :address, :port

      def initialize(*args)
        @change, @address, @port = args
        @type = TYPE
      end

      def self.decode(protocol_version, buffer, length, trace_id=nil)
        new(buffer.read_string, *buffer.read_inet)
      end

      def to_s
        %(EVENT #@type #@change #@address:#@port)
      end

      private

      EVENT_TYPES[TYPE] = self
    end
  end
end
