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

      def self.decode!(buffer)
        new(read_string!(buffer), *read_inet!(buffer))
      end

      def to_s
        %(EVENT #@type #@change #@address:#@port)
      end
    end
  end
end
