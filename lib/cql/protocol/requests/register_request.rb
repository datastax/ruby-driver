# encoding: utf-8

module Cql
  module Protocol
    class RegisterRequest < Request
      attr_reader :events

      def initialize(*events)
        super(11)
        @events = events
      end

      def write(protocol_version, buffer)
        buffer.append_string_list(@events)
      end

      def to_s
        %(REGISTER #@events)
      end
    end
  end
end
