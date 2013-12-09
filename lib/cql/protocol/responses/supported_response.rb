# encoding: utf-8

module Cql
  module Protocol
    class SupportedResponse < Response
      attr_reader :options

      def initialize(options)
        @options = options
      end

      def self.decode!(buffer, trace_id=nil)
        new(read_string_multimap!(buffer))
      end

      def to_s
        %(SUPPORTED #{options})
      end
    end
  end
end
