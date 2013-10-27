# encoding: utf-8

module Cql
  module Protocol
    class ReadyResponse < Response
      def self.decode!(buffer, trace_id=nil)
        new
      end

      def eql?(rs)
        self.class === rs
      end
      alias_method :==, :eql?

      def hash
        @h ||= to_s.hash ^ 0xbadc0de
      end

      def to_s
        'READY'
      end
    end
  end
end
