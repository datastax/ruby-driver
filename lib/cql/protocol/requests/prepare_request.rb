# encoding: utf-8

module Cql
  module Protocol
    class PrepareRequest < Request
      attr_reader :cql

      def initialize(cql, trace=false)
        raise ArgumentError, 'No CQL given!' unless cql
        super(9, trace)
        @cql = cql
      end

      def write(protocol_version, buffer)
        buffer.append_long_string(@cql)
      end

      def to_s
        %(PREPARE "#@cql")
      end

      def eql?(rq)
        self.class === rq && rq.cql == self.cql
      end
      alias_method :==, :eql?

      def hash
        @h ||= @cql.hash
      end
    end
  end
end
