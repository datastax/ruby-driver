# encoding: utf-8

module Cql
  module Protocol
    class QueryRequest < Request
      attr_reader :cql, :consistency

      def initialize(cql, consistency, trace=false)
        raise ArgumentError, %(No CQL given!) unless cql
        raise ArgumentError, %(No such consistency: #{consistency.inspect}) if consistency.nil? || !CONSISTENCIES.include?(consistency)
        super(7, trace)
        @cql = cql
        @consistency = consistency
      end

      def write(io)
        write_long_string(io, @cql)
        write_consistency(io, @consistency)
      end

      def to_s
        %(QUERY "#@cql" #{@consistency.to_s.upcase})
      end

      def eql?(rq)
        self.class === rq && rq.cql.eql?(self.cql) && rq.consistency.eql?(self.consistency)
      end
      alias_method :==, :eql?

      def hash
        @h ||= (@cql.hash * 31) ^ consistency.hash
      end
    end
  end
end
