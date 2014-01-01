# encoding: utf-8

module Cql
  module Protocol
    class QueryRequest < Request
      attr_reader :cql, :consistency

      def initialize(cql, values, consistency, trace=false)
        raise ArgumentError, %(No CQL given!) unless cql
        raise ArgumentError, %(No such consistency: #{consistency.inspect}) if consistency.nil? || !CONSISTENCIES.include?(consistency)
        super(7, trace)
        @cql = cql
        @values = values || NO_VALUES
        @encoded_values = encode_values
        @consistency = consistency
      end

      def write(protocol_version, io)
        write_long_string(io, @cql)
        write_consistency(io, @consistency)
        if protocol_version > 1
          if @values.any?
            io << VALUES_FLAG
            io << @encoded_values
          else
            io << NO_FLAGS
          end
        end
        io
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

      private

      def encode_values
        buffer = ''
        write_short(buffer, @values.size)
        @values.each do |value|
          type = guess_type(value)
          TYPE_CONVERTER.to_bytes(buffer, type, value)
        end
        buffer
      end

      def guess_type(value)
        TYPE_GUESSES[value.class] || :varchar
      end

      TYPE_GUESSES = {}.freeze
      TYPE_CONVERTER = TypeConverter.new
      NO_VALUES = [].freeze
      NO_FLAGS = "\x00".freeze
      VALUES_FLAG = "\x01".freeze
    end
  end
end
