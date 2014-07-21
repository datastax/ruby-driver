# encoding: utf-8

module Cql
  module Statements
    module Batch
      class Logged
        include Batch

        def type
          :logged
        end
      end

      class Unlogged
        include Batch

        def type
          :unlogged
        end
      end

      class Counter
        include Batch

        def type
          :counter
        end
      end

      attr_reader :statements

      def initialize
        @statements = []
      end

      def add(statement, *args)
        case statement
        when String
          @statements << Simple.new(statement, args)
        when Prepared
          @statements << statement.bind(*args)
        when Bound, Simple
          @statements << statement
        else
          raise ::ArgumentError, "a batch can only consist of simple or prepared statements, #{statement.inspect} given"
        end

        self
      end

      def type
        raise ::NotImplemented, "must be implemented by a child"
      end
    end
  end
end
