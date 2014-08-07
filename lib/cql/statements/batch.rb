# encoding: utf-8

module Cql
  module Statements
    # Batch statement groups several {Cql::Statement}. There are several types of Batch statements available:
    # @see Cql::Session#batch
    # @see Cql::Session#logged_batch
    # @see Cql::Session#unlogged_batch
    # @see Cql::Session#counter_batch
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

      include Statement

      attr_reader :statements

      # @private
      def initialize
        @statements = []
      end

      # @overload add(statement)
      #   @param statement [Cql::Statements::Simple, Cql::Statements::Bound] a statements to add
      #
      # @overload add(statement, *args)
      #   @param statement [String, Cql::Statements::Prepared] a statement to construct with provided arguments
      #   @param args [*Object] arguments to the statement
      def add(statement, *args)
        case statement
        when String
          @statements << Simple.new(statement, *args)
        when Prepared
          @statements << statement.bind(*args)
        when Bound, Simple
          @statements << statement
        else
          raise ::ArgumentError, "a batch can only consist of simple or prepared statements, #{statement.inspect} given"
        end

        self
      end

      # A batch statement doesn't really have any cql of its own as it is composed of multiple different statements
      # @return [nil] nothing
      def cql
        nil
      end

      # @abstract must be implemented by children
      def type
      end
    end
  end
end
