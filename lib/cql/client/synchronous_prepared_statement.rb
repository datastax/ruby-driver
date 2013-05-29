# encoding: utf-8

module Cql
  module Client
    # @private
    class SynchronousPreparedStatement < PreparedStatement
      def initialize(async_statement)
        @async_statement = async_statement
        @metadata = async_statement.metadata
      end

      def execute(*args)
        @async_statement.execute(*args).get
      end

      def pipeline
        pl = Pipeline.new(@async_statement)
        yield pl
        pl.get
      end
    end

    # @private
    class Pipeline
      def initialize(async_statement)
        @async_statement = async_statement
        @futures = []
      end

      def execute(*args)
        @futures << @async_statement.execute(*args)
      end

      def get
        Future.combine(*@futures).get
      end
    end
  end
end