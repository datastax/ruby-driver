# encoding: utf-8

module Cql
  module Client
    # @private
    class SynchronousPreparedStatement < PreparedStatement
      include SynchronousBacktrace

      def initialize(async_statement)
        @async_statement = async_statement
        @metadata = async_statement.metadata
      end

      def execute(*args)
        synchronous_backtrace { @async_statement.execute(*args).get }
      end

      def pipeline
        pl = Pipeline.new(@async_statement)
        yield pl
        synchronous_backtrace { pl.get }
      end

      def async
        @async_statement
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
        if @futures.any?
          Future.combine(*@futures).get
        else
          []
        end
      end
    end
  end
end