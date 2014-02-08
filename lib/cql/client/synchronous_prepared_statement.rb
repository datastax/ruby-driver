# encoding: utf-8

module Cql
  module Client
    # @private
    class SynchronousPreparedStatement < PreparedStatement
      include SynchronousBacktrace

      def initialize(async_statement)
        @async_statement = async_statement
        @metadata = async_statement.metadata
        @result_metadata = async_statement.result_metadata
      end

      def execute(*args)
        synchronous_backtrace { @async_statement.execute(*args).value }
      end

      def batch(type=:logged, options={}, &block)
        if block_given?
          synchronous_backtrace { @async_statement.batch(type, options, &block).value }
        else
          SynchronousPreparedStatementBatch.new(@async_statement.batch(type, options))
        end
      end

      def pipeline
        pl = Pipeline.new(@async_statement)
        yield pl
        synchronous_backtrace { pl.value }
      end

      def async
        @async_statement
      end

      # @private
      def add_to_batch(batch, connection, bound_arguments)
        @async_statement.add_to_batch(batch, connection, bound_arguments)
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

      def value
        Future.all(*@futures).value
      end
    end
  end
end