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
    end
  end
end