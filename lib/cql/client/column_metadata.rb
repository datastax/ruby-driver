# encoding: utf-8

module Cql
  module Client
    # Represents metadata about a column in a query result set or prepared
    # statement. Apart from the keyspace, table and column names there's also
    # the type as a symbol (e.g. `:varchar`, `:int`, `:date`).
    class ColumnMetadata
      attr_reader :keyspace, :table, :column_name, :type
      
      # @private
      def initialize(*args)
        @keyspace, @table, @column_name, @type = args
      end

      # @private
      def to_ary
        [@keyspace, @table, @column_name, @type]
      end
    end
  end
end