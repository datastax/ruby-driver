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

      def eql?(other)
        self.keyspace == other.keyspace && self.table == other.table && self.column_name == other.column_name && self.type == other.type
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 0
          h = ((h & 33554431) * 31) ^ @keyspace.hash
          h = ((h & 33554431) * 31) ^ @table.hash
          h = ((h & 33554431) * 31) ^ @column_name.hash
          h = ((h & 33554431) * 31) ^ @type.hash
          h
        end
      end
    end
  end
end