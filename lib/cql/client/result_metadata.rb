# encoding: utf-8

module Cql
  module Client
    class ResultMetadata
      include Enumerable

      # @private
      def initialize(metadata)
        @metadata = metadata.each_with_object({}) { |m, h| h[m[2]] = ColumnMetadata.new(*m) }
      end

      # Returns the column metadata
      #
      # @return [ColumnMetadata] column_metadata the metadata for the column
      #
      def [](column_name)
        @metadata[column_name]
      end

      # Iterates over the metadata for each column
      #
      # @yieldparam [ColumnMetadata] metadata the metadata for each column
      # @return [Enumerable<ColumnMetadata>]
      #
      def each(&block)
        @metadata.each_value(&block)
      end
    end
  end
end