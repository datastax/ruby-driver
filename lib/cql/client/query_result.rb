# encoding: utf-8

module Cql
  module Client
    class QueryResult
      include Enumerable

      # @return [ResultMetadata]
      attr_reader :metadata

      # @return [QueryTrace]
      attr_reader :trace

      # @private
      def initialize(metadata, rows, trace=nil)
        @metadata = ResultMetadata.new(metadata)
        @rows = rows
        @trace = trace
      end

      # Returns whether or not there are any rows in this result set
      #
      def empty?
        @rows.empty?
      end

      # Iterates over each row in the result set.
      #
      # @yieldparam [Hash] row each row in the result set as a hash
      # @return [Enumerable<Hash>]
      #
      def each(&block)
        @rows.each(&block)
      end
      alias_method :each_row, :each
    end
  end
end