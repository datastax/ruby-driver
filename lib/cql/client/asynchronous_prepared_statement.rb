# encoding: utf-8

module Cql
  module Client
    # @private
    class AsynchronousPreparedStatement < PreparedStatement
      def initialize(*args)
        @client, @connection_id, @statement_id, @raw_metadata = args
        @metadata = ResultMetadata.new(@raw_metadata)
      end

      def execute(*args)
        bound_args = args.shift(@raw_metadata.size)
        consistency_level = args.shift
        @client.execute_statement(@connection_id, @statement_id, @raw_metadata, bound_args, consistency_level)
      end
    end
  end
end