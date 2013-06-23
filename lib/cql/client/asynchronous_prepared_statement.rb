# encoding: utf-8

module Cql
  module Client
    # @private
    class AsynchronousPreparedStatement < PreparedStatement
      def initialize(connection, statement_id, raw_metadata)
        @connection = connection
        @statement_id = statement_id
        @raw_metadata = raw_metadata
        @metadata = ResultMetadata.new(@raw_metadata)
        @request_runner = RequestRunner.new
      end

      def execute(*args)
        bound_args = args.shift(@raw_metadata.size)
        consistency_level = args.shift || :quorum
        request = Cql::Protocol::ExecuteRequest.new(@statement_id, @raw_metadata, bound_args, consistency_level)
        @request_runner.execute(@connection, request)
      rescue => e
        Future.failed(e)
      end
    end
  end
end