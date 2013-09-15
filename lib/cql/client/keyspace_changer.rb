# encoding: utf-8

module Cql
  module Client
    # @private
    class KeyspaceChanger
      KEYSPACE_NAME_PATTERN = /^\w[\w\d_]*$|^"\w[\w\d_]*"$/
      
      def initialize
        @request_runner = RequestRunner.new
      end

      def valid_keyspace_name?(name)
        name =~ KEYSPACE_NAME_PATTERN
      end

      def use_keyspace(keyspace, connection)
        return Future.resolved(connection) unless keyspace
        return Future.failed(InvalidKeyspaceNameError.new(%("#{keyspace}" is not a valid keyspace name))) unless valid_keyspace_name?(keyspace)
        request = Protocol::QueryRequest.new("USE #{keyspace}", :one)
        @request_runner.execute(connection, request).map { connection }
      end
    end
  end
end