# encoding: utf-8

module Cql
  module Client
    # @private
    class SynchronousClient < Client
      def initialize(async_client)
        @async_client = async_client
      end

      def connect
        @async_client.connect.get
        self
      end

      def close
        @async_client.close.get
        self
      end

      def connected?
        @async_client.connected?
      end

      def keyspace
        @async_client.keyspace
      end

      def use(keyspace)
        @async_client.use(keyspace).get
      end

      def execute(cql, consistency=nil)
        @async_client.execute(cql, consistency).get
      end

      def prepare(cql)
        async_statement = @async_client.prepare(cql).get
        SynchronousPreparedStatement.new(async_statement)
      end
    end
  end
end
