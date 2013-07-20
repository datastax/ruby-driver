# encoding: utf-8

module Cql
  module Client
    module SynchronousBacktrace
      def synchronous_backtrace
        yield
      rescue CqlError => e
        new_backtrace = caller
        if new_backtrace.first.include?(SYNCHRONOUS_BACKTRACE_METHOD_NAME)
          new_backtrace = new_backtrace.drop(1)
        end
        e.set_backtrace(new_backtrace)
        raise
      end

      private

      SYNCHRONOUS_BACKTRACE_METHOD_NAME = 'synchronous_backtrace'
    end

    # @private
    class SynchronousClient < Client
      include SynchronousBacktrace

      def initialize(async_client)
        @async_client = async_client
      end

      def connect
        synchronous_backtrace { @async_client.connect.get }
        self
      end

      def close
        synchronous_backtrace { @async_client.close.get }
        self
      end

      def connected?
        @async_client.connected?
      end

      def keyspace
        @async_client.keyspace
      end

      def use(keyspace)
        synchronous_backtrace { @async_client.use(keyspace).get }
      end

      def execute(cql, consistency=nil)
        synchronous_backtrace { @async_client.execute(cql, consistency).get }
      end

      def prepare(cql)
        async_statement = synchronous_backtrace { @async_client.prepare(cql).get }
        SynchronousPreparedStatement.new(async_statement)
      end

      def async
        @async_client
      end
    end
  end
end
