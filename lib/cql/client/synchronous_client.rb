# encoding: utf-8

module Cql
  module Client
    # @private
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
        synchronous_backtrace { @async_client.connect.value }
        self
      end

      def close
        synchronous_backtrace { @async_client.close.value }
        self
      end

      def connected?
        @async_client.connected?
      end

      def keyspace
        @async_client.keyspace
      end

      def use(keyspace)
        synchronous_backtrace { @async_client.use(keyspace).value }
      end

      def execute(cql, *values)
        synchronous_backtrace { @async_client.execute(cql, *values).value }
      end

      def prepare(cql)
        async_statement = synchronous_backtrace { @async_client.prepare(cql).value }
        SynchronousPreparedStatement.new(async_statement)
      end

      def async
        @async_client
      end
    end
  end
end
