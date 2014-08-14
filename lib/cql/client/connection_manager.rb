# encoding: utf-8

module Cql
  module Client
    # @private
    class ConnectionManager
      include Enumerable

      def initialize
        @connections = []
        @lock = Mutex.new
      end

      def add_connections(connections)
        @lock.synchronize do
          @connections.concat(connections)
          connections.each do |connection|
            connection.on_closed do
              @lock.synchronize do
                @connections.delete(connection)
              end
            end
          end
        end
      end

      def connected?
        @lock.synchronize do
          @connections.any?
        end
      end

      def snapshot
        @lock.synchronize do
          @connections.dup
        end
      end

      def random_connection
        raise Errors::NotConnectedError unless connected?
        @lock.synchronize do
          @connections.sample
        end
      end

      def each_connection(&callback)
        return self unless block_given?
        raise Errors::NotConnectedError unless connected?
        @lock.synchronize do
          @connections.each(&callback)
        end
      end
      alias_method :each, :each_connection
    end
  end
end
