# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

module Cassandra
  class Cluster
    # @private
    class ConnectionPool
      include Enumerable

      def initialize
        @connections = []
        @lock = ::Mutex.new
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
        raise Errors::IOError, 'Not connected' unless connected?
        @lock.synchronize do
          @connections.sample
        end
      end

      def each_connection(&callback)
        return self unless block_given?
        raise Errors::IOError, 'Not connected' unless connected?
        @lock.synchronize do
          @connections.each(&callback)
        end
      end
      alias_method :each, :each_connection
    end
  end
end
