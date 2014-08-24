# encoding: utf-8

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

module Cassandra
  module Client
    # @private
    class PeerDiscovery
      def initialize(seed_connections)
        @seed_connections = seed_connections
        @connection = seed_connections.sample
        @request_runner = RequestRunner.new
      end

      def new_hosts
        request = Protocol::QueryRequest.new('SELECT peer, data_center, host_id, rpc_address FROM system.peers', nil, nil, :one)
        response = @request_runner.execute(@connection, request)
        response.map do |result|
          result.each_with_object([]) do |row, new_peers|
            if include?(row['host_id'], row['data_center'])
              rpc_address = row['rpc_address'].to_s
              rpc_address = row['peer'].to_s if rpc_address == '0.0.0.0'
              new_peers << rpc_address
            end
          end
        end
      end

      private

      def include?(host_id, dc)
        @seed_connections.any? { |c| c[:data_center] == dc } && @seed_connections.none? { |c| c[:host_id] == host_id }
      end
    end
  end
end
