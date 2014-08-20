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

module Cql
  module Client
    # @private
    class KeyspaceChanger
      KEYSPACE_NAME_PATTERN = /^\w[\w\d_]*$|^"\w[\w\d_]*"$/

      def initialize(request_runner=RequestRunner.new)
        @request_runner = request_runner
      end

      def use_keyspace(connection, keyspace)
        return Ione::Future.resolved(connection) unless keyspace
        return Ione::Future.failed(InvalidKeyspaceNameError.new(%("#{keyspace}" is not a valid keyspace name))) unless valid_keyspace_name?(keyspace)
        request = Protocol::QueryRequest.new("USE #{keyspace}", nil, nil, :one)
        @request_runner.execute(connection, request).map(connection)
      end

      private

      def valid_keyspace_name?(name)
        name =~ KEYSPACE_NAME_PATTERN
      end
    end
  end
end