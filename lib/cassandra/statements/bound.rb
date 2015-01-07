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
  module Statements
    # a Bound statement is created using {Cassandra::Statements::Prepared#bind}
    class Bound
      include Statement

      # @return [String] original cql used to prepare this statement
      attr_reader :cql
      # @return [Array<Object>] a list of positional parameters for the cql
      attr_reader :params
      # @private
      attr_reader :params_types, :result_metadata, :keyspace, :partition_key

      # @private
      def initialize(cql, params_types, result_metadata, params, keyspace = nil, partition_key = nil)
        @cql             = cql
        @params_types    = params_types
        @result_metadata = result_metadata
        @params          = params
        @keyspace        = keyspace
        @partition_key   = partition_key
      end

      # @return [String] a CLI-friendly bound statement representation
      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect} @params=#{@params}>"
      end
    end
  end
end
