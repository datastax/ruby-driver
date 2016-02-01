# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
  module Execution
    class Info
      # @return [Hash<String, String>] a map of string keys and byte buffer
      #   values, containing custom payloads sent by custom query handlers
      attr_reader :payload
      # @return [Array<String>] a list of string warnings from the server
      attr_reader :warnings
      # @return [String] keyspace used for the query
      attr_reader :keyspace
      # @return [Cassandra::Statement] original statement
      attr_reader :statement
      # @return [Cassandra::Execution::Options] original execution options
      attr_reader :options
      # @return [Array<Cassandra::Host>] a list of attempted hosts
      attr_reader :hosts
      # Actual consistency used, it can differ from consistency in
      #   {Cassandra::Execution::Info#options} if a retry policy modified it.
      # @return [Symbol] one of {Cassandra::CONSISTENCIES}
      attr_reader :consistency
      # @return [Integer] number of retries
      attr_reader :retries
      # Returns {Cassandra::Execution::Trace} if `trace: true` was passed to
      #   {Cassandra::Session#execute} or {Cassandra::Session#execute_async}
      # @return [Cassandra::Execution::Trace, nil] a Trace if it has been enabled for
      #   request
      attr_reader :trace

      # @private
      def initialize(payload,
                     warnings,
                     keyspace,
                     statement,
                     options,
                     hosts,
                     consistency,
                     retries,
                     trace)
        @payload     = payload
        @warnings    = warnings
        @keyspace    = keyspace
        @statement   = statement
        @options     = options
        @hosts       = hosts
        @consistency = consistency
        @retries     = retries
        @trace       = trace
      end
    end
  end
end
