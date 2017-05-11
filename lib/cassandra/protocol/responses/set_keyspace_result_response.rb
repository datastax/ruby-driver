# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
  module Protocol
    class SetKeyspaceResultResponse < ResultResponse
      # @private
      RESULT_TYPES[0x03] = self

      attr_reader :keyspace

      def initialize(custom_payload, warnings, keyspace, trace_id)
        super(custom_payload, warnings, trace_id)
        @keyspace = keyspace
      end

      def to_s
        %(RESULT SET_KEYSPACE "#{@keyspace}")
      end
    end
  end
end
