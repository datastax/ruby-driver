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
    class ResultResponse < Response
      # @private
      RESPONSE_TYPES[0x08] = self
      # @private
      RESULT_TYPES = [
        # populated by subclasses
      ]

      attr_reader :custom_payload, :warnings, :trace_id

      def initialize(custom_payload, warnings, trace_id)
        @custom_payload = custom_payload
        @warnings = warnings
        @trace_id = trace_id
      end

      def void?
        false
      end
    end
  end
end
