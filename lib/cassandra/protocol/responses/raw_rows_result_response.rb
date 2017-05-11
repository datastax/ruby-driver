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
    class RawRowsResultResponse < RowsResultResponse
      def initialize(custom_payload,
                     warnings,
                     protocol_version,
                     raw_rows,
                     paging_state,
                     trace_id,
                     custom_type_handlers = nil)
        super(custom_payload, warnings, nil, nil, paging_state, trace_id)
        @protocol_version = protocol_version
        @raw_rows = raw_rows
        @custom_type_handlers = custom_type_handlers
      end

      def materialize(metadata)
        @metadata = metadata

        @rows = if @protocol_version == 4
                  Coder.read_values_v4(@raw_rows, @metadata, @custom_type_handlers)
                elsif @protocol_version == 3
                  Coder.read_values_v3(@raw_rows, @metadata)
                else
                  Coder.read_values_v1(@raw_rows, @metadata)
                end

        @rows
      end

      def rows
        raise Errors::DecodingError, 'Not materialized!' unless @rows
        @rows
      end

      def to_s
        %(RESULT ROWS (raw))
      end
    end
  end
end
