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
  module Protocol
    class RawRowsResultResponse < RowsResultResponse
      def initialize(protocol_version, raw_rows, paging_state, trace_id)
        super(nil, nil, paging_state, trace_id)
        @protocol_version = protocol_version
        @raw_rows = raw_rows
      end

      def materialize(metadata)
        @metadata = metadata

        if @protocol_version == 3
          @rows = Coder.read_values_v3(@raw_rows, @metadata)
        else
          @rows = Coder.read_values_v1(@raw_rows, @metadata)
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
