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
    class PreparedResultResponse < ResultResponse
      # @private
      RESULT_TYPES[0x04] = self

      attr_reader :id, :metadata, :result_metadata, :pk_idx

      def initialize(custom_payload,
                     warnings,
                     id,
                     metadata,
                     result_metadata,
                     pk_idx,
                     trace_id)
        super(custom_payload, warnings, trace_id)
        @id              = id
        @metadata        = metadata
        @result_metadata = result_metadata
        @pk_idx          = pk_idx
      end

      def eql?(other)
        id == other.id && metadata == other.metadata && trace_id == other.trace_id
      end
      alias == eql?

      def hash
        @h ||= begin
          h = 17
          h = 31 * h + @id.hash
          h = 31 * h + @metadata.hash
          h = 31 * h + @trace_id.hash
          h
        end
      end

      def to_s
        hex_id = @id.each_byte.map { |x| x.to_s(16).rjust(2, '0') }.join('')
        %(RESULT PREPARED #{hex_id} #{@metadata})
      end
    end
  end
end
