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
    class PreparedResultResponse < ResultResponse
      attr_reader :id, :metadata, :result_metadata

      def initialize(id, metadata, result_metadata, trace_id)
        super(trace_id)
        @id, @metadata, @result_metadata = id, metadata, result_metadata
      end

      def eql?(other)
        self.id == other.id && self.metadata == other.metadata && self.trace_id == other.trace_id
      end
      alias_method :==, :eql?

      def hash
        @h ||= begin
          h = 0
          h = ((h & 0x01ffffff) * 31) ^ @id.hash
          h = ((h & 0x01ffffff) * 31) ^ @metadata.hash
          h = ((h & 0x01ffffff) * 31) ^ @trace_id.hash
          h
        end
      end

      def to_s
        hex_id = @id.each_byte.map { |x| x.to_s(16).rjust(2, '0') }.join('')
        %(RESULT PREPARED #{hex_id} #@metadata)
      end

      private

      RESULT_TYPES[0x04] = self
    end
  end
end
