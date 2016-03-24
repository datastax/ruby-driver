# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
    class ReadyResponse < Response
      # @private
      RESPONSE_TYPES[0x02] = self

      def eql?(rs)
        rs.is_a?(self.class)
      end
      alias == eql?

      def hash
        @h ||= begin
          h = 17
          h = 31 * h + 'READY'.hash
          h
        end
      end

      def to_s
        'READY'
      end
    end
  end
end
