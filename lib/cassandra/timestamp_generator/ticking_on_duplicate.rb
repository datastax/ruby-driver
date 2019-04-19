# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
  module TimestampGenerator
    # In JRuby, {::Time} has millisecond precision. We require client timestamps to have microsecond precision to
    # minimize clashes in C*. This generator keeps track of the last generated timestamp, and if the current-time
    # is within the same millisecond as the last, it fills the microsecond portion of the new timestamp with the
    # value of an incrementing counter.
    #
    # For example, if the generator triggers twice at time 12345678000 (microsecond granularity, but ms precisions
    # as shown by 0's for the three least-significant digits), it'll return 12345678000 and 12345678001.
    class TickingOnDuplicate
      include MonitorMixin
      include TimestampGenerator

      # @private
      def initialize
        mon_initialize
        @last = 0
      end

      # Create a new timestamp, as a 64-bit integer.
      #
      # @return [Integer] an integer representing a timestamp in microseconds.
      def next
        now = ::Time.now
        now_millis = now.tv_sec * 1000 + now.tv_usec / 1000
        synchronize do
          millis = @last / 1000
          counter = @last % 1000
          if millis >= now_millis
            counter += 1
          else
            millis = now_millis
            counter = 0
          end
          @last = millis * 1000 + counter
        end
      end
    end
  end
end
