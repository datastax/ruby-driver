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
    module Timestamp
      # In JRuby, {::Time} has millisecond precision. We require client timestamps to have microsecond precision to
      # minimize clashes in C*. This generator keeps track of the last generated timestamp, and if the current-time
      # is within the same millisecond as the last, it fills the microsecond portion of the new timestamp with the
      # value of an incrementing counter.
      #
      # For example, if the generator triggers twice at time 12345678000 (microsecond granularity, but ms precisions
      # as shown by 0's for the three least-significant digits), it'll return 12345678000 and 12345678001.
      class JRubyTimeBasedGenerator
        include MonitorMixin

        # @private
        def initialize
          mon_initialize
          @last = 0
        end

        # Create a new timestamp, as a 64-bit integer.
        #
        # @return [Integer] an integer representing a timestamp.
        def next
          now = ::Time.now
          now_millis = now.tv_sec * 1000 + now.tv_usec / 1000
          last = @last
          last_millis = last / 1000
          if last_millis < now_millis
            # Since milliseconds changed, we don't need to do the special counter logic to avoid collisions.
            new_last = now_millis * 1000

            synchronize do
              if last == @last
                # last didn't change beneath us while we did the above calculations, so we can set it with our new value.
                @last = new_last
              else
                # Since last did change, it means our calculations above are now out-of-date and we need to
                # recalculate and update. Now that we're under the lock, no one else can pull the rug out from
                # under us and the calculation is stable.
                update_last(now_millis)
              end
              return @last
            end
          end

          # If we get here, it means we're in the same millisecond as 'last', so we need to grab the lock,
          # re-check that we're still in the same ms as 'last', and update last appropriately.
          synchronize do
            update_last(now_millis)
          end
        end

        private
        # @private
        def update_last(now_millis)
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
