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
  module Reconnection
    module Policies
      # A reconnection policy that returns a constant exponentially growing
      # reconnection interval up to a given maximum
      class Exponential < Policy
        # @private
        class Schedule
          def initialize(start, max, exponent)
            @interval = start
            @max      = max
            @exponent = exponent
          end

          def next
            @interval.tap { backoff if @interval < @max }
          end

          private

          def backoff
            new_interval = @interval * @exponent

            if new_interval >= @max
              @interval = @max
            else
              @interval = new_interval
            end
          end
        end

        # @param start    [Numeric] beginning interval
        # @param max      [Numeric] maximum reconnection interval
        # @param exponent [Numeric] (2) interval exponent to use
        #
        # @example Using this policy
        #   policy   = Cassandra::Reconnection::Policies::Exponential.new(0.5, 10, 2)
        #   schedule = policy.schedule
        #   schedule.next # 0.5
        #   schedule.next # 1.0
        #   schedule.next # 2.0
        #   schedule.next # 4.0
        #   schedule.next # 8.0
        #   schedule.next # 10.0
        #   schedule.next # 10.0
        #   schedule.next # 10.0
        def initialize(start, max, exponent = 2)
          @start    = start
          @max      = max
          @exponent = exponent
        end

        # @return [Cassandra::Reconnection::Schedule] an exponential
        #   reconnection schedule
        def schedule
          Schedule.new(@start, @max, @exponent)
        end
      end
    end
  end
end
