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
  module Reconnection
    module Policies
      # A reconnection policy that returns a constant reconnection interval
      class Constant < Policy
        # @private
        class Schedule
          def initialize(interval)
            @interval = interval
          end

          def next
            @interval
          end
        end

        # @param interval [Numeric] reconnection interval (in seconds)
        def initialize(interval)
          @schedule = Schedule.new(Float(interval))
        end

        # @return [Cassandra::Reconnection::Schedule] reconnection schedule
        #   with constant interval
        attr_reader :schedule
      end
    end
  end
end
