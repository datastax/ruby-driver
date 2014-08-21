# encoding: utf-8

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

module Cql
  module Reconnection
    # @!parse [ruby]
    #   class Schedule
    #     # @return [Numeric] the next reconnection interval in seconds
    #     def next
    #     end
    #   end

    module Policy
      # Returns a reconnection schedule
      #
      # @abstract implementation should be provided by an actual policy
      # @note reconnection schedule doesn't need to extend
      #   {Cql::Reconnection::Schedule}, only conform to its interface
      # @return [Cql::Reconnection::Schedule] reconnection schedule
      def schedule
      end
    end
  end
end
