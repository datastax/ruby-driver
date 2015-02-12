# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
    # Reconnection schedule
    # @abstract Actual reconnection schedules returned from
    #   {Cassandra::Reconnection::Policy} implementation don't need to inherit
    #   this class. This class exists for documentation purposes only.
    class Schedule
      # @return [Numeric] the next reconnection interval in seconds
      def next
      end
    end

    # A reconnection policy
    # @abstract Actual reconnection policies supplied as `:reconnection_policy`
    #   option to {Cassandra.cluster} don't need to inherit this class, only
    #   implement its methods. This class exists for documentation purposes
    #   only.
    class Policy
      # Returns a reconnection schedule
      #
      # @note Reconnection schedule returned from this method doesn't need to
      #   extend {Cassandra::Reconnection::Schedule}, only conform to its
      #   interface.
      # @return [Cassandra::Reconnection::Schedule] reconnection schedule
      def schedule
      end
    end
  end
end

require 'cassandra/reconnection/policies'
