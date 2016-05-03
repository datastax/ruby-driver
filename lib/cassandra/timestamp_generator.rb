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
  # A generator is used to create client-timestamps (in the form of long integers) to send with C* requests when
  # the `:client_timestamps` cluster option is set to true.
  #
  # @abstract A timestamp generator given to {Cassandra.cluster} doesn't need to include this module, but needs to
  #   implement the same methods. This module exists only for documentation purposes.
  module TimestampGenerator
    # Create a new timestamp, as a 64-bit integer. Calls must return monotonically increasing values.
    #
    # @return [Integer] an integer representing a timestamp in microseconds.
    # @raise [NotImplementedError] if a class including this module does not define this method.
    def next
      raise NotImplementedError, "#{self.class} class must implement the 'next' method"
    end
  end
end

require 'cassandra/timestamp_generator/ticking_on_duplicate'
require 'cassandra/timestamp_generator/simple'
