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
      # Generate long integer timestamps from current time. This implementation relies on the {::Time} class to return
      # microsecond precision time.
      # @note It is not appropriate for use with JRuby because its {::Time#now} returns millisecond precision time.
      class TimeBasedGenerator
        # Create a new timestamp, as a 64-bit integer. This is just a wrapper around Time::now.
        #
        # @return [Integer] an integer representing a timestamp.
        def next
          # Use Time.now, which has microsecond precision on MRI (and probably Rubinius) to make an int representing
          # client timestamp in protocol requests.
          timestamp = ::Time.now
          timestamp.tv_sec * 1000000 + timestamp.tv_usec
        end
      end
    end
  end
end
