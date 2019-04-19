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
  # A variant of UUID which can extract its time component.
  #
  # You can use {Cassandra::Uuid::Generator} to generate {Cassandra::TimeUuid}s
  class TimeUuid < Uuid
    include Comparable

    # @private
    LOWER_HALF_MASK = 0xffffffff_ffffffff
    # @private
    GREGORIAN_OFFSET = 122192928000000000

    # Returns the time component from this UUID as a Time.
    #
    # @return [Time]
    def to_time
      t = time_bits - GREGORIAN_OFFSET
      seconds = t / 10_000_000
      microseconds = (t - seconds * 10_000_000) / 10.0

      ::Time.at(seconds, microseconds).utc
    end

    # Returns the date component from this UUID as Date.
    #
    # This just sugar around {#to_time}
    #
    # @return [Date]
    def to_date
      to_time.to_date
    end

    # Compares this timeuuid with another timeuuid
    #
    # @param other [Cassandra::TimeUuid] another timeuuid to compare
    # @see Comparable
    #
    # @return [nil] when other is not a {Cassandra::Uuid}
    # @return [Integer] `-1` when less than `other`, `0` when equal to `other`
    #   and `1` when greater than `other`
    def <=>(other)
      return nil unless other.is_a?(Cassandra::Uuid)
      c = value <=> other.value
      return c if c == 0 || !other.is_a?(Cassandra::TimeUuid)
      time_bits <=> other.time_bits
    end

    protected

    # @private
    def time_bits
      n = (value >> 64)
      t = 0
      t |= (n & 0x0000000000000fff) << 48
      t |= (n & 0x00000000ffff0000) << 16
      t |= (n & 0xffffffff00000000) >> 32
      t
    end
  end
end
