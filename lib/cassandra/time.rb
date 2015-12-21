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
  # Represents a Cassandra time type.
  class Time
    # @private
    NANOSECONDS_IN_MILISECOND = 1_000_000
    # @private
    NANOSECONDS_IN_SECOND     = NANOSECONDS_IN_MILISECOND * 1000
    # @private
    NANOSECONDS_IN_MINUTE     = NANOSECONDS_IN_SECOND * 60
    # @private
    NANOSECONDS_IN_HOUR       = NANOSECONDS_IN_MINUTE * 60
    # @private
    NANOSECONDS_IN_DAY        = NANOSECONDS_IN_HOUR * 24

    include ::Comparable

    # @private
    def initialize(nanoseconds = 0)
      if nanoseconds < 0 && nanoseconds > NANOSECONDS_IN_DAY - 1
        raise ::ArgumentError, "value must be between 0 and " \
                               "#{NANOSECONDS_IN_DAY}, #{value.inspect} given"
      end

      @nanoseconds = nanoseconds
    end

    # @return [Integer] an integer between 0 and 24, the number of full hours
    #   since midnight that this time represents
    def hours
      @nanoseconds / NANOSECONDS_IN_HOUR
    end

    # @return [Integer] an integer between 0 and 60, the number of full minutes
    #   since the last full hour that this time represents
    def minutes
      (@nanoseconds - (hours * NANOSECONDS_IN_HOUR)) / NANOSECONDS_IN_MINUTE
    end

    # @return [Integer] an integer between 0 and 60, the number of full seconds
    #   since the last full minutes that this time represents
    def seconds
      (@nanoseconds - (hours * NANOSECONDS_IN_HOUR) - (minutes * NANOSECONDS_IN_MINUTE)) / NANOSECONDS_IN_SECOND
    end

    # @return [Integer] an integer between 0 and 60, the number of full
    #   miliseconds since the last full second that this time represents
    def miliseconds
      (@nanoseconds - (hours * NANOSECONDS_IN_HOUR) - (minutes * NANOSECONDS_IN_MINUTE) - (seconds * NANOSECONDS_IN_SECOND)) / NANOSECONDS_IN_MILISECOND
    end

    # @return [String] a "%H:%M%S.%3N" formatted time string
    def to_s
      '%.2d:%.2d:%.2d.%.3d' % [hours, minutes, seconds, miliseconds]
    end

    # @return [Integer] an integer between 0 and 86400000000000, the number of
    #   nanoseconds since midnight that this time represents
    def to_nanoseconds
      @nanoseconds
    end

    # @private
    def eql?(other)
      other.is_a?(Time) && other.to_nanoseconds == @nanoseconds
    end
    alias :== :eql?

    # @private
    def <=>(other)
      other <=> nanoseconds
    end

    # @private
    def hash
      # Modeled after http://developer.android.com/reference/java/lang/Object.html#writing_hashCode, but
      # simplified since only one field participates in the hash.
      @hash ||= 31 * 17 + @nanoseconds.hash
    end
  end
end
