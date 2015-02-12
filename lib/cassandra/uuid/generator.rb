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
  class Uuid
    # A UUID generator.
    #
    # This class can be used to genereate Apache Cassandra timeuuid and uuid
    # values.
    #
    # @see Cassandra::Uuid::Generator#now Generating a sequence time UUIDs
    #   with reasonable uniqueness guarantees.
    # @see Cassandra::Uuid::Generator#at Generating a time UUID for a given
    #   time object or unix timestamp.
    # @see Cassandra::Uuid::Generator#uuid Generating completely random v4
    #   UUIDs.
    #
    # @note Instances of this class are absolutely not threadsafe. You should
    #   never share instances between threads.
    #
    class Generator
      # Create a new UUID generator.
      #
      # The clock ID and node ID components are set to random numbers when the
      # generator is created. These are used for generation of time UUIDs only.
      #
      # @param [Integer] node_id an alternate node ID
      # @param [Integer] clock_id an alternate clock ID
      # @param [Object<#now>] clock used to generate timeuuid from current time
      #
      # @raise [ArgumentError] if clock doesn't respond to `now`
      def initialize(node_id = (SecureRandom.random_number(2**47) | 0x010000000000), clock_id = SecureRandom.random_number(65536), clock = Time)
        raise ::ArgumentError, "invalid clock" unless clock.respond_to?(:now)

        @node_id  = Integer(node_id)
        @clock_id = Integer(clock_id)
        @clock    = clock
      end

      # Returns a new UUID with a time component that is the current time.
      #
      # If two calls to {#now} happen within the time afforded by the system
      # clock resolution a counter is incremented and added to the time
      # component.
      #
      # If the clock moves backwards the clock ID is reset to a new random
      # number.
      #
      # @example Creating a sequential TimeUuids for the current time
      #   generator = Cassandra::Uuid::Generator.new
      #   timeuuids = 5.times.map { generator.now }
      #
      #   puts timeuuids.zip(timeuuids.map(&:to_time)).map(&:inspect)
      #
      #   # Outputs:
      #   # [8614b7d0-5646-11e4-8e54-6761d3995ef3, 2014-10-17 21:42:42 UTC]
      #   # [8614b91a-5646-11e4-8e54-6761d3995ef3, 2014-10-17 21:42:42 UTC]
      #   # [8614b960-5646-11e4-8e54-6761d3995ef3, 2014-10-17 21:42:42 UTC]
      #   # [8614b99c-5646-11e4-8e54-6761d3995ef3, 2014-10-17 21:42:42 UTC]
      #   # [8614b9ce-5646-11e4-8e54-6761d3995ef3, 2014-10-17 21:42:42 UTC]
      #
      # @see Time.now
      #
      # @return [Cassandra::TimeUuid] a new UUID
      def now
        now   = @clock.now
        usecs = now.to_i * 1_000_000 + now.usec
        if @last_usecs && @last_usecs - @sequence <= usecs && usecs <= @last_usecs
          @sequence += 1
        elsif @last_usecs && @last_usecs > usecs
          @sequence = 0
          @clock_id = SecureRandom.random_number(65536)
        else
          @sequence = 0
        end
        @last_usecs = usecs + @sequence
        from_usecs(@last_usecs)
      end

      # Returns a new UUID with a time component based on the specified Time.
      # A piece of jitter is added to ensure that multiple calls with the same
      # time do not generate the same UUID (if you want determinism you can set
      # the second parameter to zero).
      #
      # @overload at(time, jitter = SecureRandom.random_number(65536))
      #   @param [Time] time a Time instance
      #   @param [Integer] jitter a number of microseconds to add to the time
      #   @return [Cassandra::TimeUuid] a new UUID
      # @overload at(seconds_with_frac, jitter = SecureRandom.random_number(65536))
      #   @param [Numeric] seconds_with_frac can be {Integer}, {Float},
      #     {Rational}, or other {Numeric}
      #   @param [Integer] jitter a number of microseconds to add to the time
      #   @return [Cassandra::TimeUuid] a new UUID
      # @overload at(seconds, microseconds_with_frac, jitter = SecureRandom.random_number(65536))
      #   @param [Integer] seconds
      #   @param [Numeric] microseconds_with_frac can be {Integer}, {Float},
      #     {Rational}, or other {Numeric}
      #   @param [Integer] jitter a number of microseconds to add to the time
      #   @return [Cassandra::TimeUuid] a new UUID
      #
      # @note the `jitter` argument accepted by all variants of this method is
      #   required to add randomness to generated {Cassandra::TimeUuid} and
      #   might affect the order of generated timestamps. You should set
      #   `jitter` to 0 when the source time(stamp)s are unique.
      #
      # @example Creating a TimeUuid from a Time instance
      #   generator = Cassandra::Uuid::Generator.new
      #   timeuuid  = generator.at(Time.at(1413582460))
      #
      #   puts timeuuid.to_time
      #
      #   # Outputs:
      #   # 2014-10-17 21:47:40 UTC
      #
      # @example Creating a TimeUuid from a timestamp
      #   generator = Cassandra::Uuid::Generator.new
      #   timeuuid  = generator.at(1413582423)
      #
      #   puts timeuuid.to_time
      #
      #   # Outputs:
      #   # 2014-10-17 21:47:03 UTC
      #
      # @example Avoid jitter in generated TimeUuid
      #   timestamp = 1413582418
      #   generator = Cassandra::Uuid::Generator.new
      #   timeuuid  = generator.at(timestamp, 0)
      #
      #   puts timeuuid.to_time.to_i
      #
      #   # Outputs:
      #   # 1413582418
      #
      # @raise [ArgumentError] when given no arguments or more than 3 arguments
      #
      # @see Time.at
      def at(*args)
        raise ::ArgumentError, "not enough arguments" if args.empty?
        raise ::ArgumentError, "too many arguments"   if args.size > 3

        if args.first.is_a?(Time)
          time   = args.shift
          jitter = args.empty? ? SecureRandom.random_number(65536) : Integer(args.shift)
        else
          jitter = args.size > 2 ? Integer(args.pop) : SecureRandom.random_number(65536)
          time   = Time.at(*args)
        end

        from_usecs(time.to_i * 1_000_000 + time.usec + jitter)
      end

      # Returns a completely random version 4 UUID.
      #
      # @example Generating a random Uuid
      #   generator = Cassandra::Uuid::Generator.new
      #   uuid      = generator.uuid
      #
      #   puts uuid
      #
      #   # Outputs:
      #   # 664dedae-e162-4bc0-9066-b9f1968252aa
      #
      # @see SecureRandom.uuid
      #
      # @return [Cassandra::Uuid] a new UUID
      def uuid
        Uuid.new(SecureRandom.uuid)
      end

      private

      # @private
      def from_usecs(usecs)
        t = TimeUuid::GREGORIAN_OFFSET + usecs * 10
        time_hi  = t & 0x0fff000000000000
        time_mid = t & 0x0000ffff00000000
        time_low = t & 0x00000000ffffffff
        version  = 1
        clock_id = @clock_id & 0x3fff
        node_id  = @node_id & 0xffffffffffff
        variant  = 0x8000

        n  = (time_low << 96) | (time_mid << 48) | (time_hi << 16)
        n |= version << 76
        n |= (clock_id | variant) << 48
        n |= node_id

        TimeUuid.new(n)
      end
    end
  end
end
