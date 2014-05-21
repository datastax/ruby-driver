# encoding: utf-8

module Cql
  # A variant of UUID which can extract its time component.
  #
  class TimeUuid < Uuid
    include Comparable

    # Returns the time component from this UUID as a Time.
    #
    # @return [Time]
    #
    def to_time
      t = time_bits - GREGORIAN_OFFSET
      seconds = t/10_000_000
      microseconds = (t - seconds * 10_000_000)/10.0
      Time.at(seconds, microseconds).utc
    end

    def <=>(other)
      c = self.time_bits <=> other.time_bits
      return c unless c == 0
      (self.value & LOWER_HALF_MASK) <=> (other.value & LOWER_HALF_MASK)
    end

    protected

    def time_bits
      n = (value >> 64)
      t = 0
      t |= (n & 0x0000000000000fff) << 48
      t |= (n & 0x00000000ffff0000) << 16
      t |= (n & 0xffffffff00000000) >> 32
      t
    end

    private

    LOWER_HALF_MASK = 0xffffffff_ffffffff

    public

    # A UUID version 1, variant 1 generator. It can generate a sequence of UUIDs
    # with reasonable uniqueness guarantees:
    #
    # * The clock ID and node ID components are set to random numbers when the
    #   generator is created.
    # * If two calls to {#next} happen within the time afforded by the system
    #   clock resolution a counter is incremented and added to the time
    #   component.
    # * If the clock moves backwards the clock ID is reset to a new random
    #   number.
    #
    # Instances of this class are absolutely not threadsafe. You should
    # never share instances between threads.
    #
    class Generator
      # Create a new UUID generator.
      #
      # @param [Integer] node_id an alternate node ID (defaults to a random number)
      # @param [Integer] clock_id an alternate clock ID (defaults to a random number)
      #
      def initialize(node_id=nil, clock_id=nil, clock=Time)
        @node_id = node_id || (rand(2**47) | 0x010000000000)
        @clock_id = clock_id || rand(2**16)
        @clock = clock
      end

      # Returns a new UUID with a time component that is the current time.
      #
      # @return [Cql::TimeUuid] a new UUID
      #
      def next
        now = @clock.now
        usecs = now.to_i * 1_000_000 + now.usec
        if @last_usecs && @last_usecs - @sequence <= usecs && usecs <= @last_usecs
          @sequence += 1
        elsif @last_usecs && @last_usecs > usecs
          @sequence = 0
          @clock_id = rand(2**16)
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
      # @param [Time] time the time to encode into the UUID
      # @param [Integer] jitter a number of microseconds to add to the time to make it unique
      # @return [Cql::TimeUuid] a new UUID
      #
      def from_time(time, jitter=rand(2**16))
        usecs = time.to_i * 1_000_000 + time.usec + jitter
        from_usecs(usecs)
      end

      # @private
      def from_usecs(usecs)
        t = GREGORIAN_OFFSET + usecs * 10
        time_hi  = t & 0x0fff000000000000
        time_mid = t & 0x0000ffff00000000
        time_low = t & 0x00000000ffffffff
        version = 1
        clock_id = @clock_id & 0x3fff
        node_id = @node_id & 0xffffffffffff
        variant = 0x8000
        n = (time_low << 96) | (time_mid << 48) | (time_hi << 16)
        n |= version << 76
        n |= (clock_id | variant) << 48
        n |= node_id
        TimeUuid.new(n)
      end
    end

    private

    GREGORIAN_OFFSET = 122192928000000000
  end
end