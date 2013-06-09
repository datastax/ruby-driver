# encoding: utf-8

module Cql
  class TimeUuid < Uuid
    def to_time
      n = (value >> 64)
      t = 0
      t |= (n & 0x0000000000000fff) << 48
      t |= (n & 0x00000000ffff0000) << 16
      t |= (n & 0xffffffff00000000) >> 32
      t -= GREGORIAN_OFFSET
      seconds = t/10_000_000
      microseconds = (t - seconds * 10_000_000)/10.0
      Time.at(seconds, microseconds).utc
    end

    class Generator
      def initialize(node_id=nil, clock_id=nil, clock=Time)
        @node_id = node_id || (rand(2**47) | 0x010000000000)
        @clock_id = clock_id || rand(2**16)
        @clock = clock
      end

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

      def from_time(time, jitter=rand(2**16))
        usecs = time.to_i * 1_000_000 + time.usec + jitter
        from_usecs(usecs)
      end

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