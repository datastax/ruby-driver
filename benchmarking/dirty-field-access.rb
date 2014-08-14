#!/usr/bin/ruby

# This file is NOT a benchmark per-se.  It is a test showing the possible
# synchronization issues that will happen in jruby but not in ruby (MRI) when a
# shared variable is not properly guarded by a mutex.
#
# When this file is executed with ruby, it terminates after 5~10 seconds.
#
# When this file is executed with jruby, it will never terminate because some
# updates will be lost on "stopped_threads" variable, because of conflicting
# writes.

require 'thread'

threads = Array.new
should_start = false
stopped_threads = 0

1000.times do
    threads.push(Thread.new {
        until should_start
            sleep(rand)
        end
        100.times do
            sleep(rand / 10)
        end
        stopped_threads += 1
    })
end

puts "Sleeping 1 second so that all threads are running..."
sleep 1
should_start = true

puts "# Waiting for #{threads.size} threads to finish..."
until stopped_threads == threads.size
    puts "# #{stopped_threads} have stopped..."
    sleep 1
end

