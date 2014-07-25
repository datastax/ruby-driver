#!/usr/bin/ruby

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

