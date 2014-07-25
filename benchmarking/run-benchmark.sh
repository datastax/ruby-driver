#!/bin/bash

current_dir=`dirname $0`
iterations=10
queries=$((1000 * 100))

for i in `seq $iterations`
do
    ruby -rbundler/setup benchmark-ruby-driver.rb $queries > $current_dir/benchmark-ruby-driver-$i.log &
    ruby_pid=$!
    top -c d -stats command,pid,cpu,threads,csw -pid $ruby_pid -l 0 > $current_dir/top-ruby-driver-$i.log &
    top_pid=$!
    wait $ruby_pid
    kill $top_pid

    ruby -rbundler/setup benchmark-cql-rb.rb $queries > $current_dir/benchmark-cql-rb-$i.log &
    ruby_pid=$!
    top -c d -stats command,pid,cpu,threads,csw -pid $ruby_pid -l 0 > $current_dir/top-cql-rb-$i.log &
    top_pid=$!
    wait $ruby_pid
    kill $top_pid
done

