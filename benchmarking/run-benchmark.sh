#!/bin/bash

current_dir=`dirname $0`
iterations=10
queries=$((1000 * 100))

for i in `seq $iterations`
do
    n=`printf '%02d' $i`
    ruby -rbundler/setup benchmark-ruby-driver.rb $queries > $current_dir/benchmark-ruby-driver-$n.log &
    ruby_pid=$!
    top -c d -stats command,pid,cpu,threads,csw -pid $ruby_pid -l 0 > $current_dir/top-ruby-driver-$n.log &
    top_pid=$!
    wait $ruby_pid
    kill $top_pid

    ruby -rbundler/setup benchmark-cql-rb.rb $queries > $current_dir/benchmark-cql-rb-$n.log &
    ruby_pid=$!
    top -c d -stats command,pid,cpu,threads,csw -pid $ruby_pid -l 0 > $current_dir/top-cql-rb-$n.log &
    top_pid=$!
    wait $ruby_pid
    kill $top_pid
done

