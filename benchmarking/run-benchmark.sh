#!/bin/bash

# Script defaults : 10 measurements of 10k unprepared selects
iterations=10
queries=$((1000 * 10))
preparation="unprepared"
operation="select"
help=false

while getopts "hupsiln:" opt; do
    case $opt in
        u) preparation="unprepared" ;;
        p) preparation="prepared" ;;
        s) operation="select" ;;
        i) operation="insert" ;;
        l) queries=$((1000 * 100)) ;;
        n) queries=$OPTARG ;;
        :) echo "Option -$OPTARG requires an argument." >&2 ; help=true ;;
        *) help=true ;; # Wrong argument == print help
    esac
done

# Print help if asked
if [ $help = true ] ; then
    echo "Usage: $0 [option...]
    Options:
    -h      Print this help
    -n <n>  Execute <n> queries in each run (default 10.000)
    -l      Equivalent to -n 100000
    -u      Execute only unprepared statements (default)
    -p      Execute only prepared statements
    -s      Execute only select statements (default)
    -i      Execute only insert statements" \
        >&2
    exit
fi

echo "# Will run $iterations times $queries queries from :"
echo "# * $preparation-$operation-cql-rb.rb"
echo "# * $preparation-$operation-ruby-driver.rb"

output_dir=`dirname $0`/`date '+%Y-%m-%dT%H:%M'`
mkdir $output_dir

for i in `seq $iterations`
do
    n=`printf '%02d' $i`
    ruby -rbundler/setup $preparation-$operation-cql-rb.rb $queries > $output_dir/output-$preparation-$operation-cql-rb-$n.log &
    ruby_pid=$!
    top -c d -stats command,pid,cpu,threads,csw -pid $ruby_pid -l 0 > $output_dir/top-$preparation-$operation-cql-rb-$n.log &
    top_pid=$!
    wait $ruby_pid
    kill $top_pid

    ruby -rbundler/setup $preparation-$operation-ruby-driver.rb $queries > $output_dir/output-$preparation-$operation-ruby-driver-$n.log &
    ruby_pid=$!
    top -c d -stats command,pid,cpu,threads,csw -pid $ruby_pid -l 0 > $output_dir/top-$preparation-$operation-ruby-driver-$n.log &
    top_pid=$!
    wait $ruby_pid
    kill $top_pid
done

