#!/bin/bash

# Script defaults : 10 measurements of 10k unprepared selects
iterations=10
queries=$((1000 * 10))
help=false

while getopts "hln:" opt; do
    case $opt in
        l) queries=$((1000 * 100)) ;;
        n) queries=$OPTARG ;;
        :) echo "Option -$OPTARG requires an argument." >&2 ; help=true ;;
        *) help=true ;; # Wrong argument == print help
    esac
done

# Print help if asked
if [ $help = true ] ; then
    echo "Usage: $0 [option...] file...
    Options:
    -h      Print this help
    -n <n>  Execute <n> queries in each run (default 10.000)
    -l      Equivalent to -n 100000

    Arguments:
    The list of files (ruby benchmarks) to execute" \
        >&2
    exit
fi
shift $((OPTIND-1))

echo "# Will run $iterations times $queries queries from : $@"

output_dir=`dirname $0`/`date '+%Y-%m-%dT%H:%M'`
mkdir $output_dir

for iteration in `seq $iterations`
do
    for file in $@
    do
        test_name=${file%.*}
        n=`printf '%02d' $iteration`
        ruby -rbundler/setup $file $queries > $output_dir/output-$test_name-$n.log &
        ruby_pid=$!
        top -c d -stats command,pid,cpu,threads,csw -pid $ruby_pid -l 0 > $output_dir/top-$test_name-$n.log &
        top_pid=$!
        wait $ruby_pid
        kill $top_pid
    done
done

