#!/bin/bash
set -e
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $DIR/setup

tests=(PK_ND BATCH_ND PPIS IS FTS)
prepend=1
for test in ${tests[@]}
do
	MYSQL_PWD=$password mysql -u$username -h$dbHost $db -e "select test as \"\#test\", numThreads, count(distinct run) as runs, avg(throughput), avg(avgLatency) from ( select run , test, threads*5 as numThreads, sum(speed) as throughput, avg(latency) as avgLatency from results where test=\"$test\" group by threads, test, run  order by numThreads, avgLatency) as T group by test, numThreads" > "$test-$prepend.dat"
done

