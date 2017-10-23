#!/bin/bash
set -e
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $DIR/setup

MYSQL_PWD="$password" mysql -uhop -hhadoop36 test -e "select test, threads*5 as numThreads, sum(speed) as throughput, avg(latency) as avgLatency from results where test=\"$1\" group by threads, test order by numThreads, avgLatency" 
