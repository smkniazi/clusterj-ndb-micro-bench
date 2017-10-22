select test, threads, sum(speed), avg(latency) as l from results group by threads, test order by threads, l;

