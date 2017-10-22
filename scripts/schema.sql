delimiter $$
create table `test` (`partition_id` int, `id` int, data1 int, data2 int, PRIMARY KEY (`partition_id`,`id`), KEY `dindex` (`data1`)) partition by key (partition_id);$$
delimiter $$
create table `results` (`id` int AUTO_INCREMENT, `test` varchar(25), `threads` int, `speed` double, `latency` double, PRIMARY KEY (`id`)) $$
delimiter $$
