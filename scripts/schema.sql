delimiter $$
create table `test` (`partition_id` int, `id` int, data1 int, data2 int, PRIMARY KEY (`partition_id`,`id`), KEY `dindex` (`data1`)) partition by key (partition_id);$$
delimiter $$
