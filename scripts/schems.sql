delimiter $$
create table `table1` (`partition_id` int, `id` int, data int, PRIMARY KEY (`partition_id`,`id`)) partition by key (partition_id);$$
delimiter $$
create table `table2` (`id` int, `partition_id` int, data int, PRIMARY KEY (`id`));$$
delimiter $$
