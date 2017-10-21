package se.kth.ndb.test;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 * Created by salman on 2016-09-02.
 */

/*

create table `test_table` (`partition_id` int, `id` int, int_col1 int, int_col2 int, str_col1 varchar(3000),
str_col2 varchar(3000), str_col3 varchar(3000), PRIMARY KEY (`partition_id`,`id`)) partition by key (partition_id);

*/




public interface Table {}

