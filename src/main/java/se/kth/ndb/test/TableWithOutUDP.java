package se.kth.ndb.test;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
/*

create table `test_table` (`partition_id` int, `id` int, data int, PRIMARY KEY (`partition_id`,`id`)) partition by key (partition_id);

*/

@PersistenceCapable(table = "table1")
public interface TableWithOutUDP extends Table {
  @PrimaryKey
  @Column(name = "id")
  int getId();
  void setId(int id);

  @Column(name = "partition_id")
  int getPartitionId();
  void setPartitionId(int partitionId);

  @Column(name = "data")
  int getData();
  void setData(int data);
}

