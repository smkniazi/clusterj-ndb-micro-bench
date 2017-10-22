package se.kth.ndb.test;

import com.mysql.clusterj.annotation.*;

/*

create table `test_table` (`partition_id` int, `id` int, data int, PRIMARY KEY (`partition_id`,`id`)) partition by key (partition_id);

*/
@PersistenceCapable(table = "test")
@PartitionKey(column = "partition_id")
public interface Table {
  @PrimaryKey
  @Column(name = "partition_id")
  int getPartitionId();
  void setPartitionId(int partitionId);

  @PrimaryKey
  @Column(name = "id")
  int getId();
  void setId(int id);

  @Column(name = "data1")
  @Index(name = "dindex")
  int getData1();
  void setData1(int data);


  @Column(name = "data2")
  int getData2();
  void setData2(int data);
}

