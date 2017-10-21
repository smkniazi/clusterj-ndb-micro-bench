package se.kth.ndb.test;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 * Created by salman on 2016-09-02.
 */

public interface Table {
  int getId();
  void setId(int id);

  int getPartitionId();
  void setPartitionId(int partitionId);

  int getData();
  void setData(int data);
}

