package com.lc.test;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

public class LeaderTable {

  public static final String TABLE_NAME = "hdfs_le_descriptors";
  public static final String ID = "id";
  public static final String COUNTER = "counter";
  public static final String RPC_ADDRESSES = "rpc_addresses";
  public static final String HTTP_ADDRESS = "http_address";
  public static final String PARTITION_VAL = "partition_val";
  public static final String LOCATION_DOMAIN_ID = "location_domain_id";

  @PersistenceCapable(table = TABLE_NAME)
  public interface LeaderDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();
    void setId(long id);

    @PrimaryKey
    @Column(name = PARTITION_VAL)
    int getPartitionVal();
    void setPartitionVal(int partitionVal);

    @Column(name = COUNTER)
    long getCounter();
    void setCounter(long counter);

    @Column(name = RPC_ADDRESSES)
    String getHostname();
    void setHostname(String hostname);

    @Column(name = HTTP_ADDRESS)
    String getHttpAddress();
    void setHttpAddress(String httpAddress);

    @Column(name = LOCATION_DOMAIN_ID)
    byte getLocationDomainId();
    void setLocationDomainId(byte domainId);
  }
}
