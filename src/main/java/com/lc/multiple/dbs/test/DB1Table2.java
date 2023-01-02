package com.lc.multiple.dbs.test;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

public class DB1Table2 {
  public static final String TABLE_NAME = "db1_table2";
  public static final String ID = "id";
  public static final String VALUE = "value";

  @PersistenceCapable(table = TABLE_NAME)
  public interface DTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();
    void setId(int id);

    @Column(name = VALUE)
    String getValue();
    void setValue(String value);
  }
}
