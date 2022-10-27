package com.lc.test;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import site.ycsb.db.table.ClassGenerator;

import java.util.Properties;
import java.util.Random;

public class DynamicClasses {
  public SessionFactory sessionFactory;

  public static void main(String argv[]) throws Exception {
    new DynamicClasses().test();
  }

  public void test() throws Exception {
    String key = "somekey";
    String field = "field1";
    byte[] data = new byte[9];


    insert("usertable", key, field, data);
  }

  public void insert(String table, String key, String field, byte[] data) throws Exception {
    setUpDBConnection();
    ClassGenerator classGenerator = new ClassGenerator();
    Session session = sessionFactory.getSession();
    Class<?> tableClass = classGenerator.generateClass("usertable2");

    DynamicObject tableClassObj = (DynamicObject) session.newInstance(tableClass);


    for (int i = 0; i < tableClassObj.columnMetadata().length; i++) {
      String fieldName = tableClassObj.columnMetadata()[i].name();
      if (fieldName.equals("key")) {
        tableClassObj.set(i, key);
      } else if (fieldName.equals(field)) {
        tableClassObj.set(i, data);
        System.out.println("setting column "+fieldName);
      }
    }
    session.savePersistent(tableClassObj);
    session.close();

  }

  public void findLE(long id, int partitionKey, Session session) {
    Object[] keys = new Object[]{id, partitionKey};
    LeaderTable.LeaderDTO lTable = (LeaderTable.LeaderDTO) session.find(LeaderTable.LeaderDTO.class,
      keys);
    //if (lTable != null) {
    //  System.out.println("Data read. ID: " + lTable.getId() + " PartID: " + lTable.getPartitionVal() +
    //    " Counter: " + lTable.getCounter());
    //} else {
    //  System.out.println("NULL");
    //}
    System.out.println("Reading OK");
  }

  public void setUpDBConnection() throws Exception {
    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", "localhost");
    props.setProperty("com.mysql.clusterj.database", "ycsb");
    props.setProperty("com.mysql.clusterj.connect.retries", "4");
    props.setProperty("com.mysql.clusterj.connect.delay", "5");
    props.setProperty("com.mysql.clusterj.connect.verbose", "1");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "60");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "5");
    props.setProperty("com.mysql.clusterj.max.transactions", "1024");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "1");
    props.setProperty("com.mysql.clusterj.max.cached.instances", "256");
    props.setProperty("com.mysql.clusterj.connection.reconnect.timeout", "5");

    try {
      sessionFactory = ClusterJHelper.getSessionFactory(props);
    } catch (ClusterJException ex) {
      throw ex;
    }
    System.out.println("Connected");
  }
}
