package com.lc.test;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.util.Properties;
import java.util.Random;


// for clusterj logging set environment variables
// export CLUSTERJ_LOGGER_FACTORY=com.lc.test.HopsLoggerFactory
// export ENABLE_CLUSTERJ_LOGS=true
public class MainFloat {

  @PersistenceCapable(table = "float_table2")
  public interface FloatTable {

    @PrimaryKey
    @Column(name = "id0")
    int getId0();
    void setId0(int id);

    @Column(name = "col0")
    float getCol0();
    void setCol0(float id);

    @Column(name = "col1")
     float getCol1();
    void setCol1(float id);
  }

  public SessionFactory sessionFactory;

  public static void main(String argv[]) throws Exception {
    new MainFloat().write();
    new MainFloat().read();

  }

  public void write() throws Exception {
    setUpDBConnection();
    Session session = sessionFactory.getSession();
    FloatTable data =session.newInstance(FloatTable.class);
    data.setId0(1);
    data.setCol0(-1);
    data.setCol1(-1);
    session.savePersistent(data);
//    System.out.println("Data: ID: "+data.getId0()+" Col0: "+data.getCol0()+" Col1: "+data.getCol1());
  }

  public void read() throws Exception {
    setUpDBConnection();
    Session session = sessionFactory.getSession();
    FloatTable data =session.find(FloatTable.class, 1);
    System.out.println("Data: ID: "+data.getId0()+" Col0: "+data.getCol0()+" Col1: "+data.getCol1());
  }


  public void setUpDBConnection() throws Exception {
    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", "localhost");
    props.setProperty("com.mysql.clusterj.database", "test");
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
