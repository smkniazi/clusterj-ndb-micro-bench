package com.lc.multiple.dbs.test;

import com.mysql.clusterj.*;
import testsuite.clusterj.AbstractClusterJModelTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class UnloadSchemaAfterDeleteTest {

  private static final String TABLE = "fgtest";
  private static String DROP_TABLE_CMD = "drop table if exists " + TABLE;

  private static String CREATE_TABLE_CMD1 = "CREATE TABLE " + TABLE + " ( id int NOT NULL," +
          " number  int DEFAULT NULL, PRIMARY KEY (id))";

  // table with same name a above but different columns
  private static String CREATE_TABLE_CMD2 = "CREATE TABLE " + TABLE + " ( id int NOT NULL," +
          " name varchar(1000) COLLATE utf8_unicode_ci DEFAULT NULL, PRIMARY KEY (id))";

  boolean useCache = false;

  Session getSession(String db) {
    if (db == null) {
      return sessionFactory.getSession();
    } else {
      return sessionFactory.getSession(db);
    }
  }

  void returnSession(Session s) {
    if (useCache) {
      s.closeCache();
    } else {
      s.close();
    }
  }

  void closeDTO(Session s, DynamicObject dto, Class dtoClass) {
    if (useCache) {
      s.releaseCache(dto, dtoClass);
    } else {
      s.release(dto);
    }
  }

  public static class FGTest extends DynamicObject {
    @Override
    public String table() {
      return TABLE;
    }
  }

  public void runSQLCMD(AbstractClusterJModelTest test, String cmd) {
    PreparedStatement preparedStatement = null;

    try {
      preparedStatement = connection.prepareStatement(cmd);
      preparedStatement.executeUpdate();
      System.out.println(cmd);
    } catch (SQLException e) {
      //TODO uncomment this line
      //test.error("Failed to drop table. Error: "+e.getMessage());
      throw new RuntimeException("Failed to command: ", e);
    }
  }

  // Testing RONDB-195
  // https://hopsworks.atlassian.net/browse/RONDB-195
  public void test() throws Exception {
    setupMySQLConnection();
    setUpRonDBConnection();

    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD1); //TODO: replace null

    // write something
    Session session = getSession(DEFAULT_DB);
    DynamicObject e = session.newInstance(FGTest.class);
    setFields(null, e, 0); //TODO; replace null with "this"
    session.savePersistent(e);
    closeDTO(session, e, FGTest.class);
    returnSession(session);

    // delete the table and create a new table with the same name
    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD2); //TODO: replace null

    // unload schema
    session = getSession(DEFAULT_DB);
    session.unloadSchema(FGTest.class);
    returnSession(session);

    // write something to the new table
    session = getSession(DEFAULT_DB);
    e = session.newInstance(FGTest.class);
    setFields(null, e, 0); //TODO; replace null with "this"
    session.savePersistent(e);
    closeDTO(session, e, FGTest.class);
    returnSession(session);

    System.out.println("PASS");
  }

  public void setFields(AbstractClusterJModelTest test, DynamicObject e, int num) {
    for (int i = 0; i < e.columnMetadata().length; i++) {
      String fieldName = e.columnMetadata()[i].name();
      if (fieldName.equals("id")) {
        e.set(i, num);
      } else if (fieldName.equals("name")) {
        e.set(i, Integer.toString(num));
      } else if (fieldName.equals("number")) {
        e.set(i, num);
      } else {
        // TODO uncomment the following file
        //test.error("Unexpected Column");
        System.out.println("Unexpected Column " + fieldName);
      }
    }
  }
// --------------------------------------------------

  public static void main(String argv[]) throws Exception {
    UnloadSchemaAfterDeleteTest test = new UnloadSchemaAfterDeleteTest();
    test.test();
  }

  Properties props = null;
  String DEFAULT_DB = "test";
  public SessionFactory sessionFactory;
  public Connection connection;

  public void setUpRonDBConnection() throws Exception {
    props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", "localhost");
    props.setProperty("com.mysql.clusterj.database", DEFAULT_DB);
    props.setProperty("com.mysql.clusterj.connect.retries", "4");
    props.setProperty("com.mysql.clusterj.connect.delay", "5");
    props.setProperty("com.mysql.clusterj.connect.verbose", "1");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "60");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "5");
    props.setProperty("com.mysql.clusterj.max.transactions", "1024");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "1");
    props.setProperty("com.mysql.clusterj.max.cached.instances", "1024");
    props.setProperty("com.mysql.clusterj.max.cached.sessions", "20");
    props.setProperty("com.mysql.clusterj.connection.reconnect.timeout", "5");

    try {
      sessionFactory = ClusterJHelper.getSessionFactory(props);
    } catch (ClusterJException ex) {
      throw ex;
    }
    System.out.println("Connected to NDB");
  }

  protected void setupMySQLConnection() throws ClassNotFoundException, SQLException {
    Properties mysqlProps = new Properties();
//    com.mysql.cj.jdbc.MysqlDataSource a ;
    Class.forName("com.mysql.cj.jdbc.MysqlDataSource");
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "hop", "hop");
    System.out.println("Connected to MySQL Server");
  }

}
