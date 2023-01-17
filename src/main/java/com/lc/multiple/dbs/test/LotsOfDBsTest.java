package com.lc.multiple.dbs.test;

import com.mysql.clusterj.*;
import testsuite.clusterj.AbstractClusterJModelTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class LotsOfDBsTest {

  private String getDropDBCmd(String DB) {
    return "drop database if exists " + DB;
  }

  private String getCreateDBCmd(String DB) {
    return "Create database " + DB;
  }

  private String getUseCmd(String DB) {
    return "USE " + DB;
  }

  private String getDropTableCmd(String TABLE) {
    return "drop table if exists " + TABLE;
  }

  private String getCreateTableCmd(String TABLE) {
    return "CREATE TABLE "+TABLE+"(id0 INT, col0 FLOAT, col1 FLOAT UNSIGNED, PRIMARY KEY(id0))" +
            " ENGINE=ndbcluster";
  }

  private String getInsertCommand1(String TABLE) {
    return "INSERT INTO  "+TABLE+" VALUES(1,-123.123,123.123)";
  }

  private String getInsertCommand2(String TABLE) {
    return "INSERT INTO  "+TABLE+" VALUES(0,0,0)";
  }

  private String getInsertCommand3(String TABLE) {
    return "INSERT INTO  "+TABLE+" set id0=2";
  }

  Session getSession(String db) {
    if (db == null) {
      return ndbSessionFactory.getSession();
    } else {
      return ndbSessionFactory.getSession(db);
    }
  }

  void returnSession(Session s) {
    s.close();
  }

  void closeDTO(Session s, DynamicObject dto) {
    s.release(dto);
  }

  public static class FGTest1 extends DynamicObject {
    @Override
    public String table() {
      return "fgtest";
    }
  }

  public static class FGTest2 extends DynamicObject {
    @Override
    public String table() {
      return "fgtest";
    }
  }

  public void runSQLCMD(AbstractClusterJModelTest test, String cmd) {
    PreparedStatement preparedStatement = null;

    try {
      preparedStatement = jdbdConnection.prepareStatement(cmd);
      preparedStatement.executeUpdate();
      System.out.println(cmd);
    } catch (SQLException e) {
      //TODO uncomment this line
      //test.error("Failed to drop table. Error: "+e.getMessage());
      throw new RuntimeException("Failed to command: ", e);
    }
  }

  public void test() throws Exception {

    setupMySQLConnection();
    setUpRonDBConnection();

    for(int i = 0; i < 100; i++){
      String db = "test"+i;
      String table = "tbl"+i;
      testInt(db, table);
    }

    System.out.println("PASS");
  }

  public void testInt(String db, String table) throws Exception {
    setupMySQLConnection();
    setUpRonDBConnection();
    runSQLCMD(null, getDropDBCmd(db)); // TODO: replace null
    runSQLCMD(null, getCreateDBCmd(db)); // TODO: replace null
    runSQLCMD(null, getUseCmd(db)); // TODO: replace null
    runSQLCMD(null, getCreateTableCmd(table)); //TODO: replace null
    runSQLCMD(null, getInsertCommand1(table)); //TODO: replace null
    runSQLCMD(null, getInsertCommand2(table)); //TODO: replace null
    runSQLCMD(null, getInsertCommand3(table)); //TODO: replace null

    try {
      ClassGenerator classGenerator = new ClassGenerator();
      Class<?> tableClass = classGenerator.generateClass(table);

      Session session = ndbSessionFactory.getSession(db);
//      DynamicObject dto = (DynamicObject) session.newInstance(tableClass);
//      setFields(null, dto);
      DynamicObject retDto = (DynamicObject) session.find(tableClass, 0);
      if (retDto == null) {
        System.out.println("FAIL. Row not found");
        return;
      }
      session.close();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }

    runSQLCMD(null, getDropDBCmd(db)); //TODO: replace null
    shutdownNDBConnection();
    shutdownMySQLConnection();
  }

  public void setFields(AbstractClusterJModelTest test, DynamicObject e) {
    for (int i = 0; i < e.columnMetadata().length; i++) {
      String fieldName = e.columnMetadata()[i].name();
      if (fieldName.equals("id")) {
        e.set(i, 0);
        return;
      }
      //id column not found
      //test.error("Unexpected Column");
    }
  }

// --------------------------------------------------

  public static void main(String argv[]) throws Exception {
    LotsOfDBsTest test = new LotsOfDBsTest();
    test.test();
  }

  Properties props = null;
  String DEFAULT_DB = "test";
  public SessionFactory ndbSessionFactory;
  public Connection jdbdConnection;

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
      ndbSessionFactory = ClusterJHelper.getSessionFactory(props);
    } catch (ClusterJException ex) {
      throw ex;
    }
    System.out.println("Connected to NDB");
  }

  public void shutdownNDBConnection() throws Exception {
    ndbSessionFactory.close();
  }

  protected void setupMySQLConnection() throws ClassNotFoundException, SQLException {
    Properties mysqlProps = new Properties();
//    com.mysql.cj.jdbc.MysqlDataSource a ;
    Class.forName("com.mysql.cj.jdbc.MysqlDataSource");
    jdbdConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "hop", "hop");
    System.out.println("Connected to MySQL Server");
  }

  public void shutdownMySQLConnection() throws Exception {
    jdbdConnection.close();
  }

}
