package com.lc.multiple.dbs.test;

import com.mysql.clusterj.*;
import testsuite.clusterj.AbstractClusterJModelTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/*
Fixes for recreating a table with the same name while using session cache.
 */
public class UnloadSchemaAfterDeleteWithCacheTest {

  private static final String TABLE = "fgtest";
  private static String DROP_TABLE_CMD = "drop table if exists " + TABLE;

  private static String CREATE_TABLE_CMD1 = "CREATE TABLE " + TABLE + " ( id int NOT NULL," +
          " numberSecond1  int DEFAULT NULL,  numberSecond2  int DEFAULT NULL, PRIMARY KEY (id))";

  // table with same name a above but different columns
  private static String CREATE_TABLE_CMD2 = "CREATE TABLE " + TABLE + " ( id int NOT NULL," +
          " numberFirst1  int DEFAULT NULL,numberFirst2  int DEFAULT NULL, numberFirst3  int " +
          "DEFAULT NULL, " +
          "PRIMARY KEY (id))";


  Session getSession(String db) {
    if (db == null) {
      return sessionFactory.getSession();
    } else {
      return sessionFactory.getSession(db);
    }
  }

  void returnSession(Session s) {
//    s.closeCache();
    s.close();
  }

  void closeDTO(Session s, DynamicObject dto, Class dtoClass) {
    s.releaseCache(dto, dtoClass);
  }

  public static class FGTest1 extends DynamicObject {
    @Override
    public String table() {
      return TABLE;
    }
  }

  public static class FGTest2 extends DynamicObject {
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

  public void test() throws Exception {
    setupMySQLConnection();
    setUpRonDBConnection();

    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD2); //TODO: replace null

    // write something
    int tries = 1;
    Session session;
    DynamicObject dto;
    session = getSession(DEFAULT_DB);
    dto = (DynamicObject) session.newInstance(FGTest1.class);
    setFields(null, dto, 0); //TODO; replace null with "this"
    session.savePersistent(dto);
    closeDTO(session, dto, FGTest1.class);
    returnSession(session);

    // delete the table and create a new table with the same name
    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD1); //TODO: replace null

    Session session1 = getSession(DEFAULT_DB);
    // unload schema
    session = getSession(DEFAULT_DB);
    System.out.println("Session 1: " + session1 + " session2: " + session);

    session.unloadSchema(FGTest2.class); // unload the schema using new dynamic class
    returnSession(session);

    // write something to the new table
    dto = (DynamicObject) session.newInstance(FGTest2.class);
    setFields(null, dto, 0); //TODO; replace null with "this"
    session.savePersistent(dto);
    closeDTO(session, dto, FGTest2.class);
    returnSession(session);

    System.out.println("PASS");
  }

  public void testFromRalf() throws Exception {
    setupMySQLConnection();
    setUpRonDBConnection();

    runSQLCMD(null, DROP_TABLE_CMD);
    runSQLCMD(null, CREATE_TABLE_CMD2);

    DEFAULT_DB = null; // or "test", both should work
    // -------------------------------------------------------------------------
    // write something
    Session session = getSession(DEFAULT_DB);
    Session session1 = getSession(DEFAULT_DB);

    Class cls1 = FGTest1.class;
    DynamicObject dto = (DynamicObject) session.newInstance(cls1);
    setFields(null, dto, 0);
    session.savePersistent(dto);
    closeDTO(session, dto, cls1);

    DynamicObject dto1 = (DynamicObject) session1.newInstance(cls1);
    setFields(null, dto1, 0);
    session1.savePersistent(dto1);
    session1.releaseCache(dto1, cls1);

    session.closeCache();
    session1.closeCache();

    // recreate same able different cols
    runSQLCMD(null, DROP_TABLE_CMD);
    runSQLCMD(null, CREATE_TABLE_CMD1);

    // -------------------------------------------------------------------------
    //unload schema
    // by this time there are two sessions in the cache
    // use one session for unload and one for inserting data such that
    // data insert fails

    // FGTest1.class / FGTest2.class both should work
    session = getSession(DEFAULT_DB); //use for unload
    session1 = getSession(DEFAULT_DB); //use for insert

    dto = (DynamicObject) session.newInstance(cls1);
    setFields(null, dto, 0);

    session.unloadSchema(FGTest1.class);
    session.closeCache();

    try {
      session1.savePersistent(dto);
      System.out.println("FAIL");
      return;
    } catch (ClusterJException e) {
      System.out.println("It was expected : " + e);
    }
    closeDTO(session, dto, cls1);
    // returning bad session to the cache. But I will recommend to close it
    session.releaseCache(dto, cls1);

    // -------------------------------------------------------------------------
    // by this time there are two sessions in the cache
    // Use both of these sessions
    session = getSession(DEFAULT_DB);
    session1 = getSession(DEFAULT_DB);

    // write something
    // Class cls2 = FGTest1.class;  // this will fail as you will get cached instances for older schema
    Class cls2 = FGTest2.class;
    dto = (DynamicObject) session.newInstance(cls2);
    setFields(null, dto, 0);
    session.savePersistent(dto);
    closeDTO(session, dto, cls2);
    session.releaseCache(dto, cls2);

    dto1 = (DynamicObject) session1.newInstance(cls2);
    setFields(null, dto1, 0);
    session1.savePersistent(dto1);
    session1.releaseCache(dto1, cls2);


    session.closeCache();
    session1.closeCache();

    System.out.println("PASS");
  }

  public void test2() throws Exception {
    setupMySQLConnection();
    setUpRonDBConnection();

    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD2); //TODO: replace null

    // write something
    Session session1;
    Session session2;
    DynamicObject dto;
    session1 = getSession(DEFAULT_DB);
    dto = (DynamicObject) session1.newInstance(FGTest1.class);
    setFields(null, dto, 0); //TODO; replace null with "this"
    session1.savePersistent(dto);
    closeDTO(session1, dto, FGTest1.class);
    returnSession(session1);

    // delete the table and create a new table with the same name
    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD1); //TODO: replace null

    session2 = getSession(DEFAULT_DB);
    session2.currentTransaction().begin();
    dto = (DynamicObject) session2.newInstance(FGTest2.class);
    setFields(null, dto, 0); //TODO; replace null with "this"

    // unload schema
    session1 = getSession(DEFAULT_DB);
    session1.unloadSchema(FGTest2.class); // unload the schema using new dynamic class
    returnSession(session1);

    session2.savePersistent(dto);
    closeDTO(session2, dto, FGTest2.class);

    try {
      session2.currentTransaction().commit();
      System.out.println("FAIL");
      return;
    } catch (Exception e) {
      System.out.println("Got exception as expected: " + e);
      returnSession(session2);
    }

    // at this time there will be two session objects in the cache. test both session objects
    session1 = getSession(DEFAULT_DB);
    session2 = getSession(DEFAULT_DB);
    //session 1
    dto = (DynamicObject) session1.newInstance(FGTest2.class);
    setFields(null, dto, 100); //TODO; replace null with "this"
    session1.savePersistent(dto);
    closeDTO(session1, dto, FGTest1.class);
    //session 2
    dto = (DynamicObject) session2.newInstance(FGTest2.class);
    setFields(null, dto, 100); //TODO; replace null with "this"
    session2.savePersistent(dto);
    closeDTO(session2, dto, FGTest1.class);
    returnSession(session1);
    returnSession(session2);

    System.out.println("PASS");
  }

  public void test3() throws Exception {
    setupMySQLConnection();
    setUpRonDBConnection();

    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD2); //TODO: replace null

    // write something
    int tries = 1;
    Session session;
    DynamicObject dto;
    session = getSession(DEFAULT_DB);
    dto = (DynamicObject) session.newInstance(FGTest1.class);
    setFields(null, dto, 0); //TODO; replace null with "this"
    session.savePersistent(dto);
    closeDTO(session, dto, FGTest1.class);
    returnSession(session);

    // delete the table and create a new table with the same name
    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD1); //TODO: replace null

    Session session1 = getSession(DEFAULT_DB);
    session1.currentTransaction().begin();
    dto = (DynamicObject) session1.newInstance(FGTest2.class);
    setFields(null, dto, 0); //TODO; replace null with "this"

    // unload schema
    session = getSession(DEFAULT_DB);
    session.unloadSchema(FGTest2.class); // unload the schema using new dynamic class
    returnSession(session);

    for (int i = 0; i < dto.columnMetadata().length; i++) {
      System.out.println("Field is " + dto.columnMetadata()[i].name());
    }
    session1.savePersistent(dto);
    closeDTO(session1, dto, FGTest2.class);
    session1.currentTransaction().commit();
    returnSession(session1);

    System.out.println("PASS");
  }

  public void setFields(AbstractClusterJModelTest test, DynamicObject e, int num) {
    for (int i = 0; i < e.columnMetadata().length; i++) {
      String fieldName = e.columnMetadata()[i].name();
      if (fieldName.equals("id")) {
        e.set(i, num);
      } else if (fieldName.startsWith("name")) {
        e.set(i, Integer.toString(num));
      } else if (fieldName.startsWith("number")) {
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
    UnloadSchemaAfterDeleteWithCacheTest test = new UnloadSchemaAfterDeleteWithCacheTest();
    test.testFromRalf();
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
    props.setProperty("com.mysql.clusterj.max.cached.instances", "1");
    props.setProperty("com.mysql.clusterj.max.cached.sessions", "10");
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
