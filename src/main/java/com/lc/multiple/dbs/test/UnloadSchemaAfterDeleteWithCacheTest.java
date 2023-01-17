package com.lc.multiple.dbs.test;

import com.mysql.clusterj.*;
import testsuite.clusterj.AbstractClusterJModelTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/*
When a table is deleted and recreated with different schema then we need to unload
the schema otherwise we will get schema version mismatch errors. However, the
SessionFactoryImpl.typeToHandlerMap uses Class as keys. The Dynamic class that will represent the
new table will not match with any key in SessionFactoryImpl.typeToHandlerMap and unloadSchema
will not do anything.

We have changed unloadSchema such that if class is not found in SessionFactoryImpl.typeToHandlerMap
then we iterate over the keys in the SessionFactoryImpl.typeToHandlersMap and check if the table
name matches with the user supplied class. If a match is found then we unload that table and
return.
 */
public class UnloadSchemaAfterDeleteWithCacheTest {

  private static final String TABLE = "fgtest";
  private static String DROP_TABLE_CMD = "drop table if exists " + TABLE;

  private static String CREATE_TABLE_CMD1 = "CREATE TABLE " + TABLE + " ( id int NOT NULL," +
          " number1  int DEFAULT NULL,  number2  int DEFAULT NULL, PRIMARY KEY (id))";

  // table with same name a above but different columns
  private static String CREATE_TABLE_CMD2 = "CREATE TABLE " + TABLE + " ( id int NOT NULL," +
          " number1  int DEFAULT NULL, PRIMARY KEY (id))";


  Session getSession(String db) {
    if (db == null) {
      return sessionFactory.getSession();
    } else {
      return sessionFactory.getSession(db);
    }
  }

  void returnSession(Session s) {
    s.closeCache();
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
    runSQLCMD(null, CREATE_TABLE_CMD1); //TODO: replace null

    // write something
    int tries = 10;
    Session session;
    DynamicObject dto;
    for (int i = 0; i < tries; i++) {
      session = getSession(DEFAULT_DB);
      dto = (DynamicObject) session.newInstance(FGTest1.class);
      setFields(null, dto, i); //TODO; replace null with "this"
      session.savePersistent(dto);
      closeDTO(session, dto, FGTest1.class);
      returnSession(session);

    }

    // delete the table and create a new table with the same name
    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD2); //TODO: replace null

    // unload schema
    session = getSession(DEFAULT_DB);
    session.unloadSchema(FGTest2.class); // unload the schema using new dynamic class
    returnSession(session);

    // write something to the new table
    for (int i = 0; i < tries; i++) {
      session = getSession(DEFAULT_DB);
      dto = (DynamicObject) session.newInstance(FGTest2.class);
      setFields(null, dto, i); //TODO; replace null with "this"
      session.savePersistent(dto);
      closeDTO(session, dto, FGTest2.class);
      returnSession(session);
    }

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
    props.setProperty("com.mysql.clusterj.max.cached.instances", "1");
    props.setProperty("com.mysql.clusterj.max.cached.sessions", "1");
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
