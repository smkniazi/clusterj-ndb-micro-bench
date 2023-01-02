package com.lc.multiple.dbs.test;

import com.mysql.clusterj.*;
import testsuite.clusterj.AbstractClusterJModelTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class UnloadSchemaTest {

  private static final String TABLE = "fgtest";
  private static String DROP_TABLE_CMD = "drop table if exists " + TABLE;
  private static String CREATE_TABLE_CMD = "CREATE TABLE " + TABLE + " ( id int NOT NULL, col_1 " +
          "int DEFAULT NULL, col_2 varchar(1000) COLLATE utf8_unicode_ci DEFAULT NULL, PRIMARY " +
          "KEY (id))";
  private static String ADD_COL_3_COPY =
          "alter table " + TABLE + " add column col_3 bigint NOT NULL DEFAULT '0', ALGORITHM=COPY";
  private static String ADD_COL_3_INPLACE =
          "alter table " + TABLE + " add column (col_3 bigint DEFAULT NULL), ALGORITHM=INPLACE";
  private static String ADD_COL_4_COPY =
          "alter table " + TABLE + " add column col_4 varchar(100) COLLATE utf8_unicode_ci NOT " +
                  "NULL DEFAULT 'abc_default', ALGORITHM=COPY";
  private static String ADD_COL_4_INPLACE =
          "alter table " + TABLE + " add column col_4 varchar(100) COLLATE utf8_unicode_ci, algorithm=INPLACE";
  private static String TRUNCATE_TABLE =
          "truncate table fgtest";

  private static String defaultDB = "test";
  private static final int NUM_THREADS = 10;
  private int SLEEP_TIME = 3000;

  private static boolean USE_COPY_ALGO = true;

  boolean useCache = true;

  public void cleanUpInt(String db, Class c) {
    Session s = getSession(db);
    s.deletePersistentAll(c);
    returnSession(s);
  }

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
      return "fgtest";
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


  class DataInsertWorker extends Thread {
    private boolean run = true;
    private boolean running = false;
    private int startIndex = 0;
    private int insertsCounter = 0;

    DataInsertWorker(int startIndex) {
      this.startIndex = startIndex;
    }

    @Override
    public void run() {

      while (run) {
        Session session = getSession(DEFAULT_DB);
        DynamicObject e = null;
        boolean rowInserted = false;
        try {
          e = (DynamicObject) session.newInstance(FGTest.class);
          setFields(null, e, startIndex++); //TODO; replace null with "this"
          session.savePersistent(e);
          closeDTO(session, e, FGTest.class);
          insertsCounter++;
          rowInserted = true;
          Thread.sleep(10);
        } catch (Exception ex) {
//          ex.printStackTrace();
          System.out.println(ex.getMessage());
        } finally {
          if (!rowInserted) {
            session.unloadSchema(FGTest.class);
            session.close();
          } else {
            returnSession(session);
          }
        }
      }
      running = false;
    }

    public void stopDataInsertion() {
      run = false;
    }

    public int getInsertsCounter() {
      return insertsCounter;
    }

    public void setFields(AbstractClusterJModelTest test, DynamicObject e, int num) {
      for (int i = 0; i < e.columnMetadata().length; i++) {
        String fieldName = e.columnMetadata()[i].name();
        if (fieldName.equals("id")) {
          e.set(i, num);
        } else if (fieldName.equals("col_1")) {
          e.set(i, num);
        } else if (fieldName.equals("col_2")) {
          e.set(i, Long.toString(num));
        } else if (fieldName.equals("col_3")) {
          e.set(i, new Long(num));
        } else if (fieldName.equals("col_4")) {
          e.set(i, Long.toString(num));
        } else {
          // TODO uncomment the following file
          //test.error("Unexpected Column");
          System.out.println("Unexpected Column");
        }
      }
    }
  }

  public void test() throws Exception {
    setupMySQLConnection();
    runSQLCMD(null, DROP_TABLE_CMD); // TODO: replace null
    runSQLCMD(null, CREATE_TABLE_CMD); //TODO: replace null

    setUpRonDBConnection();

    List<DataInsertWorker> threads = new ArrayList<>(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      DataInsertWorker t = new DataInsertWorker(i*1000000);
      threads.add(t);
      t.start();
    }

    Thread.sleep(SLEEP_TIME);

    if(USE_COPY_ALGO) {
      runSQLCMD(null, ADD_COL_3_COPY); // TODO: replace null
    }else{
      runSQLCMD(null, ADD_COL_3_INPLACE); // TODO: replace null
    }

    Thread.sleep(SLEEP_TIME);

    if(USE_COPY_ALGO) {
      runSQLCMD(null, ADD_COL_4_COPY); // TODO: replace null
    }else {
      runSQLCMD(null, ADD_COL_4_INPLACE); // TODO: replace null
    }

    Thread.sleep(SLEEP_TIME);

    for (int i = 0; i < NUM_THREADS; i++) {
      threads.get(i).stopDataInsertion();
    }

    int totalInsertions = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
      threads.get(i).join();
      totalInsertions += threads.get(i).getInsertsCounter();
    }
    System.out.println("PASS: Total Insertions " + totalInsertions);
  }

// --------------------------------------------------

  public static void main(String argv[]) throws Exception {
    UnloadSchemaTest test = new UnloadSchemaTest();
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
