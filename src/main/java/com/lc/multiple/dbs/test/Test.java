package com.lc.multiple.dbs.test;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

  /*
create database db1;
create database db2;
use db1;
create table db1_table1 (id int, value int, primary key (`id`));
create table db1_table2 (id int, value varchar(50) NOT NULL, primary key (`id`));
use db2;
create table db2_table1 (id int, value int, primary key (`id`));
   */


public class Test {
  {
    Map<String, String> envs = new HashMap<>();
    envs.put("CLUSTERJ_LOGGER_FACTORY", "com.lc.test.HopsLoggerFactory");
    setEnv(envs);
  }

  public SessionFactory sessionFactory;
  public final String DEFAULT_DB = "db1";
  Properties props = null;
  private boolean useDTOCache = false;
  private boolean useSessionCache = false;
  private boolean unloadSchema = true;

  public static void main(String argv[]) throws Exception {
    Random rand = new Random(System.currentTimeMillis());
    new Test().saveNULLValues(rand.nextInt(), rand.nextInt());
  }



  public static class  DODB1Table2 extends DynamicObject {
    @Override
    public String table() {
      return "db1_table2";
    }
  }
  public void saveNULLValues(int id, int value) throws Exception {

    setUpDBConnection();
    runSQLCMD("truncate table db1.db1_table2");

    Session session1 = sessionFactory.getSession("db1");
    try {
//      DB1Table2.DTO dto1 = null;
//      dto1 = (DB1Table2.DTO) session1.newInstance(DB1Table2.DTO.class);
//      session1.currentTransaction().begin();
//      dto1.setId(id);
//      dto1.setValue(null);
//      session1.savePersistent(dto1);
//      session1.currentTransaction().commit();


      session1.currentTransaction().begin();
      DODB1Table2 dto2 = (DODB1Table2) session1.newInstance(DODB1Table2.class);
      System.out.println("After creating the object");
      setFieldsDB1Table2(dto2, id);
      session1.savePersistent(dto2);
      session1.currentTransaction().commit();



      System.out.println("Data saved.");
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error: " + e.getMessage());
    }
  }

  public void setFieldsDB1Table2(DynamicObject e, int num) {
    for (int i = 0; i < e.columnMetadata().length; i++) {
      String fieldName = e.columnMetadata()[i].name();
      if (fieldName.equals("id")) {
        e.set(i, num);
      } else if (fieldName.equals("value")) {
        e.set(i, null);
      } else {
        // TODO uncomment the following file
        //test.error("Unexpected Column");
        System.out.println("Unexpected Column");
      }
    }
  }




  public void testNPE(int id, int value) throws Exception {

    setUpDBConnection();

    DB1Table1.DTO dto1 = null;
    DB1Table1.DTO dto2 = null;
    Session session1 = sessionFactory.getSession("db1");
    Session session2 = sessionFactory.getSession("db1");
    try {
      dto1 = (DB1Table1.DTO) session1.newInstance(DB1Table1.DTO.class);

      session1.currentTransaction().begin();
      dto1.setId(id+1);
      unloadSchema(session1, DB1Table1.DTO.class);

      session2.currentTransaction().begin();
      dto2 = (DB1Table1.DTO) session2.newInstance(DB1Table1.DTO.class);
      dto2.setId(id);
      dto2.setValue(value);
      session2.savePersistent(dto2);
      session2.currentTransaction().commit();

//      dto1.setValue(value);
      session1.savePersistent(dto1);
      session1.currentTransaction().commit();


      System.out.println("Data saved.");
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
//      e.printStackTrace();
    } finally {
      session1.release(dto1);
      session2.release(dto2);
    }
  }


  public void test() throws Exception {
    setUpDBConnection();

    for (int i = 0; i < 100000; i++) {
      Session session1 = sessionFactory.getSession("db1");
      DB1Table1.DTO dto1 = session1.newInstance(DB1Table1.DTO.class);
      session1.releaseCache(dto1, DB1Table1.DTO.class);
      session1.closeCache();
    }
    System.out.println("Warmup complete");


    int numThreads = 1;
    List<Thread> threads = new ArrayList<Thread>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      threads.add(i, new Thread(new Worker(i, 200)));
      threads.get(i).start();
    }
    for (int i = 0; i < numThreads; i++) {
      threads.get(i).join();
    }

    System.out.println("Exiting...");
  }

  public void test2() throws Exception {
    setUpDBConnection();

    ClassGenerator classGenerator = new ClassGenerator();
    Class<?> tableClass = classGenerator.generateClass("testing");
    System.out.println("App: generated DTO class");

//    insert("db1", "db1_table1", 1, 1);
//    insert("db2", "db2_table1", 1, 1);
    for (int i = 0; i < 1000; i++) {
      insertDTONPE(i, i);
      Thread.sleep(2000);
    }
    System.out.println("Exiting...");
  }


  public void testGetTable(String db, Class<?> table) throws Exception {


    try {
      Session session = sessionFactory.getSession(db);


      DynamicObject tableClassObj = (DynamicObject) session.newInstance(table);
      System.out.println("App: got class instance session.newInstance");

      session.close();
      System.out.println("Data saved");
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }


//  public void insert(String db, String table, int id, int value) throws Exception {
//
//
//    Session session = sessionFactory.getSession(db);
//    System.out.println("App: changed db name");
//
//    ClassGenerator classGenerator = new ClassGenerator();
//    Class<?> tableClass = classGenerator.generateClass(table);
//    System.out.println("App: generated DTO class");
//
//    DynamicObject tableClassObj = (DynamicObject) session.newInstance(tableClass);
//    System.out.println("App: got class instance session.newInstance");
//
//    for (int i = 0; i < tableClassObj.columnMetadata().length; i++) {
//      String fieldName = tableClassObj.columnMetadata()[i].name();
//      if (fieldName.equals("id")) {
//        tableClassObj.set(i, id);
//      } else if (fieldName.equals("value")) {
//        tableClassObj.set(i, value);
//      }
//    }
//    session.savePersistent(tableClassObj);
//    session.close();
//    System.out.println("Data saved");
//  }


  public void insertDTO(int id, int value) throws Exception {


    try {
      Session session = sessionFactory.getSession("db1");
//      Session session = sessionFactory.getSession();
      unloadSchema(session, DB1Table1.DTO.class);

      session.currentTransaction().begin();
      session.setPartitionKey(DB1Table1.DTO.class, 1);

      DB1Table1.DTO dto1 = (DB1Table1.DTO) session.newInstance(DB1Table1.DTO.class);
      dto1.setId(id);
      dto1.setValue(value);
      session.savePersistent(dto1);
      session.releaseCache(dto1, DB1Table1.DTO.class);
      session.close();

      session = sessionFactory.getSession("db2");
      unloadSchema(session, DB2Table1.DTO.class);
//
      DB2Table1.DTO dto2 = (DB2Table1.DTO) session.newInstance(DB2Table1.DTO.class);
      dto2.setId(id);
      dto2.setValue(value);
      session.savePersistent(dto2);
      session.releaseCache(dto2, DB2Table1.DTO.class);
      session.close();
      System.out.println("Data saved");
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
//      e.printStackTrace();
    }
  }

  public void insertDTONPE(int id, int value) throws Exception {


    DB1Table1.DTO dto1 = null;
    DB1Table1.DTO dto2 = null;
    Session session1 = sessionFactory.getSession("db1");
    Session session2 = sessionFactory.getSession("db1");
    try {
      dto1 = (DB1Table1.DTO) session1.newInstance(DB1Table1.DTO.class);
      unloadSchema(session1, DB1Table1.DTO.class);

      dto2 = (DB1Table1.DTO) session2.newInstance(DB1Table1.DTO.class);
      unloadSchema(session1, DB1Table1.DTO.class);
      dto2.setId(id);
      dto2.setValue(value);
      session2.savePersistent(dto2);
      session2.close();

      session1.close();

      System.out.println("Data saved.");
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
//      e.printStackTrace();
    } finally {
      session1.release(dto1);
      session2.release(dto2);
      session2.release(dto2);
    }
  }


  private void unloadSchema(Session s, Class cls) {
    if (unloadSchema) {
      if (!s.isClosed()) {
        System.out.println("Unloading schema");
        s.unloadSchema(cls);
      }
    }
  }

  private void releaseDTO(Session s, Object obj) {
    if (useDTOCache) {
      s.releaseCache(obj, DB2Table1.DTO.class);
    } else {
      s.release(obj);
    }
  }

  private void releaseSession(Session s) {
    if (useSessionCache) {
      s.closeCache();
    } else {
      s.dropInstanceCache();
      s.close();
    }
  }

  public Connection connection;
  protected void setupMySQLConnection() throws ClassNotFoundException, SQLException {
    Properties mysqlProps = new Properties();
//    com.mysql.cj.jdbc.MysqlDataSource a ;
    Class.forName("com.mysql.cj.jdbc.MysqlDataSource");
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "hop", "hop");
    System.out.println("Connected to MySQL Server");
  }
  public void runSQLCMD( String cmd) {
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

  public void setUpDBConnection() throws Exception {
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
    props.setProperty("com.mysql.clusterj.max.cached.instances", "100000");
    props.setProperty("com.mysql.clusterj.max.cached.sessions", "100000");
    props.setProperty("com.mysql.clusterj.connection.reconnect.timeout", "5");

    try {
      sessionFactory = ClusterJHelper.getSessionFactory(props);
      setupMySQLConnection();
    } catch (ClusterJException ex) {
      throw ex;
    }
    System.out.println("Connected");
  }

  private void setEnv(Map<String, String> newenv) {
    try {
      try {
        Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
        Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
        theEnvironmentField.setAccessible(true);
        Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
        env.putAll(newenv);
        Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
        theCaseInsensitiveEnvironmentField.setAccessible(true);
        Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
        cienv.putAll(newenv);
      } catch (NoSuchFieldException e) {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      }
    } catch (Throwable t) {
      System.err.println(t.getMessage());
      System.exit(1);
    }
  }

  HashMap<Integer, Integer> hashMap = new HashMap();

  class Worker implements Runnable {
    int counter = 0;
    int MAXCOUNT = 0;
    Session session1;

    Worker(int id, int MAXCOUNT) {
      this.MAXCOUNT = MAXCOUNT;
      this.counter = id * MAXCOUNT;
    }

    @Override
    public void run() {
      session1 = sessionFactory.getSession("db1");
      for (int i = 0; i < MAXCOUNT; i++) {
        work();
        counter++;
      }
      session1.close();
    }

    public void work() {
      DB1Table1.DTO dto1 = null;
      try {
        dto1 = session1.newInstance(DB1Table1.DTO.class);

        if (hashMap.containsKey(dto1.hashCode())) {
          System.err.println("Hash matches");
        } else {
          hashMap.put(dto1.hashCode(), dto1.hashCode());
        }

        //unloadSchema(session1, DB1Table1.DTO.class);
        dto1.setId(counter);
        dto1.setValue(counter);
        session1.savePersistent(dto1);
//        System.out.println("Data saved.");
      } catch (Exception e) {
        if (!e.getMessage().contains("the underlying metadata is stale")) {
          System.err.println("Error: " + e.getMessage());
          e.printStackTrace();
        }
      } finally {
        try {
//          session1.releaseCache(dto1, DB1Table1.DTO.class);
          session1.release(dto1);
        } catch (Exception e) {
          System.err.println("----> Error: " + e.getMessage());
          e.printStackTrace();
        }
      }

    }
  }

}
