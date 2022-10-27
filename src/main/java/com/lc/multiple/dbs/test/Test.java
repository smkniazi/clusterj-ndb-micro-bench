package com.lc.multiple.dbs.test;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

  /*
create database db1;
create database db2;
use db1;
create table db1_table1 (id int, value int, primary key (`id`));
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
  public final String DEFAULT_DB = "mysql";
  Properties props = null;
  private boolean useDTOCache  = true;
  private boolean useSessionCache  = true;

  public static void main(String argv[]) throws Exception {
    new Test().test();
  }

  public void test() throws Exception {
    setUpDBConnection();

//    insert("db1", "db1_table1", 1, 1);
//    insert("db2", "db2_table1", 1, 1);
    for (int i = 0; i < 1000; i++) {
      insertDTO(i, i);
      Thread.sleep(400);
//      if (i == 10) {
//        sessionFactory.reconnect(2);
//        System.out.println("Reconnecting");
//        Thread.sleep(10000000);
//      }
    }
    System.out.println("Exiting...");
  }

  public void insert(String db, String table, int id, int value) throws Exception {


    Session session = sessionFactory.getSession(db);
    System.out.println("App: changed db name");

    ClassGenerator classGenerator = new ClassGenerator();
    Class<?> tableClass = classGenerator.generateClass(table);
    System.out.println("App: generated DTO class");

    DynamicObject tableClassObj = (DynamicObject) session.newInstance(tableClass);
    System.out.println("App: got class instance session.newInstance");

    for (int i = 0; i < tableClassObj.columnMetadata().length; i++) {
      String fieldName = tableClassObj.columnMetadata()[i].name();
      if (fieldName.equals("id")) {
        tableClassObj.set(i, id);
      } else if (fieldName.equals("value")) {
        tableClassObj.set(i, value);
      }
    }
    session.savePersistent(tableClassObj);
    session.close();
    System.out.println("Data saved");
  }


  public void insertDTO(int id, int value) throws Exception {


    try {
      Session session = sessionFactory.getSession("db1");

      DB1Table1.DTO dto1 = (DB1Table1.DTO) session.newInstance(DB1Table1.DTO.class);
      dto1.setId(id);
      dto1.setValue(value);
      session.savePersistent(dto1);
      session.releaseCache(dto1, DB1Table1.DTO.class);
      session.close();

      session = sessionFactory.getSession("db2");
      DB2Table1.DTO dto2 = (DB2Table1.DTO) session.newInstance(DB2Table1.DTO.class);
      dto2.setId(id);
      dto2.setValue(value);
      session.savePersistent(dto2);
      session.releaseCache(dto2, DB2Table1.DTO.class);
      session.close();
      System.out.println("Data saved");
    } catch (Exception e) {
      System.err.println("---> Error: " + e.getMessage());
    }
  }



  private void releaseDTO(Session s, Object obj) {
    if(useDTOCache){
      s.releaseCache(obj, DB2Table1.DTO.class);
    } else {
      s.release(obj);
    }
  }

  private void releaseSession(Session s){
    if(useSessionCache){
      s.closeCache();
    } else {
      s.close();
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
    props.setProperty("com.mysql.clusterj.max.cached.instances", "10");
    props.setProperty("com.mysql.clusterj.max.cached.sessions", "10");
    props.setProperty("com.mysql.clusterj.connection.reconnect.timeout", "5");

    try {
      sessionFactory = ClusterJHelper.getSessionFactory(props);
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
}
