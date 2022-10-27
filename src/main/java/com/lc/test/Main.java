package com.lc.test;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJFatalException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;

import java.util.Currency;
import java.util.Properties;
import java.util.Random;


// for clusterj logging set environment variables
// export CLUSTERJ_LOGGER_FACTORY=com.lc.test.HopsLoggerFactory
// export ENABLE_CLUSTERJ_LOGS=true
public class Main {
  public SessionFactory sessionFactory;

  public static void main(String argv[]) throws Exception {
    System.out.println("CLUSTERJ_LOGGER_FACTORY: " + System.getenv("CLUSTERJ_LOGGER_FACTORY"));
    System.out.println("ENABLE_CLUSTERJ_LOGS: " + System.getenv("ENABLE_CLUSTERJ_LOGS"));
//    new Main().test_fixed();
//    new Main().testIdentifyReconnect();
    new DynamicClasses().test();
  }

  public void testIdentifyReconnect() throws Exception {
    setUpDBConnection();
    long startTime = System.currentTimeMillis();
    int numSessions = 10;
    Session sessions[] = new Session[numSessions];
    for(int i = 0; i < numSessions; i++){
      sessions[i] = sessionFactory.getSession();
    }
    Random rand = new Random(System.currentTimeMillis());
    while (true) {

      int index = rand.nextInt(numSessions);
      Session session = sessions[index];
      try {
        Thread.sleep(1000);
        System.out.println("Session 1: isClosed: "+ session.isClosed());
        System.out.println("SessionFactory: Session Count: "+ sessionFactory.getConnectionPoolSessionCounts()+" State: "+sessionFactory.currentState());
        findLE(1,0, session);

      } catch (ClusterJException e) {
        System.out.println("Current state: " + sessionFactory.currentState() + " " + e);
        session.close();
        sessions[index] = sessionFactory.getSession();
      }
    }
  }

  public void test_fixed() throws Exception {
    setUpDBConnection();
    long startTime = System.currentTimeMillis();
    Session session = sessionFactory.getSession();
    while (true) {

      try {
        Thread.sleep(1000);
        System.out.println("Reading from DB");
        session = renewSessionIfClosed(session);
        findLE(1, 0, session);

      } catch (ClusterJException e) {
        System.out.println("Current state: " + sessionFactory.currentState() + " " + e);

        if (e instanceof ClusterJUserException) {
          ClusterJUserException ue = (ClusterJUserException) e;
          System.out.println("ClusterjUserException " + ue + " Cause: " + ue.getCause());

          if (ue.getMessage().contains("No more operations can be performed while this Db is closing")) {
            System.out.println("Is session closed: " + session.isClosed());
            session.close();
            session = null;
            reconnect();
          }
        }
      }
    }
  }

  long lastReconnectTime = 0;

  public synchronized void reconnect() {
//    if ((System.currentTimeMillis() - lastReconnectTime) > (5 * 1000)) {
//      System.out.println("XXX ---> Reconnecting ");
//      sessionFactory.reconnect(5);
//      lastReconnectTime = System.currentTimeMillis();
//    }
  }

  public Session renewSessionIfClosed(Session session) {
//    if (session.isClosed()) {
//      System.out.println("Getting a new session");
//      return sessionFactory.getSession();
//    } else {
//      return session;
//    }

    if (session == null) {
      return sessionFactory.getSession();
    } else {
      return session;
    }
  }


  public void test() throws Exception {
    setUpDBConnection();
    long startTime = System.currentTimeMillis();
    Session session = sessionFactory.getSession();
    while (true) {

      try {
        System.out.println("Reading from DB");
        findLE(1, 0, session);
        System.out.println("Reading OK");

      } catch (Throwable e) {


        System.out.println("Operation failed. " + e);
        e.printStackTrace();

        try {
          if (e.getMessage().contains("The session has been closed")) {
            session.close();
            System.out.println("Getting new session");
            session = sessionFactory.getSession();
          }
        } catch (Throwable ee) {
          System.out.println("Failed to get new session " + ee);
          ee.printStackTrace();
        }

        try {
          if (e.getMessage().contains("message Cluster Failure")) {
            System.out.println("Calling reconnect");
            sessionFactory.reconnect(1);
          }
        } catch (Throwable ee) {
          System.out.println("Reconnection error " + ee);
          ee.printStackTrace();
        }
      }

      Thread.sleep(5000);
      if ((System.currentTimeMillis() - startTime) > 5 * 60 * 1000) {
        break;
      }
    }
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
    props.setProperty("com.mysql.clusterj.database", "hops");
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
