package se.kth.ndb.test;


import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import se.kth.ndb.test.Table.TableDTO;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MicroBenchMain {
  @Option(name = "-numThreads", usage = "Number of threads. Default is 1")
  private static int numThreads = 1;

  @Option(name = "-lockMode", usage = "Lock Mode. (RC, S, E). READ_COMMITTED, SHARED, EXCLUSIVE. Default is RC")
  private static String lockModeStr = "RC";
  private static LockMode lockMode = LockMode.READ_COMMITTED;

  @Option(name = "-microBenchType", usage = "PK, BATCH, PPIS, IS, FTS")
  private static String MicroBenchTypeStr = "PK";
  private static MicroBenchType microBenchType = MicroBenchType.PK;

  @Option(name = "-singlePartitionBatch", usage = "All the operations in a batch will be performed on a single partition")
  private static boolean singlePartitionBatch = false;

  @Option(name = "-distributedBatch", usage = "All the operations in the batch operations will go to different database partition")
  private static boolean distributedBatch = false;

  @Option(name = "-schema", usage = "DB schemma name. Default is test")
  static private String schema = "test";

  @Option(name = "-dbHost", usage = "com.mysql.clusterj.connectstring.")
  static private String dbHost = "";

  @Option(name = "-numRows", usage = "Number of rows that are read in each transaction")
  static private int numRows = 1;

  @Option(name = "-maxOperationToPerform", usage = "Total operations to perform. Default is 1000. Recommended 1 million or more")
  static private long maxOperationToPerform = 100;

  private static AtomicInteger opsCompleted = new AtomicInteger(0);
  private static AtomicInteger successfulOps = new AtomicInteger(0);
  private static AtomicInteger failedOps = new AtomicInteger(0);
  private static long lastOutput = 0;

  Random rand = new Random(System.currentTimeMillis());
  ExecutorService executor = null;
  SessionFactory sf = null;
  @Option(name = "-help", usage = "Print usages")
  private boolean help = false;

  private Worker[] workers;
  private AtomicLong speed = new AtomicLong(0);

  public static StringBuilder systemStats() {
    Runtime runtime = Runtime.getRuntime();
    NumberFormat format = NumberFormat.getInstance();
    StringBuilder sb = new StringBuilder();
    long maxMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    sb.append("\nFree Mem: " + format.format(freeMemory / (1024 * 1024)) + " MB. ");
    sb.append("Allocated Mem: " + format.format(allocatedMemory / (1024 * 1024)) + " MB. ");
    sb.append("Max Mem: " + format.format(maxMemory / (1024 * 1024)) + " MB. ");
    sb.append("Tot Free Mem: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / (1024 * 1024)) + " MB. ");
    sb.append("Direct Mem: " + sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed()/(1024*1024) +  " MB. \n");
    return sb;
  }

  public void startApplication(String[] args) throws Exception {
    parseArgs(args);

    setUpDBConnection();

    deleteAllData();

    populateDB();

    System.out.println("Press enter to start execution");
    System.in.read();
    startWorkers();

    System.out.println("Press enter to shut down");
    System.in.read();
    sf.close();
    executor = null;
  }

  private void parseArgs(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
      if (lockModeStr.compareToIgnoreCase("E") == 0) {
        lockMode = LockMode.EXCLUSIVE;
      } else if (lockModeStr.compareToIgnoreCase("S") == 0) {
        lockMode = LockMode.SHARED;
      } else if (lockModeStr.compareToIgnoreCase("RC") == 0) {
        lockMode = LockMode.READ_COMMITTED;
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println();
      System.exit(-1);
    }

    if (help) {
      parser.printUsage(System.err);
      System.exit(0);
    }
  }

  public void setUpDBConnection() throws Exception {
    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", dbHost);
    props.setProperty("com.mysql.clusterj.database", schema);
    props.setProperty("com.mysql.clusterj.connect.retries", "4");
    props.setProperty("com.mysql.clusterj.connect.delay", "5");
    props.setProperty("com.mysql.clusterj.connect.verbose", "1");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "30");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "20");
    props.setProperty("com.mysql.clusterj.max.transactions", "1024");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "1");
    sf = ClusterJHelper.getSessionFactory(props);
  }

  public void startWorkers() throws InterruptedException, IOException {
    workers = new Workers[numThreads];
    executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      Workers worker = new Workers(i);
      workers[i] = worker;
    }

    for (int i = 0; i < numThreads; i++) {
      executor.execute(workers[i]);
    }
    executor.shutdown();
    long startTime = System.currentTimeMillis();
    while (!executor.isTerminated()) {
      Thread.sleep(1000);
      printMemUsageAndSpeed();
    }
  }

  private void deleteAllData() throws Exception {
    Session session = sf.getSession();
    session.deletePersistentAll(TableDTO.class);
    session.close();
  }

  private void populateDB() throws Exception {
    Session session = sf.getSession();
    session.currentTransaction().begin();
    for (int j = 0; j < numThreads; j++) {
      for (int i = 0; i < numRows; i++) {
        TableDTO row = session.newInstance(TableDTO.class);
        row.setPartitionId(j);
        row.setId(i);
        row.setIntCol1(j);
        row.setIntCol2(j);
        row.setStrCol1(j + "");
        row.setStrCol2(j + "");
        row.setStrCol3(j + "");
        session.savePersistent(row);
      }
    }
    session.currentTransaction().commit();
    session.close();
    System.out.println("Test data created.");
  }

  private void printMemUsageAndSpeed() {
    long curTime = System.currentTimeMillis();
    if ((curTime - lastOutput) > 1000) {
      StringBuilder sb = systemStats();
      sb.append("Speed: " + speed + " ops/sec. Total Successful Ops: " + successfulOps + " Failed Ops: " +
              failedOps);
      speed.set(0);
      System.out.println(sb.toString());
      lastOutput = curTime;
    }
  }
}
