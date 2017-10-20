package se.kth.ndb.test;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import se.kth.ndb.test.Table.TableDTO;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ScanAndMemoryTest {
  @Option(name = "-numThreads", usage = "Number of threads. Default is 1")
  private static int numThreads = 1;
  @Option(name = "-lockMode", usage = "Lock Mode. (RC, S, E). READ_COMMITTED, SHARED, EXCLUSIVE. Default is RC")
  private static String lockModeStr = "RC";
  private static LockMode lockMode = LockMode.READ_COMMITTED;
  @Option(name = "-schema", usage = "DB schemma name. Default is hop_salman")
  static private String schema = "hop_salman";
  @Option(name = "-dbHost", usage = "com.mysql.clusterj.connectstring. Default is cloud1")
  static private String dbHost = "cloud1";
  @Option(name = "-noOfScanRows", usage = "No of scan rows. Default is 20")
  static private int noOfScanRows = 20;
  @Option(name = "-totalOps", usage = "Total operations to perform. Default is 1000. Recommended 1 million or more")
  static private long totalOps = 1000;
  private static AtomicInteger opsCompleted = new AtomicInteger(0);
  private static AtomicInteger successfulOps = new AtomicInteger(0);
  private static AtomicInteger failedOps = new AtomicInteger(0);
  private static long lastOutput = 0;
  Random rand = new Random(System.currentTimeMillis());
  ExecutorService executor = null;
  SessionFactory sf = null;
  @Option(name = "-help", usage = "Print usages")
  private boolean help = false;
  private DBWriter[] workers;
  private AtomicLong speed = new AtomicLong(0);
  // ----

  public static StringBuilder systemStats() {
    Runtime runtime = Runtime.getRuntime();
    NumberFormat format = NumberFormat.getInstance();
    StringBuilder sb = new StringBuilder();
    long maxMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    sb.append("Free Mem: " + format.format(freeMemory / (1024 * 1024)) + " MB ");
    sb.append("Allocated Mem: " + format.format(allocatedMemory / (1024 * 1024)) + " MB ");
    sb.append("Max Mem: " + format.format(maxMemory / (1024 * 1024)) + " MB ");
    sb.append(
        "Tot Free Mem: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / (1024 * 1024)) + " MB \n");
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

    if(help){
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
    workers = new DBWriter[numThreads];
    executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      DBWriter worker = new DBWriter(i);
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
      for (int i = 0; i < noOfScanRows; i++) {
        TableDTO row = session.newInstance(TableDTO.class);
        row.setPartitionId(j);
        row.setId(rand.nextInt());
        row.setIntCol1(rand.nextInt());
        row.setIntCol2(rand.nextInt());
        row.setStrCol1(rand.nextInt() + "");
        row.setStrCol2(rand.nextInt() + "");
        row.setStrCol3(rand.nextInt() + "");
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

  public void readData(Session session, int partitionId) throws Exception {

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<TableDTO> dobj = qb.createQueryDefinition(TableDTO.class);
    Predicate pred = dobj.get("partitionId").equal(dobj.param("partitionIDParam"));
    dobj.where(pred);
    Query<TableDTO> query = session.createQuery(dobj);
    query.setParameter("partitionIDParam", partitionId);

    List<TableDTO> results = query.getResultList();
    if (results.size() != noOfScanRows) {
      throw new IllegalStateException("Wrong number of rows returned in the scan. Expecting " + noOfScanRows + " got" +
          results.size());
    }
    session.release(results); // does not do shit
  }

  public class DBWriter implements Runnable {
    final int partitionId;

    public DBWriter(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    public void run() {

      Session dbSession = sf.getSession();
      while (true) {
        try {
          dbSession.currentTransaction().begin();
          Object key[] = new Object[2];
          key[0] = partitionId;
          key[1] = new Integer(0);

          dbSession.setPartitionKey(TableDTO.class, key);

          dbSession.setLockMode(lockMode);
          readData(dbSession, partitionId);

          dbSession.currentTransaction().commit();
          successfulOps.addAndGet(1);
          speed.incrementAndGet();
        } catch (Throwable e) {
          opsCompleted.incrementAndGet();
          failedOps.addAndGet(1);
          e.printStackTrace();
          dbSession.currentTransaction().rollback();
        } finally {
          if (opsCompleted.incrementAndGet() >= totalOps) {
            break;
          }
        }
      }
      dbSession.close();
    }

    @Override
    protected void finalize() throws Throwable {
      System.out.println("Help I am dying ... ");
    }
  }
}
