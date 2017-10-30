package se.kth.ndb.test;

import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker implements Runnable {
  final AtomicInteger successfulOps;
  final AtomicInteger failedOps;
  final AtomicInteger speed;
  final long maxOperationsToPerform;
  final MicroBenchType microBenchType;
  final SessionFactory sf;
  final int rowsPerTx;
  final boolean distributedPKOps;
  final boolean updateRows;
  final LockMode lockMode;
  final SynchronizedDescriptiveStatistics latency;
  Random rand = new Random(System.nanoTime());
  int counter = 0;

  final List<Set<Row>> dataSet = new ArrayList<Set<Row>>();

  public Worker(AtomicInteger successfulOps, AtomicInteger failedOps,
                AtomicInteger speed, long maxOperationsToPerform, MicroBenchType microBenchType, SessionFactory sf,
                int rowsPerTx, boolean distributedPKOps, LockMode lockMode,
                SynchronizedDescriptiveStatistics lagency, boolean updateRows) {
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.speed = speed;
    this.maxOperationsToPerform = maxOperationsToPerform;
    this.microBenchType = microBenchType;
    this.sf = sf;
    this.rowsPerTx = rowsPerTx;
    this.distributedPKOps = distributedPKOps;
    this.lockMode = lockMode;
    this.latency = lagency;
    this.updateRows = updateRows;
  }

  @Override
  public void run() {
    Session dbSession = sf.getSession();
    while (true) {
      try {
        long startTime = System.nanoTime();
        dbSession.currentTransaction().begin();
        performOperation(dbSession);
        dbSession.currentTransaction().commit();
        long opExeTime=(System.nanoTime()-startTime);
        latency.addValue(opExeTime);
        successfulOps.incrementAndGet();
        speed.incrementAndGet();
      } catch (Throwable e) {
        failedOps.incrementAndGet();
        e.printStackTrace();
        dbSession.currentTransaction().rollback();
      } finally {
        if (successfulOps.get() >= maxOperationsToPerform) {
          break;
        }
      }
    }
    dbSession.close();
  }

  @Override
  protected void finalize() throws Throwable {
  }

  public void performOperation(Session session) throws Exception {
    switch (microBenchType) {
      case PK_D:
      case PK_ND:
        pkTest(session);
        return;
      case BATCH_D:
      case BATCH_ND:
        batchTest(session);
        return;
      case PPIS:
        ppisTest(session);
        return;
      case IS:
        isTest(session);
        return;
      case FTS:
        ftsTest(session);
        return;
      default:
        throw new IllegalStateException("Micro bench mark not supported");
    }
  }

  void pkTest(Session session){
    List<Table> readRows = pkRead(session);
    if(updateRows){
      for(Table row : readRows){
        row.setData1(counter++);
        row.setData2(counter++);
      }
      session.updatePersistentAll(readRows);
      release(session, readRows);
    }
  }

  List<Table> pkRead(Session session) {
    ArrayList<Table> readRows = new ArrayList<Table>();
    boolean partitionKeyHintSet = false;
    int index = rand.nextInt(dataSet.size());
    Set<Row> set  = dataSet.get(index);

    for (Row row : set) {
      Object key[] = new Object[2];
      key[0] = row.getPratitionKey();
      key[1] = row.getId();

      if (!partitionKeyHintSet) {
        session.setPartitionKey(Table.class, key);
        partitionKeyHintSet = true;
      }
      session.setLockMode(lockMode);
      Table dbRow = session.find(Table.class, key);
      if(dbRow == null){
        throw new IllegalStateException("Read null");
      }
      readRows.add(dbRow);
    }
    return readRows;
  }

  void batchTest(Session session){
    List<Table> readRows = batchRead(session);
    if(updateRows){
      for(Table row : readRows){
        row.setData1(counter++);
        row.setData2(counter++);
      }
      session.updatePersistentAll(readRows);
      release(session, readRows);
    }
  }

  List<Table> batchRead(Session session) {
    int index = rand.nextInt(dataSet.size());
    Set<Row> set  = dataSet.get(index);

    List<Table> batch = new ArrayList<Table>();
    for (Row row : set) {
      Table dbRow = getTableInstance(session);
      dbRow.setId(row.getId());
      dbRow.setPartitionId(row.getPratitionKey());
      dbRow.setData1(-1);
      dbRow.setData2(-1);
      batch.add(dbRow);
    }

    Object key[] = new Object[2];
    Table row = (Table)batch.get(0);
    key[0] = row.getPartitionId();
    key[1] = row.getId();
    session.setPartitionKey(Table.class, key);

    session.setLockMode(lockMode);

    session.load(batch);
    session.flush();

    for(Object obj : batch){
      row = (Table) obj;
      if(row.getData1() == -1 || row.getData2() == -1 ){
        throw new IllegalStateException("Wrong data read");
      }
    }
    return  batch;
  }

  void ppisTest(Session session){
    List<Table> readRows = ppisRead(session);
    if(updateRows){
      for(Table row : readRows){
        row.setData1(counter++);
        row.setData2(counter++);
      }
      session.updatePersistentAll(readRows);
      release(session, readRows);
    }
  }

  List<Table> ppisRead(Session session) {
    int index = rand.nextInt(dataSet.size());
    Set<Row> set  = dataSet.get(index);

    Iterator<Row> itr = set.iterator();
    Row firstElement = itr.next();
    int partitionKey = firstElement.getPratitionKey();

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<Table> qdty = qb.createQueryDefinition(Table.class);
    Predicate pred1 = qdty.get("partitionId").equal(qdty.param("partitionIdParam"));
    qdty.where(pred1);

    Query<Table> query = session.createQuery(qdty);
    query.setParameter("partitionIdParam", partitionKey );

    Object key[] = new Object[2];
    key[0] = partitionKey;
    key[1] = firstElement.getId();
    session.setPartitionKey(Table.class, key);
    session.setLockMode(lockMode);
    List<Table> lists = query.getResultList();
    if(lists.size() != rowsPerTx){
      throw new IllegalStateException("Wrong number of rows read. Expecting: "+rowsPerTx+" Got: "+ lists.size());
    }

    return lists;
  }

  void isTest(Session session){
    List<Table> readRows = isRead(session);
    if(updateRows){
      for(Table row : readRows){
        row.setData2(counter++);
      }
      session.updatePersistentAll(readRows);
      release(session, readRows);
    }
  }

  List<Table> isRead(Session session) {
    int index = rand.nextInt(dataSet.size());
    Set<Row> set  = dataSet.get(index);

    Iterator<Row> itr = set.iterator();
    Row firstElement = itr.next();
    int partitionKey = firstElement.getPratitionKey();

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<Table> qdty = qb.createQueryDefinition(Table.class);
    Predicate pred1 = qdty.get("data1").equal(qdty.param("data1Param"));
    qdty.where(pred1);

    Query<Table> query = session.createQuery(qdty);
    query.setParameter("data1Param", partitionKey);

    session.setLockMode(lockMode);
    List<Table> lists = query.getResultList();
    if(lists.size() != rowsPerTx){
      throw new IllegalStateException("Wrong number of rows read");
    }
    return lists;
  }

  void ftsTest(Session session){
    List<Table> readRows = ftsRead(session);
    if(updateRows){
      for(Table row : readRows){
        row.setData1(counter++);
      }
      session.updatePersistentAll(readRows);
      release(session, readRows);
    }
  }

  List<Table> ftsRead(Session session) {
    int index = rand.nextInt(dataSet.size());
    Set<Row> set  = dataSet.get(index);

    Iterator<Row> itr = set.iterator();
    Row firstElement = itr.next();
    int partitionKey = firstElement.getPratitionKey();

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<Table> qdty = qb.createQueryDefinition(Table.class);
    Predicate pred1 = qdty.get("data2").equal(qdty.param("data2Param"));
    qdty.where(pred1);

    Query<Table> query = session.createQuery(qdty);
    query.setParameter("data2Param", partitionKey );

    session.setLockMode(lockMode);
    List<Table> lists = query.getResultList();
    if(lists.size() != rowsPerTx){
      throw new IllegalStateException("Wrong number of rows read");
    }

    return lists;
  }


  protected void saveSet(Session session, Set<Row> rows){
    session.currentTransaction().begin();
    for(Row row : rows){
      Table dbRow = getTableInstance(session);
      dbRow.setId(row.getId());
      dbRow.setPartitionId(row.getPratitionKey());
      dbRow.setData1(row.getData1()); // setting the data partition id, used in FTS, and IS
      dbRow.setData2(row.getData2()); // setting the data partition id, used in FTS, and IS
      session.makePersistent(dbRow);
    }
    session.currentTransaction().commit();
  }

  protected void writeData() throws Exception {
    Session session = sf.getSession();

    for ( int i = 0; i < 32*10;){
      int partitionKey = rand.nextInt();

      Set<Row> rows = new HashSet<Row>();
      for(int j = 0; j < rowsPerTx; j++) {
        int id = rand.nextInt();
        Row row  = null;
        if(microBenchType == MicroBenchType.PK_D || microBenchType == MicroBenchType.BATCH_D){
          row = new Row(id, id, partitionKey, partitionKey);
        } else {
          row = new Row(partitionKey, id, partitionKey, partitionKey);
        }
        rows.add(row);
      }

      try {
        saveSet(session, rows);
      } catch (Exception e){
          e.printStackTrace();
        continue;
      }
      dataSet.add(rows);
//      System.out.println("Created Set "+i);
//      System.out.println(Arrays.toString(rows.toArray()));
      i++;
    }
    session.close();
  }

  private Table getTableInstance(Session session) {
      return session.newInstance(Table.class);
  }

  private void release(Session session, List<Table> rows){
    for(Table row: rows){
      release(session, row);
    }
  }

  private void release(Session session, Table row){
    session.release(row);
  }

}

