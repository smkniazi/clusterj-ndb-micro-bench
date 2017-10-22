package se.kth.ndb.test;

import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker implements Runnable {
  final int threadId;
  final AtomicInteger opsCompleted;
  final AtomicInteger successfulOps;
  final AtomicInteger failedOps;
  final AtomicInteger speed;
  final long maxOperationsToPerform;
  final MicroBenchType microBenchType;
  final SessionFactory sf;
  final int rowStartId;
  final int rowsPerTx;
  final boolean distributedPKOps;
  final LockMode lockMode;
  final SynchronizedDescriptiveStatistics latency;

  public Worker(int threadId, AtomicInteger opsCompleted, AtomicInteger successfulOps, AtomicInteger failedOps,
                AtomicInteger speed, long maxOperationsToPerform, MicroBenchType microBenchType, SessionFactory sf,
                int rowStartId, int rowsPerTx, boolean distributedPKOps, LockMode lockMode,
                SynchronizedDescriptiveStatistics lagency) {
    this.threadId = threadId;
    this.opsCompleted = opsCompleted;
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.speed = speed;
    this.maxOperationsToPerform = maxOperationsToPerform;
    this.microBenchType = microBenchType;
    this.sf = sf;
    this.rowStartId = rowStartId;
    this.rowsPerTx = rowsPerTx;
    this.distributedPKOps = distributedPKOps;
    this.lockMode = lockMode;
    this.latency = lagency;
  }

  @Override
  public void run() {
    Session dbSession = sf.getSession();
    while (true) {
      try {
        long startTime = System.nanoTime();
        dbSession.currentTransaction().begin();
        readData(dbSession);
        dbSession.currentTransaction().commit();
        long opExeTime=(System.nanoTime()-startTime);
        latency.addValue(opExeTime);
        successfulOps.incrementAndGet();
        speed.incrementAndGet();
      } catch (Throwable e) {
        opsCompleted.incrementAndGet();
        failedOps.incrementAndGet();
        e.printStackTrace();
        dbSession.currentTransaction().rollback();
      } finally {
        if (opsCompleted.incrementAndGet() >= maxOperationsToPerform) {
          break;
        }
      }
    }
    dbSession.close();
  }

  @Override
  protected void finalize() throws Throwable {
  }

  public void readData(Session session) throws Exception {
    switch (microBenchType) {
      case PK:
        pkRead(session);
        return;
      case BATCH:
        batchRead(session);
        return;
      case PPIS:
        ppisRead(session);
        return;
      case IS:
        isRead(session);
        return;
      case FTS:
        ftsRead(session);
        return;
      default:
        throw new IllegalStateException("Micro bench mark not supported");
    }
  }

  void pkRead(Session session) {
    boolean partitionKeyHintSet = false;
    for (int i = 0; i < rowsPerTx; i++) {
      int rowId = rowStartId + i;
      Object key[] = new Object[2];
      key[0] = getPartitionKey(rowId);
      key[1] = rowId;

      if (!partitionKeyHintSet) {
        session.setPartitionKey(getTableClass(), key);
        partitionKeyHintSet = true;
      }
      session.setLockMode(lockMode);
      Table row = session.find(TableWithUDP.class, key);
      if(row == null){
        throw new IllegalStateException("Read null");
      }
    }
  }

  void batchRead(Session session) {
    List<Object> batch = new ArrayList<Object>();
    for (int i = 0; i < rowsPerTx; i++) {
      int rowId = rowStartId + i;
      int partKey = getPartitionKey(rowId);
      Table row = getTableInstance(session);
      row.setId(rowId);
      row.setPartitionId(partKey);
      row.setData(-1);
      batch.add(row);
    }

    Object key[] = new Object[2];
    Table row = (Table)batch.get(0);
    key[0] = row.getPartitionId();
    key[1] = row.getId();
    session.setPartitionKey(getTableClass(), key);

    session.setLockMode(lockMode);

    session.load(batch);
    session.flush();

    for(Object obj : batch){
      row = (Table) obj;
      if(row.getData() == -1 ){
        throw new IllegalStateException("Wrong data read");
      }
    }
  }

  void ppisRead(Session session) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<TableWithUDP> qdty = qb.createQueryDefinition(TableWithUDP.class);
    Predicate pred1 = qdty.get("partitionId").equal(qdty.param("partitionIdParam"));
    qdty.where(pred1);

    Query<TableWithUDP> query = session.createQuery(qdty);
    query.setParameter("partitionIdParam", threadId );

    Object key[] = new Object[2];
    key[0] = getPartitionKey(rowStartId);
    key[1] = rowStartId;
    session.setPartitionKey(getTableClass(), key);
    session.setLockMode(lockMode);
    List<TableWithUDP> lists = query.getResultList();
    if(lists.size() != rowsPerTx){
      throw new IllegalStateException("Wrong number of rows read. Expecting: "+rowsPerTx+" Got: "+ lists.size());
    }
  }

  void isRead(Session session) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<TableWithOutUDP> qdty = qb.createQueryDefinition(TableWithOutUDP.class);
    Predicate pred1 = qdty.get("partitionId").equal(qdty.param("partitionIdParam"));
    qdty.where(pred1);

    Query<TableWithOutUDP> query = session.createQuery(qdty);
    query.setParameter("partitionIdParam", threadId );

    session.setLockMode(lockMode);
    List<TableWithOutUDP> lists = query.getResultList();
    if(lists.size() != rowsPerTx){
      throw new IllegalStateException("Wrong number of rows read");
    }
  }

  void ftsRead(Session session) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<TableWithOutUDP> qdty = qb.createQueryDefinition(TableWithOutUDP.class);
    Predicate pred1 = qdty.get("data").equal(qdty.param("dataParam"));
    qdty.where(pred1);

    Query<TableWithOutUDP> query = session.createQuery(qdty);
    query.setParameter("dataParam", threadId );

    session.setLockMode(lockMode);
    List<TableWithOutUDP> lists = query.getResultList();
    if(lists.size() != rowsPerTx){
      throw new IllegalStateException("Wrong number of rows read");
    }
  }

  protected void writeData() throws Exception {
    Session session = sf.getSession();
    session.currentTransaction().begin();

    System.out.println("Wriring Data for thread no: " + threadId);
    for (int i = 0; i < rowsPerTx; i++) {
      Table row = getTableInstance(session);
      int rowId = rowStartId + i;
      int partitionId = getPartitionKey(rowId);
      row.setId(rowId);
      row.setPartitionId(partitionId);
      row.setData(partitionId); // setting the data partition id, used in FTS
      System.out.println(row.getId() + "\t\t" + row.getPartitionId() + "\t\t" + row.getData());
      //session.makePersistent(row);
      session.savePersistent(row);
    }
    session.currentTransaction().commit();
    session.close();
  }

  private int getPartitionKey(int rowId) {

    switch (microBenchType) {
      case PK:
      case BATCH:
        if (distributedPKOps) {
          return rowId;
        } else {
          return threadId;
        }
      case PPIS:
        return threadId;
      case IS:
        return threadId;
      case FTS:
        return threadId;
      default:
        throw new IllegalStateException("Micro bench mark not supported");
    }
  }

  private Class getTableClass(){
    if (microBenchType == MicroBenchType.BATCH || microBenchType == MicroBenchType.PK ||
            microBenchType == MicroBenchType.PPIS) {
      return TableWithUDP.class;
    } else if (microBenchType == MicroBenchType.FTS || microBenchType == MicroBenchType.IS) {
      return TableWithOutUDP.class;
    } else {
      throw new IllegalStateException("Micro benchmark type not supported");
    }
  }

  private Table getTableInstance(Session session) {
    if (microBenchType == MicroBenchType.BATCH || microBenchType == MicroBenchType.PK ||
            microBenchType == MicroBenchType.PPIS) {
      return session.newInstance(TableWithUDP.class);
    } else if (microBenchType == MicroBenchType.FTS || microBenchType == MicroBenchType.IS) {
      return session.newInstance(TableWithOutUDP.class);
    } else {
      throw new IllegalStateException("Micro benchmark type not supported");
    }
  }


}

