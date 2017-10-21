package se.kth.ndb.test;

import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;

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

  public Worker(int threadId, AtomicInteger opsCompleted, AtomicInteger successfulOps, AtomicInteger failedOps,
                AtomicInteger speed, long maxOperationsToPerform, MicroBenchType microBenchType, SessionFactory sf,
                int rowStartId, int rowsPerTx, boolean distributedPKOps, LockMode lockMode) {
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
  }

  @Override
  public void run() {
    Session dbSession = sf.getSession();
    while (true) {
      try {
        dbSession.currentTransaction().begin();
        readData(dbSession);
        dbSession.currentTransaction().commit();
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

//    Object keys[][] = new Object[2][numRows];
//    ArrayList<TableDTO> batch = new ArrayList<TableDTO>(numRows);
//    for(int i = 0; i < numRows; i++){
//      TableDTO dto = session.newInstance(TableDTO.class);
//      dto.setPartitionId(partitionId);
//      dto.setId(i);
//      dto.setIntCol1(-100);
//      batch.add(dto);
//    }
//
//    session.load(batch);
//
//    session.flush();
//
//    for(int i = 0; i < numRows; i++){
//      TableDTO dto  = batch.get(i);
//      if(dto.getIntCol1() != partitionId){
//        System.out.println("Wrong data read. Expecting: "+partitionId+" read: "+dto.getIntCol1());
//      }
//    }
//    session.release(batch);

  void pkRead(Session session) {
    boolean partitionKeyHintSet = false;
    for (int i = 0; i < rowsPerTx; i++) {
      int rowId = rowStartId + i;
      Object key[] = new Object[2];
      key[0] = getPartitionKey(rowId);
      key[1] = rowId;

      if (!partitionKeyHintSet) {
        session.setPartitionKey(TableWithUDP.class, key);
        partitionKeyHintSet = true;
      }
      session.setLockMode(lockMode);
      session.find(TableWithUDP.class, key);
    }
  }

  void batchRead(Session session) {

  }

  void ppisRead(Session session) {

  }

  void isRead(Session session) {

  }

  void ftsRead(Session session) {

  }

  protected void writeData() throws Exception {
    Session session = sf.getSession();
    session.currentTransaction().begin();

    System.out.println("Wriring Data for thread no: " + threadId);
    for (int i = 0; i < rowsPerTx; i++) {
      Table row = getTableInstance(session);
      int rowId = rowStartId + i;
      row.setId(rowId);
      row.setPartitionId(getPartitionKey(rowId));
      row.setData(0);
      System.out.println(row.getId() + "\t\t" + row.getPartitionId() + "\t\t" + row.getData());
      session.makePersistent(row);
    }
    session.currentTransaction().commit();
    session.close();
  }

  private void deleteAllData() throws Exception {
    Session session = sf.getSession();
    session.deletePersistentAll(TableWithOutUDP.class);
    session.deletePersistentAll(TableWithUDP.class);
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

