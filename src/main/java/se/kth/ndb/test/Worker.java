package se.kth.ndb.test;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class Worker implements Runnable {
  final int threadId;
  final AtomicInteger opsCompleted;
  final AtomicInteger successfulOps;
  final AtomicInteger failedOps;
  final long maxOperationToPerform;
  final MicroBenchType microiBenchType;
  final int clientId;
  final SessionFactory sf;

  public Worker(int threadId, AtomicInteger opsCompleted, AtomicInteger successfulOps, AtomicInteger failedOps,
                long maxOperationToPerform, MicroBenchType microBenchType, SessionFactory sf,
                int clientId) {
    this.opsCompleted = opsCompleted;
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.threadId = threadId;
    this.maxOperationToPerform = maxOperationToPerform;
    this.microiBenchType = microBenchType;
    this.sf = sf;
    this.clientId = clientId;
  }

  @Override
  public void run() {
    Session dbSession = sf.getSession();
    while (true) {
      try {
        dbSession.currentTransaction().begin();
//        Object key[] = new Object[2];
//        key[0] = partitionId;
//        key[1] = new Integer(0);
//
//        dbSession.setPartitionKey(TableDTO.class, key);
//
//        dbSession.setLockMode(lockMode);
//        readData(dbSession, partitionId);
//
//        dbSession.currentTransaction().commit();
//        successfulOps.addAndGet(1);
//        speed.incrementAndGet();
//      } catch (Throwable e) {
//        opsCompleted.incrementAndGet();
//        failedOps.addAndGet(1);
//        e.printStackTrace();
//        dbSession.currentTransaction().rollback();
      } finally {
        if (opsCompleted.incrementAndGet() >= maxOperationToPerform) {
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

  public void readData(Session session, int partitionId) throws Exception {
    Object keys[][] = new Object[2][numRows];
    ArrayList<TableDTO> batch = new ArrayList<TableDTO>(numRows);
    for(int i = 0; i < numRows; i++){
      TableDTO dto = session.newInstance(TableDTO.class);
      dto.setPartitionId(partitionId);
      dto.setId(i);
      dto.setIntCol1(-100);
      batch.add(dto);
    }

    session.load(batch);

    session.flush();

    for(int i = 0; i < numRows; i++){
      TableDTO dto  = batch.get(i);
      if(dto.getIntCol1() != partitionId){
        System.out.println("Wrong data read. Expecting: "+partitionId+" read: "+dto.getIntCol1());
      }
    }
    session.release(batch);
  }

  private void write() throws Exception {
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
        session.makePersistent(row);
      }
    }
    session.currentTransaction().commit();
    session.close();
    System.out.println("Test data created.");
  }

  private void deleteAllData() throws Exception {
    Session session = sf.getSession();
    session.deletePersistentAll(TableDTO.class);
    session.close();
  }
}

