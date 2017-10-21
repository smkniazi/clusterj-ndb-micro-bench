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
  final MicroBenchType microBenchType;
  final SessionFactory sf;
  final int rowStartId;
  final int rowsPerTx;
  final boolean distributedBatch;

  public Worker(int threadId, AtomicInteger opsCompleted, AtomicInteger successfulOps, AtomicInteger failedOps,
                long maxOperationToPerform, MicroBenchType microBenchType, SessionFactory sf,
                int rowStartId, int rowsPerTx, boolean distributedBatch) {
    this.threadId=threadId;
    this.opsCompleted = opsCompleted;
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.maxOperationToPerform = maxOperationToPerform;
    this.microBenchType = microBenchType;
    this.sf = sf;
    this.rowStartId = rowStartId;
    this.rowsPerTx = rowsPerTx;
    this.distributedBatch = distributedBatch;
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
  }

  public void readData(Session session, int partitionId) throws Exception {
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
  }

  protected void writeData() throws Exception {
    Session session = sf.getSession();
    session.currentTransaction().begin();

    for (int i = 0; i < rowsPerTx; i++) {
        Table row = getTableInstance(session);
        int rowId = rowStartId + i;
        row.setId(rowId);
        row.setPartitionId(getPartitionKey(rowId));
        row.setData(0);
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

  private int getPartitionKey(int rowId){

    switch (microBenchType){
      case PK:
        return rowId;
      case BATCH:
        if(distributedBatch){
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
        throw  new IllegalStateException("Micro bench mark not supported");
    }
  }

  private Table getTableInstance(Session session){
    if(microBenchType == MicroBenchType.BATCH || microBenchType == MicroBenchType.PK ||
            microBenchType == MicroBenchType.PPIS){
      return session.newInstance(TableWithUDP.class);
    } else if( microBenchType == MicroBenchType.FTS || microBenchType == MicroBenchType.IS){
      return session.newInstance(TableWithOutUDP.class);
    } else{
      throw new IllegalStateException("Micro benchmark type not supported");
    }
  }
}

