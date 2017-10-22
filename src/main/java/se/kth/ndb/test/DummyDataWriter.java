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

public class DummyDataWriter implements Runnable {
  final AtomicInteger successfulOps;
  final AtomicInteger speed;
  final SessionFactory sf;
  final int rowStartId;
  final int rowEndId;


  public DummyDataWriter(int threadId, AtomicInteger successfulOps, AtomicInteger speed, SessionFactory sf,
                         int rowStartId, int rowEndId) {
    this.successfulOps = successfulOps;
    this.speed = speed;
    this.sf = sf;
    this.rowStartId = rowStartId;
    this.rowEndId = rowEndId;
  }

  @Override
  public void run() {
    Session dbSession = sf.getSession();
    for (int i = rowStartId; i < rowEndId; i++) {
      try {
        dbSession.currentTransaction().begin();
        writeData(dbSession, i);
        dbSession.currentTransaction().commit();
        successfulOps.incrementAndGet();
        speed.incrementAndGet();
      } catch (Throwable e) {
        e.printStackTrace();
        dbSession.currentTransaction().rollback();
      }
    }
    dbSession.close();
  }

  @Override
  protected void finalize() throws Throwable {
  }


  protected void writeData(Session session, int id) throws Exception {
    Table row = session.newInstance(TableWithUDP.class);
    row.setId(id);
    row.setPartitionId(id);
    row.setData(id);
    session.savePersistent(row);

    row = session.newInstance(TableWithOutUDP.class);
    row.setId(id);
    row.setPartitionId(id);
    row.setData(id);
    session.savePersistent(row);
  }
}

