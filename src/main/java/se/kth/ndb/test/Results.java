package se.kth.ndb.test;

import com.mysql.clusterj.annotation.*;

@PersistenceCapable(table = "results")
public interface Results {
  @PrimaryKey
  @Column(name = "id")
  int getId();
  void setId(int id);

  @Column(name = "test")
  String  getTest();
  void setTest(String test);

  @Column(name = "threads")
  int getThreads();
  void setThreads(int threads);

  @Column(name = "speed")
  double getSpeed();
  void setSpeed(double speed);


  @Column(name = "latency")
  double getLatency();
  void setLatency(double latency);

  @Column(name = "run")
  int getRun();
  void setRun(int run);
}

