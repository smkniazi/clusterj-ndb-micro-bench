package se.kth.ndb.test;

/**
 * Created by salman on 2016-08-17.
 */
public class Main {
  public static void main(String argv[]) throws Exception {
    new MicroBenchMain().startApplication(argv);
//    test();
  }

  public static void test() throws Exception {
    String argv[] = {"-schema", "test", "-dbHost", "localhost:1186", "-rowsPerTx", "10",
                    "-numThreads", "2", "-microBenchType","PK", "-distributedPKOps",
            "-bmDuration", "10000"};
    new MicroBenchMain().startApplication(argv);
  }
}
