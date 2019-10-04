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
                    "-numThreads", "1", "-microBenchType","RANGE_SCAN",
            "-bmDuration", "10000", "-rangScanMaxRowID", "10000", "-rangScanSize", "100",
            "-skipPrompt"};
    new MicroBenchMain().startApplication(argv);
  }
}
