package se.kth.ndb.test;

public enum MicroBenchType {
  PK_D ("PK_D"),
  PK_ND ("PK_ND"),
  BATCH_D ("BATCH_D"),
  BATCH_ND ("BATCH_ND"),
  PPIS("PPIS"),
  IS("IS"),
  FTS("FTS");
  private final String name;
  private MicroBenchType(String name){
    this.name = name;
  }
}
