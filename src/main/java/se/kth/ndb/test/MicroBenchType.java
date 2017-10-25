package se.kth.ndb.test;

public enum MicroBenchType {
  PK_D ("PK_D"),
  PK_ND ("PK_ND"),
  BATCH_D ("BATCH_D"),
  BATCH_ND ("BATCH_ND"),
  PPIS("PPIS"),
  IS("IS"),
  FTS("FTS"),
  PK_D_WRITE("PK_D_WRITE"),
  PK_ND_WRITE("PK_ND_WRITE"),
  BATCH_D_WRITE("BATCH_D_WRITE"),
  BATCH_ND_WRITE("BATCH_ND_WRITE");
  private final String name;
  private MicroBenchType(String name){
    this.name = name;
  }

  public boolean equalsName(String otherName) {
    // (otherName == null) check is not needed because name.equals(null) returns false
    return name.equals(otherName);
  }

  public String toString() {
    return this.name;
  }

}
