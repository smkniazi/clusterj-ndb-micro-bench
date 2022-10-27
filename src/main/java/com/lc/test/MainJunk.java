package com.lc.test;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

public class MainJunk {
  public static void main(String argv[]){
    SecureRandom sr = new SecureRandom();
    byte buffer1[] = new byte[16];
    byte buffer2[] = new byte[64];
    sr.nextBytes(buffer1);
    byte[] b1encoded = Base64.getEncoder().encode(buffer1);
    sr.nextBytes(buffer2);
    byte[] b2encoded = Base64.getEncoder().encode(buffer2);

    System.out.println(new String(b1encoded, StandardCharsets.UTF_8));
    System.out.println(new String(b2encoded, StandardCharsets.UTF_8));
  }
}
