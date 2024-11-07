package org.svv.iceberg.kafka.utils;

import java.util.stream.Stream;

public class TestUtils {

  public static String randomString(int length) {
    String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int index = (int) (characters.length() * Math.random());
      sb.append(characters.charAt(index));
    }
    return sb.toString();
  }

  public static Stream<String> randomStringStream() {
    return Stream.generate(() -> randomString(1000));
  }
}
