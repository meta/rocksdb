package org.rocksdb.ffm.util;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

public class FFMSegment {

  public static String ofStringOfSize(MemorySegment stringSegment, MemorySegment sizeSegment) {
    long size = sizeSegment.get(ValueLayout.JAVA_LONG, 0);
    byte[] bytes = new byte[Math.toIntExact(size)];
    MemorySegment result = MemorySegment.ofArray(bytes);
    MemorySegment.copy(stringSegment, ValueLayout.JAVA_BYTE, 0, result, ValueLayout.JAVA_BYTE, 0, bytes.length);
    return new String(bytes, 0, (int) size, StandardCharsets.UTF_8);
  }
}
