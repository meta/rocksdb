package org.rocksdb.ffm.util;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.MemorySegment.NULL;
import static java.nio.charset.StandardCharsets.UTF_8;

public class FFMSegment {

  public static String ofStringOfSize(MemorySegment stringSegment, MemorySegment sizeSegment) {
    long size = sizeSegment.get(ValueLayout.JAVA_LONG, 0);
    byte[] bytes = new byte[Math.toIntExact(size)];
    MemorySegment result = MemorySegment.ofArray(bytes);
    MemorySegment.copy(stringSegment, ValueLayout.JAVA_BYTE, 0, result, ValueLayout.JAVA_BYTE, 0, bytes.length);
    return new String(bytes, 0, (int) size, StandardCharsets.UTF_8);
  }

  public static MemorySegment segmentOf(SegmentAllocator allocator, String s) {
    MemorySegment segment = allocator.allocateFrom(s, UTF_8);
    return segment.asSlice(0, segment.byteSize() - 1);
  }

  /**
   * Extract a string from a segment
   * `MemorySegment.getString(offset, charset)` doesn't work for us, as it expects a C-style
   * null-terminated string
   *
   * @param segment to interpret as a String
   * @return the Java string contained in `segment`
   */
  public static String stringOf(MemorySegment segment) {
    int byteSize = (int) segment.byteSize();
    byte[] bytes = new byte[byteSize];
    MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, bytes, 0, byteSize);
    return new String(bytes, UTF_8);
  }

  public static MemorySegment dereference(MemorySegment reference, MemorySegment length) {
    if (reference.address() == NULL.address()) {
      // NULL returned as a value, usually when a get() status is NOT_FOUND
      return null;
    }
    long lengthValue = length.get(ValueLayout.JAVA_LONG, 0);
    return reference.asSlice(0L, lengthValue);
  }
}
