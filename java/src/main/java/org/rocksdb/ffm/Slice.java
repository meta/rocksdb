package org.rocksdb.ffm;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Helper class for converting efficiently between Java and the RocksDB slice_t type which is heavily used in RocksDB.
 */
public class Slice {

  private static final VarHandle SLICE_ARRAY_DATA = rocksdb_slice_t.layout().arrayElementVarHandle(MemoryLayout.PathElement.groupElement("data"));
  private static final VarHandle SLICE_ARRAY_SIZE = rocksdb_slice_t.layout().arrayElementVarHandle(MemoryLayout.PathElement.groupElement("size"));

  /**
   * Allocate and fill an array of native slice_t entries with slices for the supplied set of (presumably) keys
   * First allocate a native segment for each key, and copy the key into the native segment.
   * The slices we create will refer to the allocated native segments.
   *
   * @param allocator a native allocator which the client can manage the lifecycle of.
   * @param keys to set as a slice array
   * @return a memory segment which is a native slice array, and can be passed to `C` APIs.
   */
  public static MemorySegment keysSegment(SegmentAllocator allocator, byte[][] keys) {
    MemorySegment slices = allocator.allocate(rocksdb_slice_t.layout(), keys.length);
    for (int i = 0; i < keys.length; i++) {
      byte[] key = keys[i];
      MemorySegment keyISegment = allocator.allocate(key.length, ValueLayout.JAVA_BYTE.byteAlignment());
      MemorySegment.copy(key, 0, keyISegment, ValueLayout.JAVA_BYTE, 0, key.length);
      SLICE_ARRAY_DATA.set(slices, 0L, (long)i, keyISegment);
      SLICE_ARRAY_SIZE.set(slices, 0L, (long)i, (long)key.length);
    }
    return slices;
  }

  /**
   * Allocate and fill an array of native slice_t entries with slices for the supplied set of (presumably) keys
   * First allocate a native segment for each key, and copy the key into the native segment.
   * The slices we create will refer to the allocated native segments.
   *
   * TODO (AP) - this method currently double copies, it's just a convenience wrapper to the {@code byte[]} variant.
   * We need to use a {@code CharsetEncoder.encodeLoop} to directly encode to a {@code ByteBuffer}
   * wrapping a big-enough {@code MemorySegment}
   *
   * @param allocator a native allocator which the client can manage the lifecycle of.
   * @param keys to set as a slice array
   * @return a memory segment which is a native slice array, and can be passed to `C` APIs.
   */
  public static MemorySegment keysSegment(SegmentAllocator allocator, String[] keys) {
    byte[][] keyBytes = new byte[keys.length][];
    for (int i = 0; i < keys.length; i++) {
      keyBytes[i] = keys[i].getBytes();
    }
    return keysSegment(allocator, keyBytes);
  }
}
