package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;

public abstract class NativeSegment<T extends NativeSegment<T>> {
  protected final MemorySegment memorySegment;

  protected NativeSegment(MemorySegment memorySegment) {
    this.memorySegment = memorySegment;
  }

  public MemorySegment nativeSegment() {
    return memorySegment;
  }
}
