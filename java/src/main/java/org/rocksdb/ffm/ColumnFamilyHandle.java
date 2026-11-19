package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;

public class ColumnFamilyHandle extends NativeSegment<ColumnFamilyHandle> {
  protected ColumnFamilyHandle(MemorySegment memorySegment) {
    super(memorySegment);
  }
}
