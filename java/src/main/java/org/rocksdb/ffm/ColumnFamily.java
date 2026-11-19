package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;

public interface ColumnFamily {

  public ColumnFamilyHandle defaultFamilyHandle();
}
