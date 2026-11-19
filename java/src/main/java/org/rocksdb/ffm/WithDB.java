package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;

abstract class WithDB extends WithAllocator {
  protected final MemorySegment db;

  protected WithDB(SegmentAllocator allocator, MemorySegment db) {
    super(allocator);
    this.db = db;
  }
}
