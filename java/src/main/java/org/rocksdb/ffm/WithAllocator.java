package org.rocksdb.ffm;

import java.lang.foreign.SegmentAllocator;

class WithAllocator {
  protected final SegmentAllocator allocator;

  protected WithAllocator(SegmentAllocator allocator) {
    this.allocator = allocator;
  }
}
