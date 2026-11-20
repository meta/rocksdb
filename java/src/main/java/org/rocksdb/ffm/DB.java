package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;

public interface DB extends AutoCloseable {
  Getter get();

  Putter put();

  MultiGetter multiGet();

  MultiGetter multiGet(SegmentAllocator allocator);

  ColumnFamily columnFamily();

  ColumnFamily columnFamily(SegmentAllocator allocator);

  void close();
}
