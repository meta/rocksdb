package org.rocksdb.ffm;

import java.lang.foreign.SegmentAllocator;

public interface DB extends AutoCloseable {
  Getter get();

  Getter get(SegmentAllocator allocator);

  Putter put();

  Putter put(SegmentAllocator allocator);

  MultiGetter multiGet();

  MultiGetter multiGet(SegmentAllocator allocator);

  ColumnFamily columnFamily();

  ColumnFamily columnFamily(SegmentAllocator allocator);

  void close();
}
