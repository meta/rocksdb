package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;

public interface Putter {
  void put(RocksdbWriteoptions writeoptions, MemorySegment key, MemorySegment value) throws RocksFFMException;
  void put(RocksdbWriteoptions writeoptions, ColumnFamilyHandle columnFamilyHandle, MemorySegment key, MemorySegment value) throws RocksFFMException;
}
