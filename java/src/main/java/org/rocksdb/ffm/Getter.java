package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;

public interface Getter {
  MemorySegment get(RocksdbReadoptions readOptions, MemorySegment key) throws RocksFFMException;
  MemorySegment get(RocksdbReadoptions readOptions, ColumnFamilyHandle columnFamilyHandle, MemorySegment key) throws RocksFFMException;
}
