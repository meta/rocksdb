package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;

public interface Getter {
  MemorySegment invoke() throws RocksFFMException;
}
