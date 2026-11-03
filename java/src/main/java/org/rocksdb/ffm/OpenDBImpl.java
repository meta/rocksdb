package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.rocksdb.ffm.RocksFFM.*;

final class OpenDBImpl extends WithDB implements RocksFFM.OpenDB {
  OpenDBImpl(Arena arena, MemorySegment db) {
    super(arena, db);
  }

  public RocksFFM.Getter get(MemorySegment key) {
    return new GetterImpl(arena, db, key);
  }

  public RocksFFM.Getter get(String key) {
    return new GetterImpl(arena, db, RocksFFM.segmentOf(arena, key));
  }

  public RocksFFM.Putter put(MemorySegment key, MemorySegment value) {
    return new PutterImpl(arena, db, key, value);
  }

  public RocksFFM.Putter put(String key, MemorySegment value) {
    return new PutterImpl(arena, db, RocksFFM.segmentOf(arena, key), value);
  }
}
