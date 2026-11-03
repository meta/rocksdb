package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemorySegment.NULL;
import static org.rocksdb.ffm.RocksFFM.dereference;
import static org.rocksdb.ffm.RocksFFM.throwOnError;

final class GetterImpl extends WithDB implements RocksFFM.Getter {
  private final MemorySegment key;
  private final MemorySegment readOptions;

  GetterImpl(Arena arena, MemorySegment db, MemorySegment key) {
    super(arena, db);
    this.key = key;
    readOptions = rocksdb_c_api.rocksdb_readoptions_create();
  }

  public MemorySegment invoke() throws RocksFFMException {
    MemorySegment valueLength = arena.allocateFrom(ValueLayout.JAVA_LONG, 0L);
    MemorySegment error = arena.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment value =
        rocksdb_c_api_2.rocksdb_get(db, readOptions, key, key.byteSize(), valueLength, error);
    throwOnError(error, message -> String.format("rocksdb_get() %s", message));
    return dereference(value, valueLength);
  }
}
