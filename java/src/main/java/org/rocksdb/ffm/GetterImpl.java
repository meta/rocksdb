package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemorySegment.NULL;
import static org.rocksdb.ffm.util.FFMSegment.dereference;
import static org.rocksdb.ffm.RocksFFMException.throwOnError;

final class GetterImpl extends WithDB implements Getter {
  private final MemorySegment key;
  private final MemorySegment readOptions;

  GetterImpl(SegmentAllocator allocator, MemorySegment db, MemorySegment key) {
    super(allocator, db);
    this.key = key;
    readOptions = rocksdb_c_api.rocksdb_readoptions_create();
  }

  public MemorySegment invoke() throws RocksFFMException {
    MemorySegment valueLength = allocator.allocateFrom(ValueLayout.JAVA_LONG, 0L);
    MemorySegment error = allocator.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment value =
        rocksdb_c_api.rocksdb_get(db, readOptions, key, key.byteSize(), valueLength, error);
    throwOnError(error, message -> String.format("rocksdb_get() %s", message));
    return dereference(value, valueLength);
  }
}
