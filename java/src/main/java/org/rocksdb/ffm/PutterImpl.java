package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemorySegment.NULL;
import static org.rocksdb.ffm.RocksFFMException.throwOnError;

final class PutterImpl extends WithDB implements Putter {
  PutterImpl(SegmentAllocator allocator, MemorySegment db) {
    super(allocator, db);
  }

  @Override
  public void put(RocksdbWriteoptions writeOptions, MemorySegment key, MemorySegment value) throws RocksFFMException {
    MemorySegment error = allocator.allocateFrom(ValueLayout.ADDRESS, NULL);
    rocksdb_c_api.rocksdb_put(
        db, writeOptions.nativeSegment(), key, key.byteSize(), value, value.byteSize(), error);
    throwOnError(error, message -> String.format("rocksdb_get() %s", message));
  }
}
