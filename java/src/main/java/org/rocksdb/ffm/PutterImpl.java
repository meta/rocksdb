package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemorySegment.NULL;
import static org.rocksdb.ffm.RocksFFMException.throwOnError;

final class PutterImpl extends WithDB implements Putter {
  private final MemorySegment key;
  private final MemorySegment value;
  private final MemorySegment writeOptions;

  PutterImpl(SegmentAllocator allocator, MemorySegment db, MemorySegment key, MemorySegment value) {
    super(allocator, db);
    this.key = key;
    this.value = value;
    writeOptions = rocksdb_c_api.rocksdb_writeoptions_create();
  }

  @Override
  public void invoke() throws RocksFFMException {
    MemorySegment error = allocator.allocateFrom(ValueLayout.ADDRESS, NULL);
    rocksdb_c_api.rocksdb_put(
        db, writeOptions, key, key.byteSize(), value, value.byteSize(), error);
    throwOnError(error, message -> String.format("rocksdb_get() %s", message));
  }
}
