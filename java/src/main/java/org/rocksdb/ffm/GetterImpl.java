package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemorySegment.NULL;
import static org.rocksdb.ffm.util.FFMSegment.dereference;
import static org.rocksdb.ffm.RocksFFMException.throwOnError;

final class GetterImpl extends WithDB implements Getter {

  GetterImpl(SegmentAllocator allocator, MemorySegment db) {
    super(allocator, db);
  }

  @Override
  public MemorySegment get(RocksdbReadoptions readOptions, MemorySegment key) throws RocksFFMException {
    MemorySegment valueLength = allocator.allocateFrom(ValueLayout.JAVA_LONG, 0L);
    MemorySegment error = allocator.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment value =
      rocksdb_c_api.rocksdb_get(db, readOptions.nativeSegment(), key, key.byteSize(), valueLength, error);
    throwOnError(error, message -> String.format("rocksdb_get() %s", message));
    return dereference(value, valueLength);
  }

  @Override
  public MemorySegment get(RocksdbReadoptions readOptions, ColumnFamilyHandle columnFamilyHandle, MemorySegment key) throws RocksFFMException {
    MemorySegment valueLength = allocator.allocateFrom(ValueLayout.JAVA_LONG, 0L);
    MemorySegment error = allocator.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment value =
      rocksdb_c_api.rocksdb_get_cf(db, readOptions.nativeSegment(), columnFamilyHandle.nativeSegment(), key, key.byteSize(), valueLength, error);
    throwOnError(error, message -> String.format("rocksdb_get() %s", message));
    return dereference(value, valueLength);
  }
}
