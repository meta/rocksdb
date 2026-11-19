package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemorySegment.NULL;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.rocksdb.ffm.RocksFFMException.throwOnError;

final class DBImpl extends WithAllocator implements DB {

  DBImpl(SegmentAllocator allocator) {
    super(allocator);
  }

  @Override
  public OpenDB open(String path) throws RocksFFMException {
    try (RocksdbOptions dbOptions = new RocksdbOptions()) {
      return open(dbOptions, path);
    }
  }

  @Override
  public OpenDB open(RocksdbOptions dbOptions, String path) throws RocksFFMException {
    // See RocksDB rules on options structs
    MemorySegment pathName = allocator.allocateFrom(path, UTF_8);
    MemorySegment errptr = allocator.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment db = rocksdb_c_api_2.rocksdb_open(dbOptions.nativeSegment(), pathName, errptr);
    throwOnError(errptr, message -> String.format("rocksdb_get() %s", message));

    return new OpenDBImpl(allocator, db);
  }
}
