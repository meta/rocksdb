package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemorySegment.NULL;
import static org.rocksdb.ffm.RocksFFMException.throwOnError;
import static org.rocksdb.ffm.util.FFMSegment.segmentOf;

public class ColumnFamilyImpl extends WithDB implements ColumnFamily {

  protected ColumnFamilyImpl(SegmentAllocator allocator, MemorySegment db) {
    super(allocator, db);
  }

  @Override
  public ColumnFamilyHandle defaultFamilyHandle() {
    return new ColumnFamilyHandle(rocksdb_c_api.rocksdb_get_default_column_family_handle(db));
  }

  @Override
  public ColumnFamilyHandle createColumnFamily(String columnFamilyName, RocksdbOptions columnFamilyOptions) throws RocksFFMException {
    MemorySegment error = allocator.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment segment = rocksdb_c_api.rocksdb_create_column_family(
      db,
      columnFamilyOptions.nativeSegment(),
      segmentOf(allocator, columnFamilyName),
      error);
    throwOnError(error, message -> String.format("rocksdb_get() %s", message));

    return new ColumnFamilyHandle(segment);
  }
}
