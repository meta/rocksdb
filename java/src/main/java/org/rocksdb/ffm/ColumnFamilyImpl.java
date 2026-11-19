package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;

public class ColumnFamilyImpl extends WithDB implements ColumnFamily {

  protected ColumnFamilyImpl(SegmentAllocator allocator, MemorySegment db) {
    super(allocator, db);
  }

  @Override
  public ColumnFamilyHandle defaultFamilyHandle() {
    return new ColumnFamilyHandle(rocksdb_c_api.rocksdb_get_default_column_family_handle(db));
  }
}
