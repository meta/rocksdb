package org.rocksdb.ffm;

import java.lang.foreign.MemorySegment;

public class ColumnFamilyHandle extends NativeSegment<ColumnFamilyHandle> implements AutoCloseable {
  protected ColumnFamilyHandle(MemorySegment memorySegment) {
    super(memorySegment);
  }

  @Override
  public void close() {
    rocksdb_c_api.rocksdb_column_family_handle_destroy(nativeSegment());
  }
}
