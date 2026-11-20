package org.rocksdb.ffm;

import org.rocksdb.ffm.util.FFMSegment;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;

final class DBImpl extends WithDB implements DB {
  DBImpl(SegmentAllocator allocator, MemorySegment db) {
    super(allocator, db);
  }

  public Getter get() {
    return new GetterImpl(allocator, db);
  }

  public Putter put(MemorySegment key, MemorySegment value) {
    return new PutterImpl(allocator, db, key, value);
  }

  @Override
  public MultiGetter multiGet() {
    return new MultiGetter(allocator, db);
  }

  /**
   * A multi-getter, but override the DB's allocator for allocations when performing the {@code }multiGet()}
   *
   * @param allocator to be used by the multi-getter, in place of the DB's allocator
   * @return
   */
  @Override
  public MultiGetter multiGet(SegmentAllocator allocator) {
    return new MultiGetter(allocator, db);
  }

  @Override
  public ColumnFamily columnFamily() {
    return new ColumnFamilyImpl(allocator, db);
  }

  @Override
  public ColumnFamily columnFamily(SegmentAllocator allocator) {
    return new ColumnFamilyImpl(allocator, db);
  }

  public Putter put(String key, MemorySegment value) {
    return new PutterImpl(allocator, db, FFMSegment.segmentOf(allocator, key), value);
  }

  @Override
  public void close() {
    rocksdb_c_api_2.rocksdb_close(db);
  }
}
