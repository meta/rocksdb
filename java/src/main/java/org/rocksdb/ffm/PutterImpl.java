package org.rocksdb.ffm;

import static java.lang.foreign.MemorySegment.NULL;
import static org.rocksdb.ffm.RocksFFM.throwOnError;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.rocksdb.RocksDBException;

final class PutterImpl extends WithDB implements RocksFFM.Putter {
  private final MemorySegment key;
  private final MemorySegment value;
  private final MemorySegment writeOptions;

  PutterImpl(Arena arena, MemorySegment db, MemorySegment key, MemorySegment value) {
    super(arena, db);
    this.key = key;
    this.value = value;
    writeOptions = rocksdb_c_api.rocksdb_writeoptions_create();
  }

  @Override
  public void invoke() throws RocksDBException {
    MemorySegment error = arena.allocateFrom(ValueLayout.ADDRESS, NULL);
    rocksdb_c_api_2.rocksdb_put(
        db, writeOptions, key, key.byteSize(), value, value.byteSize(), error);
    throwOnError(error, message -> String.format("rocksdb_get() %s", message));
  }
}
