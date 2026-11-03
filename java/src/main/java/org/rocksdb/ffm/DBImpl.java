package org.rocksdb.ffm;

import static java.lang.foreign.MemorySegment.NULL;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.rocksdb.ffm.RocksFFM.*;

final class DBImpl extends ArenaHolder implements RocksFFM.DB {
  private final MemorySegment dbOptions = rocksdb_c_api_2.rocksdb_options_create();

  DBImpl(Arena arena) {
    super(arena);
  }

  public DBImpl option() {
    return this;
  }

  @Override
  public OpenDB open(String path) {
    // See RocksDB rules on options structs
    MemorySegment pathName = arena.allocateFrom(path, UTF_8);
    MemorySegment errptr = arena.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment db = rocksdb_c_api_2.rocksdb_open(dbOptions, pathName, errptr);

    return new OpenDBImpl(arena, db);
  }
}
