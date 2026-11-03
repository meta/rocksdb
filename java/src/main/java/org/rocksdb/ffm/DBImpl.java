package org.rocksdb.ffm;

import org.rocksdb.ffm.RocksFFM.OpenDB;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.MemorySegment.NULL;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.rocksdb.ffm.RocksFFM.throwOnError;

final class DBImpl extends ArenaHolder implements RocksFFM.DB {
  private final MemorySegment dbOptions = rocksdb_c_api_2.rocksdb_options_create();

  DBImpl(Arena arena) {
    super(arena);
  }

  public DBImpl option() {
    return this;
  }

  @Override
  public OpenDB open(String path) throws RocksFFMException {
    // See RocksDB rules on options structs
    MemorySegment pathName = arena.allocateFrom(path, UTF_8);
    MemorySegment errptr = arena.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment db = rocksdb_c_api_2.rocksdb_open(dbOptions, pathName, errptr);
    throwOnError(errptr, message -> String.format("rocksdb_get() %s", message));

    return new OpenDBImpl(arena, db);
  }
}
