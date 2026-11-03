package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

abstract class WithDB extends ArenaHolder {
  protected final MemorySegment db;

  protected WithDB(Arena arena, MemorySegment db) {
    super(arena);
    this.db = db;
  }
}
