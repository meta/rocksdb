package org.rocksdb.ffm;

import java.lang.foreign.Arena;

class ArenaHolder {
  protected final Arena arena;

  protected ArenaHolder(Arena arena) {
    this.arena = arena;
  }
}
