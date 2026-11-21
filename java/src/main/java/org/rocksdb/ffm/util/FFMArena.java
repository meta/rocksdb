package org.rocksdb.ffm.util;

import java.lang.foreign.Arena;

public class FFMArena {

  private static Arena smallAuto = Arena.ofAuto();

  /**
   * An Arena for allocating small segments which can be GC-ed
   *
   * @return the static arena created for this purpose.
   */
  public static Arena smallAuto() {
    return smallAuto;
  }
}
