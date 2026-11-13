//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum PerfLevel {
  /**
   * Unknown setting
   */
  UNINITIALIZED((byte) 0),
  /**
   * disable perf stats
   */
  DISABLE((byte) 1),
  /**
   * enable only count stats
   */
  ENABLE_COUNT((byte) 2),
  /**
   * Other than count stats, also enable time stats except for mutexes
   */
  ENABLE_TIME_EXCEPT_FOR_MUTEX((byte) 3),

  /**
   * Other than time, also measure CPU time counters. Still don't measure
   * time (neither wall time nor CPU time) for mutexes
   */
  ENABLE_TIME_AND_CPU_TIME_EXCEPT_FOR_MUTEX((byte) 4),
  /**
   * enable count and time stats
   */
  ENABLE_TIME((byte) 5),

  /**
   * Do not use
   * @deprecated It's here to just keep parity with C++ API.
   */
  @Deprecated OUT_OF_BOUNDS((byte) 6);

  PerfLevel(byte _value) {
    this._value = _value;
  }

  private final byte _value;

  public byte getValue() {
    return _value;
  }

  public static PerfLevel getPerfLevel(byte level) {
    for (PerfLevel l : values()) {
      if (l.getValue() == level) {
        return l;
      }
    }
    throw new IllegalArgumentException("Uknknown PerfLevel constant : " + level);
  }
}
