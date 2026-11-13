// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum BackgroundErrorReason {
  FLUSH((byte) 0x0),
  COMPACTION((byte) 0x1),
  WRITE_CALLBACK((byte) 0x2),
  MEMTABLE((byte) 0x3);

  private final byte value;

  BackgroundErrorReason(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation.
   *
   * @return the internal representation
   */
  byte getValue() {
    return value;
  }

  /**
   * Get the BackgroundErrorReason from the internal representation value.
   *
   * @return the background error reason.
   *
   * @throws IllegalArgumentException if the value is unknown.
   */
  static BackgroundErrorReason fromValue(final byte value) {
    for (final BackgroundErrorReason backgroundErrorReason : values()) {
      if (backgroundErrorReason.value == value) {
        return backgroundErrorReason;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for BackgroundErrorReason: " + value);
  }
}
