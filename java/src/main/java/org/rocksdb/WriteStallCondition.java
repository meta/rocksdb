// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum WriteStallCondition {
  DELAYED((byte) 0x0),
  STOPPED((byte) 0x1),
  NORMAL((byte) 0x2);

  private final byte value;

  WriteStallCondition(final byte value) {
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
   * Get the WriteStallCondition from the internal representation value.
   *
   * @return the flush reason.
   *
   * @throws IllegalArgumentException if the value is unknown.
   */
  static WriteStallCondition fromValue(final byte value) {
    for (final WriteStallCondition writeStallCondition : values()) {
      if (writeStallCondition.value == value) {
        return writeStallCondition;
      }
    }

    throw new IllegalArgumentException("Illegal value provided for WriteStallCondition: " + value);
  }
}
