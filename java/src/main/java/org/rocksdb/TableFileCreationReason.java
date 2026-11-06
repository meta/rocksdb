// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum TableFileCreationReason {
  FLUSH((byte) 0x00),
  COMPACTION((byte) 0x01),
  RECOVERY((byte) 0x02),
  MISC((byte) 0x03),
  SST_FILE_WRITER((byte) 0x04);

  private final byte value;

  TableFileCreationReason(final byte value) {
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
   * Get the TableFileCreationReason from the internal representation value.
   *
   * @return the table file creation reason.
   *
   * @throws IllegalArgumentException if the value is unknown.
   */
  static TableFileCreationReason fromValue(final byte value) {
    for (final TableFileCreationReason tableFileCreationReason : values()) {
      if (tableFileCreationReason.value == value) {
        return tableFileCreationReason;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for TableFileCreationReason: " + value);
  }
}
