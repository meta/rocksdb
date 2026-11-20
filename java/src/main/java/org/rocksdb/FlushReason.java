// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum FlushReason {
  OTHERS((byte) 0x00),
  GET_LIVE_FILES((byte) 0x01),
  SHUTDOWN((byte) 0x02),
  EXTERNAL_FILE_INGESTION((byte) 0x03),
  MANUAL_COMPACTION((byte) 0x04),
  WRITE_BUFFER_MANAGER((byte) 0x05),
  WRITE_BUFFER_FULL((byte) 0x06),
  TEST((byte) 0x07),
  DELETE_FILES((byte) 0x08),
  AUTO_COMPACTION((byte) 0x09),
  MANUAL_FLUSH((byte) 0x0a),
  ERROR_RECOVERY((byte) 0x0b),
  ERROR_RECOVERY_RETRY_FLUSH((byte) 0x0c),
  WAL_FULL((byte) 0x0d),
  CATCH_UP_AFTER_ERROR_RECOVERY((byte) 0x0e),
  MEMTABLE_MAX_RANGE_DELETIONS((byte) 0x0f);

  private final byte value;

  FlushReason(final byte value) {
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
   * Get the FlushReason from the internal representation value.
   *
   * @return the flush reason.
   *
   * @throws IllegalArgumentException if the value is unknown.
   */
  static FlushReason fromValue(final byte value) {
    for (final FlushReason flushReason : values()) {
      if (flushReason.value == value) {
        return flushReason;
      }
    }

    throw new IllegalArgumentException("Illegal value provided for FlushReason: " + value);
  }
}
