//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum SanityLevel {
  NONE((byte) 0x0),
  LOOSELY_COMPATIBLE((byte) 0x1),
  EXACT_MATCH((byte) 0xFF);

  private final byte value;

  SanityLevel(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation value.
   *
   * @return the internal representation value.
   */
  byte getValue() {
    return value;
  }

  /**
   * Get the SanityLevel from the internal representation value.
   *
   * @param value the internal representation value.
   *
   * @return the SanityLevel
   *
   * @throws IllegalArgumentException if the value does not match a
   *     SanityLevel
   */
  static SanityLevel fromValue(final byte value) throws IllegalArgumentException {
    for (final SanityLevel level : values()) {
      if (level.value == value) {
        return level;
      }
    }
    throw new IllegalArgumentException("Unknown value for SanityLevel: " + value);
  }
}
