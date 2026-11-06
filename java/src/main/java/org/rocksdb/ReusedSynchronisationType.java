// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * Determines the type of synchronisation primitive used
 * in native code.
 */
public enum ReusedSynchronisationType {
  /**
   * Standard mutex.
   */
  MUTEX((byte)0x0),

  /**
   * Use adaptive mutex, which spins in the user space before resorting
   * to kernel. This could reduce context switch when the mutex is not
   * heavily contended. However, if the mutex is hot, we could end up
   * wasting spin time.
   */
  ADAPTIVE_MUTEX((byte)0x1),

  /**
   * There is a reused buffer per-thread.
   */
  THREAD_LOCAL((byte)0x2);

  private final byte value;

  ReusedSynchronisationType(final byte value) {
    this.value = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * Get ReusedSynchronisationType by byte value.
   *
   * @param value byte representation of ReusedSynchronisationType.
   *
   * @return {@link org.rocksdb.ReusedSynchronisationType} instance.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static ReusedSynchronisationType getReusedSynchronisationType(
      final byte value) {
    for (final ReusedSynchronisationType reusedSynchronisationType : values()) {
      if (reusedSynchronisationType.getValue() == value) {
        return reusedSynchronisationType;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for ReusedSynchronisationType.");
  }
}
