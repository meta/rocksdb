// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The IO priority for rate limiting.
 */
public enum IOPriority {
  IO_LOW((byte) 0x0),
  IO_MID((byte) 0x1),
  IO_HIGH((byte) 0x2),
  IO_USER((byte) 0x3),
  IO_TOTAL((byte) 0x4);

  private final byte value;

  IOPriority(final byte value) {
    this.value = value;
  }

  /**
   * <p>Returns the byte value of the enumerations value.</p>
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * <p>Returns the IOPriority enumeration value for the provided byte value.</p>
   *
   * @param value byte representation of IOPriority.
   *
   * @return {@link org.rocksdb.IOPriority} instance.
   *
   * @throws IllegalArgumentException if IOPriority cannot be found for the provided value.
   */
  public static IOPriority getIOPriority(final byte value) {
    for (final IOPriority ioPriority : IOPriority.values()) {
      if (ioPriority.getValue() == value) {
        return ioPriority;
      }
    }
    throw new IllegalArgumentException("Illegal value provided for IOPriority: " + value);
  }
}
