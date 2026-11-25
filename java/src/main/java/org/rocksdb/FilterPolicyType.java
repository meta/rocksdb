// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * IndexType used in conjunction with BlockBasedTable.
 */
public enum FilterPolicyType {
  kUnknownFilterPolicy((byte) 0),

  /**
   * This is a user-facing policy that automatically choose between
   * LegacyBloom and FastLocalBloom based on context at build time,
   * including compatibility with format_version.
   */
  kBloomFilterPolicy((byte) 1),

  /**
   * This is a user-facing policy that chooses between Standard128Ribbon
   * and FastLocalBloom based on context at build time (LSM level and other
   * factors in extreme cases).
   */
  kRibbonFilterPolicy((byte) 2);

  public Filter createFilter(final long handle, final double param) {
    if (this == kBloomFilterPolicy) {
      return new BloomFilter(handle, param);
    } else if (this == kRibbonFilterPolicy) {
      return new RibbonFilter(handle, param);
    }
    return null;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  FilterPolicyType(byte value) {
    value_ = value;
  }

  private final byte value_;
}
