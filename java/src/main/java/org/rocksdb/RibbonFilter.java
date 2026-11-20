// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Objects;

public class RibbonFilter extends Filter {
  private static final double DEFAULT_BITS_PER_KEY = 10.0;
  private static final int DEFAULT_BLOOM_BEFORE_LEVEL = -1;

  private final double bitsPerKey;
  private final int bloomBeforeLevel;

  /**
   * Creates a new Ribbon filter with default bits per key (10.0).
   */
  public RibbonFilter() {
    this(DEFAULT_BITS_PER_KEY);
  }

  /**
   * Creates a new Ribbon filter with the specified bits per key.
   *
   * @param bitsPerKey the number of bits per key (typical: 8-12)
   */
  public RibbonFilter(final double bitsPerKey) {
    this(createNewRibbonFilter(bitsPerKey, DEFAULT_BLOOM_BEFORE_LEVEL), bitsPerKey,
        DEFAULT_BLOOM_BEFORE_LEVEL);
  }

  /**
   * Internal constructor for FilterPolicyType
   *
   * @param nativeHandle handle to existing ribbon filter at RocksDB C++ side
   * @param bitsPerKey number of bits to use - recorded for comparison
   */
  RibbonFilter(final long nativeHandle, final double bitsPerKey) {
    this(nativeHandle, bitsPerKey, DEFAULT_BLOOM_BEFORE_LEVEL);
  }

  /**
   * Creates a new Ribbon filter with bits per key and bloom_before_level.
   *
   * @param bitsPerKey the number of bits per key (typical: 8-12)
   * @param bloomBeforeLevel use Bloom filters for levels below this, Ribbon for levels at or above
   *                         (-1 means always use Ribbon, 0 means use Bloom for L0,
   *                         1 means Bloom for L0-L1, etc.)
   */
  public RibbonFilter(final double bitsPerKey, final int bloomBeforeLevel) {
    this(createNewRibbonFilter(bitsPerKey, bloomBeforeLevel), bitsPerKey, bloomBeforeLevel);
  }

  /**
   * Internal constructor
   *
   * @param nativeHandle handle to existing ribbon filter at RocksDB C++ side
   * @param bitsPerKey number of bits to use - recorded for comparison
   * @param bloomBeforeLevel bloom before level setting - recorded for comparison
   */
  private RibbonFilter(
      final long nativeHandle, final double bitsPerKey, final int bloomBeforeLevel) {
    super(nativeHandle);
    this.bitsPerKey = bitsPerKey;
    this.bloomBeforeLevel = bloomBeforeLevel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    RibbonFilter that = (RibbonFilter) o;
    return Double.compare(that.bitsPerKey, bitsPerKey) == 0
        && bloomBeforeLevel == that.bloomBeforeLevel;
  }

  @Override
  public int hashCode() {
    return Objects.hash(bitsPerKey, bloomBeforeLevel);
  }

  private static native long createNewRibbonFilter(
      final double bitsPerKey, final int bloomBeforeLevel);
}
