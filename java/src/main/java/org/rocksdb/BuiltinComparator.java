// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Builtin RocksDB comparators
 *
 * <ol>
 *   <li>BYTEWISE_COMPARATOR - Sorts all keys in ascending bytewise
 *   order.</li>
 *   <li>REVERSE_BYTEWISE_COMPARATOR - Sorts all keys in descending bytewise
 *   order.</li>
 *   <li>BYTEWISE_COMPARATOR_WITH_U64_TS - Sorts keys in ascending bytewise
 *   order, with newer unsigned 64-bit timestamps first for each key.</li>
 *   <li>REVERSE_BYTEWISE_COMPARATOR_WITH_U64_TS - Sorts keys in descending
 *   bytewise order, with newer unsigned 64-bit timestamps first for each key.</li>
 * </ol>
 */
public enum BuiltinComparator {
  BYTEWISE_COMPARATOR,
  REVERSE_BYTEWISE_COMPARATOR,
  /**
   * Sorts keys in ascending bytewise order, with newer unsigned 64-bit
   * timestamps first for each key. Timestamps use eight little-endian bytes.
   * Reopening a column family requires the same comparator.
   *
   * @see #encodeU64Ts(long)
   */
  BYTEWISE_COMPARATOR_WITH_U64_TS,
  /**
   * Sorts keys in descending bytewise order, with newer unsigned 64-bit
   * timestamps first for each key. Timestamps use eight little-endian bytes.
   * Reopening a column family requires the same comparator.
   *
   * @see #encodeU64Ts(long)
   */
  REVERSE_BYTEWISE_COMPARATOR_WITH_U64_TS;

  /**
   * Encodes an unsigned 64-bit timestamp as eight little-endian bytes for
   * {@link #BYTEWISE_COMPARATOR_WITH_U64_TS} or
   * {@link #REVERSE_BYTEWISE_COMPARATOR_WITH_U64_TS}.
   *
   * <p>All bits of the {@code long} are preserved. Negative values represent
   * unsigned timestamps at or above 2^63; {@code -1L} is the maximum timestamp.
   *
   * @param timestamp the unsigned timestamp represented by a {@code long}
   * @return a new array containing the encoded timestamp
   * @see #decodeU64Ts(byte[])
   */
  public static byte[] encodeU64Ts(final long timestamp) {
    return ByteBuffer.allocate(Long.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putLong(timestamp)
        .array();
  }

  /**
   * Decodes an eight-byte little-endian timestamp used by
   * {@link #BYTEWISE_COMPARATOR_WITH_U64_TS} or
   * {@link #REVERSE_BYTEWISE_COMPARATOR_WITH_U64_TS}.
   *
   * <p>The returned {@code long} preserves the unsigned timestamp's bits.
   * Use {@link Long#compareUnsigned(long, long)} to compare decoded timestamps.
   *
   * @param timestamp the encoded timestamp
   * @return the unsigned timestamp represented by a {@code long}
   * @throws IllegalArgumentException if the array does not contain exactly eight bytes
   * @throws NullPointerException if {@code timestamp} is null
   * @see #encodeU64Ts(long)
   */
  public static long decodeU64Ts(final byte[] timestamp) {
    if (timestamp.length != Long.BYTES) {
      throw new IllegalArgumentException("U64 timestamp must contain exactly 8 bytes.");
    }
    return ByteBuffer.wrap(timestamp).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }
}
