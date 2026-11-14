// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Second-tier cache below the primary block cache.
 * Stores blocks in compressed form or on non-volatile storage.
 */
public abstract class SecondaryCache extends RocksObject {
  protected SecondaryCache(final long nativeHandle) {
    super(nativeHandle);
  }

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  /**
   * Creates a CompressedSecondaryCache with default settings.
   *
   * @param capacity cache size in bytes
   * @return a new CompressedSecondaryCache
   */
  public static SecondaryCache newCompressedSecondaryCache(final long capacity) {
    return new CompressedSecondaryCache(capacity);
  }

  /**
   * Creates a CompressedSecondaryCache with custom options.
   *
   * @param capacity cache size in bytes
   * @param numShardBits shard bits (2^n shards), -1 for auto
   * @param strictCapacityLimit if true, reject inserts when full
   * @param highPriPoolRatio fraction reserved for high-priority entries (0.0-1.0)
   * @return a new CompressedSecondaryCache
   */
  public static SecondaryCache newCompressedSecondaryCache(final long capacity,
      final int numShardBits, final boolean strictCapacityLimit, final double highPriPoolRatio) {
    return new CompressedSecondaryCache(
        capacity, numShardBits, strictCapacityLimit, highPriPoolRatio);
  }

  /**
   * Compressed cache using LZ4 compression.
   */
  private static class CompressedSecondaryCache extends SecondaryCache {
    private CompressedSecondaryCache(final long capacity) {
      super(newCompressedSecondaryCacheInstance(capacity));
    }

    private CompressedSecondaryCache(final long capacity, final int numShardBits,
        final boolean strictCapacityLimit, final double highPriPoolRatio) {
      super(newCompressedSecondaryCacheInstance(
          capacity, numShardBits, strictCapacityLimit, highPriPoolRatio));
    }

    private static native long newCompressedSecondaryCacheInstance(long capacity);

    private static native long newCompressedSecondaryCacheInstance(
        long capacity, int numShardBits, boolean strictCapacityLimit, double highPriPoolRatio);
  }

  private static native void disposeInternalJni(final long handle);
}
