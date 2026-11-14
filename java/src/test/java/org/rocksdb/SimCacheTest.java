// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

public class SimCacheTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Test
  public void newSimCache() {
    final long capacity = 80000000;
    final int numShardBits = 16;
    final boolean strictCapacityLimit = true;
    final double highPriPoolRatio = 0.5;
    final double lowPriPoolRatio = 0.5;
    try (final Cache lruCache = new LRUCache(
          capacity, numShardBits, strictCapacityLimit, highPriPoolRatio, lowPriPoolRatio)) {
      try (final Cache simCache = new SimCache(
            lruCache.getNativeHandle(), capacity, numShardBits)) {
        //no op
        assertThat(simCache.getUsage()).isGreaterThanOrEqualTo(0);
        assertThat(simCache.getPinnedUsage()).isGreaterThanOrEqualTo(0);
      }
    }
  }
}
