// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SecondaryCacheTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void newCompressedSecondaryCache() {
    try (final SecondaryCache secondaryCache =
             SecondaryCache.newCompressedSecondaryCache(1024 * 1024)) {
      assertThat(secondaryCache).isNotNull();
    }
  }

  @Test
  public void newCompressedSecondaryCacheWithOptions() {
    try (final SecondaryCache secondaryCache =
             SecondaryCache.newCompressedSecondaryCache(1024 * 1024, // 1MB capacity
                 0, // num_shard_bits (single shard for testing)
                 false, // strict_capacity_limit
                 0.5)) { // high_pri_pool_ratio
      assertThat(secondaryCache).isNotNull();
    }
  }

  @Test
  public void testLRUCacheWithSecondaryCache() throws RocksDBException {
    // Create a secondary cache
    try (final SecondaryCache secondaryCache =
             SecondaryCache.newCompressedSecondaryCache(8 * 1024 * 1024);
        // Create an LRU cache with the secondary cache
        final Cache primaryCache = new LRUCache(1024 * 1024, // 1MB primary cache
            0, // num_shard_bits
            false, // strict_capacity_limit
            0.5, // high_pri_pool_ratio
            0.0, // low_pri_pool_ratio
            secondaryCache)) {
      assertThat(primaryCache).isNotNull();
      assertThat(primaryCache.getUsage()).isGreaterThanOrEqualTo(0);
      assertThat(primaryCache.getPinnedUsage()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  public void testWithBlockBasedTableConfig() throws RocksDBException {
    try (final SecondaryCache secondaryCache =
             SecondaryCache.newCompressedSecondaryCache(16 * 1024 * 1024);
        final Cache primaryCache =
            new LRUCache(4 * 1024 * 1024, -1, false, 0.5, 0.0, secondaryCache);
        final Options options = new Options().setCreateIfMissing(true)) {
      final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
      tableConfig.setBlockCache(primaryCache);
      tableConfig.setBlockSize(4 * 1024);
      options.setTableFormatConfig(tableConfig);

      try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        assertThat(db).isNotNull();

        // Write some data
        for (int i = 0; i < 100; i++) {
          db.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        // Read some data
        for (int i = 0; i < 100; i++) {
          final byte[] value = db.get(("key" + i).getBytes());
          assertThat(value).isEqualTo(("value" + i).getBytes());
        }

        // Check that the cache is being used
        assertThat(primaryCache.getUsage()).isGreaterThan(0);
      }
    }
  }

  @Test
  public void testSecondaryCacheWithLargeDataset() throws RocksDBException {
    // Create a small primary cache with larger secondary cache
    // This simulates a real-world tiered caching scenario
    try (final SecondaryCache secondaryCache =
             SecondaryCache.newCompressedSecondaryCache(32 * 1024 * 1024);
        final Cache primaryCache =
            new LRUCache(2 * 1024 * 1024, -1, false, 0.5, 0.0, secondaryCache);
        final Options options = new Options().setCreateIfMissing(true)) {
      final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
      tableConfig.setBlockCache(primaryCache);
      tableConfig.setBlockSize(4 * 1024);
      options.setTableFormatConfig(tableConfig);

      try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        // Write enough data to overflow the primary cache
        final byte[] largeValue = new byte[1024];
        for (int i = 0; i < 1000; i++) {
          db.put(("largekey" + i).getBytes(), largeValue);
        }

        db.flush(new FlushOptions());

        // Read back the data - some should come from secondary cache
        for (int i = 0; i < 1000; i++) {
          final byte[] value = db.get(("largekey" + i).getBytes());
          assertThat(value).isNotNull();
          assertThat(value.length).isEqualTo(1024);
        }
      }
    }
  }
}
