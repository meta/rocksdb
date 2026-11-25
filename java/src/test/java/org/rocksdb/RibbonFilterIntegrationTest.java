// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RibbonFilterIntegrationTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void ribbonFilterBasicFunctionality() throws RocksDBException {
    // Test that Ribbon filter actually works in a real database scenario
    try (final RibbonFilter ribbonFilter = new RibbonFilter(10.0);
        final Options options = new Options().setCreateIfMissing(true)) {
      final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
      tableConfig.setFilterPolicy(ribbonFilter);
      options.setTableFormatConfig(tableConfig);

      try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        // Write some data
        final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
        final byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
        final byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);

        db.put(key1, value1);
        db.put(key2, value2);

        // Read the data back
        assertThat(db.get(key1)).isEqualTo(value1);
        assertThat(db.get(key2)).isEqualTo(value2);

        // Flush to create SST file with Ribbon filter
        db.flush(new FlushOptions().setWaitForFlush(true));

        // Verify data still accessible after flush
        assertThat(db.get(key1)).isEqualTo(value1);
        assertThat(db.get(key2)).isEqualTo(value2);
      }
    }
  }

  @Test
  public void ribbonFilterHybridMode() throws RocksDBException {
    // Test hybrid Bloom/Ribbon configuration
    try (final RibbonFilter ribbonFilter = new RibbonFilter(10.0, 1);
        final Options options = new Options().setCreateIfMissing(true)) {
      final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
      tableConfig.setFilterPolicy(ribbonFilter);
      options.setTableFormatConfig(tableConfig);

      try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        // Write and verify data works with hybrid mode
        final byte[] key = "testkey".getBytes(StandardCharsets.UTF_8);
        final byte[] value = "testvalue".getBytes(StandardCharsets.UTF_8);

        db.put(key, value);
        assertThat(db.get(key)).isEqualTo(value);
      }
    }
  }
}
