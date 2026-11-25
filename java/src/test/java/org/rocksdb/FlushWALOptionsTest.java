// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FlushWALOptionsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void newFlushWALOptions() {
    try (final FlushWALOptions flushWALOptions = new FlushWALOptions()) {
      assertThat(flushWALOptions).isNotNull();
      // Verify defaults
      assertThat(flushWALOptions.sync()).isFalse();
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_TOTAL);
    }
  }

  @Test
  public void newFlushWALOptionsWithSync() {
    try (final FlushWALOptions flushWALOptions = new FlushWALOptions(true)) {
      assertThat(flushWALOptions).isNotNull();
      assertThat(flushWALOptions.sync()).isTrue();
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_TOTAL);
    }
  }

  @Test
  public void sync() {
    try (final FlushWALOptions flushWALOptions = new FlushWALOptions()) {
      // Default value should be false
      assertThat(flushWALOptions.sync()).isFalse();

      // Test setting to true
      flushWALOptions.setSync(true);
      assertThat(flushWALOptions.sync()).isTrue();

      // Test setting to false
      flushWALOptions.setSync(false);
      assertThat(flushWALOptions.sync()).isFalse();
    }
  }

  @Test
  public void rateLimiterPriority() {
    try (final FlushWALOptions flushWALOptions = new FlushWALOptions()) {
      // Default value should be IO_TOTAL
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_TOTAL);

      // Test all IOPriority values
      flushWALOptions.setRateLimiterPriority(IOPriority.IO_LOW);
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_LOW);

      flushWALOptions.setRateLimiterPriority(IOPriority.IO_MID);
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_MID);

      flushWALOptions.setRateLimiterPriority(IOPriority.IO_HIGH);
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_HIGH);

      flushWALOptions.setRateLimiterPriority(IOPriority.IO_USER);
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_USER);

      flushWALOptions.setRateLimiterPriority(IOPriority.IO_TOTAL);
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_TOTAL);
    }
  }

  @Test
  public void fluentAPI() {
    try (final FlushWALOptions flushWALOptions = new FlushWALOptions()) {
      // Test fluent chaining
      FlushWALOptions result =
          flushWALOptions.setSync(true).setRateLimiterPriority(IOPriority.IO_HIGH);

      assertThat(result).isSameAs(flushWALOptions);
      assertThat(flushWALOptions.sync()).isTrue();
      assertThat(flushWALOptions.rateLimiterPriority()).isEqualTo(IOPriority.IO_HIGH);
    }
  }

  @Test
  public void flushWalWithOptions() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true).setManualWalFlush(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final FlushWALOptions flushWALOptions = new FlushWALOptions()) {
      // Put some data
      db.put("key1".getBytes(), "value1".getBytes());

      // Flush WAL without sync
      flushWALOptions.setSync(false);
      db.flushWal(flushWALOptions);

      // Put more data
      db.put("key2".getBytes(), "value2".getBytes());

      // Flush WAL with sync
      flushWALOptions.setSync(true);
      db.flushWal(flushWALOptions);

      // Verify data is accessible
      assertThat(new String(db.get("key1".getBytes()))).isEqualTo("value1");
      assertThat(new String(db.get("key2".getBytes()))).isEqualTo("value2");
    }
  }

  @Test
  public void flushWalWithRateLimiter() throws RocksDBException {
    try (final RateLimiter rateLimiter = new RateLimiter(10000000); // 10 MB/s
        final Options options =
            new Options().setCreateIfMissing(true).setManualWalFlush(true).setRateLimiter(
                rateLimiter);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final FlushWALOptions flushWALOptions = new FlushWALOptions()) {
      // Put some data
      db.put("key1".getBytes(), "value1".getBytes());

      // Flush WAL with high priority
      flushWALOptions.setSync(true);
      flushWALOptions.setRateLimiterPriority(IOPriority.IO_HIGH);
      db.flushWal(flushWALOptions);

      // Verify data is accessible
      assertThat(new String(db.get("key1".getBytes()))).isEqualTo("value1");

      // Test with different priority
      db.put("key2".getBytes(), "value2".getBytes());
      flushWALOptions.setRateLimiterPriority(IOPriority.IO_LOW);
      db.flushWal(flushWALOptions);

      assertThat(new String(db.get("key2".getBytes()))).isEqualTo("value2");
    }
  }

  @Test
  public void backwardCompatibility() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true).setManualWalFlush(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      // Test old flushWal(boolean) still works
      db.put("key1".getBytes(), "value1".getBytes());
      db.flushWal(false);

      db.put("key2".getBytes(), "value2".getBytes());
      db.flushWal(true);

      // Verify data is accessible
      assertThat(new String(db.get("key1".getBytes()))).isEqualTo("value1");
      assertThat(new String(db.get("key2".getBytes()))).isEqualTo("value2");
    }
  }
}
