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

public class WaitForCompactOptionsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void defaultValues() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      assertThat(options.abortOnPause()).isFalse();
      assertThat(options.flush()).isFalse();
      assertThat(options.waitForPurge()).isFalse();
      assertThat(options.closeDb()).isFalse();
      assertThat(options.timeout()).isEqualTo(0);
    }
  }

  @Test
  public void abortOnPause() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      assertThat(options.abortOnPause()).isFalse();
      options.setAbortOnPause(true);
      assertThat(options.abortOnPause()).isTrue();
      options.setAbortOnPause(false);
      assertThat(options.abortOnPause()).isFalse();
    }
  }

  @Test
  public void flush() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      assertThat(options.flush()).isFalse();
      options.setFlush(true);
      assertThat(options.flush()).isTrue();
      options.setFlush(false);
      assertThat(options.flush()).isFalse();
    }
  }

  @Test
  public void waitForPurge() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      assertThat(options.waitForPurge()).isFalse();
      options.setWaitForPurge(true);
      assertThat(options.waitForPurge()).isTrue();
      options.setWaitForPurge(false);
      assertThat(options.waitForPurge()).isFalse();
    }
  }

  @Test
  public void closeDb() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      assertThat(options.closeDb()).isFalse();
      options.setCloseDb(true);
      assertThat(options.closeDb()).isTrue();
      options.setCloseDb(false);
      assertThat(options.closeDb()).isFalse();
    }
  }

  @Test
  public void timeout() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      assertThat(options.timeout()).isEqualTo(0);
      options.setTimeout(1000000); // 1 second in microseconds
      assertThat(options.timeout()).isEqualTo(1000000);
      options.setTimeout(5000000); // 5 seconds
      assertThat(options.timeout()).isEqualTo(5000000);
      options.setTimeout(0);
      assertThat(options.timeout()).isEqualTo(0);
    }
  }

  @Test
  public void chainedSetters() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      options.setAbortOnPause(true)
          .setFlush(true)
          .setWaitForPurge(true)
          .setCloseDb(true)
          .setTimeout(2000000);

      assertThat(options.abortOnPause()).isTrue();
      assertThat(options.flush()).isTrue();
      assertThat(options.waitForPurge()).isTrue();
      assertThat(options.closeDb()).isTrue();
      assertThat(options.timeout()).isEqualTo(2000000);
    }
  }

  @Test
  public void multipleToggle() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      // Toggle each field multiple times to ensure state is maintained correctly
      for (int i = 0; i < 3; i++) {
        options.setAbortOnPause(true);
        assertThat(options.abortOnPause()).isTrue();
        options.setAbortOnPause(false);
        assertThat(options.abortOnPause()).isFalse();

        options.setFlush(true);
        assertThat(options.flush()).isTrue();
        options.setFlush(false);
        assertThat(options.flush()).isFalse();

        options.setWaitForPurge(true);
        assertThat(options.waitForPurge()).isTrue();
        options.setWaitForPurge(false);
        assertThat(options.waitForPurge()).isFalse();

        options.setCloseDb(true);
        assertThat(options.closeDb()).isTrue();
        options.setCloseDb(false);
        assertThat(options.closeDb()).isFalse();
      }
    }
  }

  @Test
  public void timeoutBoundaryValues() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      // Test zero timeout (no timeout)
      options.setTimeout(0);
      assertThat(options.timeout()).isEqualTo(0);

      // Test very small timeout
      options.setTimeout(1);
      assertThat(options.timeout()).isEqualTo(1);

      // Test typical timeout values (1 second to 1 hour in microseconds)
      options.setTimeout(1000000); // 1 second
      assertThat(options.timeout()).isEqualTo(1000000);

      options.setTimeout(60000000); // 1 minute
      assertThat(options.timeout()).isEqualTo(60000000);

      options.setTimeout(3600000000L); // 1 hour
      assertThat(options.timeout()).isEqualTo(3600000000L);

      // Test very large timeout
      options.setTimeout(Long.MAX_VALUE);
      assertThat(options.timeout()).isEqualTo(Long.MAX_VALUE);

      // Reset to zero
      options.setTimeout(0);
      assertThat(options.timeout()).isEqualTo(0);
    }
  }

  @Test
  public void allCombinations() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      // Test all true
      options.setAbortOnPause(true).setFlush(true).setWaitForPurge(true).setCloseDb(true);
      assertThat(options.abortOnPause()).isTrue();
      assertThat(options.flush()).isTrue();
      assertThat(options.waitForPurge()).isTrue();
      assertThat(options.closeDb()).isTrue();

      // Test all false
      options.setAbortOnPause(false).setFlush(false).setWaitForPurge(false).setCloseDb(false);
      assertThat(options.abortOnPause()).isFalse();
      assertThat(options.flush()).isFalse();
      assertThat(options.waitForPurge()).isFalse();
      assertThat(options.closeDb()).isFalse();

      // Test mixed combinations
      options.setAbortOnPause(true).setFlush(false).setWaitForPurge(true).setCloseDb(false);
      assertThat(options.abortOnPause()).isTrue();
      assertThat(options.flush()).isFalse();
      assertThat(options.waitForPurge()).isTrue();
      assertThat(options.closeDb()).isFalse();

      options.setAbortOnPause(false).setFlush(true).setWaitForPurge(false).setCloseDb(true);
      assertThat(options.abortOnPause()).isFalse();
      assertThat(options.flush()).isTrue();
      assertThat(options.waitForPurge()).isFalse();
      assertThat(options.closeDb()).isTrue();
    }
  }

  @Test
  public void independentOptions() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      // Verify that setting one option doesn't affect others
      options.setAbortOnPause(true);
      assertThat(options.abortOnPause()).isTrue();
      assertThat(options.flush()).isFalse();
      assertThat(options.waitForPurge()).isFalse();
      assertThat(options.closeDb()).isFalse();
      assertThat(options.timeout()).isEqualTo(0);

      options.setFlush(true);
      assertThat(options.abortOnPause()).isTrue();
      assertThat(options.flush()).isTrue();
      assertThat(options.waitForPurge()).isFalse();
      assertThat(options.closeDb()).isFalse();
      assertThat(options.timeout()).isEqualTo(0);

      options.setTimeout(5000000);
      assertThat(options.abortOnPause()).isTrue();
      assertThat(options.flush()).isTrue();
      assertThat(options.waitForPurge()).isFalse();
      assertThat(options.closeDb()).isFalse();
      assertThat(options.timeout()).isEqualTo(5000000);

      options.setWaitForPurge(true);
      assertThat(options.abortOnPause()).isTrue();
      assertThat(options.flush()).isTrue();
      assertThat(options.waitForPurge()).isTrue();
      assertThat(options.closeDb()).isFalse();
      assertThat(options.timeout()).isEqualTo(5000000);

      options.setCloseDb(true);
      assertThat(options.abortOnPause()).isTrue();
      assertThat(options.flush()).isTrue();
      assertThat(options.waitForPurge()).isTrue();
      assertThat(options.closeDb()).isTrue();
      assertThat(options.timeout()).isEqualTo(5000000);
    }
  }

  @Test
  public void resetToDefaults() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      // Set all options to non-default values
      options.setAbortOnPause(true)
          .setFlush(true)
          .setWaitForPurge(true)
          .setCloseDb(true)
          .setTimeout(10000000);

      // Reset back to defaults
      options.setAbortOnPause(false)
          .setFlush(false)
          .setWaitForPurge(false)
          .setCloseDb(false)
          .setTimeout(0);

      // Verify all are back to default
      assertThat(options.abortOnPause()).isFalse();
      assertThat(options.flush()).isFalse();
      assertThat(options.waitForPurge()).isFalse();
      assertThat(options.closeDb()).isFalse();
      assertThat(options.timeout()).isEqualTo(0);
    }
  }

  @Test
  public void resourceCleanup() {
    // Test that options can be created and closed multiple times
    for (int i = 0; i < 10; i++) {
      try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
        options.setAbortOnPause(true);
        assertThat(options.abortOnPause()).isTrue();
      }
    }
  }

  @Test
  public void builderPattern() {
    try (final WaitForCompactOptions options = new WaitForCompactOptions()) {
      // Test that setter methods return 'this' for builder pattern
      WaitForCompactOptions result = options.setAbortOnPause(true);
      assertThat(result).isSameAs(options);

      result = options.setFlush(true);
      assertThat(result).isSameAs(options);

      result = options.setWaitForPurge(true);
      assertThat(result).isSameAs(options);

      result = options.setCloseDb(true);
      assertThat(result).isSameAs(options);

      result = options.setTimeout(1000000);
      assertThat(result).isSameAs(options);
    }
  }

  // Functional integration tests with actual RocksDB instance

  @Test
  public void waitForCompactBasic() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final WaitForCompactOptions waitForCompactOptions = new WaitForCompactOptions()) {
      // Put some data to trigger potential compactions
      for (int i = 0; i < 100; i++) {
        db.put(("key" + i).getBytes(), ("value" + i).getBytes());
      }

      // Flush to create SST files
      db.flush(new FlushOptions().setWaitForFlush(true));

      // Wait for compactions to complete
      waitForCompactOptions.setFlush(false);
      waitForCompactOptions.setTimeout(5000000); // 5 seconds timeout
      db.waitForCompact(waitForCompactOptions);

      // Verify data is still accessible
      assertThat(new String(db.get("key0".getBytes()))).isEqualTo("value0");
      assertThat(new String(db.get("key99".getBytes()))).isEqualTo("value99");
    }
  }

  @Test
  public void waitForCompactWithFlush() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final WaitForCompactOptions waitForCompactOptions = new WaitForCompactOptions()) {
      // Put some data
      for (int i = 0; i < 50; i++) {
        db.put(("key" + i).getBytes(), ("value" + i).getBytes());
      }

      // Wait for both flush and compaction
      waitForCompactOptions.setFlush(true);
      waitForCompactOptions.setTimeout(5000000); // 5 seconds timeout
      db.waitForCompact(waitForCompactOptions);

      // Verify data is still accessible
      assertThat(new String(db.get("key0".getBytes()))).isEqualTo("value0");
      assertThat(new String(db.get("key49".getBytes()))).isEqualTo("value49");
    }
  }

  @Test
  public void waitForCompactWithTimeout() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final WaitForCompactOptions waitForCompactOptions = new WaitForCompactOptions()) {
      // Put minimal data
      db.put("test".getBytes(), "value".getBytes());

      // Wait with a very short timeout - should complete quickly
      waitForCompactOptions.setTimeout(1000000); // 1 second
      db.waitForCompact(waitForCompactOptions);

      // Verify data integrity
      assertThat(new String(db.get("test".getBytes()))).isEqualTo("value");
    }
  }

  @Test
  public void waitForCompactEmptyDatabase() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final WaitForCompactOptions waitForCompactOptions = new WaitForCompactOptions()) {
      // Wait on empty database - should complete immediately
      waitForCompactOptions.setTimeout(5000000); // 5 seconds timeout
      db.waitForCompact(waitForCompactOptions);

      // Database should still be functional
      db.put("after_wait".getBytes(), "test".getBytes());
      assertThat(new String(db.get("after_wait".getBytes()))).isEqualTo("test");
    }
  }

  @Test
  public void waitForCompactWithAllOptions() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final WaitForCompactOptions waitForCompactOptions = new WaitForCompactOptions()) {
      // Put some data
      for (int i = 0; i < 30; i++) {
        db.put(("key" + i).getBytes(), ("value" + i).getBytes());
      }

      // Configure all options
      waitForCompactOptions.setAbortOnPause(false).setFlush(true).setWaitForPurge(true).setTimeout(
          10000000); // 10 seconds timeout

      db.waitForCompact(waitForCompactOptions);

      // Verify all data is intact
      for (int i = 0; i < 30; i++) {
        assertThat(new String(db.get(("key" + i).getBytes()))).isEqualTo("value" + i);
      }
    }
  }

  @Test
  public void waitForCompactMultipleTimes() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final WaitForCompactOptions waitForCompactOptions = new WaitForCompactOptions()) {
      waitForCompactOptions.setTimeout(5000000); // 5 seconds timeout

      // Call waitForCompact multiple times
      for (int round = 0; round < 3; round++) {
        // Add some data
        for (int i = 0; i < 20; i++) {
          db.put(("round" + round + "_key" + i).getBytes(),
              ("round" + round + "_value" + i).getBytes());
        }

        // Wait for compaction
        db.waitForCompact(waitForCompactOptions);

        // Verify data from this round
        assertThat(new String(db.get(("round" + round + "_key0").getBytes())))
            .isEqualTo("round" + round + "_value0");
      }
    }
  }
}
