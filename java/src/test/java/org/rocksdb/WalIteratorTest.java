// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class WalIteratorTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void walIterator() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final WalIterator walIterator = db.getUpdatesSince(0)) {
      //no-op
    }
  }

  @Test
  public void getBatch() throws RocksDBException {
    final int numberOfPuts = 5;
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setWalTtlSeconds(1000)
        .setWalSizeLimitMB(10);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      for (int i = 0; i < numberOfPuts; i++) {
        db.put(String.valueOf(i).getBytes(),
            String.valueOf(i).getBytes());
      }
      db.flush(new FlushOptions().setWaitForFlush(true));

      // the latest sequence number is 5 because 5 puts
      // were written beforehand
      assertThat(db.getLatestSequenceNumber()).
          isEqualTo(numberOfPuts);

      // insert 5 writes into a cf

      try (final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes());
          final ColumnFamilyHandle cfHandle = db.createColumnFamily(newCF)) {
        for (int i = 0; i < numberOfPuts; i++) {
          db.put(cfHandle, String.valueOf(i).getBytes(),
              String.valueOf(i).getBytes());
        }
        // the latest sequence number is 10 because
        // (5 + 5) puts were written beforehand
        assertThat(db.getLatestSequenceNumber()).
            isEqualTo(numberOfPuts + numberOfPuts);

        // Get updates since the beginning
        try (final WalIterator walIterator = db.getUpdatesSince(0)) {
          assertThat(walIterator.isValid()).isTrue();
          walIterator.status();

          // The first sequence number is 1
          final WalIterator.BatchResult batchResult = walIterator.getBatch();
          assertThat(batchResult.sequenceNumber()).isEqualTo(1);
        }
      }
    }
  }

  @Test
  public void walIteratorStallAtLastRecord() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setWalTtlSeconds(1000)
        .setWalSizeLimitMB(10);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      db.put("key1".getBytes(), "value1".getBytes());
      // Get updates since the beginning
      try (final WalIterator walIterator = db.getUpdatesSince(0)) {
        walIterator.status();
        assertThat(walIterator.isValid()).isTrue();
        walIterator.next();
        // Caught up, but not spent: a later write is picked up by calling
        // next() again.
        assertThat(walIterator.isValid()).isFalse();
        walIterator.status();
        db.put("key2".getBytes(), "value2".getBytes());
        walIterator.next();
        walIterator.status();
        assertThat(walIterator.isValid()).isTrue();
      }
    }
  }

  @Test
  public void walIteratorCheckAfterRestart() throws RocksDBException {
    final int numberOfKeys = 2;
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setWalTtlSeconds(1000)
        .setWalSizeLimitMB(10)) {

      try (final RocksDB db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath())) {
        db.put("key1".getBytes(), "value1".getBytes());
        db.put("key2".getBytes(), "value2".getBytes());
        db.flush(new FlushOptions().setWaitForFlush(true));

      }

      // reopen
      try (final RocksDB db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath())) {
        assertThat(db.getLatestSequenceNumber()).isEqualTo(numberOfKeys);

        try (final WalIterator walIterator = db.getUpdatesSince(0)) {
          for (int i = 0; i < numberOfKeys; i++) {
            walIterator.status();
            assertThat(walIterator.isValid()).isTrue();
            walIterator.next();
          }
        }
      }
    }
  }
}
