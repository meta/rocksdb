//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.rocksdb.BuiltinComparator.encodeU64Ts;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TimestampedWriteTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void timestampedWritesPreserveHistory() throws Exception {
    for (final BuiltinComparator comparator :
        new BuiltinComparator[] {BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS,
            BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR_WITH_U64_TS}) {
      final String path = dbFolder.newFolder().getAbsolutePath();
      final byte[] key = "key".getBytes(UTF_8);
      final byte[] first = "first".getBytes(UTF_8);
      final byte[] second = "second".getBytes(UTF_8);
      try (final Options options = new Options().setCreateIfMissing(true).setComparator(comparator);
          final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().setComparator(comparator);
          final WriteOptions write = new WriteOptions()) {
        try (final RocksDB db = RocksDB.open(options, path);
            final ColumnFamilyHandle cf = db.createColumnFamily(
                new ColumnFamilyDescriptor("versions".getBytes(UTF_8), cfOptions))) {
          db.put(write, key, encodeU64Ts(10), first);
          db.put(write, key, encodeU64Ts(Long.MIN_VALUE), second);
          db.delete(write, key, encodeU64Ts(-1L));
          db.put(cf, write, key, encodeU64Ts(10), first);
          db.put(cf, write, key, encodeU64Ts(Long.MIN_VALUE), second);
          db.delete(cf, write, key, encodeU64Ts(-1L));
          assertHistory(db, db.getDefaultColumnFamily(), key, first, second);
          assertHistory(db, cf, key, first, second);
          db.compactRange();
          assertHistory(db, db.getDefaultColumnFamily(), key, first, second);
          db.dropColumnFamily(cf);
        }
        try (final RocksDB db = RocksDB.open(options, path)) {
          assertHistory(db, db.getDefaultColumnFamily(), key, first, second);
        }
      }
    }
  }

  @Test
  public void timestampedWritesAcceptEmptyKeysAndValues() throws Exception {
    final byte[] empty = new byte[0];
    try (final Options options = new Options().setCreateIfMissing(true).setComparator(
             BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS);
        final RocksDB db = RocksDB.open(options, dbFolder.newFolder().getAbsolutePath());
        final WriteOptions write = new WriteOptions();
        final Slice initial = new Slice(encodeU64Ts(10));
        final Slice deleted = new Slice(encodeU64Ts(20));
        final ReadOptions read = new ReadOptions()) {
      db.put(write, empty, encodeU64Ts(10), empty);
      db.delete(db.getDefaultColumnFamily(), write, empty, encodeU64Ts(20));
      assertThat(db.get(read.setTimestamp(initial), empty)).isEmpty();
      assertThat(db.get(read.setTimestamp(deleted), empty)).isNull();
    }
  }

  @Test
  public void invalidTimestampsReportNativeErrors() throws Exception {
    final byte[] key = {1};
    final byte[] value = {2};
    try (final Options options = new Options().setCreateIfMissing(true).setComparator(
             BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS);
        final RocksDB db = RocksDB.open(options, dbFolder.newFolder().getAbsolutePath());
        final WriteOptions write = new WriteOptions()) {
      db.put(write, key, encodeU64Ts(10), value);
      for (final byte[] invalid : new byte[][] {new byte[0], new byte[7], new byte[9]}) {
        try {
          db.put(write, key, invalid, value);
          fail("Expected an invalid timestamp to be rejected");
        } catch (final RocksDBException e) {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.InvalidArgument);
        }
        try {
          db.delete(db.getDefaultColumnFamily(), write, key, invalid);
          fail("Expected an invalid timestamp to be rejected");
        } catch (final RocksDBException e) {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.InvalidArgument);
        }
      }
      try (final Slice ts = new Slice(encodeU64Ts(10));
          final ReadOptions read = new ReadOptions().setTimestamp(ts)) {
        assertThat(db.get(read, key)).isEqualTo(value);
      }
    }
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.newFolder().getAbsolutePath());
        final WriteOptions write = new WriteOptions()) {
      try {
        db.put(write, key, encodeU64Ts(10), value);
        fail("Expected an invalid timestamp to be rejected");
      } catch (final RocksDBException e) {
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.InvalidArgument);
      }
      try {
        db.delete(write, key, encodeU64Ts(10));
        fail("Expected an invalid timestamp to be rejected");
      } catch (final RocksDBException e) {
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.InvalidArgument);
      }
    }
  }

  @Test
  public void fullHistoryTsLow() throws Exception {
    final byte[] key = "key".getBytes(UTF_8);
    final byte[] first = "first".getBytes(UTF_8);
    final byte[] second = "second".getBytes(UTF_8);
    final String path = dbFolder.newFolder().getAbsolutePath();
    try (final Options options = new Options().setCreateIfMissing(true).setComparator(
             BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS);
        final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().setComparator(
            BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS);
        final WriteOptions write = new WriteOptions();
        final Slice initial = new Slice(encodeU64Ts(10));
        final Slice retained = new Slice(encodeU64Ts(Long.MIN_VALUE));
        final Slice deleted = new Slice(encodeU64Ts(-1L));
        final ReadOptions read = new ReadOptions()) {
      try (final RocksDB db = RocksDB.open(options, path);
          final ColumnFamilyHandle cf = db.createColumnFamily(
              new ColumnFamilyDescriptor("versions".getBytes(UTF_8), cfOptions))) {
        assertThat(db.getFullHistoryTsLow(null)).isEmpty();
        assertThat(db.getFullHistoryTsLow(cf)).isEmpty();
        for (final ColumnFamilyHandle handle :
            new ColumnFamilyHandle[] {db.getDefaultColumnFamily(), cf}) {
          db.put(handle, write, key, encodeU64Ts(10), first);
          db.put(handle, write, key, encodeU64Ts(Long.MIN_VALUE), second);
          db.delete(handle, write, key, encodeU64Ts(-1L));
        }
        db.increaseFullHistoryTsLow(null, encodeU64Ts(10));
        assertThat(db.getFullHistoryTsLow(null)).isEqualTo(encodeU64Ts(10));
        assertThat(db.get(read.setTimestamp(initial), key)).isEqualTo(first);
        db.increaseFullHistoryTsLow(null, encodeU64Ts(Long.MIN_VALUE));
        assertThat(db.getFullHistoryTsLow(db.getDefaultColumnFamily()))
            .isEqualTo(encodeU64Ts(Long.MIN_VALUE));
        assertThat(db.getFullHistoryTsLow(cf)).isEmpty();
        db.increaseFullHistoryTsLow(cf, encodeU64Ts(-1L));
        assertThat(db.getFullHistoryTsLow(cf)).isEqualTo(encodeU64Ts(-1L));
        assertThat(db.getFullHistoryTsLow(null)).isEqualTo(encodeU64Ts(Long.MIN_VALUE));
        db.compactRange();
        db.compactRange(cf);
        try {
          db.get(read.setTimestamp(initial), key);
          fail("Expected a read below the full history timestamp low bound to be rejected");
        } catch (final RocksDBException e) {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.InvalidArgument);
        }
        assertThat(db.get(read.setTimestamp(retained), key)).isEqualTo(second);
        assertThat(db.get(read.setTimestamp(deleted), key)).isNull();
        assertThat(db.get(cf, read, key)).isNull();
        db.dropColumnFamily(cf);
      }
      try (final RocksDB db = RocksDB.open(options, path)) {
        assertThat(db.getFullHistoryTsLow(null)).isEqualTo(encodeU64Ts(Long.MIN_VALUE));
        assertThat(db.get(read.setTimestamp(retained), key)).isEqualTo(second);
        assertThat(db.get(read.setTimestamp(deleted), key)).isNull();
      }
    }
  }

  @Test
  public void invalidFullHistoryTimestampsReportNativeErrors() throws Exception {
    try (final Options options = new Options().setCreateIfMissing(true).setComparator(
             BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS);
        final RocksDB db = RocksDB.open(options, dbFolder.newFolder().getAbsolutePath())) {
      db.increaseFullHistoryTsLow(null, encodeU64Ts(Long.MIN_VALUE));
      for (final byte[] invalid :
          new byte[][] {new byte[0], new byte[7], new byte[9], encodeU64Ts(10)}) {
        try {
          db.increaseFullHistoryTsLow(db.getDefaultColumnFamily(), invalid);
          fail("Expected an invalid full history timestamp to be rejected");
        } catch (final RocksDBException e) {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.InvalidArgument);
        }
      }
      assertThat(db.getFullHistoryTsLow(null)).isEqualTo(encodeU64Ts(Long.MIN_VALUE));
    }
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.newFolder().getAbsolutePath())) {
      try {
        db.increaseFullHistoryTsLow(null, encodeU64Ts(10));
        fail("Expected a column family without timestamps to be rejected");
      } catch (final RocksDBException e) {
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.InvalidArgument);
      }
      try {
        db.getFullHistoryTsLow(null);
        fail("Expected a column family without timestamps to be rejected");
      } catch (final RocksDBException e) {
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.InvalidArgument);
      }
    }
  }

  private static void assertHistory(final RocksDB db, final ColumnFamilyHandle cf, final byte[] key,
      final byte[] first, final byte[] second) throws RocksDBException {
    try (final Slice before = new Slice(encodeU64Ts(9));
        final Slice initial = new Slice(encodeU64Ts(10));
        final Slice updated = new Slice(encodeU64Ts(Long.MIN_VALUE));
        final Slice deleted = new Slice(encodeU64Ts(-1L));
        final ReadOptions read = new ReadOptions()) {
      assertThat(db.get(cf, read.setTimestamp(before), key)).isNull();
      assertThat(db.get(cf, read.setTimestamp(initial), key)).isEqualTo(first);
      assertThat(db.get(cf, read.setTimestamp(updated), key)).isEqualTo(second);
      assertThat(db.get(cf, read.setTimestamp(deleted), key)).isNull();
    }
  }

}
