// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BuiltinComparatorTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void builtinForwardComparator()
      throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
         final RocksDB rocksDb = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      rocksDb.put("abc1".getBytes(), "abc1".getBytes());
      rocksDb.put("abc2".getBytes(), "abc2".getBytes());
      rocksDb.put("abc3".getBytes(), "abc3".getBytes());

      try(final RocksIterator rocksIterator = rocksDb.newIterator()) {
        // Iterate over keys using a iterator
        rocksIterator.seekToFirst();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc2".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc2".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isFalse();
        // Get last one
        rocksIterator.seekToLast();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        // Seek for abc
        rocksIterator.seek("abc".getBytes());
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
      }
    }
  }

  @Test
  public void builtinReverseComparator()
      throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
         final RocksDB rocksDb = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {

      rocksDb.put("abc1".getBytes(), "abc1".getBytes());
      rocksDb.put("abc2".getBytes(), "abc2".getBytes());
      rocksDb.put("abc3".getBytes(), "abc3".getBytes());

      try (final RocksIterator rocksIterator = rocksDb.newIterator()) {
        // Iterate over keys using a iterator
        rocksIterator.seekToFirst();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc2".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc2".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isFalse();
        // Get last one
        rocksIterator.seekToLast();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        // Will be invalid because abc is after abc1
        rocksIterator.seek("abc".getBytes());
        assertThat(rocksIterator.isValid()).isFalse();
        // Will be abc3 because the next one after abc999
        // is abc3
        rocksIterator.seek("abc999".getBytes());
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
      }
    }
  }

  @Test
  public void builtinU64TimestampComparators() throws Exception {
    final byte[] first = "a".getBytes(UTF_8);
    final byte[] second = "b".getBytes(UTF_8);
    final byte[] timestamp =
        ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(10).array();
    for (final BuiltinComparator comparator :
        new BuiltinComparator[] {BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS,
            BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR_WITH_U64_TS}) {
      try (final Options options = new Options().setCreateIfMissing(true).setComparator(comparator);
          final RocksDB db = RocksDB.open(options, dbFolder.newFolder().getAbsolutePath());
          final WriteOptions write = new WriteOptions(); final Slice ts = new Slice(timestamp);
          final ReadOptions read = new ReadOptions().setTimestamp(ts)) {
        db.put(write, second, timestamp, second);
        db.put(write, first, timestamp, first);
        final boolean forward = comparator == BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS;
        try (final RocksIterator iterator = db.newIterator(read)) {
          iterator.seekToFirst();
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo(forward ? first : second);
          iterator.next();
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo(forward ? second : first);
          iterator.next();
          assertThat(iterator.isValid()).isFalse();
          iterator.status();
        }
      }
    }
  }

  @Test
  public void builtinComparatorEnum(){
    assertThat(BuiltinComparator.BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(0);
    assertThat(
        BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(1);
    assertThat(BuiltinComparator.BYTEWISE_COMPARATOR_WITH_U64_TS.ordinal()).isEqualTo(2);
    assertThat(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR_WITH_U64_TS.ordinal()).isEqualTo(3);
    assertThat(BuiltinComparator.values().length).isEqualTo(4);
    assertThat(BuiltinComparator.valueOf("BYTEWISE_COMPARATOR")).
        isEqualTo(BuiltinComparator.BYTEWISE_COMPARATOR);
  }
}
