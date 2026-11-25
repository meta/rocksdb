// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

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
