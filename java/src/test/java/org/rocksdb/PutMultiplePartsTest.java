//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PutMultiplePartsTest {
  @Parameterized.Parameters
  public static List<Integer> data() {
    return Arrays.asList(2, 3, 250, 20000);
  }

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  private final int numParts;

  public PutMultiplePartsTest(final Integer numParts) {
    this.numParts = numParts;
  }

  @Test
  public void putUntracked() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB txnDB =
             TransactionDB.open(options, txnDbOptions, dbFolder.getRoot().getAbsolutePath())) {
      try (final Transaction transaction = txnDB.beginTransaction(new WriteOptions())) {
        final byte[][] keys = generateItems("key", ":", numParts);
        final byte[][] values = generateItems("value", "", numParts);
        transaction.putUntracked(keys, values);
        transaction.commit();
      }
      txnDB.syncWal();
    }

    validateResults();
  }

  @Test
  public void put() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB txnDB =
             TransactionDB.open(options, txnDbOptions, dbFolder.getRoot().getAbsolutePath())) {
      try (final Transaction transaction = txnDB.beginTransaction(new WriteOptions())) {
        final byte[][] keys = generateItems("key", ":", numParts);
        final byte[][] values = generateItems("value", "", numParts);
        transaction.put(keys, values);
        transaction.commit();
      }
      txnDB.syncWal();
    }

    validateResults();
  }

  @Test
  public void putUntrackedCF() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
        final TransactionDB txnDB =
            TransactionDB.open(options, txnDbOptions, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor testCF = new ColumnFamilyDescriptor("cfTest".getBytes());
        final ColumnFamilyHandle columnFamilyHandle = txnDB.createColumnFamily(testCF)) {
      try (final Transaction transaction = txnDB.beginTransaction(new WriteOptions())) {
        final byte[][] keys = generateItems("key", ":", numParts);
        final byte[][] values = generateItems("value", "", numParts);
        transaction.putUntracked(columnFamilyHandle, keys, values);
        transaction.commit();
      }
      txnDB.syncWal();
    }

    validateResultsCF();
  }
  @Test
  public void putCF() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
        final TransactionDB txnDB =
            TransactionDB.open(options, txnDbOptions, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor testCF = new ColumnFamilyDescriptor("cfTest".getBytes());
        final ColumnFamilyHandle columnFamilyHandle = txnDB.createColumnFamily(testCF)) {
      try (final Transaction transaction = txnDB.beginTransaction(new WriteOptions())) {
        final byte[][] keys = generateItems("key", ":", numParts);
        final byte[][] values = generateItems("value", "", numParts);
        transaction.put(columnFamilyHandle, keys, values);
        transaction.commit();
      }
      txnDB.syncWal();
    }

    validateResultsCF();
  }

  private void validateResults() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(new Options(), dbFolder.getRoot().getAbsolutePath())) {
      final List<byte[]> keys = generateItemsAsList("key", ":", numParts);
      final byte[][] values = generateItems("value", "", numParts);

      final StringBuilder singleKey = new StringBuilder();
      for (int i = 0; i < numParts; i++) {
        singleKey.append(new String(keys.get(i), StandardCharsets.UTF_8));
      }
      final byte[] result = db.get(singleKey.toString().getBytes());
      final StringBuilder singleValue = new StringBuilder();
      for (int i = 0; i < numParts; i++) {
        singleValue.append(new String(values[i], StandardCharsets.UTF_8));
      }
      assertThat(result).isEqualTo(singleValue.toString().getBytes());
    }
  }

  private void validateResultsCF() throws RocksDBException {
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try (final ColumnFamilyDescriptor testCF = new ColumnFamilyDescriptor("cfTest".getBytes());
        final ColumnFamilyDescriptor defaultCF = new ColumnFamilyDescriptor("default".getBytes());
        final RocksDB db = RocksDB.open(new DBOptions(), dbFolder.getRoot().getAbsolutePath(),
            Arrays.asList(testCF, defaultCF), columnFamilyHandles)) {
      final List<byte[]> keys = generateItemsAsList("key", ":", numParts);
      final byte[][] values = generateItems("value", "", numParts);

      final StringBuilder singleKey = new StringBuilder();
      for (int i = 0; i < numParts; i++) {
        singleKey.append(new String(keys.get(i), StandardCharsets.UTF_8));
      }
      final byte[] result = db.get(columnFamilyHandles.get(0), singleKey.toString().getBytes());
      final StringBuilder singleValue = new StringBuilder();
      for (int i = 0; i < numParts; i++) {
        singleValue.append(new String(values[i], StandardCharsets.UTF_8));
      }
      assertThat(result).isEqualTo(singleValue.toString().getBytes());
    }
  }

  private byte[][] generateItems(final String prefix, final String suffix, final int numItems) {
    return generateItemsAsList(prefix, suffix, numItems).toArray(new byte[0][0]);
  }

  private List<byte[]> generateItemsAsList(
      final String prefix, final String suffix, final int numItems) {
    final List<byte[]> items = new ArrayList<>();
    for (int i = 0; i < numItems; i++) {
      items.add((prefix + i + suffix).getBytes());
    }
    return items;
  }
}
