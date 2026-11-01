//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test for changes made by
 * <a link="https://github.com/meta/rocksdb/issues/9006">transactional multiGet problem</a>
 * the tests here were previously broken by the nonsense removed by that change.
 */
@RunWith(Parameterized.class)
public class MultiColumnRegressionTest {
  @Parameterized.Parameters
  public static List<Params> data() {
    return Arrays.asList(new Params(3, 100), new Params(3, 1000000));
  }

  public static class Params {
    final int numColumns;
    final int keySize;

    public Params(final int numColumns, final int keySize) {
      this.numColumns = numColumns;
      this.keySize = keySize;
    }
  }

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  private final Params params;

  public MultiColumnRegressionTest(final Params params) {
    this.params = params;
  }

  @Test
  public void transactionDB() throws RocksDBException {
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    for (int i = 0; i < params.numColumns; i++) {
      final StringBuilder sb = new StringBuilder();
      sb.append("cf" + i);
      for (int j = 0; j < params.keySize; j++) sb.append("_cf");
      columnFamilyDescriptors.add(new ColumnFamilyDescriptor(sb.toString().getBytes()));
    }
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.createColumnFamilies(columnFamilyDescriptors);
    }

    columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try (final TransactionDB tdb = TransactionDB.open(new DBOptions().setCreateIfMissing(true),
             new TransactionDBOptions(), dbFolder.getRoot().getAbsolutePath(),
             columnFamilyDescriptors, columnFamilyHandles)) {
      final WriteOptions writeOptions = new WriteOptions();
      try (final Transaction transaction = tdb.beginTransaction(writeOptions)) {
        for (int i = 0; i < params.numColumns; i++) {
          transaction.put(
              columnFamilyHandles.get(i), ("key" + i).getBytes(), ("value" + (i - 7)).getBytes());
        }
        transaction.put("key".getBytes(), "value".getBytes());
        transaction.commit();
      }
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
        columnFamilyHandle.close();
      }
    }

    final List<ColumnFamilyHandle> columnFamilyHandles2 = new ArrayList<>();
    try (final TransactionDB tdb = TransactionDB.open(new DBOptions().setCreateIfMissing(true),
             new TransactionDBOptions(), dbFolder.getRoot().getAbsolutePath(),
             columnFamilyDescriptors, columnFamilyHandles2)) {
      try (final Transaction transaction = tdb.beginTransaction(new WriteOptions())) {
        final ReadOptions readOptions = new ReadOptions();
        for (int i = 0; i < params.numColumns; i++) {
          final byte[] value =
              transaction.get(columnFamilyHandles2.get(i), readOptions, ("key" + i).getBytes());
          assertThat(value).isEqualTo(("value" + (i - 7)).getBytes());
        }
        transaction.commit();
      }
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles2) {
        columnFamilyHandle.close();
      }
    }
  }

  @Test
  public void optimisticDB() throws RocksDBException {
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    for (int i = 0; i < params.numColumns; i++) {
      columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
    }

    columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try (final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(
             new DBOptions().setCreateIfMissing(true), dbFolder.getRoot().getAbsolutePath(),
             columnFamilyDescriptors, columnFamilyHandles)) {
      try (final Transaction transaction = otdb.beginTransaction(new WriteOptions())) {
        for (int i = 0; i < params.numColumns; i++) {
          transaction.put(
              columnFamilyHandles.get(i), ("key" + i).getBytes(), ("value" + (i - 7)).getBytes());
        }
        transaction.put("key".getBytes(), "value".getBytes());
        transaction.commit();
      }
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
        columnFamilyHandle.close();
      }
    }

    final List<ColumnFamilyHandle> columnFamilyHandles2 = new ArrayList<>();
    try (final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(
             new DBOptions().setCreateIfMissing(true), dbFolder.getRoot().getAbsolutePath(),
             columnFamilyDescriptors, columnFamilyHandles2)) {
      try (final Transaction transaction = otdb.beginTransaction(new WriteOptions())) {
        final ReadOptions readOptions = new ReadOptions();
        for (int i = 0; i < params.numColumns; i++) {
          final byte[] value =
              transaction.get(columnFamilyHandles2.get(i), readOptions, ("key" + i).getBytes());
          assertThat(value).isEqualTo(("value" + (i - 7)).getBytes());
        }
        transaction.commit();
      }
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles2) {
        columnFamilyHandle.close();
      }
    }
  }
}
