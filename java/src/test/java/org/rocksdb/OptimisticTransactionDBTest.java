// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OptimisticTransactionDBTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void open() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(options,
                 dbFolder.getRoot().getAbsolutePath())) {
      assertThat(otdb).isNotNull();
    }
  }

  @Test
  public void open_OptimisticTransactionDBOptions() throws RocksDBException {
    try (final DBOptions dbOptions =
             new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final OptimisticTransactionDBOptions optimisticOptions =
             new OptimisticTransactionDBOptions().setOccValidationPolicy(
                 OccValidationPolicy.VALIDATE_SERIAL);
         final ColumnFamilyOptions myCfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("myCf".getBytes(), myCfOpts));

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(dbOptions,
               optimisticOptions, dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors,
               columnFamilyHandles)) {
        try {
          assertThat(otdb).isNotNull();
          assertThat(otdb.occValidationPolicy())
              .isEqualTo(optimisticOptions.occValidationPolicy());
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test
  public void open_columnFamilies() throws RocksDBException {
    try(final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions myCfOpts = new ColumnFamilyOptions()) {

      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Arrays.asList(
              new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
              new ColumnFamilyDescriptor("myCf".getBytes(), myCfOpts));

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(dbOptions,
               dbFolder.getRoot().getAbsolutePath(),
               columnFamilyDescriptors, columnFamilyHandles)) {
        try {
          assertThat(otdb).isNotNull();
          assertThat(otdb.occValidationPolicy())
              .isEqualTo(OccValidationPolicy.VALIDATE_PARALLEL);
        } finally {
          for (final ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
          }
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void open_columnFamilies_no_default() throws RocksDBException {
    try (final DBOptions dbOptions =
             new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final ColumnFamilyOptions myCfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Collections.singletonList(new ColumnFamilyDescriptor("myCf".getBytes(), myCfOpts));

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      OptimisticTransactionDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors, columnFamilyHandles);
    }
  }

  @Test
  public void beginTransaction() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(
             options, dbFolder.getRoot().getAbsolutePath());
        final WriteOptions writeOptions = new WriteOptions()) {

      try(final Transaction txn = otdb.beginTransaction(writeOptions)) {
        assertThat(txn).isNotNull();
      }
    }
  }

  @Test
  public void beginTransaction_transactionOptions() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(
             options, dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions();
         final OptimisticTransactionOptions optimisticTxnOptions =
             new OptimisticTransactionOptions()) {

      try(final Transaction txn = otdb.beginTransaction(writeOptions,
          optimisticTxnOptions)) {
        assertThat(txn).isNotNull();
      }
    }
  }

  @Test
  public void beginTransaction_withOld() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(
             options, dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions()) {

      try(final Transaction txn = otdb.beginTransaction(writeOptions)) {
        final Transaction txnReused = otdb.beginTransaction(writeOptions, txn);
        assertThat(txnReused).isSameAs(txn);
      }
    }
  }

  @Test
  public void beginTransaction_withOld_transactionOptions()
      throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(
             options, dbFolder.getRoot().getAbsolutePath());
         final WriteOptions writeOptions = new WriteOptions();
         final OptimisticTransactionOptions optimisticTxnOptions =
             new OptimisticTransactionOptions()) {

      try(final Transaction txn = otdb.beginTransaction(writeOptions)) {
        final Transaction txnReused = otdb.beginTransaction(writeOptions,
            optimisticTxnOptions, txn);
        assertThat(txnReused).isSameAs(txn);
      }
    }
  }

  @Test
  public void baseDB() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final OptimisticTransactionDB otdb = OptimisticTransactionDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      assertThat(otdb).isNotNull();
      final RocksDB db = otdb.getBaseDB();
      assertThat(db).isNotNull();
      assertThat(db.isOwningHandle()).isFalse();
    }
  }

  @Test
  public void otdbSimpleIterator() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true).setMaxCompactionBytes(0);
         final OptimisticTransactionDB otdb =
             OptimisticTransactionDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      otdb.put("keyI".getBytes(), "valueI".getBytes());
      try (final RocksIterator iterator = otdb.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("keyI".getBytes());
        assertThat(iterator.value()).isEqualTo("valueI".getBytes());
        iterator.next();
        assertThat(iterator.isValid()).isFalse();
      }
    }
  }
}
