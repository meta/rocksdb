// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReadOnlyTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void readOnlyOpen() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());
    }
    try (final RocksDB db = RocksDB.openReadOnly(dbFolder.getRoot().getAbsolutePath())) {
      assertThat("value").isEqualTo(new String(db.get("key".getBytes())));
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts);) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
      cfDescriptors.add(defaultCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(
               dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList);
          final ColumnFamilyDescriptor newCF =
              new ColumnFamilyDescriptor("new_cf".getBytes(), cfOpts);
          final ColumnFamilyDescriptor newCF2 =
              new ColumnFamilyDescriptor("new_cf2".getBytes(), cfOpts);) {
        columnFamilyHandleList.add(db.createColumnFamily(newCF));
        columnFamilyHandleList.add(db.createColumnFamily(newCF2));
        db.put(columnFamilyHandleList.get(2), "key2".getBytes(), "value2".getBytes());
      }

      columnFamilyHandleList.clear();
      try (final RocksDB db = RocksDB.openReadOnly(
               dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList)) {
        assertThat(db.get("key2".getBytes())).isNull();
        assertThat(db.get(columnFamilyHandleList.get(0), "key2".getBytes())).isNull();
      }

      columnFamilyHandleList.clear();
      try (final ColumnFamilyDescriptor defaultCF2 =
               new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts);
          final ColumnFamilyDescriptor newCF2 =
              new ColumnFamilyDescriptor("new_cf2".getBytes(), cfOpts);
          final RocksDB db = RocksDB.openReadOnly(dbFolder.getRoot().getAbsolutePath(),
              Arrays.asList(defaultCF2, newCF2), columnFamilyHandleList)) {
        assertThat(new String(db.get(columnFamilyHandleList.get(1), "key2".getBytes())))
            .isEqualTo("value2");
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToWriteInReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        // no-op
      }
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList = new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(dbFolder.getRoot().getAbsolutePath(),
               cfDescriptors, readOnlyColumnFamilyHandleList)) {
        // test that put fails in readonly mode
        rDb.put("key".getBytes(), "value".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToCFWriteInReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);
      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList)) {
        rDb.put(readOnlyColumnFamilyHandleList.get(0), "key".getBytes(), "value".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToRemoveInReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();

      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList)) {
        rDb.delete("key".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToCFRemoveInReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList)) {
        rDb.delete(readOnlyColumnFamilyHandleList.get(0), "key".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToWriteBatchReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList);
           final WriteBatch wb = new WriteBatch();
           final WriteOptions wOpts = new WriteOptions()) {
        wb.put("key".getBytes(), "value".getBytes());
        rDb.write(wOpts, wb);
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToCFWriteBatchReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList);
           final WriteBatch wb = new WriteBatch();
           final WriteOptions wOpts = new WriteOptions()) {
        wb.put(readOnlyColumnFamilyHandleList.get(0), "key".getBytes(), "value".getBytes());
        rDb.write(wOpts, wb);
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void errorIfWalFileExists() throws RocksDBException, IOException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      // no-op
    }
    Files.write(dbFolder.getRoot().toPath().resolve("999999.log"), new byte[] {1});

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options = new DBOptions();
           final RocksDB ignored =
               RocksDB.openReadOnly(options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
                   readOnlyColumnFamilyHandleList, true)) {
        // no-op... should have raised an error as errorIfWalFileExists=true
      }
    }
  }
}
