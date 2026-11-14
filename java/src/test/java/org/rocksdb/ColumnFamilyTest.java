// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.*;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ColumnFamilyTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void columnFamilyDescriptorName() throws RocksDBException {
    final byte[] cfName = "some_name".getBytes(UTF_8);

    try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName, cfOptions)) {
      assertThat(cfDescriptor.getName()).isEqualTo(cfName);
    }
  }

  @Test
  public void columnFamilyDescriptorOptions() throws RocksDBException {
    final byte[] cfName = "some_name".getBytes(UTF_8);

    try (final ColumnFamilyOptions cfOptions =
             new ColumnFamilyOptions().setCompressionType(CompressionType.BZLIB2_COMPRESSION);
        final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName, cfOptions)) {
      assertThat(cfDescriptor.getOptions().compressionType())
          .isEqualTo(CompressionType.BZLIB2_COMPRESSION);
    }
  }

  @Test
  public void listColumnFamilies() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      // Test listColumnFamilies
      final List<byte[]> columnFamilyNames =
          RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
      assertThat(columnFamilyNames).isNotNull();
      assertThat(columnFamilyNames.size()).isGreaterThan(0);
      assertThat(columnFamilyNames.size()).isEqualTo(1);
      assertThat(new String(columnFamilyNames.get(0))).isEqualTo("default");
    }
  }

  @Test
  public void defaultColumnFamily() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      try (final ColumnFamilyHandle cfh = db.getDefaultColumnFamily()) {
        assertThat(cfh).isNotNull();

        assertThat(cfh.getName()).isEqualTo("default".getBytes(UTF_8));
        assertThat(cfh.getID()).isEqualTo(0);
        assertThat(cfh.getDescriptor().getName()).isEqualTo("default".getBytes(UTF_8));

        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();

        db.put(cfh, key, value);

        final byte[] actualValue = db.get(cfh, key);

        assertThat(cfh).isNotNull();
        assertThat(actualValue).isEqualTo(value);
      }
    }
  }

  @Test
  public void createColumnFamily() throws RocksDBException {
    final byte[] cfName = "new_cf".getBytes(UTF_8);

    try (final ColumnFamilyDescriptor cfDescriptor =
             new ColumnFamilyDescriptor(cfName, new ColumnFamilyOptions());
        final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      try (final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(cfDescriptor)) {
        assertThat(columnFamilyHandle.getName()).isEqualTo(cfName);
        assertThat(columnFamilyHandle.getID()).isEqualTo(1);

        final ColumnFamilyDescriptor latestDescriptor = columnFamilyHandle.getDescriptor();
        assertThat(latestDescriptor.getName()).isEqualTo(cfName);

        final List<byte[]> columnFamilyNames =
            RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
        assertThat(columnFamilyNames).isNotNull();
        assertThat(columnFamilyNames.size()).isGreaterThan(0);
        assertThat(columnFamilyNames.size()).isEqualTo(2);
        assertThat(new String(columnFamilyNames.get(0))).isEqualTo("default");
        assertThat(new String(columnFamilyNames.get(1))).isEqualTo("new_cf");
      }
    }
  }

  @Test
  public void openWithColumnFamilies() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(defaultCF, newCF);

      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      // Test open database with column family names
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(
              options, dbFolder.getRoot().getAbsolutePath(), cfNames, columnFamilyHandleList)) {
        assertThat(columnFamilyHandleList.size()).isEqualTo(2);
        db.put("dfkey1".getBytes(), "dfvalue".getBytes());
        db.put(columnFamilyHandleList.get(0), "dfkey2".getBytes(), "dfvalue".getBytes());
        db.put(columnFamilyHandleList.get(1), "newcfkey1".getBytes(), "newcfvalue".getBytes());

        final String retVal =
            new String(db.get(columnFamilyHandleList.get(1), "newcfkey1".getBytes()));
        assertThat(retVal).isEqualTo("newcfvalue");
        assertThat((db.get(columnFamilyHandleList.get(1), "dfkey1".getBytes()))).isNull();
        db.delete(columnFamilyHandleList.get(1), "newcfkey1".getBytes());
        assertThat((db.get(columnFamilyHandleList.get(1), "newcfkey1".getBytes()))).isNull();
        db.delete(columnFamilyHandleList.get(0), new WriteOptions(), "dfkey2".getBytes());
        assertThat(db.get(columnFamilyHandleList.get(0), new ReadOptions(), "dfkey2".getBytes()))
            .isNull();
      }
    }
  }

  @Test
  public void defaultColumnFamilySynonyms() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("dfkey_syn_1".getBytes(), "dfvalue_syn_1".getBytes());
    }

    try (ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(
              options, dbFolder.getRoot().getAbsolutePath(), cfNames, columnFamilyHandleList)) {
        assertThat(columnFamilyHandleList.size()).isEqualTo(3);
        assertThat(db.get(columnFamilyHandleList.get(1), "dfkey_syn_1".getBytes()))
            .isEqualTo("dfvalue_syn_1".getBytes());
        db.put(columnFamilyHandleList.get(1), "dfkey_syn_2".getBytes(), "dfvalue_syn_2".getBytes());
      }
    }

    try (ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames2 = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList2 = new ArrayList<>();

      try (final RocksDB db = RocksDB.open(new DBOptions(), dbFolder.getRoot().getAbsolutePath(),
               cfNames2, columnFamilyHandleList2)) {
        assertThat(db.get("dfkey_syn_2".getBytes())).isEqualTo("dfvalue_syn_2".getBytes());
      }
    }
  }

  @Test
  public void defaultColumnFamilySynonymsReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("dfkey_syn_1".getBytes(), "dfvalue_syn_1".getBytes());
    }

    try (final ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(
              options, dbFolder.getRoot().getAbsolutePath(), cfNames, columnFamilyHandleList)) {
        assertThat(columnFamilyHandleList.size()).isEqualTo(3);
        assertThat(db.get(columnFamilyHandleList.get(1), "dfkey_syn_1".getBytes()))
            .isEqualTo("dfvalue_syn_1".getBytes());
        db.put(columnFamilyHandleList.get(1), "dfkey_syn_2".getBytes(), "dfvalue_syn_2".getBytes());
      }
    }

    try (final ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames2 = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList2 = new ArrayList<>();

      try (final RocksDB db = RocksDB.openReadOnly(new DBOptions(),
               dbFolder.getRoot().getAbsolutePath(), cfNames2, columnFamilyHandleList2)) {
        assertThat(db.get("dfkey_syn_2".getBytes())).isEqualTo("dfvalue_syn_2".getBytes());
      }
    }
  }

  @Test
  public void defaultColumnFamilySynonymsOTDB() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final OptimisticTransactionDB db =
             OptimisticTransactionDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("dfkey_syn_1".getBytes(), "dfvalue_syn_1".getBytes());
    }

    try (final ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final OptimisticTransactionDB db = OptimisticTransactionDB.open(
              options, dbFolder.getRoot().getAbsolutePath(), cfNames, columnFamilyHandleList)) {
        assertThat(columnFamilyHandleList.size()).isEqualTo(3);
        assertThat(db.get(columnFamilyHandleList.get(1), "dfkey_syn_1".getBytes()))
            .isEqualTo("dfvalue_syn_1".getBytes());
        db.put(columnFamilyHandleList.get(1), "dfkey_syn_2".getBytes(), "dfvalue_syn_2".getBytes());
      }
    }

    try (final ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames2 = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList2 = new ArrayList<>();

      try (final OptimisticTransactionDB db = OptimisticTransactionDB.open(new DBOptions(),
               dbFolder.getRoot().getAbsolutePath(), cfNames2, columnFamilyHandleList2)) {
        assertThat(db.get("dfkey_syn_2".getBytes())).isEqualTo("dfvalue_syn_2".getBytes());
      }
    }
  }

  @Test
  public void defaultColumnFamilySynonymsTDB() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
         final TransactionDB db = TransactionDB.open(
             options, transactionDBOptions, dbFolder.getRoot().getAbsolutePath())) {
      db.put("dfkey_syn_1".getBytes(), "dfvalue_syn_1".getBytes());
    }

    try (final ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
          final TransactionDB db = TransactionDB.open(options, transactionDBOptions,
              dbFolder.getRoot().getAbsolutePath(), cfNames, columnFamilyHandleList)) {
        assertThat(columnFamilyHandleList.size()).isEqualTo(3);
        assertThat(db.get(columnFamilyHandleList.get(1), "dfkey_syn_1".getBytes()))
            .isEqualTo("dfvalue_syn_1".getBytes());
        db.put(columnFamilyHandleList.get(1), "dfkey_syn_2".getBytes(), "dfvalue_syn_2".getBytes());
      }
    }

    try (final ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames2 = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList2 = new ArrayList<>();

      try (final TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
          final TransactionDB db = TransactionDB.open(new DBOptions(), transactionDBOptions,
              dbFolder.getRoot().getAbsolutePath(), cfNames2, columnFamilyHandleList2)) {
        assertThat(db.get("dfkey_syn_2".getBytes())).isEqualTo("dfvalue_syn_2".getBytes());
      }
    }
  }

  @Test
  public void defaultColumnFamilySynonymsTTLDB() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TtlDB db = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("dfkey_syn_1".getBytes(), "dfvalue_syn_1".getBytes());
    }

    try (final ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final TtlDB db = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath(), cfNames,
              columnFamilyHandleList, Arrays.asList(10, 10, 10), false)) {
        assertThat(columnFamilyHandleList.size()).isEqualTo(3);
        assertThat(db.get(columnFamilyHandleList.get(1), "dfkey_syn_1".getBytes()))
            .isEqualTo("dfvalue_syn_1".getBytes());
        db.put(columnFamilyHandleList.get(1), "dfkey_syn_2".getBytes(), "dfvalue_syn_2".getBytes());
      }
    }

    try (final ColumnFamilyDescriptor newCF1 = new ColumnFamilyDescriptor("new_cf1".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF2 = new ColumnFamilyDescriptor("new_cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfNames2 = Arrays.asList(newCF1, defaultCF, newCF2);
      final List<ColumnFamilyHandle> columnFamilyHandleList2 = new ArrayList<>();

      try (final TtlDB db = TtlDB.open(new DBOptions(), dbFolder.getRoot().getAbsolutePath(),
               cfNames2, columnFamilyHandleList2, Arrays.asList(10, 10, 10), false)) {
        assertThat(db.get("dfkey_syn_2".getBytes())).isEqualTo("dfvalue_syn_2".getBytes());
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void openColumnFamiliesNoDefault() throws RocksDBException {
    try (final DBOptions dbOptions =
             new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions myCfOpts = new ColumnFamilyOptions();
        ColumnFamilyDescriptor myCf = new ColumnFamilyDescriptor("myCf".getBytes(), myCfOpts)) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(myCf);

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors,
          columnFamilyHandles);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void openColumnFamiliesNoDefaultReadOnly() throws RocksDBException {
    try (final DBOptions dbOptions =
             new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyDescriptor myCf = new ColumnFamilyDescriptor("myCf".getBytes());
        final ColumnFamilyDescriptor defaultCf =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.asList(myCf, defaultCf);

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors,
          columnFamilyHandles);
    }

    try (final DBOptions dbOptions = new DBOptions();
        ColumnFamilyDescriptor myCf = new ColumnFamilyDescriptor("myCf".getBytes())) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(myCf);

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      final RocksDB db = RocksDB.openReadOnly(dbOptions, dbFolder.getRoot().getAbsolutePath(),
          columnFamilyDescriptors, columnFamilyHandles);
      db.close();
    }
  }

  @Test
  public void getWithOutValueAndCf() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      // Test open database with column family names
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.put(columnFamilyHandleList.get(0), new WriteOptions(), "key1".getBytes(),
            "value".getBytes());
        db.put("key2".getBytes(), "12345678".getBytes());
        final byte[] outValue = new byte[5];
        // not found value
        int getResult = db.get("keyNotFound".getBytes(), outValue);
        assertThat(getResult).isEqualTo(RocksDB.NOT_FOUND);
        // found value which fits in outValue
        getResult = db.get(columnFamilyHandleList.get(0), "key1".getBytes(), outValue);
        assertThat(getResult).isEqualTo("value".getBytes().length);
        assertThat(outValue).isEqualTo("value".getBytes());
        // found value which fits partially
      }
    }
  }

  @Test
  public void getWithOutValueAndCfPartial() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.put(columnFamilyHandleList.get(0), "key1".getBytes(), "value".getBytes());
        db.put("key2".getBytes(), "12345678".getBytes());

        final byte[] partialOutValue = new byte[5];
        int getResult = db.get(columnFamilyHandleList.get(0), "key2".getBytes(), partialOutValue);
        assertThat(getResult).isEqualTo("12345678".getBytes().length);
        assertThat(partialOutValue).isEqualTo("12345".getBytes());

        final byte[] offsetKeyValue = "abckey2hjk".getBytes();
        assertThat(offsetKeyValue.length).isEqualTo(10);
        final byte[] offsetOutValue = "abcdefghjk".getBytes();
        assertThat(offsetOutValue.length).isEqualTo(10);

        getResult =
            db.get(columnFamilyHandleList.get(0), offsetKeyValue, 3, 4, offsetOutValue, 2, 5);
        assertThat(getResult).isEqualTo("12345678".getBytes().length);
        assertThat(offsetOutValue).isEqualTo("ab12345hjk".getBytes());
      }
    }
  }

  @Test
  public void getWithOutValueAndCfPartialAndOptions() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.put(columnFamilyHandleList.get(0), new WriteOptions(), "key1".getBytes(),
            "value".getBytes());
        db.put("key2".getBytes(), "12345678".getBytes());

        final byte[] partialOutValue = new byte[5];
        int getResult = db.get(
            columnFamilyHandleList.get(0), new ReadOptions(), "key2".getBytes(), partialOutValue);
        assertThat(getResult).isEqualTo("12345678".getBytes().length);
        assertThat(partialOutValue).isEqualTo("12345".getBytes());

        final byte[] offsetKeyValue = "abckey2hjk".getBytes();
        assertThat(offsetKeyValue.length).isEqualTo(10);
        final byte[] offsetOutValue = "abcdefghjk".getBytes();
        assertThat(offsetOutValue.length).isEqualTo(10);

        getResult = db.get(columnFamilyHandleList.get(0), new ReadOptions(), offsetKeyValue, 3, 4,
            offsetOutValue, 2, 5);
        assertThat(getResult).isEqualTo("12345678".getBytes().length);
        assertThat(offsetOutValue).isEqualTo("ab12345hjk".getBytes());
      }
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void getWithOutValueAndCfIndexOutOfBounds() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Collections.singletonList(defaultCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.put(columnFamilyHandleList.get(0), new WriteOptions(), "key1".getBytes(),
            "value".getBytes());
        db.put("key2".getBytes(), "12345678".getBytes());

        final byte[] offsetKeyValue = "abckey2hjk".getBytes();
        final byte[] partialOutValue = new byte[5];

        int getResult = db.get(columnFamilyHandleList.get(0), new ReadOptions(), offsetKeyValue, 3,
            4, partialOutValue, 2, 5);
      }
    }
  }

  @Test
  public void createWriteDropColumnFamily() throws RocksDBException {
    try (ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        final ColumnFamilyHandle tmpColumnFamilyHandle;
        tmpColumnFamilyHandle = db.createColumnFamily(
            new ColumnFamilyDescriptor("tmpCF".getBytes(), new ColumnFamilyOptions()));
        db.put(tmpColumnFamilyHandle, "key".getBytes(), "value".getBytes());
        db.dropColumnFamily(tmpColumnFamilyHandle);
        assertThat(tmpColumnFamilyHandle.isOwningHandle()).isTrue();
      }
    }
  }

  @Test
  public void createWriteDropColumnFamilies() throws RocksDBException {
    try (ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        final ColumnFamilyHandle tmpColumnFamilyHandle;
        final ColumnFamilyHandle tmpColumnFamilyHandle2;
        tmpColumnFamilyHandle = db.createColumnFamily(
            new ColumnFamilyDescriptor("tmpCF".getBytes(), new ColumnFamilyOptions()));
        tmpColumnFamilyHandle2 = db.createColumnFamily(
            new ColumnFamilyDescriptor("tmpCF2".getBytes(), new ColumnFamilyOptions()));
        db.put(tmpColumnFamilyHandle, "key".getBytes(), "value".getBytes());
        db.put(tmpColumnFamilyHandle2, "key".getBytes(), "value".getBytes());
        db.dropColumnFamilies(Arrays.asList(tmpColumnFamilyHandle, tmpColumnFamilyHandle2));
        assertThat(tmpColumnFamilyHandle.isOwningHandle()).isTrue();
        assertThat(tmpColumnFamilyHandle2.isOwningHandle()).isTrue();
      }
    }
  }

  @Test
  public void writeBatch() throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
        final ColumnFamilyOptions defaultCfOptions =
            new ColumnFamilyOptions().setMergeOperator(stringAppendOperator);
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultCfOptions);
        ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(options,
               dbFolder.getRoot().getAbsolutePath(),
               cfDescriptors, columnFamilyHandleList);
           final WriteBatch writeBatch = new WriteBatch();
           final WriteOptions writeOpt = new WriteOptions()) {
        writeBatch.put("key".getBytes(), "value".getBytes());
        writeBatch.put(db.getDefaultColumnFamily(), "mergeKey".getBytes(), "merge".getBytes());
        writeBatch.merge(db.getDefaultColumnFamily(), "mergeKey".getBytes(), "merge".getBytes());
        writeBatch.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(), "value".getBytes());
        writeBatch.put(columnFamilyHandleList.get(1), "newcfkey2".getBytes(), "value2".getBytes());
        writeBatch.delete("xyz".getBytes());
        writeBatch.delete(columnFamilyHandleList.get(1), "xyz".getBytes());
        db.write(writeOpt, writeBatch);

        assertThat(db.get(columnFamilyHandleList.get(1), "xyz".getBytes())).isNull();
        assertThat(new String(db.get(columnFamilyHandleList.get(1), "newcfkey".getBytes())))
            .isEqualTo("value");
        assertThat(new String(db.get(columnFamilyHandleList.get(1), "newcfkey2".getBytes())))
            .isEqualTo("value2");
        assertThat(new String(db.get("key".getBytes()))).isEqualTo("value");
        // check if key is merged
        assertThat(new String(db.get(db.getDefaultColumnFamily(), "mergeKey".getBytes())))
            .isEqualTo("merge,merge");
      }
    }
  }

  @Test
  public void iteratorOnColumnFamily() throws RocksDBException {
    try (ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(), "value".getBytes());
        db.put(columnFamilyHandleList.get(1), "newcfkey2".getBytes(), "value2".getBytes());
        try (final RocksIterator rocksIterator = db.newIterator(columnFamilyHandleList.get(1))) {
          rocksIterator.seekToFirst();
          final Map<String, String> refMap = new HashMap<>();
          refMap.put("newcfkey", "value");
          refMap.put("newcfkey2", "value2");
          int i = 0;
          while (rocksIterator.isValid()) {
            i++;
            assertThat(refMap.get(new String(rocksIterator.key())))
                .isEqualTo(new String(rocksIterator.value()));
            rocksIterator.next();
          }
          assertThat(i).isEqualTo(2);
        }
      }
    }
  }

  @FunctionalInterface
  public interface RocksDBTriFunction<T1, T2, T3, R> {
    R apply(T1 t1, T2 t2, T3 t3) throws IllegalArgumentException, RocksDBException;
  }

  private void multiGetHelper(
      RocksDBTriFunction<RocksDB, List<ColumnFamilyHandle>, List<byte[]>, List<byte[]>> multiGetter)
      throws RocksDBException {
    try (ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.put(columnFamilyHandleList.get(0), "key".getBytes(), "value".getBytes());
        db.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(), "value".getBytes());

        final List<byte[]> keys = Arrays.asList("key".getBytes(), "newcfkey".getBytes());

        List<byte[]> retValues = multiGetter.apply(db, columnFamilyHandleList, keys);
        assertThat(retValues.size()).isEqualTo(2);
        assertThat(new String(retValues.get(0))).isEqualTo("value");
        assertThat(new String(retValues.get(1))).isEqualTo("value");
      }
    }
  }

  @Test
  public void multiGet() throws RocksDBException {
    multiGetHelper(RocksDB::multiGetAsList);
  }

  @Test
  public void multiGetReadOptions() throws RocksDBException {
    multiGetHelper(
        (db, columnFamilies, keys) -> db.multiGetAsList(new ReadOptions(), columnFamilies, keys));
  }

  @Test
  public void multiGetAsList() throws RocksDBException {
    try (ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.put(columnFamilyHandleList.get(0), "key".getBytes(), "value".getBytes());
        db.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(), "value".getBytes());

        final List<byte[]> keys = Arrays.asList("key".getBytes(), "newcfkey".getBytes());
        List<byte[]> retValues = db.multiGetAsList(columnFamilyHandleList, keys);
        assertThat(retValues.size()).isEqualTo(2);
        assertThat(new String(retValues.get(0))).isEqualTo("value");
        assertThat(new String(retValues.get(1))).isEqualTo("value");
        retValues = db.multiGetAsList(new ReadOptions(), columnFamilyHandleList, keys);
        assertThat(retValues.size()).isEqualTo(2);
        assertThat(new String(retValues.get(0))).isEqualTo("value");
        assertThat(new String(retValues.get(1))).isEqualTo("value");
      }
    }
  }

  @Test
  public void properties() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        assertThat(db.getProperty("rocksdb.estimate-num-keys")).isNotNull();
        assertThat(db.getLongProperty(columnFamilyHandleList.get(0), "rocksdb.estimate-num-keys"))
            .isGreaterThanOrEqualTo(0);
        assertThat(db.getProperty("rocksdb.stats")).isNotNull();
        assertThat(db.getProperty(columnFamilyHandleList.get(0), "rocksdb.sstables")).isNotNull();
        assertThat(db.getProperty(columnFamilyHandleList.get(1), "rocksdb.estimate-num-keys"))
            .isNotNull();
        assertThat(db.getProperty(columnFamilyHandleList.get(1), "rocksdb.stats")).isNotNull();
        assertThat(db.getProperty(columnFamilyHandleList.get(1), "rocksdb.sstables")).isNotNull();
        assertThat(db.getAggregatedLongProperty("rocksdb.estimate-num-keys")).isNotNull();
        assertThat(db.getAggregatedLongProperty("rocksdb.estimate-num-keys"))
            .isGreaterThanOrEqualTo(0);
      }
    }
  }


  @Test
  public void iterators() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        List<RocksIterator> iterators = null;
        try {
          iterators = db.newIterators(columnFamilyHandleList);
          assertThat(iterators.size()).isEqualTo(2);
          RocksIterator iter = iterators.get(0);
          iter.seekToFirst();
          final Map<String, String> defRefMap = new HashMap<>();
          defRefMap.put("dfkey1", "dfvalue");
          defRefMap.put("key", "value");
          while (iter.isValid()) {
            assertThat(defRefMap.get(new String(iter.key()))).isEqualTo(new String(iter.value()));
            iter.next();
          }
          // iterate over new_cf key/value pairs
          final Map<String, String> cfRefMap = new HashMap<>();
          cfRefMap.put("newcfkey", "value");
          cfRefMap.put("newcfkey2", "value2");
          iter = iterators.get(1);
          iter.seekToFirst();
          while (iter.isValid()) {
            assertThat(cfRefMap.get(new String(iter.key()))).isEqualTo(new String(iter.value()));
            iter.next();
          }
        } finally {
          if (iterators != null) {
            for (final RocksIterator rocksIterator : iterators) {
              rocksIterator.close();
            }
          }
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failPutDisposedCF() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.dropColumnFamily(columnFamilyHandleList.get(1));
        db.put(columnFamilyHandleList.get(1), "key".getBytes(), "value".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failRemoveDisposedCF() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.dropColumnFamily(columnFamilyHandleList.get(1));
        db.delete(columnFamilyHandleList.get(1), "key".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failGetDisposedCF() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        db.dropColumnFamily(columnFamilyHandleList.get(1));
        db.get(columnFamilyHandleList.get(1), "key".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failMultiGetWithoutCorrectNumberOfCF() throws RocksDBException {
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath(),
              cfDescriptors, columnFamilyHandleList)) {
        final List<byte[]> keys = new ArrayList<>();
        keys.add("key".getBytes());
        keys.add("newcfkey".getBytes());
        final List<ColumnFamilyHandle> cfCustomList = new ArrayList<>();
        db.multiGetAsList(cfCustomList, keys);
      }
    }
  }

  @Test
  public void testByteCreateFolumnFamily() throws RocksDBException {
    final byte[] b0 = new byte[] {(byte) 0x00};
    final byte[] b1 = new byte[] {(byte) 0x01};
    final byte[] b2 = new byte[] {(byte) 0x02};
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor cfB0 = new ColumnFamilyDescriptor(b0);
        final ColumnFamilyDescriptor cfB1 = new ColumnFamilyDescriptor(b1);
        final ColumnFamilyDescriptor cfB2 = new ColumnFamilyDescriptor(b2);) {
      db.createColumnFamily(cfB0);
      db.createColumnFamily(cfB1);
      final List<byte[]> families =
          RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
      assertThat(families).contains("default".getBytes(), b0, b1);

      db.createColumnFamily(cfB2);
    }
  }

  @Test
  public void testCFNamesWithZeroBytes() throws RocksDBException {
    final byte[] b0 = new byte[] {0, 0};
    final byte[] b1 = new byte[] {0, 1};
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor cfB0 = new ColumnFamilyDescriptor(b0);
        final ColumnFamilyDescriptor cfB1 = new ColumnFamilyDescriptor(b1);) {
      db.createColumnFamily(cfB0);
      db.createColumnFamily(cfB1);
      final List<byte[]> families =
          RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
      assertThat(families).contains("default".getBytes(), b0, b1);
    }
  }

  @Test
  public void testCFNameSimplifiedChinese() throws RocksDBException {
    final String simplifiedChinese = "\u7b80\u4f53\u5b57";
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor chineseCf =
            new ColumnFamilyDescriptor(simplifiedChinese.getBytes());) {
      db.createColumnFamily(chineseCf);

      final List<byte[]> families =
          RocksDB.listColumnFamilies(options, dbFolder.getRoot().getAbsolutePath());
      assertThat(families).contains("default".getBytes(), simplifiedChinese.getBytes());
    }
  }

  @Test
  public void testDestroyColumnFamilyHandle() throws RocksDBException {
    final byte[] name1 = "cf1".getBytes();
    final byte[] name2 = "cf2".getBytes();
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor desc1 = new ColumnFamilyDescriptor(name1);
        final ColumnFamilyDescriptor desc2 = new ColumnFamilyDescriptor(name2)) {
      final ColumnFamilyHandle cf1 = db.createColumnFamily(desc1);
      final ColumnFamilyHandle cf2 = db.createColumnFamily(desc2);
      assertTrue(cf1.isOwningHandle());
      assertTrue(cf2.isOwningHandle());
      assertFalse(cf1.isDefaultColumnFamily());
      db.destroyColumnFamilyHandle(cf1);
      // At this point cf1 should not be used!
      assertFalse(cf1.isOwningHandle());
      assertTrue(cf2.isOwningHandle());
    }
  }
}
