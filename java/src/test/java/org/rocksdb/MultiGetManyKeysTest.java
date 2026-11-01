// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.util.Environment;

@RunWith(Parameterized.class)
public class MultiGetManyKeysTest {
  @Parameterized.Parameters
  public static List<Integer> data() {
    return Arrays.asList(2, 3, 250, 60000, 70000, 150000, 750000);
  }

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  private final int numKeys;

  public MultiGetManyKeysTest(final Integer numKeys) {
    this.numKeys = numKeys;
  }

  @BeforeClass
  public static void beforeAllTest() {
    Assume.assumeFalse("We are not running this test on 32bit systems dues to memory constraints",
        !Environment.is64Bit());
  }

  /**
   * Test for <a link="https://github.com/meta/rocksdb/issues/8039">multiGet problem</a>
   */
  @Test
  public void multiGetAsListLarge() throws RocksDBException {
    final List<byte[]> keys = generateRandomKeys(numKeys);
    final Map<Key, byte[]> keyValues = generateRandomKeyValues(keys, 10);
    putKeysAndValues(keyValues);

    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final List<byte[]> values = db.multiGetAsList(keys);
      assertKeysAndValues(keys, keyValues, values);
    }
  }

  /**
   * Test for <a link="https://github.com/meta/rocksdb/issues/9006">transactional multiGet
   * problem</a>
   */
  @Test
  public void multiGetAsListLargeTransactional() throws RocksDBException {
    final List<byte[]> keys = generateRandomKeys(numKeys);
    final Map<Key, byte[]> keyValues = generateRandomKeyValues(keys, 10);
    putKeysAndValues(keyValues);

    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB txnDB =
             TransactionDB.open(options, txnDbOptions, dbFolder.getRoot().getAbsolutePath())) {
      try (final Transaction transaction = txnDB.beginTransaction(new WriteOptions())) {
        final List<byte[]> values = transaction.multiGetAsList(new ReadOptions(), keys);
        assertKeysAndValues(keys, keyValues, values);
      }
    }
  }

  /**
   * Test for <a link="https://github.com/meta/rocksdb/issues/9006">transactional multiGet
   * problem</a>
   */
  @Test
  public void multiGetForUpdateAsListLargeTransactional() throws RocksDBException {
    final List<byte[]> keys = generateRandomKeys(numKeys);
    final Map<Key, byte[]> keyValues = generateRandomKeyValues(keys, 10);
    putKeysAndValues(keyValues);

    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB txnDB =
             TransactionDB.open(options, txnDbOptions, dbFolder.getRoot().getAbsolutePath())) {
      try (final Transaction transaction = txnDB.beginTransaction(new WriteOptions())) {
        final List<byte[]> values = transaction.multiGetForUpdateAsList(new ReadOptions(), keys);
        assertKeysAndValues(keys, keyValues, values);
      }
    }
  }

  /**
   * Test for <a link="https://github.com/meta/rocksdb/issues/9006">transactional multiGet
   * problem</a>
   */
  @Test
  public void multiGetAsListLargeTransactionalCF() throws RocksDBException {
    final List<byte[]> keys = generateRandomKeys(numKeys);
    final Map<Key, byte[]> keyValues = generateRandomKeyValues(keys, 10);
    final ColumnFamilyDescriptor columnFamilyDescriptor =
        new ColumnFamilyDescriptor("cfTest".getBytes());
    putKeysAndValues(columnFamilyDescriptor, keyValues);

    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    columnFamilyDescriptors.add(columnFamilyDescriptor);
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB txnDB = TransactionDB.open(new DBOptions(options), txnDbOptions,
             dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles)) {
      final List<ColumnFamilyHandle> columnFamilyHandlesForMultiGet = new ArrayList<>(numKeys);
      for (int i = 0; i < numKeys; i++)
        columnFamilyHandlesForMultiGet.add(columnFamilyHandles.get(0));
      try (final Transaction transaction = txnDB.beginTransaction(new WriteOptions())) {
        final List<byte[]> values =
            transaction.multiGetAsList(new ReadOptions(), columnFamilyHandlesForMultiGet, keys);
        assertKeysAndValues(keys, keyValues, values);
      }
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
        columnFamilyHandle.close();
      }
    }
  }

  /**
   * Test for <a link="https://github.com/meta/rocksdb/issues/9006">transactional multiGet
   * problem</a>
   */
  @Test
  public void multiGetForUpdateAsListLargeTransactionalCF() throws RocksDBException {
    final List<byte[]> keys = generateRandomKeys(numKeys);
    final Map<Key, byte[]> keyValues = generateRandomKeyValues(keys, 10);
    final ColumnFamilyDescriptor columnFamilyDescriptor =
        new ColumnFamilyDescriptor("cfTest".getBytes());
    putKeysAndValues(columnFamilyDescriptor, keyValues);

    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    columnFamilyDescriptors.add(columnFamilyDescriptor);
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try (final Options options = new Options().setCreateIfMissing(true);
         final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
         final TransactionDB txnDB = TransactionDB.open(new DBOptions(options), txnDbOptions,
             dbFolder.getRoot().getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles)) {
      final List<ColumnFamilyHandle> columnFamilyHandlesForMultiGet = new ArrayList<>(numKeys);
      for (int i = 0; i < numKeys; i++)
        columnFamilyHandlesForMultiGet.add(columnFamilyHandles.get(0));
      try (final Transaction transaction = txnDB.beginTransaction(new WriteOptions())) {
        final List<byte[]> values = transaction.multiGetForUpdateAsList(
            new ReadOptions(), columnFamilyHandlesForMultiGet, keys);
        assertKeysAndValues(keys, keyValues, values);
      }
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
        columnFamilyHandle.close();
      }
    }
  }

  private List<byte[]> generateRandomKeys(final int numKeys) {
    final Random rand = new Random();
    final List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      final byte[] key = new byte[4];
      rand.nextBytes(key);
      keys.add(key);
    }
    return keys;
  }

  private Map<Key, byte[]> generateRandomKeyValues(final List<byte[]> keys, final int percent) {
    final Random rand = new Random();
    final Map<Key, byte[]> keyValues = new HashMap<>();
    for (int i = 0; i < numKeys; i++) {
      if (rand.nextInt(100) < percent) {
        final byte[] value = new byte[1024];
        rand.nextBytes(value);
        keyValues.put(new Key(keys.get(i)), value);
      }
    }
    return keyValues;
  }

  private void putKeysAndValues(final Map<Key, byte[]> keyValues) throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      for (final Map.Entry<Key, byte[]> keyValue : keyValues.entrySet()) {
        db.put(keyValue.getKey().get(), keyValue.getValue());
      }
    }
  }

  private void putKeysAndValues(final ColumnFamilyDescriptor columnFamilyDescriptor,
      final Map<Key, byte[]> keyValues) throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
         final ColumnFamilyHandle columnFamilyHandle =
             db.createColumnFamily(columnFamilyDescriptor)) {
      for (final Map.Entry<Key, byte[]> keyValue : keyValues.entrySet()) {
        db.put(columnFamilyHandle, keyValue.getKey().get(), keyValue.getValue());
      }
    }
  }

  private void assertKeysAndValues(
      final List<byte[]> keys, final Map<Key, byte[]> keyValues, final List<byte[]> values) {
    assertThat(values.size()).isEqualTo(keys.size());
    for (int i = 0; i < numKeys; i++) {
      final Key key = new Key(keys.get(i));
      final byte[] value = values.get(i);
      if (keyValues.containsKey(key)) {
        assertThat(value).isEqualTo(keyValues.get(key));
      } else {
        assertThat(value).isNull();
      }
    }
  }

  private static class Key {
    private final byte[] bytes;
    public Key(final byte[] bytes) {
      this.bytes = bytes;
    }

    public byte[] get() {
      return this.bytes;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      final Key key = (Key) o;
      return Arrays.equals(bytes, key.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }
}
