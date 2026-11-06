//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class KeyExistsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();
  @Rule public TemporaryFolder dbFolder2 = new TemporaryFolder();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  List<ColumnFamilyDescriptor> cfDescriptors;
  List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
  RocksDB db;
  @Before
  public void before() throws RocksDBException {
    cfDescriptors = Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final DBOptions options =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);

    db = RocksDB.open(
        options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList);
  }

  @After
  public void after() {
    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
      columnFamilyHandle.close();
    }
    db.close();
  }

  @Test
  public void keyExistAfterDbOpen() throws RocksDBException {
    try (RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {
      db2.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
      assertThat(db2.keyExists("key".getBytes(UTF_8))).isTrue();
      assertThat(db2.keyExists("key2".getBytes(UTF_8))).isFalse();
      assertThat(db2.keyMayExist("key".getBytes(UTF_8), null)).isTrue();
    }
    try (RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {
      assertThat(db2.keyMayExist("key".getBytes(UTF_8), null)).isTrue();
      assertThat(db2.keyExists("key".getBytes(UTF_8))).isTrue();
      assertThat(db2.keyExists("key2".getBytes(UTF_8))).isFalse();
    }
    try (RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {
      assertThat(db2.keyMayExist("key".getBytes(UTF_8), null)).isTrue();
    }
    try (RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {
      assertThat(db2.keyExists("key".getBytes(UTF_8))).isTrue();
    }
    try (RocksDB db2 = RocksDB.open(dbFolder2.getRoot().getAbsolutePath())) {
      assertThat(db2.keyExists("key2".getBytes(UTF_8))).isFalse();
    }
  }

  @Test
  public void keyExistInTtlDb() throws RocksDBException, IOException {
    final byte[] KNOWN_KEY = "random_key".getBytes(UTF_8);
    final long PSEUDO_RANDOM_SEED = 15875551233124l;

    try (Options options = new Options().setCreateIfMissing(true);
        TtlDB db2 = TtlDB.open(options, dbFolder2.getRoot().getAbsolutePath(), 86400, false);
        WriteOptions writeOptions = new WriteOptions()) {
      writeOptions.setDisableWAL(true);
      db2.put(KNOWN_KEY, "value".getBytes(UTF_8));

      ByteBuffer key = ByteBuffer.allocateDirect(16);
      ByteBuffer value = ByteBuffer.allocateDirect(16);
      Random r = new Random(PSEUDO_RANDOM_SEED);
      for (int i = 0; i < 1000; i++) {
        key.clear();
        key.putLong(r.nextLong());
        key.putLong(r.nextLong());
        key.flip();

        value.clear();
        value.putLong(r.nextLong());
        value.putLong(r.nextLong());
        value.flip();
        db2.put(writeOptions, key, value);
      }
      db2.compactRange();
    }
    try (Options options = new Options();
        TtlDB db2 = TtlDB.open(options, dbFolder2.getRoot().getAbsolutePath(), 86400, true);) {
      assertThat(db2.keyExists(KNOWN_KEY)).isTrue();
    }
  }

  @Test
  public void keyExists() throws RocksDBException {
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    boolean exists = db.keyExists("key".getBytes(UTF_8));
    assertThat(exists).isTrue();
    exists = db.keyExists("key2".getBytes(UTF_8));
    assertThat(exists).isFalse();
  }

  @Test
  public void keyExistsColumnFamily() throws RocksDBException {
    byte[] key1 = "keyBBCF0".getBytes(UTF_8);
    byte[] key2 = "keyBBCF1".getBytes(UTF_8);
    db.put(columnFamilyHandleList.get(0), key1, "valueBBCF0".getBytes(UTF_8));
    db.put(columnFamilyHandleList.get(1), key2, "valueBBCF1".getBytes(UTF_8));

    assertThat(db.keyExists(columnFamilyHandleList.get(0), key1)).isTrue();
    assertThat(db.keyExists(columnFamilyHandleList.get(0), key2)).isFalse();

    assertThat(db.keyExists(columnFamilyHandleList.get(1), key1)).isFalse();
    assertThat(db.keyExists(columnFamilyHandleList.get(1), key2)).isTrue();
  }

  @Test
  public void keyExistsColumnFamilyReadOptions() throws RocksDBException {
    try (final ReadOptions readOptions = new ReadOptions()) {
      byte[] key1 = "keyBBCF0".getBytes(UTF_8);
      byte[] key2 = "keyBBCF1".getBytes(UTF_8);
      db.put(columnFamilyHandleList.get(0), key1, "valueBBCF0".getBytes(UTF_8));
      db.put(columnFamilyHandleList.get(1), key2, "valueBBCF1".getBytes(UTF_8));

      assertThat(db.keyExists(columnFamilyHandleList.get(0), readOptions, key1)).isTrue();
      assertThat(db.keyExists(columnFamilyHandleList.get(0), readOptions, key2)).isFalse();

      assertThat(db.keyExists(columnFamilyHandleList.get(1), readOptions, key1)).isFalse();
      assertThat(db.keyExists(columnFamilyHandleList.get(1), readOptions, key2)).isTrue();
    }
  }

  @Test
  public void keyExistsReadOptions() throws RocksDBException {
    try (final ReadOptions readOptions = new ReadOptions()) {
      db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
      boolean exists = db.keyExists(readOptions, "key".getBytes(UTF_8));
      assertThat(exists).isTrue();
      exists = db.keyExists("key2".getBytes(UTF_8));
      assertThat(exists).isFalse();
    }
  }

  @Test
  public void keyExistsAfterDelete() throws RocksDBException {
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    boolean exists = db.keyExists(null, null, "key".getBytes(UTF_8), 0, 3);
    assertThat(exists).isTrue();
    db.delete("key".getBytes(UTF_8));
    exists = db.keyExists(null, null, "key".getBytes(UTF_8), 0, 3);
    assertThat(exists).isFalse();
  }

  @Test
  public void keyExistsArrayIndexOutOfBoundsException() throws RocksDBException {
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    exceptionRule.expect(IndexOutOfBoundsException.class);
    db.keyExists(null, null, "key".getBytes(UTF_8), 0, 5);
  }

  @Test()
  public void keyExistsArrayIndexOutOfBoundsExceptionWrongOffset() throws RocksDBException {
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    exceptionRule.expect(IndexOutOfBoundsException.class);
    db.keyExists(null, null, "key".getBytes(UTF_8), 6, 2);
  }

  @Test
  public void keyExistsDirectByteBuffer() throws RocksDBException {
    byte[] key = "key".getBytes(UTF_8);

    db.put(key, "value".getBytes(UTF_8));
    ByteBuffer buff = ByteBuffer.allocateDirect(key.length);
    buff.put(key);
    buff.flip();
    boolean exists = db.keyExists(buff);
    assertThat(exists).isTrue();
  }

  @Test
  public void keyExistsDirectByteBufferReadOptions() throws RocksDBException {
    try (final ReadOptions readOptions = new ReadOptions()) {
      byte[] key = "key".getBytes(UTF_8);

      db.put(key, "value".getBytes(UTF_8));
      ByteBuffer buff = ByteBuffer.allocateDirect(key.length);
      buff.put(key);
      buff.flip();

      boolean exists = db.keyExists(buff);
      assertThat(exists).isTrue();
    }
  }

  @Test
  public void keyExistsDirectByteBufferAfterDelete() throws RocksDBException {
    byte[] key = "key".getBytes(UTF_8);

    db.put(key, "value".getBytes(UTF_8));
    ByteBuffer buff = ByteBuffer.allocateDirect(key.length);
    buff.put(key);
    buff.flip();
    boolean exists = db.keyExists(buff);
    assertThat(exists).isTrue();
    db.delete(key);
    exists = db.keyExists(buff);
    assertThat(exists).isFalse();
  }

  @Test
  public void keyExistsDirectByteBufferColumnFamily() throws RocksDBException {
    byte[] key1 = "keyBBCF0".getBytes(UTF_8);
    byte[] key2 = "keyBBCF1".getBytes(UTF_8);
    db.put(columnFamilyHandleList.get(0), key1, "valueBBCF0".getBytes(UTF_8));
    db.put(columnFamilyHandleList.get(1), key2, "valueBBCF1".getBytes(UTF_8));

    ByteBuffer key1Buff = ByteBuffer.allocateDirect(key1.length);
    key1Buff.put(key1);
    key1Buff.flip();

    ByteBuffer key2Buff = ByteBuffer.allocateDirect(key2.length);
    key2Buff.put(key2);
    key2Buff.flip();

    assertThat(db.keyExists(columnFamilyHandleList.get(0), key1Buff)).isTrue();
    assertThat(db.keyExists(columnFamilyHandleList.get(0), key2Buff)).isFalse();

    assertThat(db.keyExists(columnFamilyHandleList.get(1), key1Buff)).isFalse();
    assertThat(db.keyExists(columnFamilyHandleList.get(1), key2Buff)).isTrue();
  }

  @Test
  public void keyExistsDirectByteBufferColumnFamilyReadOptions() throws RocksDBException {
    try (final ReadOptions readOptions = new ReadOptions()) {
      byte[] key1 = "keyBBCF0".getBytes(UTF_8);
      byte[] key2 = "keyBBCF1".getBytes(UTF_8);
      db.put(columnFamilyHandleList.get(0), key1, "valueBBCF0".getBytes(UTF_8));
      db.put(columnFamilyHandleList.get(1), key2, "valueBBCF1".getBytes(UTF_8));

      ByteBuffer key1Buff = ByteBuffer.allocateDirect(key1.length);
      key1Buff.put(key1);
      key1Buff.flip();

      ByteBuffer key2Buff = ByteBuffer.allocateDirect(key2.length);
      key2Buff.put(key2);
      key2Buff.flip();

      assertThat(db.keyExists(columnFamilyHandleList.get(0), readOptions, key1Buff)).isTrue();
      assertThat(db.keyExists(columnFamilyHandleList.get(0), readOptions, key2Buff)).isFalse();

      assertThat(db.keyExists(columnFamilyHandleList.get(1), readOptions, key1Buff)).isFalse();
      assertThat(db.keyExists(columnFamilyHandleList.get(1), readOptions, key2Buff)).isTrue();
    }
  }

  @Test
  public void keyExistsDirectReadOptions() throws RocksDBException {
    try (final ReadOptions readOptions = new ReadOptions()) {
      byte[] key = "key1".getBytes(UTF_8);
      db.put(key, "value".getBytes(UTF_8));
      ByteBuffer buff = ByteBuffer.allocateDirect(key.length);
      buff.put(key);
      buff.flip();
      boolean exists = db.keyExists(readOptions, key);
      assertThat(exists).isTrue();
      buff.clear();

      buff.put("key2".getBytes(UTF_8));
      buff.flip();
      exists = db.keyExists("key2".getBytes(UTF_8));
      assertThat(exists).isFalse();
    }
  }
}
