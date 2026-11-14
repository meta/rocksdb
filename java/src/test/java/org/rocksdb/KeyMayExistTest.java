// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class KeyMayExistTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  List<ColumnFamilyDescriptor> cfDescriptors;
  List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
  RocksDB db;

  // Slice key
  int offset;
  int len;

  byte[] sliceKey;
  byte[] sliceValue;

  @Before
  public void before() throws RocksDBException {
    cfDescriptors = Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final DBOptions options =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);

    db = RocksDB.open(
        options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList);

    // Build the slice key
    final StringBuilder builder = new StringBuilder("prefix");
    offset = builder.toString().length();
    builder.append("slice key 0");
    len = builder.toString().length() - offset;
    builder.append("suffix");
    sliceKey = builder.toString().getBytes(UTF_8);
    sliceValue = "slice value 0".getBytes(UTF_8);
  }

  @After
  public void after() {
    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
      columnFamilyHandle.close();
    }
    for (final ColumnFamilyDescriptor descriptor : cfDescriptors) {
      descriptor.close();
    }
    db.close();
  }

  @Test
  public void keyMayExist() throws RocksDBException {
    assertThat(columnFamilyHandleList.size()).isEqualTo(2);

    // Standard key
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));

    // Test without column family
    final Holder<byte[]> holder = new Holder<>();
    boolean exists = db.keyMayExist("key".getBytes(UTF_8), holder);
    assertThat(exists).isTrue();
    assertThat(holder.getValue()).isNotNull();
    assertThat(new String(holder.getValue(), UTF_8)).isEqualTo("value");

    exists = db.keyMayExist("key".getBytes(UTF_8), null);
    assertThat(exists).isTrue();
  }

  @Test
  public void keyMayExistReadOptions() throws RocksDBException {
    // Test without column family but with readOptions
    try (final ReadOptions readOptions = new ReadOptions()) {
      // Standard key
      db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));

      // Slice key
      db.put(sliceKey, offset, len, sliceValue, 0, sliceValue.length);

      final Holder<byte[]> holder = new Holder<>();
      boolean exists = db.keyMayExist(readOptions, "key".getBytes(UTF_8), holder);
      assertThat(exists).isTrue();
      assertThat(holder.getValue()).isNotNull();
      assertThat(new String(holder.getValue(), UTF_8)).isEqualTo("value");

      exists = db.keyMayExist(readOptions, "key".getBytes(UTF_8), null);
      assertThat(exists).isTrue();

      exists = db.keyMayExist(readOptions, sliceKey, offset, len, holder);
      assertThat(exists).isTrue();
      assertThat(holder.getValue()).isNotNull();
      assertThat(holder.getValue()).isEqualTo(sliceValue);

      exists = db.keyMayExist(readOptions, sliceKey, offset, len, null);
      assertThat(exists).isTrue();
    }
  }

  @Test
  public void keyMayExistColumnFamily() throws RocksDBException {
    // Standard key
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));

    // Slice key
    db.put(sliceKey, offset, len, sliceValue, 0, sliceValue.length);

    // Test slice key with column family
    final Holder<byte[]> holder = new Holder<>();
    boolean exists = db.keyMayExist(columnFamilyHandleList.get(0), sliceKey, offset, len, holder);
    assertThat(exists).isTrue();
    assertThat(holder.getValue()).isNotNull();
    assertThat(holder.getValue()).isEqualTo(sliceValue);

    exists = db.keyMayExist(columnFamilyHandleList.get(0), sliceKey, offset, len, null);
    assertThat(exists).isTrue();
  }

  @Test
  public void keyMayExistColumnFamilyReadOptions() throws RocksDBException {
    // Standard key
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));

    // Slice key
    db.put(sliceKey, offset, len, sliceValue, 0, sliceValue.length);

    // Test slice key with column family and read options
    final Holder<byte[]> holder = new Holder<>();
    try (final ReadOptions readOptions = new ReadOptions()) {
      boolean exists =
          db.keyMayExist(columnFamilyHandleList.get(0), readOptions, "key".getBytes(UTF_8), holder);
      assertThat(exists).isTrue();
      assertThat(holder.getValue()).isNotNull();
      assertThat(new String(holder.getValue(), UTF_8)).isEqualTo("value");

      exists =
          db.keyMayExist(columnFamilyHandleList.get(0), readOptions, "key".getBytes(UTF_8), null);
      assertThat(exists).isTrue();

      // Test slice key with column family and read options
      exists =
          db.keyMayExist(columnFamilyHandleList.get(0), readOptions, sliceKey, offset, len, holder);
      assertThat(exists).isTrue();
      assertThat(holder.getValue()).isNotNull();
      assertThat(holder.getValue()).isEqualTo(sliceValue);

      exists =
          db.keyMayExist(columnFamilyHandleList.get(0), readOptions, sliceKey, offset, len, null);
      assertThat(exists).isTrue();
    }
  }

  @Test
  public void keyMayExistSliceKey() throws RocksDBException {
    assertThat(columnFamilyHandleList.size()).isEqualTo(2);

    // Standard key
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));

    // Slice key
    db.put(sliceKey, offset, len, sliceValue, 0, sliceValue.length);

    final Holder<byte[]> holder = new Holder<>();
    boolean exists = db.keyMayExist(sliceKey, offset, len, holder);
    assertThat(exists).isTrue();
    assertThat(holder.getValue()).isNotNull();
    assertThat(holder.getValue()).isEqualTo(sliceValue);

    exists = db.keyMayExist(sliceKey, offset, len, null);
    assertThat(exists).isTrue();

    exists = db.keyMayExist("slice key".getBytes(UTF_8), null);
    assertThat(exists).isFalse();

    exists = db.keyMayExist("slice key 0".getBytes(UTF_8), null);
    assertThat(exists).isTrue();

    // Test with column family
    exists = db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(UTF_8), holder);
    assertThat(exists).isTrue();
    assertThat(holder.getValue()).isNotNull();
    assertThat(new String(holder.getValue(), UTF_8)).isEqualTo("value");

    exists = db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(UTF_8), null);
    assertThat(exists).isTrue();

    // KeyMayExist in CF1 must return null value
    exists = db.keyMayExist(columnFamilyHandleList.get(1), "key".getBytes(UTF_8), holder);
    assertThat(exists).isFalse();
    assertThat(holder.getValue()).isNull();
    exists = db.keyMayExist(columnFamilyHandleList.get(1), "key".getBytes(UTF_8), null);
    assertThat(exists).isFalse();

    // slice key
    exists = db.keyMayExist(columnFamilyHandleList.get(1), sliceKey, 1, 3, holder);
    assertThat(exists).isFalse();
    assertThat(holder.getValue()).isNull();
    exists = db.keyMayExist(columnFamilyHandleList.get(1), sliceKey, 1, 3, null);
    assertThat(exists).isFalse();
  }

  @Test
  public void keyMayExistCF1() throws RocksDBException {
    // Standard key
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));

    // Slice key
    db.put(sliceKey, offset, len, sliceValue, 0, sliceValue.length);

    // KeyMayExist in CF1 must return null value
    final Holder<byte[]> holder = new Holder<>();
    boolean exists = db.keyMayExist(columnFamilyHandleList.get(1), "key".getBytes(UTF_8), holder);
    assertThat(exists).isFalse();
    assertThat(holder.getValue()).isNull();
    exists = db.keyMayExist(columnFamilyHandleList.get(1), "key".getBytes(UTF_8), null);
    assertThat(exists).isFalse();
  }

  @Test
  public void keyMayExistCF1Slice() throws RocksDBException {
    // Standard key
    db.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));

    // Slice key
    db.put(sliceKey, offset, len, sliceValue, 0, sliceValue.length);

    // slice key
    final Holder<byte[]> holder = new Holder<>();
    boolean exists = db.keyMayExist(columnFamilyHandleList.get(1), sliceKey, 1, 3, holder);
    assertThat(exists).isFalse();
    assertThat(holder.getValue()).isNull();
    exists = db.keyMayExist(columnFamilyHandleList.get(1), sliceKey, 1, 3, null);
    assertThat(exists).isFalse();
  }

  @Test
  public void keyMayExistBB() throws RocksDBException {
    // Standard key
    db.put("keyBB".getBytes(UTF_8), "valueBB".getBytes(UTF_8));

    final byte[] key = "keyBB".getBytes(UTF_8);
    final byte[] value = "valueBB".getBytes(UTF_8);

    final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
    keyBuffer.put(key, 0, key.length);
    keyBuffer.flip();

    assertThat(db.keyMayExist(keyBuffer)).isEqualTo(true);
    keyBuffer.flip();

    final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(value.length + 24);
    valueBuffer.position(12);
    KeyMayExist keyMayExist = db.keyMayExist(keyBuffer, valueBuffer);
    keyBuffer.flip();
    assertThat(keyMayExist.exists).isEqualTo(KeyMayExist.KeyMayExistEnum.kExistsWithValue);
    assertThat(keyMayExist.valueLength).isEqualTo(value.length);
    assertThat(valueBuffer.position()).isEqualTo(12);
    assertThat(valueBuffer.limit()).isEqualTo(12 + value.length);
    byte[] valueGet = new byte[value.length];
    valueBuffer.get(valueGet);
    assertThat(valueGet).isEqualTo(value);

    valueBuffer.limit(value.length + 24);
    valueBuffer.position(25);
    keyMayExist = db.keyMayExist(keyBuffer, valueBuffer);
    assertThat(keyMayExist.exists).isEqualTo(KeyMayExist.KeyMayExistEnum.kExistsWithValue);
    assertThat(keyMayExist.valueLength).isEqualTo(value.length);
    assertThat(valueBuffer.position()).isEqualTo(25);
    assertThat(valueBuffer.limit()).isEqualTo(24 + value.length);
    valueGet = new byte[value.length - 1];
    valueBuffer.get(valueGet);
    assertThat(valueGet).isEqualTo(Arrays.copyOfRange(value, 0, value.length - 1));

    exceptionRule.expect(BufferUnderflowException.class);
    valueGet = new byte[value.length];
    valueBuffer.get(valueGet);
  }

  @Test
  public void keyMayExistBBReadOptions() throws RocksDBException {
    // Standard key
    db.put("keyBB".getBytes(UTF_8), "valueBB".getBytes(UTF_8));

    final byte[] key = "keyBB".getBytes(UTF_8);
    final byte[] value = "valueBB".getBytes(UTF_8);

    final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
    keyBuffer.put(key, 0, key.length);
    keyBuffer.flip();

    try (final ReadOptions readOptions = new ReadOptions()) {
      assertThat(db.keyMayExist(readOptions, keyBuffer)).isEqualTo(true);
      keyBuffer.flip();
      final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(value.length + 24);
      valueBuffer.position(12);
      KeyMayExist keyMayExist = db.keyMayExist(readOptions, keyBuffer, valueBuffer);
      keyBuffer.flip();
      assertThat(keyMayExist.exists).isEqualTo(KeyMayExist.KeyMayExistEnum.kExistsWithValue);
      assertThat(keyMayExist.valueLength).isEqualTo(value.length);
      assertThat(valueBuffer.position()).isEqualTo(12);
      assertThat(valueBuffer.limit()).isEqualTo(12 + value.length);
      byte[] valueGet = new byte[value.length];
      valueBuffer.get(valueGet);
      assertThat(valueGet).isEqualTo(value);

      valueBuffer.limit(value.length + 24);
      valueBuffer.position(25);
      keyMayExist = db.keyMayExist(readOptions, keyBuffer, valueBuffer);
      keyBuffer.flip();
      assertThat(keyMayExist.exists).isEqualTo(KeyMayExist.KeyMayExistEnum.kExistsWithValue);
      assertThat(keyMayExist.valueLength).isEqualTo(value.length);
      assertThat(valueBuffer.position()).isEqualTo(25);
      assertThat(valueBuffer.limit()).isEqualTo(24 + value.length);
      valueGet = new byte[value.length - 1];
      valueBuffer.get(valueGet);
      assertThat(valueGet).isEqualTo(Arrays.copyOfRange(value, 0, value.length - 1));

      exceptionRule.expect(BufferUnderflowException.class);
      valueGet = new byte[value.length];
      valueBuffer.get(valueGet);
    }
  }

  @Test
  public void keyMayExistBBNullValue() throws RocksDBException {
    // Standard key
    db.put("keyBB".getBytes(UTF_8), "valueBB".getBytes(UTF_8));

    final byte[] key = "keyBB".getBytes(UTF_8);

    final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
    keyBuffer.put(key, 0, key.length);
    keyBuffer.flip();

    exceptionRule.expect(AssertionError.class);
    exceptionRule.expectMessage(
        "value ByteBuffer parameter cannot be null. If you do not need the value, use a different version of the method");
    final KeyMayExist keyMayExist = db.keyMayExist(keyBuffer, null);
  }

  @Test
  public void keyMayExistBBCF() throws RocksDBException {
    // Standard key
    db.put(columnFamilyHandleList.get(0), "keyBBCF0".getBytes(UTF_8), "valueBBCF0".getBytes(UTF_8));
    db.put(columnFamilyHandleList.get(1), "keyBBCF1".getBytes(UTF_8), "valueBBCF1".getBytes(UTF_8));

    // 0 is the default CF
    byte[] key = "keyBBCF0".getBytes(UTF_8);
    ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
    keyBuffer.put(key, 0, key.length);
    keyBuffer.flip();

    assertThat(db.keyMayExist(keyBuffer)).isEqualTo(true);
    keyBuffer.flip();
    assertThat(db.keyMayExist(columnFamilyHandleList.get(1), keyBuffer)).isEqualTo(false);
    keyBuffer.flip();
    assertThat(db.keyMayExist(columnFamilyHandleList.get(0), keyBuffer)).isEqualTo(true);

    // 1 is just a CF
    key = "keyBBCF1".getBytes(UTF_8);
    keyBuffer = ByteBuffer.allocateDirect(key.length);
    keyBuffer.put(key, 0, key.length);
    keyBuffer.flip();

    assertThat(db.keyMayExist(keyBuffer)).isEqualTo(false);
    keyBuffer.flip();
    assertThat(db.keyMayExist(columnFamilyHandleList.get(1), keyBuffer)).isEqualTo(true);
    keyBuffer.flip();
    assertThat(db.keyMayExist(columnFamilyHandleList.get(0), keyBuffer)).isEqualTo(false);
    keyBuffer.flip();

    exceptionRule.expect(AssertionError.class);
    exceptionRule.expectMessage(
        "value ByteBuffer parameter cannot be null. If you do not need the value, use a different version of the method");
    final KeyMayExist keyMayExist = db.keyMayExist(columnFamilyHandleList.get(0), keyBuffer, null);
  }

  @Test
  public void keyMayExistBBCFReadOptions() throws RocksDBException {
    // Standard key
    db.put(columnFamilyHandleList.get(0), "keyBBCF0".getBytes(UTF_8), "valueBBCF0".getBytes(UTF_8));
    db.put(columnFamilyHandleList.get(1), "keyBBCF1".getBytes(UTF_8), "valueBBCF1".getBytes(UTF_8));

    // 0 is the default CF
    byte[] key = "keyBBCF0".getBytes(UTF_8);
    ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
    keyBuffer.put(key, 0, key.length);
    keyBuffer.flip();

    try (final ReadOptions readOptions = new ReadOptions()) {
      assertThat(db.keyMayExist(keyBuffer)).isEqualTo(true);
      keyBuffer.flip();
      assertThat(db.keyMayExist(columnFamilyHandleList.get(1), readOptions, keyBuffer))
          .isEqualTo(false);
      keyBuffer.flip();
      assertThat(db.keyMayExist(columnFamilyHandleList.get(0), readOptions, keyBuffer))
          .isEqualTo(true);

      // 1 is just a CF
      key = "keyBBCF1".getBytes(UTF_8);
      keyBuffer = ByteBuffer.allocateDirect(key.length);
      keyBuffer.put(key, 0, key.length);
      keyBuffer.flip();

      assertThat(db.keyMayExist(readOptions, keyBuffer)).isEqualTo(false);
      keyBuffer.flip();
      assertThat(db.keyMayExist(columnFamilyHandleList.get(1), readOptions, keyBuffer))
          .isEqualTo(true);
      keyBuffer.flip();
      assertThat(db.keyMayExist(columnFamilyHandleList.get(0), readOptions, keyBuffer))
          .isEqualTo(false);

      exceptionRule.expect(AssertionError.class);
      exceptionRule.expectMessage(
          "value ByteBuffer parameter cannot be null. If you do not need the value, use a different version of the method");
      final KeyMayExist keyMayExist =
          db.keyMayExist(columnFamilyHandleList.get(0), readOptions, keyBuffer, null);
    }
  }

  @Test
  public void keyMayExistBBCFOffset() throws RocksDBException {
    db.put(columnFamilyHandleList.get(1), "keyBBCF1".getBytes(UTF_8), "valueBBCF1".getBytes(UTF_8));

    final byte[] key = "keyBBCF1".getBytes(UTF_8);
    final byte[] value = "valueBBCF1".getBytes(UTF_8);

    final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
    keyBuffer.put(key, 0, key.length);
    keyBuffer.flip();

    assertThat(db.keyMayExist(columnFamilyHandleList.get(1), keyBuffer)).isEqualTo(true);
    keyBuffer.flip();
    final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(value.length + 24);
    valueBuffer.position(12);
    KeyMayExist keyMayExist = db.keyMayExist(columnFamilyHandleList.get(1), keyBuffer, valueBuffer);
    keyBuffer.flip();
    assertThat(keyMayExist.exists).isEqualTo(KeyMayExist.KeyMayExistEnum.kExistsWithValue);
    assertThat(keyMayExist.valueLength).isEqualTo(value.length);
    assertThat(valueBuffer.position()).isEqualTo(12);
    assertThat(valueBuffer.limit()).isEqualTo(12 + value.length);
    byte[] valueGet = new byte[value.length];
    valueBuffer.get(valueGet);
    assertThat(valueGet).isEqualTo(value);

    valueBuffer.limit(value.length + 24);
    valueBuffer.position(25);
    keyMayExist = db.keyMayExist(columnFamilyHandleList.get(1), keyBuffer, valueBuffer);
    assertThat(keyMayExist.exists).isEqualTo(KeyMayExist.KeyMayExistEnum.kExistsWithValue);
    assertThat(keyMayExist.valueLength).isEqualTo(value.length);
    assertThat(valueBuffer.position()).isEqualTo(25);
    assertThat(valueBuffer.limit()).isEqualTo(24 + value.length);
    valueGet = new byte[value.length - 1];
    valueBuffer.get(valueGet);
    assertThat(valueGet).isEqualTo(Arrays.copyOfRange(value, 0, value.length - 1));

    exceptionRule.expect(BufferUnderflowException.class);
    valueGet = new byte[value.length];
    valueBuffer.get(valueGet);
  }

  @Test
  public void keyMayExistBBCFOffsetReadOptions() throws RocksDBException {
    db.put(columnFamilyHandleList.get(1), "keyBBCF1".getBytes(UTF_8), "valueBBCF1".getBytes(UTF_8));

    final byte[] key = "keyBBCF1".getBytes(UTF_8);
    final byte[] value = "valueBBCF1".getBytes(UTF_8);

    final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
    keyBuffer.put(key, 0, key.length);
    keyBuffer.flip();

    try (final ReadOptions readOptions = new ReadOptions()) {
      assertThat(db.keyMayExist(columnFamilyHandleList.get(1), readOptions, keyBuffer))
          .isEqualTo(true);
      keyBuffer.flip();
      final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(value.length + 24);
      valueBuffer.position(12);
      KeyMayExist keyMayExist =
          db.keyMayExist(columnFamilyHandleList.get(1), readOptions, keyBuffer, valueBuffer);
      keyBuffer.flip();
      assertThat(keyMayExist.exists).isEqualTo(KeyMayExist.KeyMayExistEnum.kExistsWithValue);
      assertThat(keyMayExist.valueLength).isEqualTo(value.length);
      assertThat(valueBuffer.position()).isEqualTo(12);
      assertThat(valueBuffer.limit()).isEqualTo(12 + value.length);
      byte[] valueGet = new byte[value.length];
      valueBuffer.get(valueGet);
      assertThat(valueGet).isEqualTo(value);

      valueBuffer.limit(value.length + 24);
      valueBuffer.position(25);
      keyMayExist =
          db.keyMayExist(columnFamilyHandleList.get(1), readOptions, keyBuffer, valueBuffer);
      keyBuffer.flip();
      assertThat(keyMayExist.exists).isEqualTo(KeyMayExist.KeyMayExistEnum.kExistsWithValue);
      assertThat(keyMayExist.valueLength).isEqualTo(value.length);
      assertThat(valueBuffer.position()).isEqualTo(25);
      assertThat(valueBuffer.limit()).isEqualTo(24 + value.length);
      valueGet = new byte[value.length - 1];
      valueBuffer.get(valueGet);
      assertThat(valueGet).isEqualTo(Arrays.copyOfRange(value, 0, value.length - 1));

      exceptionRule.expect(BufferUnderflowException.class);
      valueGet = new byte[value.length];
      valueBuffer.get(valueGet);
    }
  }

  @Test
  public void keyMayExistNonUnicodeString() throws RocksDBException {
    final byte[] key = "key".getBytes(UTF_8);
    final byte[] value = {(byte) 0x80}; // invalid unicode code-point
    db.put(key, value);

    final byte[] buf = new byte[10];
    final int read = db.get(key, buf);
    assertThat(read).isEqualTo(1);
    assertThat(buf).startsWith(value);

    final Holder<byte[]> holder = new Holder<>();
    boolean exists = db.keyMayExist("key".getBytes(UTF_8), holder);
    assertThat(exists).isTrue();
    assertThat(holder.getValue()).isNotNull();
    assertThat(holder.getValue()).isEqualTo(value);

    exists = db.keyMayExist("key".getBytes(UTF_8), null);
    assertThat(exists).isTrue();
  }
}
