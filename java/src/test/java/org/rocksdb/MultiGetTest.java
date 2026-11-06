// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.TestUtil;

public class MultiGetTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @FunctionalInterface
  public interface RocksDBBiFunction<T1, T2, R> {
    R apply(T1 t1, T2 t2) throws RocksDBException;
  }

  private void putNThenMultiGetHelper(
      RocksDBBiFunction<RocksDB, List<byte[]>, List<byte[]>> multiGetter) throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());
      final List<byte[]> keys =
          Arrays.asList("key1".getBytes(), "key2".getBytes(), "key3".getBytes());
      final List<byte[]> values = multiGetter.apply(db, keys);
      assertThat(values.size()).isEqualTo(keys.size());
      assertThat(values.get(0)).isEqualTo("value1ForKey1".getBytes());
      assertThat(values.get(1)).isEqualTo("value2ForKey2".getBytes());
      assertThat(values.get(2)).isEqualTo("value3ForKey3".getBytes());
    }
  }

  private void putNThenMultiGetHelperWithMissing(
      RocksDBBiFunction<RocksDB, List<byte[]>, List<byte[]>> multiGetter) throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());
      final List<byte[]> keys =
          Arrays.asList("key1".getBytes(), "key2".getBytes(), "key3".getBytes());
      final List<byte[]> values = multiGetter.apply(db, keys);
      assertThat(values.size()).isEqualTo(keys.size());
      assertThat(values.get(0)).isEqualTo("value1ForKey1".getBytes());
      assertThat(values.get(1)).isEqualTo(null);
      assertThat(values.get(2)).isEqualTo("value3ForKey3".getBytes());
    }
  }

  @Test
  public void putNThenMultiGet() throws RocksDBException {
    putNThenMultiGetHelper(RocksDB::multiGetAsList);
  }

  @Test
  public void putNThenMultiGetWithMissing() throws RocksDBException {
    putNThenMultiGetHelperWithMissing(RocksDB::multiGetAsList);
  }

  @Test
  public void putNThenMultiGetReadOptions() throws RocksDBException {
    putNThenMultiGetHelper((db, keys) -> db.multiGetAsList(new ReadOptions(), keys));
  }

  @Test
  public void putNThenMultiGetReadOptionsWithMissing() throws RocksDBException {
    putNThenMultiGetHelperWithMissing((db, keys) -> db.multiGetAsList(new ReadOptions(), keys));
  }

  @Test
  public void putNThenMultiGetDirect() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ByteBufferGetStatus> results = db.multiGetByteBuffers(keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }

      {
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(new ReadOptions(), keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectWithMissing() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ByteBufferGetStatus> results = db.multiGetByteBuffers(keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo(0);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(results.get(1).value).isNull();
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }

      {
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(new ReadOptions(), keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo(0);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(results.get(1).value).isNull();
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectSliced() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      keys.add(
          ByteBuffer.allocateDirect(12).put("prefix1".getBytes()).slice().put("key1".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ByteBufferGetStatus> results = db.multiGetByteBuffers(keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);
        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value3ForKey3".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value1ForKey1".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectSlicedWithMissing() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      keys.add(
          ByteBuffer.allocateDirect(12).put("prefix1".getBytes()).slice().put("key1".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ByteBufferGetStatus> results = db.multiGetByteBuffers(keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(1).requiredSize).isEqualTo("value3ForKey3".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(0).requiredSize).isEqualTo(0);

        assertThat(results.get(0).value).isNull();
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value3ForKey3".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value1ForKey1".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectBadValuesArray() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }

      {
        final List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
          values.add(ByteBuffer.allocateDirect(24));
        }

        values.remove(0);

        try {
          db.multiGetByteBuffers(keys, values);
          fail("Expected exception when not enough value ByteBuffers supplied");
        } catch (final IllegalArgumentException e) {
          assertThat(e.getMessage()).contains("For each key there must be a corresponding value");
        }
      }

      {
        final List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
          values.add(ByteBuffer.allocateDirect(24));
        }

        values.add(ByteBuffer.allocateDirect(24));

        try {
          db.multiGetByteBuffers(keys, values);
          fail("Expected exception when too many value ByteBuffers supplied");
        } catch (final IllegalArgumentException e) {
          assertThat(e.getMessage()).contains("For each key there must be a corresponding value");
        }
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectShortValueBuffers() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }

      {
        final List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
          values.add(ByteBuffer.allocateDirect(4));
        }

        final List<ByteBufferGetStatus> statii = db.multiGetByteBuffers(keys, values);
        assertThat(statii.size()).isEqualTo(values.size());
        for (final ByteBufferGetStatus status : statii) {
          assertThat(status.status.getCode()).isEqualTo(Status.Code.Ok);
          assertThat(status.requiredSize).isEqualTo("value3ForKey3".getBytes().length);
          final ByteBuffer expected =
              ByteBuffer.allocateDirect(24).put(Arrays.copyOf("valueX".getBytes(), 4));
          expected.flip();
          assertThat(status.value).isEqualTo(expected);
        }
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectNondefaultCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath());
        ColumnFamilyDescriptor cf0 = new ColumnFamilyDescriptor("cf0".getBytes());
        ColumnFamilyDescriptor cf1 = new ColumnFamilyDescriptor("cf1".getBytes());
        ColumnFamilyDescriptor cf2 = new ColumnFamilyDescriptor("cf2".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>(0);

      cfDescriptors.add(cf0);
      cfDescriptors.add(cf1);
      cfDescriptors.add(cf2);

      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      db.put(cf.get(0), "key1".getBytes(), "value1ForKey1".getBytes());
      db.put(cf.get(0), "key2".getBytes(), "value2ForKey2".getBytes());
      db.put(cf.get(0), "key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ByteBufferGetStatus> results = db.multiGetByteBuffers(keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.NotFound);
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(0));
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(0));
        columnFamilyHandles.add(cf.get(0));
        columnFamilyHandles.add(cf.get(0));
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(0));
        columnFamilyHandles.add(cf.get(0));
        columnFamilyHandles.add(cf.get(0));

        final List<ByteBuffer> keysWithMissing = new ArrayList<>();
        keysWithMissing.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
        keysWithMissing.add(ByteBuffer.allocateDirect(12).put("key3Bad".getBytes()));
        keysWithMissing.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
        // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
        for (final ByteBuffer key : keysWithMissing) {
          key.flip();
        }

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keysWithMissing, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo(0);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(results.get(1).value).isNull();
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectCFParams() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put("key3".getBytes(), "value3ForKey3".getBytes());

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      columnFamilyHandles.add(db.getDefaultColumnFamily());
      columnFamilyHandles.add(db.getDefaultColumnFamily());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }
      try {
        db.multiGetByteBuffers(columnFamilyHandles, keys, values);
        fail("Expected exception when 2 column families supplied");
      } catch (final IllegalArgumentException e) {
        assertThat(e.getMessage()).contains("Wrong number of ColumnFamilyHandle(s) supplied");
      }

      columnFamilyHandles.clear();
      columnFamilyHandles.add(db.getDefaultColumnFamily());
      final List<ByteBufferGetStatus> results =
          db.multiGetByteBuffers(columnFamilyHandles, keys, values);

      assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
      assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

      assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
      assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
      assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

      assertThat(TestUtil.bufferBytes(results.get(0).value)).isEqualTo("value1ForKey1".getBytes());
      assertThat(TestUtil.bufferBytes(results.get(1).value)).isEqualTo("value2ForKey2".getBytes());
      assertThat(TestUtil.bufferBytes(results.get(2).value)).isEqualTo("value3ForKey3".getBytes());
    }
  }

  @Test
  public void putNThenMultiGetDirectMixedCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor cf0 = new ColumnFamilyDescriptor("cf0".getBytes());
        final ColumnFamilyDescriptor cf1 = new ColumnFamilyDescriptor("cf1".getBytes());
        final ColumnFamilyDescriptor cf2 = new ColumnFamilyDescriptor("cf2".getBytes());
        final ColumnFamilyDescriptor cf3 = new ColumnFamilyDescriptor("cf3".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

      cfDescriptors.add(cf0);
      cfDescriptors.add(cf1);
      cfDescriptors.add(cf2);
      cfDescriptors.add(cf3);

      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      db.put(cf.get(1), "key1".getBytes(), "value1ForKey1".getBytes());
      db.put("key2".getBytes(), "value2ForKey2".getBytes());
      db.put(cf.get(3), "key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(db.getDefaultColumnFamily());

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.NotFound);

        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(1));

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.NotFound);

        assertThat(results.get(0).requiredSize).isEqualTo("value2ForKey2".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(1));
        columnFamilyHandles.add(db.getDefaultColumnFamily());
        columnFamilyHandles.add(cf.get(3));

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize).isEqualTo("value2ForKey2".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("value2ForKey2".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(db.getDefaultColumnFamily());
        columnFamilyHandles.add(cf.get(1));
        columnFamilyHandles.add(cf.get(3));

        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.NotFound);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }

  @Test
  public void putNThenMultiGetDirectTruncateCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath());
        ColumnFamilyDescriptor cf0 = new ColumnFamilyDescriptor("cf0".getBytes())) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
      cfDescriptors.add(cf0);
      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      db.put(cf.get(0), "key1".getBytes(), "value1ForKey1".getBytes());
      db.put(cf.get(0), "key2".getBytes(), "value2ForKey2WithLotsOfTrailingGarbage".getBytes());
      db.put(cf.get(0), "key3".getBytes(), "value3ForKey3".getBytes());

      final List<ByteBuffer> keys = new ArrayList<>();
      keys.add(ByteBuffer.allocateDirect(12).put("key1".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key2".getBytes()));
      keys.add(ByteBuffer.allocateDirect(12).put("key3".getBytes()));
      // Java8 and lower flip() returns Buffer not ByteBuffer, so can't chain above /\/\
      for (final ByteBuffer key : keys) {
        key.flip();
      }
      final List<ByteBuffer> values = new ArrayList<>();
      for (int i = 0; i < keys.size(); i++) {
        values.add(ByteBuffer.allocateDirect(24));
      }

      {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        columnFamilyHandles.add(cf.get(0));
        final List<ByteBufferGetStatus> results =
            db.multiGetByteBuffers(columnFamilyHandles, keys, values);

        assertThat(results.get(0).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
        assertThat(results.get(2).status.getCode()).isEqualTo(Status.Code.Ok);

        assertThat(results.get(0).requiredSize).isEqualTo("value1ForKey1".getBytes().length);
        assertThat(results.get(1).requiredSize)
            .isEqualTo("value2ForKey2WithLotsOfTrailingGarbage".getBytes().length);
        assertThat(results.get(2).requiredSize).isEqualTo("value3ForKey3".getBytes().length);

        assertThat(TestUtil.bufferBytes(results.get(0).value))
            .isEqualTo("value1ForKey1".getBytes());
        assertThat(TestUtil.bufferBytes(results.get(1).value))
            .isEqualTo("valu e2Fo rKey 2Wit hLot sOfT".replace(" ", "").getBytes());
        assertThat(TestUtil.bufferBytes(results.get(2).value))
            .isEqualTo("value3ForKey3".getBytes());
      }
    }
  }

  /**
   *
   * @param db database to write to
   * @param key key to write
   * @return expected size of data written
   * @throws RocksDBException if {@code put} or {@code merge} fail
   */
  private long createIntOverflowValue(
      final RocksDB db, final ColumnFamilyHandle cf, final String key) throws RocksDBException {
    final int BUFSIZE = 100000000;
    final int BUFCOUNT = 30;
    final byte[] wbuf = new byte[BUFSIZE];
    Arrays.fill(wbuf, (byte) 10);
    for (int i = 0; i < BUFCOUNT; i++) {
      final byte[] vals = ("value" + i + "ForKey" + key).getBytes();
      System.arraycopy(vals, 0, wbuf, 0, vals.length);
      db.merge(cf, "key1".getBytes(), wbuf);
    }
    return ((long) BUFSIZE + 1) * BUFCOUNT - 1;
  }

  private void checkIntOVerflowValue(final ByteBuffer byteBuffer, final String key) {
    final int BUFSIZE = 100000000;
    final int BUFCOUNT = 30;
    for (int i = 0; i < BUFCOUNT; i++) {
      final byte[] vals = ("value" + i + "ForKey" + key).getBytes();
      final long position = (long) i * (BUFSIZE + 1);
      if (position > Integer.MAX_VALUE)
        break;
      byteBuffer.position((int) position);
      for (byte b : vals) {
        assertThat(byteBuffer.get()).isEqualTo(b);
      }
    }
  }

  private static ByteBuffer bbDirect(final String s) {
    final byte[] bytes = s.getBytes();
    final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
    byteBuffer.put(bytes);
    byteBuffer.flip();

    return byteBuffer;
  }

  /**
   * Too slow/disk space dependent for CI
   * @throws RocksDBException
   */
  @Ignore
  @Test
  public void putBigMultiGetDirect() throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setMergeOperatorName("stringappend");
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final long length = createIntOverflowValue(db, db.getDefaultColumnFamily(), "key1");
      db.put("key2".getBytes(), "value2ForKey2".getBytes());

      final List<ByteBuffer> byteBufferValues = new ArrayList<>();
      byteBufferValues.add(ByteBuffer.allocateDirect(Integer.MAX_VALUE));
      final List<ByteBuffer> byteBufferKeys = new ArrayList<>();
      byteBufferKeys.add(bbDirect("key1"));

      final List<ByteBufferGetStatus> statusList =
          db.multiGetByteBuffers(new ReadOptions(), byteBufferKeys, byteBufferValues);

      assertThat(statusList.size()).isEqualTo(1);
      final ByteBufferGetStatus status = statusList.get(0);
      assertThat(status.status.getCode()).isEqualTo(Status.Code.Incomplete);

      checkIntOVerflowValue(status.value, "key1");
    }
  }

  /**
   * Too slow/disk space dependent for CI
   * @throws RocksDBException
   */
  @Ignore
  @Test
  public void putBigMultiGetDirectCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
        final ColumnFamilyOptions cfOptions =
            new ColumnFamilyOptions().setMergeOperatorName("stringappend");
        final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor cf0 =
            new ColumnFamilyDescriptor("cf0".getBytes(), cfOptions);) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>(0);

      cfDescriptors.add(cf0);
      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      final long length = createIntOverflowValue(db, cf.get(0), "key1");
      db.put(cf.get(0), "key2".getBytes(), "value2ForKey2".getBytes());

      final List<ByteBuffer> byteBufferValues = new ArrayList<>();
      byteBufferValues.add(ByteBuffer.allocateDirect(Integer.MAX_VALUE));
      final List<ByteBuffer> byteBufferKeys = new ArrayList<>();
      byteBufferKeys.add(bbDirect("key1"));

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      columnFamilyHandles.add(cf.get(0));

      final List<ByteBufferGetStatus> statusList = db.multiGetByteBuffers(
          new ReadOptions(), columnFamilyHandles, byteBufferKeys, byteBufferValues);

      assertThat(statusList.size()).isEqualTo(1);
      final ByteBufferGetStatus status = statusList.get(0);
      assertThat(status.status.getCode()).isEqualTo(Status.Code.Incomplete);

      checkIntOVerflowValue(status.value, "key1");
    }
  }

  /**
   * Too slow/disk space dependent for CI
   * @throws RocksDBException
   */
  @Ignore
  @Test
  public void putBigMultiGetDirect2Keys() throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setMergeOperatorName("stringappend");
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final long length = createIntOverflowValue(db, db.getDefaultColumnFamily(), "key1");
      db.put("key2".getBytes(), "value2ForKey2".getBytes());

      final List<ByteBuffer> byteBufferValues = new ArrayList<>();
      byteBufferValues.add(ByteBuffer.allocateDirect(Integer.MAX_VALUE));
      byteBufferValues.add(ByteBuffer.allocateDirect(12));
      final List<ByteBuffer> byteBufferKeys = new ArrayList<>();
      byteBufferKeys.add(bbDirect("key1"));
      byteBufferKeys.add(bbDirect("key2"));

      final List<ByteBufferGetStatus> statusList =
          db.multiGetByteBuffers(new ReadOptions(), byteBufferKeys, byteBufferValues);

      assertThat(statusList.size()).isEqualTo(2);
      assertThat(statusList.get(0).status.getCode()).isEqualTo(Status.Code.Incomplete);
      checkIntOVerflowValue(statusList.get(0).value, "key1");

      assertThat(statusList.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
      final ByteBuffer bbKey2 = statusList.get(1).value;
      final byte[] bytes = new byte[bbKey2.capacity()];
      bbKey2.get(bytes);
      assertThat(bytes).isEqualTo("value2ForKey".getBytes());
    }
  }

  /**
   * Too slow/disk space dependent for CI
   * @throws RocksDBException
   */
  @Ignore
  @Test
  public void putBigMultiGetDirect2KeysCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
        final ColumnFamilyOptions cfOptions =
            new ColumnFamilyOptions().setMergeOperatorName("stringappend");
        final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor cf1 =
            new ColumnFamilyDescriptor("cf0".getBytes(), cfOptions);) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>(0);

      cfDescriptors.add(cf1);
      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      final long length = createIntOverflowValue(db, cf.get(0), "key1");
      db.put(cf.get(0), "key2".getBytes(), "value2ForKey2".getBytes());

      final List<ByteBuffer> byteBufferValues = new ArrayList<>();
      byteBufferValues.add(ByteBuffer.allocateDirect(Integer.MAX_VALUE));
      byteBufferValues.add(ByteBuffer.allocateDirect(12));
      final List<ByteBuffer> byteBufferKeys = new ArrayList<>();
      byteBufferKeys.add(bbDirect("key1"));
      byteBufferKeys.add(bbDirect("key2"));

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      columnFamilyHandles.add(cf.get(0));

      final List<ByteBufferGetStatus> statusList = db.multiGetByteBuffers(
          new ReadOptions(), columnFamilyHandles, byteBufferKeys, byteBufferValues);

      assertThat(statusList.size()).isEqualTo(2);
      assertThat(statusList.get(0).status.getCode()).isEqualTo(Status.Code.Incomplete);
      checkIntOVerflowValue(statusList.get(0).value, "key1");

      assertThat(statusList.get(1).status.getCode()).isEqualTo(Status.Code.Ok);
      final ByteBuffer bbKey2 = statusList.get(1).value;
      final byte[] bytes = new byte[bbKey2.capacity()];
      bbKey2.get(bytes);
      assertThat(bytes).isEqualTo("value2ForKey".getBytes());
    }
  }

  /**
   * Too slow/disk space dependent for CI
   * @throws RocksDBException
   */
  @Ignore
  @Test
  public void putBigMultiGetAsList() throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setMergeOperatorName("stringappend");
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final long length = createIntOverflowValue(db, db.getDefaultColumnFamily(), "key1");
      db.put("key2".getBytes(), "value2ForKey2".getBytes());

      final List<byte[]> keys = new ArrayList<>();
      keys.add("key1".getBytes());
      assertThatThrownBy(() -> { db.multiGetAsList(keys); })
          .isInstanceOf(RocksDBException.class)
          .hasMessageContaining("Requested array size exceeds VM limit");
    }
  }

  /**
   * Too slow/disk space dependent for CI
   * @throws RocksDBException
   */
  @Ignore
  @Test
  public void putBigMultiGetAsListCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
        final ColumnFamilyOptions cfOptions =
            new ColumnFamilyOptions().setMergeOperatorName("stringappend");
        final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor cf0 =
            new ColumnFamilyDescriptor("cf0".getBytes(), cfOptions)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>(0);

      cfDescriptors.add(cf0);
      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      final long length = createIntOverflowValue(db, cf.get(0), "key1");
      db.put(cf.get(0), "key2".getBytes(), "value2ForKey2".getBytes());

      final List<byte[]> keys = new ArrayList<>();
      keys.add("key1".getBytes());
      assertThatThrownBy(() -> { db.multiGetAsList(cf, keys); })
          .isInstanceOf(RocksDBException.class)
          .hasMessageContaining("Requested array size exceeds VM limit");
    }
  }

  /**
   * Too slow/disk space dependent for CI
   * @throws RocksDBException
   */
  @Ignore
  @Test
  public void putBigMultiGetAsList2Keys() throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setMergeOperatorName("stringappend");
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final long length = createIntOverflowValue(db, db.getDefaultColumnFamily(), "key1");
      db.put("key2".getBytes(), "value2ForKey2".getBytes());

      final List<byte[]> keys = new ArrayList<>();
      keys.add("key2".getBytes());
      keys.add("key1".getBytes());
      assertThatThrownBy(() -> { db.multiGetAsList(keys); })
          .isInstanceOf(RocksDBException.class)
          .hasMessageContaining("Requested array size exceeds VM limit");
    }
  }

  /**
   * Too slow/disk space dependent for CI
   * @throws RocksDBException
   */
  @Ignore
  @Test
  public void putBigMultiGetAsList2KeysCF() throws RocksDBException {
    try (final Options opt = new Options().setCreateIfMissing(true);
        final ColumnFamilyOptions cfOptions =
            new ColumnFamilyOptions().setMergeOperatorName("stringappend");
        final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor cf0 =
            new ColumnFamilyDescriptor("cf0".getBytes(), cfOptions);) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>(0);
      cfDescriptors.add(cf0);
      final List<ColumnFamilyHandle> cf = db.createColumnFamilies(cfDescriptors);

      final long length = createIntOverflowValue(db, cf.get(0), "key1");
      db.put(cf.get(0), "key2".getBytes(), "value2ForKey2".getBytes());

      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      columnFamilyHandles.add(cf.get(0));
      columnFamilyHandles.add(cf.get(0));

      final List<byte[]> keys = new ArrayList<>();
      keys.add("key2".getBytes());
      keys.add("key1".getBytes());
      assertThatThrownBy(() -> { db.multiGetAsList(columnFamilyHandles, keys); })
          .isInstanceOf(RocksDBException.class)
          .hasMessageContaining("Requested array size exceeds VM limit");
    }
  }

  /**
   * This eventually doesn't throw as expected
   * At about 3rd loop of asking (on a 64GB M1 Max Mac)
   * I presume it's a legitimate space exhaustion error in RocksDB,
   * but I think it worth having this here as a record.
   *
   * @throws RocksDBException
   */
  @Test
  @Ignore
  public void putBigMultiGetAsListRepeat() throws RocksDBException {
    try (final Options opt =
             new Options().setCreateIfMissing(true).setMergeOperatorName("stringappend");
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final long length = createIntOverflowValue(db, db.getDefaultColumnFamily(), "key1");
      db.put("key2".getBytes(), "value2ForKey2".getBytes());

      final int REPEAT = 10;
      for (int i = 0; i < REPEAT; i++) {
        final List<byte[]> keys = new ArrayList<>();
        keys.add("key1".getBytes());
        assertThatThrownBy(() -> { db.multiGetAsList(keys); })
            .isInstanceOf(RocksDBException.class)
            .hasMessageContaining("Requested array size exceeds VM limit");
      }
    }
  }
}
