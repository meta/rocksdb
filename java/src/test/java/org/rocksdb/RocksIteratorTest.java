// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RocksIteratorTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  private void validateByteBufferResult(
      final int fetched, final ByteBuffer byteBuffer, final String expected) {
    assertThat(fetched).isEqualTo(expected.length());
    assertThat(byteBuffer.position()).isEqualTo(0);
    assertThat(byteBuffer.limit()).isEqualTo(Math.min(byteBuffer.remaining(), expected.length()));
    final int bufferSpace = byteBuffer.remaining();
    final byte[] contents = new byte[bufferSpace];
    byteBuffer.get(contents, 0, bufferSpace);
    assertThat(contents).isEqualTo(
        expected.substring(0, bufferSpace).getBytes(StandardCharsets.UTF_8));
  }

  private void validateKey(
      final RocksIterator iterator, final ByteBuffer byteBuffer, final String key) {
    validateByteBufferResult(iterator.key(byteBuffer), byteBuffer, key);
  }

  private void validateValue(
      final RocksIterator iterator, final ByteBuffer byteBuffer, final String value) {
    validateByteBufferResult(iterator.value(byteBuffer), byteBuffer, value);
  }

  @Test
  public void rocksIteratorByteBuffers() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        validateKey(iterator, ByteBuffer.allocateDirect(2), "key1");
        validateKey(iterator, ByteBuffer.allocateDirect(2), "key0");
        validateKey(iterator, ByteBuffer.allocateDirect(4), "key1");
        validateKey(iterator, ByteBuffer.allocateDirect(5), "key1");
        validateValue(iterator, ByteBuffer.allocateDirect(2), "value2");
        validateValue(iterator, ByteBuffer.allocateDirect(2), "vasicu");
        validateValue(iterator, ByteBuffer.allocateDirect(8), "value1");

        validateKey(iterator, ByteBuffer.allocate(2), "key1");
        validateKey(iterator, ByteBuffer.allocate(2), "key0");
        validateKey(iterator, ByteBuffer.allocate(4), "key1");
        validateKey(iterator, ByteBuffer.allocate(5), "key1");
        validateValue(iterator, ByteBuffer.allocate(2), "value1");
        validateValue(iterator, ByteBuffer.allocate(8), "value1");
      }
    }
  }

  @Test
  public void rocksIteratorByteArrayValues() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        final byte[] valueArray0 = new byte[2];
        assertThat(iterator.value(valueArray0)).isEqualTo(6);
        assertThat(valueArray0).isEqualTo("va".getBytes());
        final byte[] valueArray1 = new byte[8];
        assertThat(iterator.value(valueArray1)).isEqualTo(6);
        assertThat(valueArray1).isEqualTo("value1\0\0".getBytes());
        final byte[] valueArray2 = new byte[10];
        assertThat(iterator.value(valueArray2, 2, 6)).isEqualTo(6);
        assertThat(valueArray2).isEqualTo("\0\0value1\0\0".getBytes());
        final byte[] valueArray3 = new byte[10];
        assertThat(iterator.value(valueArray3, 5, 5)).isEqualTo(6);
        assertThat(valueArray3).isEqualTo("\0\0\0\0\0value".getBytes());
        final byte[] valueArray4 = new byte[6];
        try {
          iterator.value(valueArray4, 1, 6);
          fail("Expected IndexOutOfBoundsException");
        } catch (final IndexOutOfBoundsException ignored) {
          // we should arrive here
        }
        final byte[] valueArray5 = new byte[7];
        assertThat(iterator.value(valueArray5, 1, 6)).isEqualTo(6);
        assertThat(valueArray5).isEqualTo("\0value1".getBytes());
      }
    }
  }

  @Test
  public void rocksIteratorByteArrayKeys() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        final byte[] keyArray0 = new byte[2];
        assertThat(iterator.key(keyArray0)).isEqualTo(4);
        assertThat(keyArray0).isEqualTo("ke".getBytes());
        final byte[] keyArray1 = new byte[8];
        assertThat(iterator.key(keyArray1)).isEqualTo(4);
        assertThat(keyArray1).isEqualTo("key1\0\0\0\0".getBytes());
        final byte[] keyArray2 = new byte[10];
        assertThat(iterator.key(keyArray2, 2, 6)).isEqualTo(4);
        assertThat(keyArray2).isEqualTo("\0\0key1\0\0\0\0".getBytes());
        final byte[] keyArray3 = new byte[10];
        assertThat(iterator.key(keyArray3, 5, 3)).isEqualTo(4);
        assertThat(keyArray3).isEqualTo("\0\0\0\0\0key\0\0".getBytes());
        final byte[] keyArray4 = new byte[4];
        try {
          iterator.key(keyArray4, 1, 4);
          fail("Expected IndexOutOfBoundsException");
        } catch (final IndexOutOfBoundsException ignored) {
          // we should arrive here
        }
        final byte[] keyArray5 = new byte[5];
        assertThat(iterator.key(keyArray5, 1, 4)).isEqualTo(4);
        assertThat(keyArray5).isEqualTo("\0key1".getBytes());
      }
    }
  }

  @Test
  public void rocksIteratorSimple() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
        iterator.next();
        assertThat(iterator.isValid()).isFalse();
        iterator.seekToLast();
        iterator.prev();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());
        iterator.seekToFirst();
        iterator.seekToLast();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
        assertThat(iterator.value()).isEqualTo("value2".getBytes());
        iterator.status();
      }
    }
  }

  @Test
  public void rocksIterator() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());
        assertThat(iterator.value()).isEqualTo("value1".getBytes());

        {
          final ByteBuffer key = ByteBuffer.allocate(12);
          key.put("key1".getBytes()).flip();
          iterator.seek(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);

          validateValue(iterator, ByteBuffer.allocateDirect(12), "value1");
          validateValue(iterator, ByteBuffer.allocateDirect(4), "valu56");
        }

        {
          final ByteBuffer key = ByteBuffer.allocate(12);
          key.put("key2".getBytes()).flip();
          iterator.seekForPrev(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value2".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }

        {
          final ByteBuffer key = ByteBuffer.allocate(12);
          key.put("key1".getBytes()).flip();
          iterator.seek(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }

        {
          // Check offsets of slice byte buffers
          final ByteBuffer key0 = ByteBuffer.allocate(24);
          key0.put("key2key2".getBytes());
          final ByteBuffer key = key0.slice();
          key.put("key1".getBytes()).flip();
          iterator.seek(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }

        {
          // Check offsets of slice byte buffers
          final ByteBuffer key0 = ByteBuffer.allocateDirect(24);
          key0.put("key2key2".getBytes());
          final ByteBuffer key = key0.slice();
          key.put("key1".getBytes()).flip();
          iterator.seek(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }

        {
          final ByteBuffer key = ByteBuffer.allocate(12);
          key.put("key2".getBytes()).flip();
          iterator.seekForPrev(key);
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.value()).isEqualTo("value2".getBytes());
          assertThat(key.position()).isEqualTo(4);
          assertThat(key.limit()).isEqualTo(4);
        }
      }
    }
  }

  @Test
  public void rocksIteratorSeekAndInsert() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seek("key0".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());

        iterator.seek("key1".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());

        iterator.seek("key1.5".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());

        iterator.seek("key2".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());

        iterator.seek("key3".getBytes());
        assertThat(iterator.isValid()).isFalse();
      }

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekForPrev("key0".getBytes());
        assertThat(iterator.isValid()).isFalse();

        iterator.seekForPrev("key1".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());

        iterator.seekForPrev("key1.5".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key1".getBytes());

        iterator.seekForPrev("key2".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());

        iterator.seekForPrev("key3".getBytes());
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key2".getBytes());
      }

      try (final RocksIterator iterator = db.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();

        byte[] lastKey;
        do {
          lastKey = iterator.key();
          iterator.next();
        } while (iterator.isValid());

        db.put("key3".getBytes(), "value3".getBytes());
        assertThat(iterator.isValid()).isFalse();
        iterator.refresh();
        iterator.seek(lastKey);
        assertThat(iterator.isValid()).isTrue();

        iterator.next();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key3".getBytes());
      }
    }
  }

  @Test
  public void rocksIteratorSeekAndInsertOnSnapshot() throws RocksDBException {
    try (final Options options =
             new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());

      try (final Snapshot snapshot = db.getSnapshot()) {
        try (final RocksIterator iterator = db.newIterator()) {
          // check for just keys 1 and 2
          iterator.seek("key0".getBytes());
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key1".getBytes());

          iterator.seek("key2".getBytes());
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key2".getBytes());

          iterator.seek("key3".getBytes());
          assertThat(iterator.isValid()).isFalse();
        }

        // add a new key (after the snapshot was taken)
        db.put("key3".getBytes(), "value3".getBytes());

        try (final RocksIterator iterator = db.newIterator()) {
          // check for keys 1, 2, and 3
          iterator.seek("key0".getBytes());
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key1".getBytes());

          iterator.seek("key2".getBytes());
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key2".getBytes());

          iterator.seek("key3".getBytes());
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key3".getBytes());

          iterator.seek("key4".getBytes());
          assertThat(iterator.isValid()).isFalse();

          // reset iterator to snapshot, iterator should now only see keys
          // there were present in the db when the snapshot was taken
          iterator.refresh(snapshot);

          // again check for just keys 1 and 2
          iterator.seek("key0".getBytes());
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key1".getBytes());

          iterator.seek("key2".getBytes());
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key2".getBytes());

          iterator.seek("key3".getBytes());
          assertThat(iterator.isValid()).isFalse();
        }
      }
    }
  }

  @Test
  public void rocksIteratorReleaseAfterCfClose() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             this.dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());

      // Test case: release iterator after default CF close
      try (final RocksIterator iterator = db.newIterator()) {
        // In fact, calling close() on default CF has no effect
        db.getDefaultColumnFamily().close();

        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("key".getBytes());
        assertThat(iterator.value()).isEqualTo("value".getBytes());
      }

      // Test case: release iterator after custom CF close
      try (final ColumnFamilyDescriptor cfd1 = new ColumnFamilyDescriptor("cf1".getBytes())) {
        final ColumnFamilyHandle cfHandle1 = db.createColumnFamily(cfd1);
        db.put(cfHandle1, "key1".getBytes(), "value1".getBytes());

        try (final RocksIterator iterator = db.newIterator(cfHandle1)) {
          cfHandle1.close();

          iterator.seekToFirst();
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key1".getBytes());
          assertThat(iterator.value()).isEqualTo("value1".getBytes());
        }
      }

      // Test case: release iterator after custom CF drop & close
      try (final ColumnFamilyDescriptor cfd2 = new ColumnFamilyDescriptor("cf2".getBytes())) {
        final ColumnFamilyHandle cfHandle2 = db.createColumnFamily(cfd2);
        db.put(cfHandle2, "key2".getBytes(), "value2".getBytes());

        try (final RocksIterator iterator = db.newIterator(cfHandle2)) {
          db.dropColumnFamily(cfHandle2);
          cfHandle2.close();

          iterator.seekToFirst();
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key2".getBytes());
          assertThat(iterator.value()).isEqualTo("value2".getBytes());
        }
      }
    }
  }
}
