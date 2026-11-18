// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.rocksdb.EntryType.kEntryDelete;
import static org.rocksdb.EntryType.kEntryMerge;
import static org.rocksdb.EntryType.kEntryPut;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.util.ByteBufferAllocator;

@RunWith(Parameterized.class)
public class SstFileReaderTest {
  private static final String SST_FILE_NAME = "test.sst";

  static class KeyValueWithOp {
    KeyValueWithOp(final String key, final String value, final OpType opType) {
      this.key = key;
      this.value = value;
      this.opType = opType;
    }

    String getKey() {
      return key;
    }

    String getValue() {
      return value;
    }

    OpType getOpType() {
      return opType;
    }

    private final String key;
    private final String value;
    private final OpType opType;
  }

  @Rule public TemporaryFolder parentFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {"direct", ByteBufferAllocator.DIRECT}, {"indirect", ByteBufferAllocator.HEAP}});
  }

  @Parameterized.Parameter() public String name;

  @Parameterized.Parameter(1) public ByteBufferAllocator byteBufferAllocator;

  enum OpType {
    PUT(kEntryPut),
    PUT_BYTES(kEntryPut),
    MERGE(kEntryMerge),
    MERGE_BYTES(kEntryMerge),
    DELETE(kEntryDelete),
    DELETE_BYTES(kEntryDelete);

    private final EntryType entryType;

    private OpType(EntryType entryType) {
      this.entryType = entryType;
    }
    public EntryType getEntryType() {
      return entryType;
    }
  }

  static File newSstFile(TemporaryFolder parentFolder, final List<KeyValueWithOp> keyValues)
      throws IOException, RocksDBException {
    final EnvOptions envOptions = new EnvOptions();
    final StringAppendOperator stringAppendOperator = new StringAppendOperator();
    final Options options = new Options().setMergeOperator(stringAppendOperator);
    final SstFileWriter sstFileWriter;
    sstFileWriter = new SstFileWriter(envOptions, options);

    final File sstFile = parentFolder.newFile(SST_FILE_NAME);
    try {
      sstFileWriter.open(sstFile.getAbsolutePath());
      for (final KeyValueWithOp keyValue : keyValues) {
        final Slice keySlice = new Slice(keyValue.getKey());
        final Slice valueSlice = new Slice(keyValue.getValue());
        final byte[] keyBytes = keyValue.getKey().getBytes();
        final byte[] valueBytes = keyValue.getValue().getBytes();
        switch (keyValue.getOpType()) {
          case PUT:
            sstFileWriter.put(keySlice, valueSlice);
            break;
          case PUT_BYTES:
            sstFileWriter.put(keyBytes, valueBytes);
            break;
          case MERGE:
            sstFileWriter.merge(keySlice, valueSlice);
            break;
          case MERGE_BYTES:
            sstFileWriter.merge(keyBytes, valueBytes);
            break;
          case DELETE:
            sstFileWriter.delete(keySlice);
            break;
          case DELETE_BYTES:
            sstFileWriter.delete(keyBytes);
            break;
          default:
            fail("Unsupported op type");
        }
        keySlice.close();
        valueSlice.close();
      }
      sstFileWriter.finish();
    } finally {
      assertThat(sstFileWriter).isNotNull();
      sstFileWriter.close();
      options.close();
      envOptions.close();
    }
    return sstFile;
  }

  @Test
  public void readSstFile() throws RocksDBException, IOException {
    final List<KeyValueWithOp> keyValues = new ArrayList<>();
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key2", "value2", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key3", "value3", OpType.PUT));

    final File sstFile = newSstFile(parentFolder, keyValues);
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final Options options =
             new Options().setCreateIfMissing(true).setMergeOperator(stringAppendOperator);
         final SstFileReader reader = new SstFileReader(options);
         final ReadOptions readOptions = new ReadOptions()) {
      // Open the sst file and iterator
      reader.open(sstFile.getAbsolutePath());
      final SstFileReaderIterator iterator = reader.newIterator(readOptions);

      // Use the iterator to read sst file
      iterator.seekToFirst();

      // Verify Checksum
      reader.verifyChecksum();

      // Verify Table Properties
      assertEquals(reader.getTableProperties().getNumEntries(), 3);

      // Check key and value
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      final ByteBuffer byteBuffer = byteBufferAllocator.allocate(128);
      byteBuffer.put("key1".getBytes()).flip();
      iterator.seek(byteBuffer);
      assertThat(byteBuffer.position()).isEqualTo(4);
      assertThat(byteBuffer.limit()).isEqualTo(4);

      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      {
        byteBuffer.clear();
        assertThat(iterator.key(byteBuffer)).isEqualTo("key1".getBytes().length);
        final byte[] dst = new byte["key1".getBytes().length];
        byteBuffer.get(dst);
        assertThat(new String(dst)).isEqualTo("key1");
      }

      {
        byteBuffer.clear();
        byteBuffer.put("PREFIX".getBytes());
        final ByteBuffer slice = byteBuffer.slice();
        assertThat(iterator.key(byteBuffer)).isEqualTo("key1".getBytes().length);
        final byte[] dst = new byte["key1".getBytes().length];
        slice.get(dst);
        assertThat(new String(dst)).isEqualTo("key1");
      }

      {
        byteBuffer.clear();
        assertThat(iterator.value(byteBuffer)).isEqualTo("value1".getBytes().length);
        final byte[] dst = new byte["value1".getBytes().length];
        byteBuffer.get(dst);
        assertThat(new String(dst)).isEqualTo("value1");
      }

      byteBuffer.clear();
      byteBuffer.put("key1point5".getBytes()).flip();
      iterator.seek(byteBuffer);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key2".getBytes());
      assertThat(iterator.value()).isEqualTo("value2".getBytes());

      byteBuffer.clear();
      byteBuffer.put("key1point5".getBytes()).flip();
      iterator.seekForPrev(byteBuffer);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      byteBuffer.clear();
      byteBuffer.put("key2point5".getBytes()).flip();
      iterator.seek(byteBuffer);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key3".getBytes());
      assertThat(iterator.value()).isEqualTo("value3".getBytes());

      byteBuffer.clear();
      byteBuffer.put("key2point5".getBytes()).flip();
      iterator.seekForPrev(byteBuffer);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key2".getBytes());
      assertThat(iterator.value()).isEqualTo("value2".getBytes());

      byteBuffer.clear();
      byteBuffer.put("PREFIX".getBytes());
      final ByteBuffer slice = byteBuffer.slice();
      slice.put("key1point5".getBytes()).flip();
      iterator.seekForPrev(slice);
      assertThat(iterator.isValid()).isTrue();
      assertThat(iterator.key()).isEqualTo("key1".getBytes());
      assertThat(iterator.value()).isEqualTo("value1".getBytes());

      iterator.close();
    }
  }
}
