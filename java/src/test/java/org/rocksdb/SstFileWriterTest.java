// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.BytewiseComparator;

public class SstFileWriterTest {
  private static final String SST_FILE_NAME = "test.sst";
  private static final String DB_DIRECTORY_NAME = "test_db";

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE
      = new RocksNativeLibraryResource();

  @Rule public TemporaryFolder parentFolder = new TemporaryFolder();

  enum OpType { PUT, PUT_BYTES, PUT_DIRECT, MERGE, MERGE_BYTES, DELETE, DELETE_BYTES, DELETE_DIRECT }

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

  private File newSstFile(final List<KeyValueWithOp> keyValues,
      final boolean useJavaBytewiseComparator) throws IOException, RocksDBException {
    final EnvOptions envOptions = new EnvOptions();
    final StringAppendOperator stringAppendOperator = new StringAppendOperator();
    final Options options = new Options().setMergeOperator(stringAppendOperator);
    final SstFileWriter sstFileWriter;
    ComparatorOptions comparatorOptions = null;
    BytewiseComparator comparator = null;
    if (useJavaBytewiseComparator) {
      comparatorOptions = new ComparatorOptions().setUseDirectBuffer(false);
      comparator = new BytewiseComparator(comparatorOptions);
      options.setComparator(comparator);
      sstFileWriter = new SstFileWriter(envOptions, options);
    } else {
      sstFileWriter = new SstFileWriter(envOptions, options);
    }

    final File sstFile = parentFolder.newFile(SST_FILE_NAME);
    try {
      sstFileWriter.open(sstFile.getAbsolutePath());
      assertThat(sstFileWriter.fileSize()).isEqualTo(0);
      for (final KeyValueWithOp keyValue : keyValues) {
        final Slice keySlice = new Slice(keyValue.getKey());
        final Slice valueSlice = new Slice(keyValue.getValue());
        final byte[] keyBytes = keyValue.getKey().getBytes();
        final byte[] valueBytes = keyValue.getValue().getBytes();
        final ByteBuffer keyDirect = ByteBuffer.allocateDirect(keyBytes.length);
        keyDirect.put(keyBytes);
        keyDirect.flip();
        final ByteBuffer valueDirect = ByteBuffer.allocateDirect(valueBytes.length);
        valueDirect.put(valueBytes);
        valueDirect.flip();
        switch (keyValue.getOpType()) {
          case PUT:
            sstFileWriter.put(keySlice, valueSlice);
            break;
          case PUT_BYTES:
            sstFileWriter.put(keyBytes, valueBytes);
            break;
          case PUT_DIRECT:
            sstFileWriter.put(keyDirect, valueDirect);
            assertThat(keyDirect.position()).isEqualTo(keyBytes.length);
            assertThat(keyDirect.limit()).isEqualTo(keyBytes.length);
            assertThat(valueDirect.position()).isEqualTo(valueBytes.length);
            assertThat(valueDirect.limit()).isEqualTo(valueBytes.length);
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
          case DELETE_DIRECT:
            sstFileWriter.delete(keyDirect);
            break;
          default:
            fail("Unsupported op type");
        }
        keySlice.close();
        valueSlice.close();
      }
      sstFileWriter.finish();
      assertThat(sstFileWriter.fileSize()).isGreaterThan(100);
    } finally {
      assertThat(sstFileWriter).isNotNull();
      sstFileWriter.close();
      options.close();
      envOptions.close();
      if (comparatorOptions != null) {
        comparatorOptions.close();
      }
      if (comparator != null) {
        comparator.close();
      }
    }
    return sstFile;
  }

  @Test
  public void generateSstFileWithJavaComparator()
      throws RocksDBException, IOException {
    final List<KeyValueWithOp> keyValues = new ArrayList<>();
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key2", "value2", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key3", "value3", OpType.MERGE));
    keyValues.add(new KeyValueWithOp("key4", "value4", OpType.MERGE));
    keyValues.add(new KeyValueWithOp("key5", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key6", "", OpType.DELETE_DIRECT));

    newSstFile(keyValues, true);
  }

  @Test
  public void generateSstFileWithNativeComparator()
      throws RocksDBException, IOException {
    final List<KeyValueWithOp> keyValues = new ArrayList<>();
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key2", "value2", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key3", "value3", OpType.MERGE));
    keyValues.add(new KeyValueWithOp("key4", "value4", OpType.MERGE));
    keyValues.add(new KeyValueWithOp("key5", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key6", "", OpType.DELETE_DIRECT));

    newSstFile(keyValues, false);
  }

  @Test
  public void ingestSstFile() throws RocksDBException, IOException {
    final List<KeyValueWithOp> keyValues = new ArrayList<>();
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key2", "value2", OpType.PUT_DIRECT));
    keyValues.add(new KeyValueWithOp("key3", "value3", OpType.PUT_BYTES));
    keyValues.add(new KeyValueWithOp("key4", "value4", OpType.MERGE));
    keyValues.add(new KeyValueWithOp("key5", "value5", OpType.MERGE_BYTES));
    keyValues.add(new KeyValueWithOp("key6", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key7", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key8", "", OpType.DELETE_DIRECT));


    final File sstFile = newSstFile(keyValues, false);
    final File dbFolder = parentFolder.newFolder(DB_DIRECTORY_NAME);
    try(final StringAppendOperator stringAppendOperator =
            new StringAppendOperator();
        final Options options = new Options()
            .setCreateIfMissing(true)
            .setMergeOperator(stringAppendOperator);
        final RocksDB db = RocksDB.open(options, dbFolder.getAbsolutePath());
        final IngestExternalFileOptions ingestExternalFileOptions =
            new IngestExternalFileOptions()) {
      db.put("key8".getBytes(), "value8".getBytes());
      assertThat(db.get("key8".getBytes())).isEqualTo("value8".getBytes());
      db.ingestExternalFile(
          Collections.singletonList(sstFile.getAbsolutePath()), ingestExternalFileOptions);

      assertThat(db.get("key1".getBytes())).isEqualTo("value1".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo("value2".getBytes());
      assertThat(db.get("key3".getBytes())).isEqualTo("value3".getBytes());
      assertThat(db.get("key4".getBytes())).isEqualTo("value4".getBytes());
      assertThat(db.get("key5".getBytes())).isEqualTo("value5".getBytes());
      assertThat(db.get("key6".getBytes())).isEqualTo(null);
      assertThat(db.get("key7".getBytes())).isEqualTo(null);
      assertThat(db.get("key8".getBytes())).isEqualTo(null);
    }
  }

  @Test
  public void ingestSstFile_cf() throws RocksDBException, IOException {
    final List<KeyValueWithOp> keyValues = new ArrayList<>();
    keyValues.add(new KeyValueWithOp("key1", "value1", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key2", "value2", OpType.PUT));
    keyValues.add(new KeyValueWithOp("key3", "value3", OpType.MERGE));
    keyValues.add(new KeyValueWithOp("key4", "", OpType.DELETE));
    keyValues.add(new KeyValueWithOp("key5", "", OpType.DELETE_DIRECT));

    final File sstFile = newSstFile(keyValues, false);
    final File dbFolder = parentFolder.newFolder(DB_DIRECTORY_NAME);
    try(final StringAppendOperator stringAppendOperator =
            new StringAppendOperator();
        final Options options = new Options()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setMergeOperator(stringAppendOperator);
        final RocksDB db = RocksDB.open(options, dbFolder.getAbsolutePath());
        final IngestExternalFileOptions ingestExternalFileOptions =
            new IngestExternalFileOptions()) {

      try(final ColumnFamilyOptions cf_opts = new ColumnFamilyOptions()
              .setMergeOperator(stringAppendOperator);
          final ColumnFamilyHandle cf_handle = db.createColumnFamily(
              new ColumnFamilyDescriptor("new_cf".getBytes(), cf_opts))) {
        db.put(cf_handle, "key5".getBytes(), "value5".getBytes());
        assertThat(db.get(cf_handle, "key5".getBytes())).isEqualTo("value5".getBytes());
        db.ingestExternalFile(cf_handle, Collections.singletonList(sstFile.getAbsolutePath()),
            ingestExternalFileOptions);

        assertThat(db.get(cf_handle,
            "key1".getBytes())).isEqualTo("value1".getBytes());
        assertThat(db.get(cf_handle,
            "key2".getBytes())).isEqualTo("value2".getBytes());
        assertThat(db.get(cf_handle,
            "key3".getBytes())).isEqualTo("value3".getBytes());
        assertThat(db.get(cf_handle,
            "key4".getBytes())).isEqualTo(null);
        assertThat(db.get(cf_handle,
            "key5".getBytes())).isEqualTo(null);
      }
    }
  }
}
