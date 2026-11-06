// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.util.ReverseBytewiseComparator;

public class ByteBufferUnsupportedOperationTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  public static class Handler {
    private final RocksDB database;
    private final Map<UUID, ColumnFamilyHandle> columnFamilies;

    public Handler(final String path, final Options options) throws RocksDBException {
      RocksDB.destroyDB(path, options);
      this.database = RocksDB.open(options, path);
      this.columnFamilies = new ConcurrentHashMap<>();
    }

    public void addTable(final UUID streamID) throws RocksDBException {
      final ColumnFamilyOptions tableOptions = new ColumnFamilyOptions();
      tableOptions.optimizeUniversalStyleCompaction();
      try (final ComparatorOptions comparatorOptions = new ComparatorOptions();
          final ColumnFamilyDescriptor tableDescriptor = new ColumnFamilyDescriptor(
              streamID.toString().getBytes(StandardCharsets.UTF_8), tableOptions)) {
        // comparatorOptions.setReusedSynchronisationType(ReusedSynchronisationType.ADAPTIVE_MUTEX);
        tableOptions.setComparator(new ReverseBytewiseComparator(comparatorOptions));
        final ColumnFamilyHandle tableHandle = database.createColumnFamily(tableDescriptor);
        columnFamilies.put(streamID, tableHandle);
      }
    }

    public void updateAll(final List<byte[][]> keyValuePairs, final UUID streamID)
        throws RocksDBException {
      final ColumnFamilyHandle currTable = columnFamilies.get(streamID);
      try (final WriteBatch batchedWrite = new WriteBatch();
           final WriteOptions writeOptions = new WriteOptions()) {
        for (final byte[][] pair : keyValuePairs) {
          final byte[] keyBytes = pair[0];
          final byte[] valueBytes = pair[1];
          batchedWrite.put(currTable, keyBytes, valueBytes);
        }
        database.write(writeOptions, batchedWrite);
      }
    }
    public boolean containsValue(final byte[] encodedValue, final UUID streamID) {
      try (final RocksIterator iter = database.newIterator(columnFamilies.get(streamID))) {
        iter.seekToFirst();
        while (iter.isValid()) {
          final byte[] val = iter.value();
          if (Arrays.equals(val, encodedValue)) {
            return true;
          }
          iter.next();
        }
      }
      return false;
    }

    public void close() {
      for (final ColumnFamilyHandle handle : columnFamilies.values()) {
        handle.close();
      }
      database.close();
    }
  }

  private void inner(final int numRepeats) throws RocksDBException {
    final Options opts = new Options();
    opts.setCreateIfMissing(true);
    final Handler handler = new Handler("testDB", opts);
    final UUID stream1 = UUID.randomUUID();

    final List<byte[][]> entries = new ArrayList<>();
    for (int i = 0; i < numRepeats; i++) {
      final byte[] value = value(i);
      final byte[] key = key(i);
      entries.add(new byte[][] {key, value});
    }
    handler.addTable(stream1);
    handler.updateAll(entries, stream1);

    for (int i = 0; i < numRepeats; i++) {
      final byte[] val = value(i);
      final boolean hasValue = handler.containsValue(val, stream1);
      if (!hasValue) {
        throw new IllegalStateException("not has value " + i);
      }
    }

    handler.close();
  }

  private static byte[] key(final int i) {
    return ("key" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] value(final int i) {
    return ("value" + i).getBytes(StandardCharsets.UTF_8);
  }

  @Test
  public void unsupportedOperation() throws RocksDBException {
    final int numRepeats = 1000;
    final int repeatTest = 10;

    // the error is not always reproducible... let's try to increase the odds by repeating the main
    // test body
    for (int i = 0; i < repeatTest; i++) {
      try {
        inner(numRepeats);
      } catch (final RuntimeException runtimeException) {
        System.out.println("Exception on repeat " + i);
        throw runtimeException;
      }
    }
  }
}
