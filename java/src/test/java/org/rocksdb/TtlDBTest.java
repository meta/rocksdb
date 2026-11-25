// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TtlDBTest {
  private static final int BATCH_ITERATION = 16;

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void ttlDBOpen() throws RocksDBException, InterruptedException {
    try (final Options options = new Options().setCreateIfMissing(true).setMaxCompactionBytes(0);
         final TtlDB ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      ttlDB.put("key".getBytes(), "value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).
          isEqualTo("value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).isNotNull();
    }
  }

  @Test
  public void ttlDBOpenWithTtl() throws RocksDBException, InterruptedException {
    try (final Options options = new Options().setCreateIfMissing(true).setMaxCompactionBytes(0);
         final TtlDB ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath(), 1, false)) {
      ttlDB.put("key".getBytes(), "value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).
          isEqualTo("value".getBytes());
      TimeUnit.SECONDS.sleep(2);
      ttlDB.compactRange();
      assertThat(ttlDB.get("key".getBytes())).isNull();
    }
  }

  @Test
  public void metadataReadsWithSnapshot() throws RocksDBException {
    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    final byte[] oldValue = "old".getBytes(StandardCharsets.UTF_8);
    final byte[] newValue = "new".getBytes(StandardCharsets.UTF_8);
    try (final Options options = new Options().setCreateIfMissing(true);
        final TtlDB ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      ttlDB.put(key, oldValue);
      try (final Snapshot snapshot = ttlDB.getSnapshot();
          final ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot)) {
        ttlDB.put(key, newValue);

        final GetWithMetadataResult result = ttlDB.getWithMetadata(readOptions, key);
        assertThat(result.value).isEqualTo(oldValue);
        assertThat(result.timestamp).isEmpty();
        assertThat(result.newerVersionPresent).isTrue();

        final List<GetWithMetadataResult> results =
            ttlDB.multiGetWithMetadata(readOptions, Arrays.asList(key));
        assertThat(results).hasSize(1);
        assertThat(results.get(0).value).isEqualTo(oldValue);
        assertThat(results.get(0).timestamp).isEmpty();
        assertThat(results.get(0).newerVersionPresent).isTrue();
      }
    }
  }

  @Test
  public void ttlDBSimpleIterator() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true).setMaxCompactionBytes(0);
         final TtlDB ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      ttlDB.put("keyI".getBytes(), "valueI".getBytes());
      try (final RocksIterator iterator = ttlDB.newIterator()) {
        iterator.seekToFirst();
        assertThat(iterator.isValid()).isTrue();
        assertThat(iterator.key()).isEqualTo("keyI".getBytes());
        assertThat(iterator.value()).isEqualTo("valueI".getBytes());
        iterator.next();
        assertThat(iterator.isValid()).isFalse();
      }
    }
  }

  @Test
  public void ttlDbOpenWithColumnFamilies() throws RocksDBException, InterruptedException {
    final List<Integer> ttlValues = Arrays.asList(0, 1);

    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final ColumnFamilyDescriptor defaultCF =
             new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes());
        final DBOptions dbOptions =
            new DBOptions().setCreateMissingColumnFamilies(true).setCreateIfMissing(true);
        final TtlDB ttlDB = TtlDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
            Arrays.asList(defaultCF, newCF), columnFamilyHandleList, ttlValues, false)) {
      try {
        ttlDB.put("key".getBytes(), "value".getBytes());
        assertThat(ttlDB.get("key".getBytes())).
            isEqualTo("value".getBytes());
        ttlDB.put(columnFamilyHandleList.get(1), "key".getBytes(),
            "value".getBytes());
        assertThat(ttlDB.get(columnFamilyHandleList.get(1),
            "key".getBytes())).isEqualTo("value".getBytes());
        TimeUnit.SECONDS.sleep(2);

        ttlDB.compactRange();
        ttlDB.compactRange(columnFamilyHandleList.get(1));

        assertThat(ttlDB.get("key".getBytes())).isNotNull();
        assertThat(ttlDB.get(columnFamilyHandleList.get(1),
            "key".getBytes())).isNull();
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void createTtlColumnFamily() throws RocksDBException,
      InterruptedException {
    try (final Options options = new Options().setCreateIfMissing(true);
        final TtlDB ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes());
        final ColumnFamilyHandle columnFamilyHandle = ttlDB.createColumnFamilyWithTtl(newCF, 1)) {
      ttlDB.put(columnFamilyHandle, "key".getBytes(),
          "value".getBytes());
      assertThat(ttlDB.get(columnFamilyHandle, "key".getBytes())).
          isEqualTo("value".getBytes());
      TimeUnit.SECONDS.sleep(2);
      ttlDB.compactRange(columnFamilyHandle);
      assertThat(ttlDB.get(columnFamilyHandle, "key".getBytes())).isNull();
    }
  }

  @Test
  public void writeBatchWithFlush() throws RocksDBException {
    try (final Options dbOptions = new Options()) {
      dbOptions.setCreateIfMissing(true);
      dbOptions.setCreateMissingColumnFamilies(true);

      try (final RocksDB db =
               TtlDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(), 100, false)) {
        try (WriteBatch wb = new WriteBatch()) {
          for (int i = 0; i < BATCH_ITERATION; i++) {
            wb.put(("key" + i).getBytes(StandardCharsets.UTF_8),
                ("value" + i).getBytes(StandardCharsets.UTF_8));
          }
          try (WriteOptions writeOptions = new WriteOptions()) {
            db.write(writeOptions, wb);
          }
          try (FlushOptions fOptions = new FlushOptions()) {
            db.flush(fOptions);
          }
        }
        for (int i = 0; i < BATCH_ITERATION; i++) {
          assertThat(db.get(("key" + i).getBytes(StandardCharsets.UTF_8)))
              .isEqualTo(("value" + i).getBytes(StandardCharsets.UTF_8));
        }
      }
    }
  }

  @Test
  public void writeBatchWithFlushAndColumnFamily() throws RocksDBException {
    try (final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyDescriptor newCF = new ColumnFamilyDescriptor("new_cf".getBytes());
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);) {
      System.out.println("Test start");
      dbOptions.setCreateIfMissing(true);
      dbOptions.setCreateMissingColumnFamilies(true);

      final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(newCF, defaultCF);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

      final List<Integer> ttlValues = Arrays.asList(0, 1);

      try (final RocksDB db = TtlDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(), cfNames,
               columnFamilyHandleList, ttlValues, false)) {
        try {
          assertThat(columnFamilyHandleList.get(1).isDefaultColumnFamily()).isTrue();

          try (WriteBatch wb = new WriteBatch()) {
            for (int i = 0; i < BATCH_ITERATION; i++) {
              wb.put(("key" + i).getBytes(StandardCharsets.UTF_8),
                  ("value" + i).getBytes(StandardCharsets.UTF_8));
            }
            try (WriteOptions writeOptions = new WriteOptions()) {
              db.write(writeOptions, wb);
            }
            try (FlushOptions fOptions = new FlushOptions()) {
              // Test both flush options, db.flush(fOptions) slush only default CF
              db.flush(fOptions);
              db.flush(fOptions, columnFamilyHandleList);
            }
          }
          for (int i = 0; i < BATCH_ITERATION; i++) {
            assertThat(db.get(("key" + i).getBytes(StandardCharsets.UTF_8)))
                .isEqualTo(("value" + i).getBytes(StandardCharsets.UTF_8));
          }
        } finally {
          // All CF handles must be closed before we close DB.
          columnFamilyHandleList.stream().forEach(ColumnFamilyHandle::close);
        }
      }
    }
  }
}
