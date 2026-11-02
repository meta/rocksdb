//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ImportColumnFamilyTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public TemporaryFolder checkpointFolder = new TemporaryFolder();

  @Test
  public void testImportColumnFamily() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
        db.put("key".getBytes(), "value".getBytes());
        db.put("key1".getBytes(), "value1".getBytes());

        try (final Checkpoint checkpoint = Checkpoint.create(db);
             final ImportColumnFamilyOptions importColumnFamilyOptions =
                 new ImportColumnFamilyOptions()) {
          ExportImportFilesMetaData default_cf_metadata =
              checkpoint.exportColumnFamily(db.getDefaultColumnFamily(),
                  checkpointFolder.getRoot().getAbsolutePath() + "/default_cf_metadata");
          ColumnFamilyDescriptor columnFamilyDescriptor =
              new ColumnFamilyDescriptor("new_cf".getBytes());
          final ColumnFamilyHandle importCfHandle = db.createColumnFamilyWithImport(
              columnFamilyDescriptor, importColumnFamilyOptions, default_cf_metadata);
          assertThat(db.get(importCfHandle, "key".getBytes())).isEqualTo("value".getBytes());
          assertThat(db.get(importCfHandle, "key1".getBytes())).isEqualTo("value1".getBytes());
        }
      }
    }
  }

  @Test
  public void ImportMultiColumnFamilyTest() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB db1 = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "db1");
           final RocksDB db2 =
               RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "db2");) {
        db1.put("key".getBytes(), "value".getBytes());
        db1.put("key1".getBytes(), "value1".getBytes());
        db2.put("key2".getBytes(), "value2".getBytes());
        db2.put("key3".getBytes(), "value3".getBytes());
        try (final Checkpoint checkpoint1 = Checkpoint.create(db1);
             final Checkpoint checkpoint2 = Checkpoint.create(db2);
             final ImportColumnFamilyOptions importColumnFamilyOptions =
                 new ImportColumnFamilyOptions()) {
          ExportImportFilesMetaData default_cf_metadata1 =
              checkpoint1.exportColumnFamily(db1.getDefaultColumnFamily(),
                  checkpointFolder.getRoot().getAbsolutePath() + "/default_cf_metadata1");
          ExportImportFilesMetaData default_cf_metadata2 =
              checkpoint2.exportColumnFamily(db2.getDefaultColumnFamily(),
                  checkpointFolder.getRoot().getAbsolutePath() + "/default_cf_metadata2");

          ColumnFamilyDescriptor columnFamilyDescriptor =
              new ColumnFamilyDescriptor("new_cf".getBytes());

          List<ExportImportFilesMetaData> importMetaDatas = new ArrayList<>();
          importMetaDatas.add(default_cf_metadata1);
          importMetaDatas.add(default_cf_metadata2);

          final ColumnFamilyHandle importCfHandle = db1.createColumnFamilyWithImport(
              columnFamilyDescriptor, importColumnFamilyOptions, importMetaDatas);
          assertThat(db1.get(importCfHandle, "key".getBytes())).isEqualTo("value".getBytes());
          assertThat(db1.get(importCfHandle, "key1".getBytes())).isEqualTo("value1".getBytes());
          assertThat(db1.get(importCfHandle, "key2".getBytes())).isEqualTo("value2".getBytes());
          assertThat(db1.get(importCfHandle, "key3".getBytes())).isEqualTo("value3".getBytes());
        }
      }
    }
  }
}
