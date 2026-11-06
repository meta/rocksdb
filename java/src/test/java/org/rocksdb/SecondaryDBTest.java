// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
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

public class SecondaryDBTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule public TemporaryFolder secondaryDbFolder = new TemporaryFolder();

  @Test
  public void openAsSecondary() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());
      db.put("key3".getBytes(), "value3".getBytes());

      // open secondary
      try (final Options secondaryOptions = new Options();
           final RocksDB secondaryDb =
               RocksDB.openAsSecondary(secondaryOptions, dbFolder.getRoot().getAbsolutePath(),
                   secondaryDbFolder.getRoot().getAbsolutePath())) {
        assertThat(secondaryDb.get("key1".getBytes())).isEqualTo("value1".getBytes());
        assertThat(secondaryDb.get("key2".getBytes())).isEqualTo("value2".getBytes());
        assertThat(secondaryDb.get("key3".getBytes())).isEqualTo("value3".getBytes());

        // write to primary
        db.put("key4".getBytes(), "value4".getBytes());
        db.put("key5".getBytes(), "value5".getBytes());
        db.put("key6".getBytes(), "value6".getBytes());

        // tell secondary to catch up
        secondaryDb.tryCatchUpWithPrimary();

        db.put("key7".getBytes(), "value7".getBytes());

        // check secondary
        assertThat(secondaryDb.get("key4".getBytes())).isEqualTo("value4".getBytes());
        assertThat(secondaryDb.get("key5".getBytes())).isEqualTo("value5".getBytes());
        assertThat(secondaryDb.get("key6".getBytes())).isEqualTo("value6".getBytes());

        assertThat(secondaryDb.get("key7".getBytes())).isNull();
      }
    }
  }

  @Test
  public void openAsSecondaryColumnFamilies() throws RocksDBException {
    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts);
        final ColumnFamilyDescriptor cfd1 =
            new ColumnFamilyDescriptor("cf1".getBytes(), cfOpts)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
      cfDescriptors.add(defaultCF);
      cfDescriptors.add(cfd1);

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

      try (final DBOptions options =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(
               options, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, cfHandles)) {
        try {
          final ColumnFamilyHandle cf1 = cfHandles.get(1);

          db.put(cf1, "key1".getBytes(), "value1".getBytes());
          db.put(cf1, "key2".getBytes(), "value2".getBytes());
          db.put(cf1, "key3".getBytes(), "value3".getBytes());

          final List<ColumnFamilyHandle> secondaryCfHandles = new ArrayList<>();

          // open secondary
          try (final DBOptions secondaryOptions = new DBOptions();
               final RocksDB secondaryDb =
                   RocksDB.openAsSecondary(secondaryOptions, dbFolder.getRoot().getAbsolutePath(),
                       secondaryDbFolder.getRoot().getAbsolutePath(), cfDescriptors,
                       secondaryCfHandles)) {
            try {
              final ColumnFamilyHandle secondaryCf1 = secondaryCfHandles.get(1);

              assertThat(secondaryDb.get(secondaryCf1, "key1".getBytes()))
                  .isEqualTo("value1".getBytes());
              assertThat(secondaryDb.get(secondaryCf1, "key2".getBytes()))
                  .isEqualTo("value2".getBytes());
              assertThat(secondaryDb.get(secondaryCf1, "key3".getBytes()))
                  .isEqualTo("value3".getBytes());

              // write to primary
              db.put(cf1, "key4".getBytes(), "value4".getBytes());
              db.put(cf1, "key5".getBytes(), "value5".getBytes());
              db.put(cf1, "key6".getBytes(), "value6".getBytes());

              // tell secondary to catch up
              secondaryDb.tryCatchUpWithPrimary();

              db.put(cf1, "key7".getBytes(), "value7".getBytes());

              // check secondary
              assertThat(secondaryDb.get(secondaryCf1, "key4".getBytes()))
                  .isEqualTo("value4".getBytes());
              assertThat(secondaryDb.get(secondaryCf1, "key5".getBytes()))
                  .isEqualTo("value5".getBytes());
              assertThat(secondaryDb.get(secondaryCf1, "key6".getBytes()))
                  .isEqualTo("value6".getBytes());

              assertThat(secondaryDb.get(secondaryCf1, "key7".getBytes())).isNull();

            } finally {
              for (final ColumnFamilyHandle secondaryCfHandle : secondaryCfHandles) {
                secondaryCfHandle.close();
              }
            }
          }
        } finally {
          for (final ColumnFamilyHandle cfHandle : cfHandles) {
            cfHandle.close();
          }
        }
      }
    }
  }
}
