// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SstPartitionerTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void sstFixedPrefix() throws RocksDBException {
    try (final SstPartitionerFixedPrefixFactory factory = new SstPartitionerFixedPrefixFactory(4);
         final Options opt =
             new Options().setCreateIfMissing(true).setSstPartitionerFactory(factory);
         final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      // writing (long)100 under key
      db.put("aaaa1".getBytes(), "A".getBytes());
      db.put("bbbb1".getBytes(), "B".getBytes());
      db.flush(new FlushOptions());

      db.put("aaaa0".getBytes(), "A2".getBytes());
      db.put("aaaa2".getBytes(), "A2".getBytes());
      db.flush(new FlushOptions());

      db.compactRange();

      final List<LiveFileMetaData> metadata = db.getLiveFilesMetaData();
      assertThat(metadata.size()).isEqualTo(2);
    }
  }

  @Test
  public void sstFixedPrefixFamily() throws RocksDBException {
    final byte[] cfName = "new_cf".getBytes(UTF_8);

    try (final SstPartitionerFixedPrefixFactory sstPartitionerFactory =
             new SstPartitionerFixedPrefixFactory(4);
        final ColumnFamilyOptions columnFamilyOptions =
            new ColumnFamilyOptions().setSstPartitionerFactory(sstPartitionerFactory);
        final ColumnFamilyDescriptor cfDescriptor =
            new ColumnFamilyDescriptor(cfName, columnFamilyOptions);
        final Options opt = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(cfDescriptor);

      // writing (long)100 under key
      db.put(columnFamilyHandle, "aaaa1".getBytes(), "A".getBytes());
      db.put(columnFamilyHandle, "bbbb1".getBytes(), "B".getBytes());
      db.flush(new FlushOptions(), columnFamilyHandle);

      db.put(columnFamilyHandle, "aaaa0".getBytes(), "A2".getBytes());
      db.put(columnFamilyHandle, "aaaa2".getBytes(), "A2".getBytes());
      db.flush(new FlushOptions(), columnFamilyHandle);

      db.compactRange(columnFamilyHandle);

      final List<LiveFileMetaData> metadata = db.getLiveFilesMetaData();
      assertThat(metadata.size()).isEqualTo(2);
    }
  }
}
