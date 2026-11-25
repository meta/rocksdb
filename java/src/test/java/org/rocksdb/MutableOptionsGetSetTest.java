// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MutableOptionsGetSetTest {
  final int minBlobSize = 65536;

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  /**
   * Validate the round-trip of  blob options into and out of the C++ core of RocksDB
   * From CF options on CF Creation to {RocksDB#getOptions}
   * Uses 2x column families with different values for their options.
   * NOTE that some constraints are applied to the options in the C++ core,
   * e.g. on {ColumnFamilyOptions#setMemtablePrefixBloomSizeRatio}
   *
   * @throws RocksDBException if the database throws an exception
   */
  @Test
  public void testGetMutableBlobOptionsAfterCreate() throws RocksDBException {
    try (final ColumnFamilyOptions columnFamilyOptions0 = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor columnFamilyDescriptor0 =
            new ColumnFamilyDescriptor("default".getBytes(UTF_8), columnFamilyOptions0)) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Collections.singletonList(columnFamilyDescriptor0);
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
              columnFamilyDescriptors, columnFamilyHandles)) {
        try (final ColumnFamilyOptions columnFamilyOptions1 = new ColumnFamilyOptions()
                 .setMinBlobSize(minBlobSize)
                 .setEnableBlobFiles(true)
                 .setBlobGarbageCollectionAgeCutoff(0.25)
                 .setBlobGarbageCollectionForceThreshold(0.80)
                 .setBlobCompactionReadaheadSize(262144)
                 .setBlobFileStartingLevel(2)
                 .setArenaBlockSize(42)
                 .setMemtablePrefixBloomSizeRatio(0.17)
                 .setExperimentalMempurgeThreshold(0.005)
                 .setMemtableWholeKeyFiltering(false)
                 .setMemtableHugePageSize(3)
                 .setMaxSuccessiveMerges(4)
                 .setMaxWriteBufferNumber(12)
                 .setInplaceUpdateNumLocks(16)
                 .setDisableAutoCompactions(false)
                 .setSoftPendingCompactionBytesLimit(112)
                 .setHardPendingCompactionBytesLimit(280)
                 .setLevel0FileNumCompactionTrigger(200)
                 .setLevel0SlowdownWritesTrigger(312)
                 .setLevel0StopWritesTrigger(584)
                 .setMaxCompactionBytes(12)
                 .setTargetFileSizeBase(99)
                 .setTargetFileSizeMultiplier(112)
                 .setMaxSequentialSkipInIterations(50)
                 .setReportBgIoStats(true);

            final ColumnFamilyOptions columnFamilyOptions2 = new ColumnFamilyOptions()
                .setMinBlobSize(minBlobSize)
                .setEnableBlobFiles(false)
                .setArenaBlockSize(42)
                .setMemtablePrefixBloomSizeRatio(0.236)
                .setExperimentalMempurgeThreshold(0.247)
                .setMemtableWholeKeyFiltering(true)
                .setMemtableHugePageSize(8)
                .setMaxSuccessiveMerges(12)
                .setMaxWriteBufferNumber(22)
                .setInplaceUpdateNumLocks(160)
                .setDisableAutoCompactions(true)
                .setSoftPendingCompactionBytesLimit(1124)
                .setHardPendingCompactionBytesLimit(2800)
                .setLevel0FileNumCompactionTrigger(2000)
                .setLevel0SlowdownWritesTrigger(5840)
                .setLevel0StopWritesTrigger(31200)
                .setMaxCompactionBytes(112)
                .setTargetFileSizeBase(999)
                .setTargetFileSizeMultiplier(1120)
                .setMaxSequentialSkipInIterations(24)
                .setReportBgIoStats(true);
            final ColumnFamilyDescriptor columnFamilyDescriptor1 = new ColumnFamilyDescriptor(
                "column_family_1".getBytes(UTF_8), columnFamilyOptions1);
            final ColumnFamilyDescriptor columnFamilyDescriptor2 = new ColumnFamilyDescriptor(
                "column_family_2".getBytes(UTF_8), columnFamilyOptions2)) {
          // Create the column family with blob options
          final ColumnFamilyHandle columnFamilyHandle1 =
              db.createColumnFamily(columnFamilyDescriptor1);
          final ColumnFamilyHandle columnFamilyHandle2 =
              db.createColumnFamily(columnFamilyDescriptor2);

          // Check the getOptions() brings back the creation options for CF1
          final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder1 =
              db.getOptions(columnFamilyHandle1);
          assertThat(builder1.enableBlobFiles()).isEqualTo(true);
          assertThat(builder1.blobGarbageCollectionAgeCutoff()).isEqualTo(0.25);
          assertThat(builder1.blobGarbageCollectionForceThreshold()).isEqualTo(0.80);
          assertThat(builder1.blobCompactionReadaheadSize()).isEqualTo(262144);
          assertThat(builder1.blobFileStartingLevel()).isEqualTo(2);
          assertThat(builder1.minBlobSize()).isEqualTo(minBlobSize);
          assertThat(builder1.arenaBlockSize()).isEqualTo(42);
          assertThat(builder1.memtablePrefixBloomSizeRatio()).isEqualTo(0.17);
          assertThat(builder1.experimentalMempurgeThreshold()).isEqualTo(0.005);
          assertThat(builder1.memtableWholeKeyFiltering()).isEqualTo(false);
          assertThat(builder1.memtableHugePageSize()).isEqualTo(3);
          assertThat(builder1.maxSuccessiveMerges()).isEqualTo(4);
          assertThat(builder1.maxWriteBufferNumber()).isEqualTo(12);
          assertThat(builder1.inplaceUpdateNumLocks()).isEqualTo(16);
          assertThat(builder1.disableAutoCompactions()).isEqualTo(false);
          assertThat(builder1.softPendingCompactionBytesLimit()).isEqualTo(112);
          assertThat(builder1.hardPendingCompactionBytesLimit()).isEqualTo(280);
          assertThat(builder1.level0FileNumCompactionTrigger()).isEqualTo(200);
          assertThat(builder1.level0SlowdownWritesTrigger()).isEqualTo(312);
          assertThat(builder1.level0StopWritesTrigger()).isEqualTo(584);
          assertThat(builder1.maxCompactionBytes()).isEqualTo(12);
          assertThat(builder1.targetFileSizeBase()).isEqualTo(99);
          assertThat(builder1.targetFileSizeMultiplier()).isEqualTo(112);
          assertThat(builder1.maxSequentialSkipInIterations()).isEqualTo(50);
          assertThat(builder1.reportBgIoStats()).isEqualTo(true);

          // Check the getOptions() brings back the creation options for CF2
          final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder2 =
              db.getOptions(columnFamilyHandle2);
          assertThat(builder2.enableBlobFiles()).isEqualTo(false);
          assertThat(builder2.minBlobSize()).isEqualTo(minBlobSize);
          assertThat(builder2.arenaBlockSize()).isEqualTo(42);
          assertThat(builder2.memtablePrefixBloomSizeRatio()).isEqualTo(0.236);
          assertThat(builder2.experimentalMempurgeThreshold()).isEqualTo(0.247);
          assertThat(builder2.memtableWholeKeyFiltering()).isEqualTo(true);
          assertThat(builder2.memtableHugePageSize()).isEqualTo(8);
          assertThat(builder2.maxSuccessiveMerges()).isEqualTo(12);
          assertThat(builder2.maxWriteBufferNumber()).isEqualTo(22);
          assertThat(builder2.inplaceUpdateNumLocks()).isEqualTo(160);
          assertThat(builder2.disableAutoCompactions()).isEqualTo(true);
          assertThat(builder2.softPendingCompactionBytesLimit()).isEqualTo(1124);
          assertThat(builder2.hardPendingCompactionBytesLimit()).isEqualTo(2800);
          assertThat(builder2.level0FileNumCompactionTrigger()).isEqualTo(2000);
          assertThat(builder2.level0SlowdownWritesTrigger()).isEqualTo(5840);
          assertThat(builder2.level0StopWritesTrigger()).isEqualTo(31200);
          assertThat(builder2.maxCompactionBytes()).isEqualTo(112);
          assertThat(builder2.targetFileSizeBase()).isEqualTo(999);
          assertThat(builder2.targetFileSizeMultiplier()).isEqualTo(1120);
          assertThat(builder2.maxSequentialSkipInIterations()).isEqualTo(24);
          assertThat(builder2.reportBgIoStats()).isEqualTo(true);
        }
      }
    }
  }

  /**
   * Validate the round-trip of  blob options into and out of the C++ core of RocksDB
   * From {RocksDB#setOptions} to {RocksDB#getOptions}
   * Uses 2x column families with different values for their options.
   * NOTE that some constraints are applied to the options in the C++ core,
   * e.g. on {ColumnFamilyOptions#setMemtablePrefixBloomSizeRatio}
   *
   * @throws RocksDBException if a database access has an error
   */
  @Test
  public void testGetMutableBlobOptionsAfterSetCF() throws RocksDBException {
    try (final ColumnFamilyOptions columnFamilyOptions0 = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor columnFamilyDescriptor0 =
            new ColumnFamilyDescriptor("default".getBytes(UTF_8), columnFamilyOptions0)) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Collections.singletonList(columnFamilyDescriptor0);
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
              columnFamilyDescriptors, columnFamilyHandles)) {
        try (final ColumnFamilyOptions columnFamilyOptions1 = new ColumnFamilyOptions();

            final ColumnFamilyOptions columnFamilyOptions2 = new ColumnFamilyOptions();
            final ColumnFamilyDescriptor columnFamilyDescriptor1 = new ColumnFamilyDescriptor(
                "column_family_1".getBytes(UTF_8), columnFamilyOptions1);
            final ColumnFamilyDescriptor columnFamilyDescriptor2 = new ColumnFamilyDescriptor(
                "column_family_2".getBytes(UTF_8), columnFamilyOptions2)) {
          // Create the column family with blob options
          final ColumnFamilyHandle columnFamilyHandle1 =
              db.createColumnFamily(columnFamilyDescriptor1);
          final ColumnFamilyHandle columnFamilyHandle2 =
              db.createColumnFamily(columnFamilyDescriptor2);
          db.flush(new FlushOptions().setWaitForFlush(true));

          final MutableColumnFamilyOptions
              .MutableColumnFamilyOptionsBuilder mutableColumnFamilyOptions1 =
              MutableColumnFamilyOptions.builder()
                  .setMinBlobSize(minBlobSize)
                  .setEnableBlobFiles(true)
                  .setBlobGarbageCollectionAgeCutoff(0.25)
                  .setBlobGarbageCollectionForceThreshold(0.80)
                  .setBlobCompactionReadaheadSize(262144)
                  .setBlobFileStartingLevel(3)
                  .setArenaBlockSize(42)
                  .setMemtablePrefixBloomSizeRatio(0.17)
                  .setExperimentalMempurgeThreshold(0.005)
                  .setMemtableWholeKeyFiltering(false)
                  .setMemtableHugePageSize(3)
                  .setMaxSuccessiveMerges(4)
                  .setMaxWriteBufferNumber(12)
                  .setInplaceUpdateNumLocks(16)
                  .setDisableAutoCompactions(false)
                  .setSoftPendingCompactionBytesLimit(112)
                  .setHardPendingCompactionBytesLimit(280)
                  .setLevel0FileNumCompactionTrigger(200)
                  .setLevel0SlowdownWritesTrigger(312)
                  .setLevel0StopWritesTrigger(584)
                  .setMaxCompactionBytes(12)
                  .setTargetFileSizeBase(99)
                  .setTargetFileSizeMultiplier(112);
          db.setOptions(columnFamilyHandle1, mutableColumnFamilyOptions1.build());

          // Check the getOptions() brings back the creation options for CF1
          final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder1 =
              db.getOptions(columnFamilyHandle1);
          assertThat(builder1.enableBlobFiles()).isEqualTo(true);
          assertThat(builder1.blobGarbageCollectionAgeCutoff()).isEqualTo(0.25);
          assertThat(builder1.blobGarbageCollectionForceThreshold()).isEqualTo(0.80);
          assertThat(builder1.blobCompactionReadaheadSize()).isEqualTo(262144);
          assertThat(builder1.blobFileStartingLevel()).isEqualTo(3);
          assertThat(builder1.minBlobSize()).isEqualTo(minBlobSize);
          assertThat(builder1.arenaBlockSize()).isEqualTo(42);
          assertThat(builder1.memtablePrefixBloomSizeRatio()).isEqualTo(0.17);
          assertThat(builder1.experimentalMempurgeThreshold()).isEqualTo(0.005);
          assertThat(builder1.memtableWholeKeyFiltering()).isEqualTo(false);
          assertThat(builder1.memtableHugePageSize()).isEqualTo(3);
          assertThat(builder1.maxSuccessiveMerges()).isEqualTo(4);
          assertThat(builder1.maxWriteBufferNumber()).isEqualTo(12);
          assertThat(builder1.inplaceUpdateNumLocks()).isEqualTo(16);
          assertThat(builder1.disableAutoCompactions()).isEqualTo(false);
          assertThat(builder1.softPendingCompactionBytesLimit()).isEqualTo(112);
          assertThat(builder1.hardPendingCompactionBytesLimit()).isEqualTo(280);
          assertThat(builder1.level0FileNumCompactionTrigger()).isEqualTo(200);
          assertThat(builder1.level0SlowdownWritesTrigger()).isEqualTo(312);
          assertThat(builder1.level0StopWritesTrigger()).isEqualTo(584);
          assertThat(builder1.maxCompactionBytes()).isEqualTo(12);
          assertThat(builder1.targetFileSizeBase()).isEqualTo(99);
          assertThat(builder1.targetFileSizeMultiplier()).isEqualTo(112);

          final MutableColumnFamilyOptions
              .MutableColumnFamilyOptionsBuilder mutableColumnFamilyOptions2 =
              MutableColumnFamilyOptions.builder()
                  .setMinBlobSize(minBlobSize)
                  .setEnableBlobFiles(false)
                  .setArenaBlockSize(42)
                  .setMemtablePrefixBloomSizeRatio(0.236)
                  .setExperimentalMempurgeThreshold(0.247)
                  .setMemtableWholeKeyFiltering(true)
                  .setMemtableHugePageSize(8)
                  .setMaxSuccessiveMerges(12)
                  .setMaxWriteBufferNumber(22)
                  .setInplaceUpdateNumLocks(160)
                  .setDisableAutoCompactions(true)
                  .setSoftPendingCompactionBytesLimit(1124)
                  .setHardPendingCompactionBytesLimit(2800)
                  .setLevel0FileNumCompactionTrigger(2000)
                  .setLevel0SlowdownWritesTrigger(5840)
                  .setLevel0StopWritesTrigger(31200)
                  .setMaxCompactionBytes(112)
                  .setTargetFileSizeBase(999)
                  .setTargetFileSizeMultiplier(1120);
          db.setOptions(columnFamilyHandle2, mutableColumnFamilyOptions2.build());

          // Check the getOptions() brings back the creation options for CF2
          final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder2 =
              db.getOptions(columnFamilyHandle2);
          assertThat(builder2.enableBlobFiles()).isEqualTo(false);
          assertThat(builder2.minBlobSize()).isEqualTo(minBlobSize);
          assertThat(builder2.arenaBlockSize()).isEqualTo(42);
          assertThat(builder2.memtablePrefixBloomSizeRatio()).isEqualTo(0.236);
          assertThat(builder2.experimentalMempurgeThreshold()).isEqualTo(0.247);
          assertThat(builder2.memtableWholeKeyFiltering()).isEqualTo(true);
          assertThat(builder2.memtableHugePageSize()).isEqualTo(8);
          assertThat(builder2.maxSuccessiveMerges()).isEqualTo(12);
          assertThat(builder2.maxWriteBufferNumber()).isEqualTo(22);
          assertThat(builder2.inplaceUpdateNumLocks()).isEqualTo(160);
          assertThat(builder2.disableAutoCompactions()).isEqualTo(true);
          assertThat(builder2.softPendingCompactionBytesLimit()).isEqualTo(1124);
          assertThat(builder2.hardPendingCompactionBytesLimit()).isEqualTo(2800);
          assertThat(builder2.level0FileNumCompactionTrigger()).isEqualTo(2000);
          assertThat(builder2.level0SlowdownWritesTrigger()).isEqualTo(5840);
          assertThat(builder2.level0StopWritesTrigger()).isEqualTo(31200);
          assertThat(builder2.maxCompactionBytes()).isEqualTo(112);
          assertThat(builder2.targetFileSizeBase()).isEqualTo(999);
          assertThat(builder2.targetFileSizeMultiplier()).isEqualTo(1120);
        }
      }
    }
  }

  /**
   * Validate the round-trip of  blob options into and out of the C++ core of RocksDB
   * From {RocksDB#setOptions} to {RocksDB#getOptions}
   * Uses 2x column families with different values for their options.
   * NOTE that some constraints are applied to the options in the C++ core,
   * e.g. on {ColumnFamilyOptions#setMemtablePrefixBloomSizeRatio}
   *
   * @throws RocksDBException if a database access has an error
   */
  @Test
  public void testGetMutableBlobOptionsAfterSet() throws RocksDBException {
    try (final ColumnFamilyOptions columnFamilyOptions0 = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor columnFamilyDescriptor0 =
            new ColumnFamilyDescriptor("default".getBytes(UTF_8), columnFamilyOptions0)) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Collections.singletonList(columnFamilyDescriptor0);
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
              columnFamilyDescriptors, columnFamilyHandles)) {
        final MutableColumnFamilyOptions
            .MutableColumnFamilyOptionsBuilder mutableColumnFamilyOptions =
            MutableColumnFamilyOptions.builder()
                .setMinBlobSize(minBlobSize)
                .setEnableBlobFiles(true)
                .setBlobGarbageCollectionAgeCutoff(0.25)
                .setBlobGarbageCollectionForceThreshold(0.80)
                .setBlobCompactionReadaheadSize(131072)
                .setBlobFileStartingLevel(4)
                .setArenaBlockSize(42)
                .setMemtablePrefixBloomSizeRatio(0.17)
                .setExperimentalMempurgeThreshold(0.005)
                .setMemtableWholeKeyFiltering(false)
                .setMemtableHugePageSize(3)
                .setMaxSuccessiveMerges(4)
                .setMaxWriteBufferNumber(12)
                .setInplaceUpdateNumLocks(16)
                .setDisableAutoCompactions(false)
                .setSoftPendingCompactionBytesLimit(112)
                .setHardPendingCompactionBytesLimit(280)
                .setLevel0FileNumCompactionTrigger(200)
                .setLevel0SlowdownWritesTrigger(312)
                .setLevel0StopWritesTrigger(584)
                .setMaxCompactionBytes(12)
                .setTargetFileSizeBase(99)
                .setTargetFileSizeMultiplier(112);
        db.setOptions(mutableColumnFamilyOptions.build());

        // Check the getOptions() brings back the creation options for CF1
        final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder1 =
            db.getOptions();
        assertThat(builder1.enableBlobFiles()).isEqualTo(true);
        assertThat(builder1.blobGarbageCollectionAgeCutoff()).isEqualTo(0.25);
        assertThat(builder1.blobGarbageCollectionForceThreshold()).isEqualTo(0.80);
        assertThat(builder1.blobCompactionReadaheadSize()).isEqualTo(131072);
        assertThat(builder1.blobFileStartingLevel()).isEqualTo(4);
        assertThat(builder1.minBlobSize()).isEqualTo(minBlobSize);
        assertThat(builder1.arenaBlockSize()).isEqualTo(42);
        assertThat(builder1.memtablePrefixBloomSizeRatio()).isEqualTo(0.17);
        assertThat(builder1.experimentalMempurgeThreshold()).isEqualTo(0.005);
        assertThat(builder1.memtableWholeKeyFiltering()).isEqualTo(false);
        assertThat(builder1.memtableHugePageSize()).isEqualTo(3);
        assertThat(builder1.maxSuccessiveMerges()).isEqualTo(4);
        assertThat(builder1.maxWriteBufferNumber()).isEqualTo(12);
        assertThat(builder1.inplaceUpdateNumLocks()).isEqualTo(16);
        assertThat(builder1.disableAutoCompactions()).isEqualTo(false);
        assertThat(builder1.softPendingCompactionBytesLimit()).isEqualTo(112);
        assertThat(builder1.hardPendingCompactionBytesLimit()).isEqualTo(280);
        assertThat(builder1.level0FileNumCompactionTrigger()).isEqualTo(200);
        assertThat(builder1.level0SlowdownWritesTrigger()).isEqualTo(312);
        assertThat(builder1.level0StopWritesTrigger()).isEqualTo(584);
        assertThat(builder1.maxCompactionBytes()).isEqualTo(12);
        assertThat(builder1.targetFileSizeBase()).isEqualTo(99);
        assertThat(builder1.targetFileSizeMultiplier()).isEqualTo(112);
      }
    }
  }

  @Test
  public void testGetMutableDBOptionsAfterSet() throws RocksDBException {
    try (final ColumnFamilyOptions columnFamilyOptions0 = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor columnFamilyDescriptor0 =
            new ColumnFamilyDescriptor("default".getBytes(UTF_8), columnFamilyOptions0)) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Collections.singletonList(columnFamilyDescriptor0);
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
              columnFamilyDescriptors, columnFamilyHandles)) {
        final MutableDBOptions.MutableDBOptionsBuilder mutableDBOptions =
            MutableDBOptions.builder()
                .setMaxBackgroundJobs(16)
                .setAvoidFlushDuringShutdown(true)
                .setWritableFileMaxBufferSize(2097152)
                .setDelayedWriteRate(67108864)
                .setMaxTotalWalSize(16777216)
                .setDeleteObsoleteFilesPeriodMicros(86400000000L)
                .setStatsDumpPeriodSec(1200)
                .setStatsPersistPeriodSec(7200)
                .setStatsHistoryBufferSize(6291456)
                .setMaxOpenFiles(8)
                .setBytesPerSync(4194304)
                .setWalBytesPerSync(1048576)
                .setStrictBytesPerSync(true)
                .setCompactionReadaheadSize(1024);

        db.setDBOptions(mutableDBOptions.build());

        final MutableDBOptions.MutableDBOptionsBuilder getBuilder = db.getDBOptions();
        assertThat(getBuilder.maxBackgroundJobs()).isEqualTo(16); // 4
        assertThat(getBuilder.avoidFlushDuringShutdown()).isEqualTo(true); // false
        assertThat(getBuilder.writableFileMaxBufferSize()).isEqualTo(2097152); // 1048576
        assertThat(getBuilder.delayedWriteRate()).isEqualTo(67108864); // 16777216
        assertThat(getBuilder.maxTotalWalSize()).isEqualTo(16777216);
        assertThat(getBuilder.deleteObsoleteFilesPeriodMicros())
            .isEqualTo(86400000000L); // 21600000000
        assertThat(getBuilder.statsDumpPeriodSec()).isEqualTo(1200); // 600
        assertThat(getBuilder.statsPersistPeriodSec()).isEqualTo(7200); // 600
        assertThat(getBuilder.statsHistoryBufferSize()).isEqualTo(6291456); // 1048576
        assertThat(getBuilder.maxOpenFiles()).isEqualTo(8); //-1
        assertThat(getBuilder.bytesPerSync()).isEqualTo(4194304); // 1048576
        assertThat(getBuilder.walBytesPerSync()).isEqualTo(1048576); // 0
        assertThat(getBuilder.strictBytesPerSync()).isEqualTo(true); // false
        assertThat(getBuilder.compactionReadaheadSize()).isEqualTo(1024); // 0
      }
    }
  }
}
