// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.ClassRule;
import org.junit.Test;
import org.rocksdb.test.RemoveEmptyValueCompactionFilterFactory;

public class OptionsTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  public static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void copyConstructor() {
    final Options origOpts = new Options();
    origOpts.setNumLevels(rand.nextInt(8));
    origOpts.setTargetFileSizeMultiplier(rand.nextInt(100));
    origOpts.setLevel0StopWritesTrigger(rand.nextInt(50));
    final Options copyOpts = new Options(origOpts);
    assertThat(origOpts.numLevels()).isEqualTo(copyOpts.numLevels());
    assertThat(origOpts.targetFileSizeMultiplier()).isEqualTo(copyOpts.targetFileSizeMultiplier());
    assertThat(origOpts.level0StopWritesTrigger()).isEqualTo(copyOpts.level0StopWritesTrigger());
  }

  @Test
  public void setIncreaseParallelism() {
    try (final Options opt = new Options()) {
      final int threads = Runtime.getRuntime().availableProcessors() * 2;
      opt.setIncreaseParallelism(threads);
    }
  }

  @Test
  public void writeBufferSize() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWriteBufferSize(longValue);
      assertThat(opt.writeBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxWriteBufferNumber() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxWriteBufferNumber(intValue);
      assertThat(opt.maxWriteBufferNumber()).isEqualTo(intValue);
    }
  }

  @Test
  public void minWriteBufferNumberToMerge() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMinWriteBufferNumberToMerge(intValue);
      assertThat(opt.minWriteBufferNumberToMerge()).isEqualTo(intValue);
    }
  }

  @Test
  public void numLevels() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setNumLevels(intValue);
      assertThat(opt.numLevels()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroFileNumCompactionTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroFileNumCompactionTrigger(intValue);
      assertThat(opt.levelZeroFileNumCompactionTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroSlowdownWritesTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroSlowdownWritesTrigger(intValue);
      assertThat(opt.levelZeroSlowdownWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void levelZeroStopWritesTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevelZeroStopWritesTrigger(intValue);
      assertThat(opt.levelZeroStopWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void targetFileSizeBase() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setTargetFileSizeBase(longValue);
      assertThat(opt.targetFileSizeBase()).isEqualTo(longValue);
    }
  }

  @Test
  public void targetFileSizeMultiplier() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setTargetFileSizeMultiplier(intValue);
      assertThat(opt.targetFileSizeMultiplier()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxBytesForLevelBase() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxBytesForLevelBase(longValue);
      assertThat(opt.maxBytesForLevelBase()).isEqualTo(longValue);
    }
  }

  @Test
  public void levelCompactionDynamicLevelBytes() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setLevelCompactionDynamicLevelBytes(boolValue);
      assertThat(opt.levelCompactionDynamicLevelBytes())
          .isEqualTo(boolValue);
    }
  }

  @Test
  public void maxBytesForLevelMultiplier() {
    try (final Options opt = new Options()) {
      final double doubleValue = rand.nextDouble();
      opt.setMaxBytesForLevelMultiplier(doubleValue);
      assertThat(opt.maxBytesForLevelMultiplier()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void maxBytesForLevelMultiplierAdditional() {
    try (final Options opt = new Options()) {
      final int intValue1 = rand.nextInt();
      final int intValue2 = rand.nextInt();
      final int[] ints = new int[]{intValue1, intValue2};
      opt.setMaxBytesForLevelMultiplierAdditional(ints);
      assertThat(opt.maxBytesForLevelMultiplierAdditional()).isEqualTo(ints);
    }
  }

  @Test
  public void maxCompactionBytes() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxCompactionBytes(longValue);
      assertThat(opt.maxCompactionBytes()).isEqualTo(longValue);
    }
  }

  @Test
  public void softPendingCompactionBytesLimit() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setSoftPendingCompactionBytesLimit(longValue);
      assertThat(opt.softPendingCompactionBytesLimit()).isEqualTo(longValue);
    }
  }

  @Test
  public void hardPendingCompactionBytesLimit() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setHardPendingCompactionBytesLimit(longValue);
      assertThat(opt.hardPendingCompactionBytesLimit()).isEqualTo(longValue);
    }
  }

  @Test
  public void level0FileNumCompactionTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevel0FileNumCompactionTrigger(intValue);
      assertThat(opt.level0FileNumCompactionTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void level0SlowdownWritesTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevel0SlowdownWritesTrigger(intValue);
      assertThat(opt.level0SlowdownWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void level0StopWritesTrigger() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setLevel0StopWritesTrigger(intValue);
      assertThat(opt.level0StopWritesTrigger()).isEqualTo(intValue);
    }
  }

  @Test
  public void arenaBlockSize() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setArenaBlockSize(longValue);
      assertThat(opt.arenaBlockSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void disableAutoCompactions() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDisableAutoCompactions(boolValue);
      assertThat(opt.disableAutoCompactions()).isEqualTo(boolValue);
    }
  }

  @Test
  public void maxSequentialSkipInIterations() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxSequentialSkipInIterations(longValue);
      assertThat(opt.maxSequentialSkipInIterations()).isEqualTo(longValue);
    }
  }

  @Test
  public void inplaceUpdateSupport() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setInplaceUpdateSupport(boolValue);
      assertThat(opt.inplaceUpdateSupport()).isEqualTo(boolValue);
    }
  }

  @Test
  public void inplaceUpdateNumLocks() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setInplaceUpdateNumLocks(longValue);
      assertThat(opt.inplaceUpdateNumLocks()).isEqualTo(longValue);
    }
  }

  @Test
  public void memtablePrefixBloomSizeRatio() {
    try (final Options opt = new Options()) {
      final double doubleValue = rand.nextDouble();
      opt.setMemtablePrefixBloomSizeRatio(doubleValue);
      assertThat(opt.memtablePrefixBloomSizeRatio()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void experimentalMempurgeThreshold() {
    try (final Options opt = new Options()) {
      final double doubleValue = rand.nextDouble();
      opt.setExperimentalMempurgeThreshold(doubleValue);
      assertThat(opt.experimentalMempurgeThreshold()).isEqualTo(doubleValue);
    }
  }

  @Test
  public void memtableWholeKeyFiltering() {
    try (final Options opt = new Options()) {
      final boolean booleanValue = rand.nextBoolean();
      opt.setMemtableWholeKeyFiltering(booleanValue);
      assertThat(opt.memtableWholeKeyFiltering()).isEqualTo(booleanValue);
    }
  }

  @Test
  public void memtableHugePageSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMemtableHugePageSize(longValue);
      assertThat(opt.memtableHugePageSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void bloomLocality() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setBloomLocality(intValue);
      assertThat(opt.bloomLocality()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxSuccessiveMerges() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxSuccessiveMerges(longValue);
      assertThat(opt.maxSuccessiveMerges()).isEqualTo(longValue);
    }
  }

  @Test
  public void optimizeFiltersForHits() {
    try (final Options opt = new Options()) {
      final boolean aBoolean = rand.nextBoolean();
      opt.setOptimizeFiltersForHits(aBoolean);
      assertThat(opt.optimizeFiltersForHits()).isEqualTo(aBoolean);
    }
  }

  @Test
  public void createIfMissing() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setCreateIfMissing(boolValue);
      assertThat(opt.createIfMissing()).
          isEqualTo(boolValue);
    }
  }

  @Test
  public void createMissingColumnFamilies() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setCreateMissingColumnFamilies(boolValue);
      assertThat(opt.createMissingColumnFamilies()).
          isEqualTo(boolValue);
    }
  }

  @Test
  public void errorIfExists() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setErrorIfExists(boolValue);
      assertThat(opt.errorIfExists()).isEqualTo(boolValue);
    }
  }

  @Test
  public void paranoidChecks() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setParanoidChecks(boolValue);
      assertThat(opt.paranoidChecks()).
          isEqualTo(boolValue);
    }
  }

  @Test
  public void maxTotalWalSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxTotalWalSize(longValue);
      assertThat(opt.maxTotalWalSize()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void maxOpenFiles() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxOpenFiles(intValue);
      assertThat(opt.maxOpenFiles()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxFileOpeningThreads() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxFileOpeningThreads(intValue);
      assertThat(opt.maxFileOpeningThreads()).isEqualTo(intValue);
    }
  }

  @Test
  public void useFsync() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseFsync(boolValue);
      assertThat(opt.useFsync()).isEqualTo(boolValue);
    }
  }

  @Test
  public void dbPaths() {
    final List<DbPath> dbPaths = new ArrayList<>();
    dbPaths.add(new DbPath(Paths.get("/a"), 10));
    dbPaths.add(new DbPath(Paths.get("/b"), 100));
    dbPaths.add(new DbPath(Paths.get("/c"), 1000));

    try (final Options opt = new Options()) {
      assertThat(opt.dbPaths()).isEqualTo(Collections.emptyList());

      opt.setDbPaths(dbPaths);

      assertThat(opt.dbPaths()).isEqualTo(dbPaths);
    }
  }

  @Test
  public void dbLogDir() {
    try (final Options opt = new Options()) {
      final String str = "path/to/DbLogDir";
      opt.setDbLogDir(str);
      assertThat(opt.dbLogDir()).isEqualTo(str);
    }
  }

  @Test
  public void walDir() {
    try (final Options opt = new Options()) {
      final String str = "path/to/WalDir";
      opt.setWalDir(str);
      assertThat(opt.walDir()).isEqualTo(str);
    }
  }

  @Test
  public void deleteObsoleteFilesPeriodMicros() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setDeleteObsoleteFilesPeriodMicros(longValue);
      assertThat(opt.deleteObsoleteFilesPeriodMicros()).
          isEqualTo(longValue);
    }
  }

  @SuppressWarnings("deprecated")
  @Test
  public void maxBackgroundCompactions() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundCompactions(intValue);
      assertThat(opt.maxBackgroundCompactions()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void maxSubcompactions() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxSubcompactions(intValue);
      assertThat(opt.maxSubcompactions()).
          isEqualTo(intValue);
    }
  }

  @SuppressWarnings("deprecated")
  @Test
  public void maxBackgroundFlushes() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundFlushes(intValue);
      assertThat(opt.maxBackgroundFlushes()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void maxBackgroundJobs() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setMaxBackgroundJobs(intValue);
      assertThat(opt.maxBackgroundJobs()).isEqualTo(intValue);
    }
  }

  @Test
  public void maxLogFileSize() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxLogFileSize(longValue);
      assertThat(opt.maxLogFileSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void logFileTimeToRoll() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setLogFileTimeToRoll(longValue);
      assertThat(opt.logFileTimeToRoll()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void keepLogFileNum() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setKeepLogFileNum(longValue);
      assertThat(opt.keepLogFileNum()).isEqualTo(longValue);
    }
  }

  @Test
  public void recycleLogFileNum() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setRecycleLogFileNum(longValue);
      assertThat(opt.recycleLogFileNum()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxManifestFileSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setMaxManifestFileSize(longValue);
      assertThat(opt.maxManifestFileSize()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void tableCacheNumshardbits() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setTableCacheNumshardbits(intValue);
      assertThat(opt.tableCacheNumshardbits()).
          isEqualTo(intValue);
    }
  }

  @Test
  public void walSizeLimitMB() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWalSizeLimitMB(longValue);
      assertThat(opt.walSizeLimitMB()).isEqualTo(longValue);
    }
  }

  @Test
  public void walTtlSeconds() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWalTtlSeconds(longValue);
      assertThat(opt.walTtlSeconds()).isEqualTo(longValue);
    }
  }

  @Test
  public void manifestPreallocationSize() throws RocksDBException {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setManifestPreallocationSize(longValue);
      assertThat(opt.manifestPreallocationSize()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void useDirectReads() {
    try(final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseDirectReads(boolValue);
      assertThat(opt.useDirectReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void useDirectIoForFlushAndCompaction() {
    try(final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseDirectIoForFlushAndCompaction(boolValue);
      assertThat(opt.useDirectIoForFlushAndCompaction()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowFAllocate() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowFAllocate(boolValue);
      assertThat(opt.allowFAllocate()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowMmapReads() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapReads(boolValue);
      assertThat(opt.allowMmapReads()).isEqualTo(boolValue);
    }
  }

  @Test
  public void allowMmapWrites() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowMmapWrites(boolValue);
      assertThat(opt.allowMmapWrites()).isEqualTo(boolValue);
    }
  }

  @Test
  public void isFdCloseOnExec() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setIsFdCloseOnExec(boolValue);
      assertThat(opt.isFdCloseOnExec()).isEqualTo(boolValue);
    }
  }

  @Test
  public void statsDumpPeriodSec() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setStatsDumpPeriodSec(intValue);
      assertThat(opt.statsDumpPeriodSec()).isEqualTo(intValue);
    }
  }

  @Test
  public void statsPersistPeriodSec() {
    try (final Options opt = new Options()) {
      final int intValue = rand.nextInt();
      opt.setStatsPersistPeriodSec(intValue);
      assertThat(opt.statsPersistPeriodSec()).isEqualTo(intValue);
    }
  }

  @Test
  public void statsHistoryBufferSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setStatsHistoryBufferSize(longValue);
      assertThat(opt.statsHistoryBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void adviseRandomOnOpen() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAdviseRandomOnOpen(boolValue);
      assertThat(opt.adviseRandomOnOpen()).isEqualTo(boolValue);
    }
  }

  @Test
  public void dbWriteBufferSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setDbWriteBufferSize(longValue);
      assertThat(opt.dbWriteBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void setWriteBufferManager() throws RocksDBException {
    try (final Options opt = new Options(); final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager writeBufferManager = new WriteBufferManager(2000L, cache)) {
      opt.setWriteBufferManager(writeBufferManager);
      assertThat(opt.writeBufferManager()).isEqualTo(writeBufferManager);
    }
  }

  @Test
  public void setWriteBufferManagerWithZeroBufferSize() throws RocksDBException {
    try (final Options opt = new Options(); final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager writeBufferManager = new WriteBufferManager(0L, cache)) {
      opt.setWriteBufferManager(writeBufferManager);
      assertThat(opt.writeBufferManager()).isEqualTo(writeBufferManager);
    }
  }

  @Test
  public void setWriteBufferManagerWithAllowStall() throws RocksDBException {
    try (final Options opt = new Options(); final Cache cache = new LRUCache(1024 * 1024);
         final WriteBufferManager writeBufferManager = new WriteBufferManager(2000L, cache, true)) {
      opt.setWriteBufferManager(writeBufferManager);
      assertThat(opt.writeBufferManager()).isEqualTo(writeBufferManager);
      assertThat(opt.writeBufferManager().allowStall()).isEqualTo(true);
      assertThat(writeBufferManager.flushPolicy())
          .isEqualTo(WriteBufferManagerFlushPolicy.FLUSH_OLDEST);
    }
  }

  @Test
  public void writeBufferManagerFlushPolicy() {
    try (final Cache cache = new LRUCache(1024 * 1024);
        final WriteBufferManager writeBufferManager = new WriteBufferManager(
            2000L, cache, true, WriteBufferManagerFlushPolicy.FLUSH_LARGEST_ACROSS_DBS)) {
      assertThat(writeBufferManager.flushPolicy())
          .isEqualTo(WriteBufferManagerFlushPolicy.FLUSH_LARGEST_ACROSS_DBS);

      writeBufferManager.setFlushPolicy(WriteBufferManagerFlushPolicy.FLUSH_LARGEST);
      assertThat(writeBufferManager.flushPolicy())
          .isEqualTo(WriteBufferManagerFlushPolicy.FLUSH_LARGEST);
    }
  }

  @Test
  public void compactionReadaheadSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setCompactionReadaheadSize(longValue);
      assertThat(opt.compactionReadaheadSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void writableFileMaxBufferSize() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWritableFileMaxBufferSize(longValue);
      assertThat(opt.writableFileMaxBufferSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void useAdaptiveMutex() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setUseAdaptiveMutex(boolValue);
      assertThat(opt.useAdaptiveMutex()).isEqualTo(boolValue);
    }
  }

  @Test
  public void bytesPerSync() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setBytesPerSync(longValue);
      assertThat(opt.bytesPerSync()).isEqualTo(longValue);
    }
  }

  @Test
  public void walBytesPerSync() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWalBytesPerSync(longValue);
      assertThat(opt.walBytesPerSync()).isEqualTo(longValue);
    }
  }

  @Test
  public void strictBytesPerSync() {
    try (final Options opt = new Options()) {
      assertThat(opt.strictBytesPerSync()).isFalse();
      opt.setStrictBytesPerSync(true);
      assertThat(opt.strictBytesPerSync()).isTrue();
    }
  }

  @Test
  public void enableThreadTracking() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setEnableThreadTracking(boolValue);
      assertThat(opt.enableThreadTracking()).isEqualTo(boolValue);
    }
  }

  @Test
  public void delayedWriteRate() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setDelayedWriteRate(longValue);
      assertThat(opt.delayedWriteRate()).isEqualTo(longValue);
    }
  }

  @Test
  public void enablePipelinedWrite() {
    try(final Options opt = new Options()) {
      assertThat(opt.enablePipelinedWrite()).isFalse();
      opt.setEnablePipelinedWrite(true);
      assertThat(opt.enablePipelinedWrite()).isTrue();
    }
  }

  @Test
  public void unordredWrite() {
    try(final Options opt = new Options()) {
      assertThat(opt.unorderedWrite()).isFalse();
      opt.setUnorderedWrite(true);
      assertThat(opt.unorderedWrite()).isTrue();
    }
  }

  @Test
  public void allowConcurrentMemtableWrite() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllowConcurrentMemtableWrite(boolValue);
      assertThat(opt.allowConcurrentMemtableWrite()).isEqualTo(boolValue);
    }
  }

  @Test
  public void enableWriteThreadAdaptiveYield() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setEnableWriteThreadAdaptiveYield(boolValue);
      assertThat(opt.enableWriteThreadAdaptiveYield()).isEqualTo(boolValue);
    }
  }

  @Test
  public void writeThreadMaxYieldUsec() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWriteThreadMaxYieldUsec(longValue);
      assertThat(opt.writeThreadMaxYieldUsec()).isEqualTo(longValue);
    }
  }

  @Test
  public void writeThreadSlowYieldUsec() {
    try (final Options opt = new Options()) {
      final long longValue = rand.nextLong();
      opt.setWriteThreadSlowYieldUsec(longValue);
      assertThat(opt.writeThreadSlowYieldUsec()).isEqualTo(longValue);
    }
  }

  @Test
  public void skipStatsUpdateOnDbOpen() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setSkipStatsUpdateOnDbOpen(boolValue);
      assertThat(opt.skipStatsUpdateOnDbOpen()).isEqualTo(boolValue);
    }
  }

  @Test
  public void walRecoveryMode() {
    try (final Options opt = new Options()) {
      for (final WALRecoveryMode walRecoveryMode : WALRecoveryMode.values()) {
        opt.setWalRecoveryMode(walRecoveryMode);
        assertThat(opt.walRecoveryMode()).isEqualTo(walRecoveryMode);
      }
    }
  }

  @Test
  public void allow2pc() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAllow2pc(boolValue);
      assertThat(opt.allow2pc()).isEqualTo(boolValue);
    }
  }

  @Test
  public void rowCache() {
    try (final Options opt = new Options()) {
      assertThat(opt.rowCache()).isNull();

      try(final Cache lruCache = new LRUCache(1000)) {
        opt.setRowCache(lruCache);
        assertThat(opt.rowCache()).isEqualTo(lruCache);
      }

      try(final Cache clockCache = new ClockCache(1000)) {
        opt.setRowCache(clockCache);
        assertThat(opt.rowCache()).isEqualTo(clockCache);
      }
    }
  }

  @Test
  public void walFilter() {
    try (final Options opt = new Options()) {
      assertThat(opt.walFilter()).isNull();

      try (final AbstractWalFilter walFilter = new AbstractWalFilter() {
        @Override
        public void columnFamilyLogNumberMap(
            final Map<Integer, Long> cfLognumber,
            final Map<String, Integer> cfNameId) {
          // no-op
        }

        @Override
        public LogRecordFoundResult logRecordFound(final long logNumber,
            final String logFileName, final WriteBatch batch,
            final WriteBatch newBatch) {
          return new LogRecordFoundResult(
              WalProcessingOption.CONTINUE_PROCESSING, false);
        }

        @Override
        public String name() {
          return "test-wal-filter";
        }
      }) {
        opt.setWalFilter(walFilter);
        assertThat(opt.walFilter()).isEqualTo(walFilter);
      }
    }
  }

  @Test
  public void dumpMallocStats() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDumpMallocStats(boolValue);
      assertThat(opt.dumpMallocStats()).isEqualTo(boolValue);
    }
  }

  @Test
  public void avoidFlushDuringRecovery() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAvoidFlushDuringRecovery(boolValue);
      assertThat(opt.avoidFlushDuringRecovery()).isEqualTo(boolValue);
    }
  }

  @Test
  public void avoidFlushDuringShutdown() {
    try (final Options opt = new Options()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setAvoidFlushDuringShutdown(boolValue);
      assertThat(opt.avoidFlushDuringShutdown()).isEqualTo(boolValue);
    }
  }


  @Test
  public void allowIngestBehind() {
    try (final Options opt = new Options()) {
      assertThat(opt.allowIngestBehind()).isFalse();
      opt.setAllowIngestBehind(true);
      assertThat(opt.allowIngestBehind()).isTrue();
    }
  }

  @Test
  public void twoWriteQueues() {
    try (final Options opt = new Options()) {
      assertThat(opt.twoWriteQueues()).isFalse();
      opt.setTwoWriteQueues(true);
      assertThat(opt.twoWriteQueues()).isTrue();
    }
  }

  @Test
  public void manualWalFlush() {
    try (final Options opt = new Options()) {
      assertThat(opt.manualWalFlush()).isFalse();
      opt.setManualWalFlush(true);
      assertThat(opt.manualWalFlush()).isTrue();
    }
  }

  @Test
  public void atomicFlush() {
    try (final Options opt = new Options()) {
      assertThat(opt.atomicFlush()).isFalse();
      opt.setAtomicFlush(true);
      assertThat(opt.atomicFlush()).isTrue();
    }
  }

  @Test
  public void env() {
    try (final Options options = new Options();
         final Env env = Env.getDefault()) {
      options.setEnv(env);
      assertThat(options.getEnv()).isSameAs(env);
    }
  }

  @Test
  public void linkageOfPrepMethods() {
    try (final Options options = new Options()) {
      options.optimizeUniversalStyleCompaction();
      options.optimizeUniversalStyleCompaction(4000);
      options.optimizeLevelStyleCompaction();
      options.optimizeLevelStyleCompaction(3000);
      options.optimizeForPointLookup(10);
      options.optimizeForSmallDb();
      options.prepareForBulkLoad();
    }
  }

  @Test
  public void compressionTypes() {
    try (final Options options = new Options()) {
      for (final CompressionType compressionType :
          CompressionType.values()) {
        options.setCompressionType(compressionType);
        assertThat(options.compressionType()).
            isEqualTo(compressionType);
        assertThat(CompressionType.valueOf("NO_COMPRESSION")).
            isEqualTo(CompressionType.NO_COMPRESSION);
      }
    }
  }

  @Test
  public void prepopulateBlobCache() {
    try (final Options options = new Options()) {
      for (final PrepopulateBlobCache prepopulateBlobCache : PrepopulateBlobCache.values()) {
        options.setPrepopulateBlobCache(prepopulateBlobCache);
        assertThat(options.prepopulateBlobCache()).isEqualTo(prepopulateBlobCache);
        assertThat(PrepopulateBlobCache.valueOf("PREPOPULATE_BLOB_DISABLE"))
            .isEqualTo(PrepopulateBlobCache.PREPOPULATE_BLOB_DISABLE);
      }
    }
  }

  @Test
  public void compressionPerLevel() {
    try (final Options options = new Options()) {
      assertThat(options.compressionPerLevel()).isEmpty();
      List<CompressionType> compressionTypeList =
          new ArrayList<>();
      for (int i = 0; i < options.numLevels(); i++) {
        compressionTypeList.add(CompressionType.NO_COMPRESSION);
      }
      options.setCompressionPerLevel(compressionTypeList);
      compressionTypeList = options.compressionPerLevel();
      for (final CompressionType compressionType : compressionTypeList) {
        assertThat(compressionType).isEqualTo(
            CompressionType.NO_COMPRESSION);
      }
    }
  }

  @Test
  public void differentCompressionsPerLevel() {
    try (final Options options = new Options()) {
      options.setNumLevels(3);

      assertThat(options.compressionPerLevel()).isEmpty();
      List<CompressionType> compressionTypeList = new ArrayList<>();

      compressionTypeList.add(CompressionType.BZLIB2_COMPRESSION);
      compressionTypeList.add(CompressionType.SNAPPY_COMPRESSION);
      compressionTypeList.add(CompressionType.LZ4_COMPRESSION);

      options.setCompressionPerLevel(compressionTypeList);
      compressionTypeList = options.compressionPerLevel();

      assertThat(compressionTypeList.size()).isEqualTo(3);
      assertThat(compressionTypeList).
          containsExactly(
              CompressionType.BZLIB2_COMPRESSION,
              CompressionType.SNAPPY_COMPRESSION,
              CompressionType.LZ4_COMPRESSION);

    }
  }

  @Test
  public void bottommostCompressionType() {
    try (final Options options = new Options()) {
      assertThat(options.bottommostCompressionType())
          .isEqualTo(CompressionType.DISABLE_COMPRESSION_OPTION);

      for (final CompressionType compressionType : CompressionType.values()) {
        options.setBottommostCompressionType(compressionType);
        assertThat(options.bottommostCompressionType())
            .isEqualTo(compressionType);
      }
    }
  }

  @Test
  public void bottommostCompressionOptions() {
    try (final Options options = new Options();
         final CompressionOptions bottommostCompressionOptions = new CompressionOptions()
             .setMaxDictBytes(123)) {

      options.setBottommostCompressionOptions(bottommostCompressionOptions);
      assertThat(options.bottommostCompressionOptions())
          .isEqualTo(bottommostCompressionOptions);
      assertThat(options.bottommostCompressionOptions().maxDictBytes())
          .isEqualTo(123);
    }
  }

  @Test
  public void compressionOptions() {
    try (final Options options = new Options();
         final CompressionOptions compressionOptions = new CompressionOptions()
             .setMaxDictBytes(123)) {

      options.setCompressionOptions(compressionOptions);
      assertThat(options.compressionOptions())
          .isEqualTo(compressionOptions);
      assertThat(options.compressionOptions().maxDictBytes())
          .isEqualTo(123);
    }
  }

  @Test
  public void compactionStyles() {
    try (final Options options = new Options()) {
      for (final CompactionStyle compactionStyle :
          CompactionStyle.values()) {
        options.setCompactionStyle(compactionStyle);
        assertThat(options.compactionStyle()).
            isEqualTo(compactionStyle);
        assertThat(CompactionStyle.valueOf("FIFO")).
            isEqualTo(CompactionStyle.FIFO);
      }
    }
  }

  @Test
  public void maxTableFilesSizeFIFO() {
    try (final Options opt = new Options()) {
      long longValue = rand.nextLong();
      // Size has to be positive
      longValue = (longValue < 0) ? -longValue : longValue;
      longValue = (longValue == 0) ? longValue + 1 : longValue;
      opt.setMaxTableFilesSizeFIFO(longValue);
      assertThat(opt.maxTableFilesSizeFIFO()).
          isEqualTo(longValue);
    }
  }

  @Test
  public void rateLimiter() {
    try (final Options options = new Options();
         final Options anotherOptions = new Options();
         final RateLimiter rateLimiter =
             new RateLimiter(1000, 100 * 1000, 1)) {
      options.setRateLimiter(rateLimiter);
      // Test with parameter initialization
      anotherOptions.setRateLimiter(
          new RateLimiter(1000));
    }
  }

  @Test
  public void sstFileManager() throws RocksDBException {
    try (final Options options = new Options();
         final SstFileManager sstFileManager =
             new SstFileManager(Env.getDefault())) {
      options.setSstFileManager(sstFileManager);
    }
  }

  @Test
  public void shouldSetTestPrefixExtractor() {
    try (final Options options = new Options()) {
      options.useFixedLengthPrefixExtractor(100);
      options.useFixedLengthPrefixExtractor(10);
    }
  }

  @Test
  public void shouldSetTestCappedPrefixExtractor() {
    try (final Options options = new Options()) {
      options.useCappedPrefixExtractor(100);
      options.useCappedPrefixExtractor(10);
    }
  }

  @Test
  public void shouldTestMemTableFactoryName()
      throws RocksDBException {
    try (final Options options = new Options()) {
      options.setMemTableConfig(new VectorMemTableConfig());
      assertThat(options.memTableFactoryName()).
          isEqualTo("VectorRepFactory");
      options.setMemTableConfig(
          new HashLinkedListMemTableConfig());
      assertThat(options.memTableFactoryName()).
          isEqualTo("HashLinkedListRepFactory");
    }
  }

  @Test
  public void statistics() {
    try(final Options options = new Options()) {
      final Statistics statistics = options.statistics();
      assertThat(statistics).isNull();
    }

    try(final Statistics statistics = new Statistics();
        final Options options = new Options().setStatistics(statistics);
        final Statistics stats = options.statistics()) {
      assertThat(stats).isNotNull();
    }
  }

  @Test
  public void compactionPriorities() {
    try (final Options options = new Options()) {
      for (final CompactionPriority compactionPriority :
          CompactionPriority.values()) {
        options.setCompactionPriority(compactionPriority);
        assertThat(options.compactionPriority()).
            isEqualTo(compactionPriority);
      }
    }
  }

  @Test
  public void reportBgIoStats() {
    try (final Options options = new Options()) {
      final boolean booleanValue = true;
      options.setReportBgIoStats(booleanValue);
      assertThat(options.reportBgIoStats()).
          isEqualTo(booleanValue);
    }
  }

  @Test
  public void ttl() {
    try (final Options options = new Options()) {
      options.setTtl(1000 * 60);
      assertThat(options.ttl()).
          isEqualTo(1000 * 60);
    }
  }

  @Test
  public void periodicCompactionSeconds() {
    try (final Options options = new Options()) {
      options.setPeriodicCompactionSeconds(1000 * 60);
      assertThat(options.periodicCompactionSeconds()).isEqualTo(1000 * 60);
    }
  }

  @Test
  public void compactionOptionsUniversal() {
    try (final Options options = new Options();
         final CompactionOptionsUniversal optUni = new CompactionOptionsUniversal()
             .setCompressionSizePercent(7)) {
      options.setCompactionOptionsUniversal(optUni);
      assertThat(options.compactionOptionsUniversal()).
          isEqualTo(optUni);
      assertThat(options.compactionOptionsUniversal().compressionSizePercent())
          .isEqualTo(7);
    }
  }

  @Test
  public void compactionOptionsFIFO() {
    try (final Options options = new Options();
         final CompactionOptionsFIFO optFifo = new CompactionOptionsFIFO()
             .setMaxTableFilesSize(2000)) {
      options.setCompactionOptionsFIFO(optFifo);
      assertThat(options.compactionOptionsFIFO()).
          isEqualTo(optFifo);
      assertThat(options.compactionOptionsFIFO().maxTableFilesSize())
          .isEqualTo(2000);
    }
  }

  @Test
  public void forceConsistencyChecks() {
    try (final Options options = new Options()) {
      final boolean booleanValue = true;
      options.setForceConsistencyChecks(booleanValue);
      assertThat(options.forceConsistencyChecks()).
          isEqualTo(booleanValue);
    }
  }

  @Test
  public void compactionFilter() {
    try(final Options options = new Options();
        final RemoveEmptyValueCompactionFilter cf = new RemoveEmptyValueCompactionFilter()) {
      options.setCompactionFilter(cf);
      assertThat(options.compactionFilter()).isEqualTo(cf);
    }
  }

  @Test
  public void compactionFilterFactory() {
    try(final Options options = new Options();
        final RemoveEmptyValueCompactionFilterFactory cff = new RemoveEmptyValueCompactionFilterFactory()) {
      options.setCompactionFilterFactory(cff);
      assertThat(options.compactionFilterFactory()).isEqualTo(cff);
    }
  }

  @Test
  public void compactionThreadLimiter() {
    try (final Options options = new Options();
         final ConcurrentTaskLimiter compactionThreadLimiter =
             new ConcurrentTaskLimiterImpl("name", 3)) {
      options.setCompactionThreadLimiter(compactionThreadLimiter);
      assertThat(options.compactionThreadLimiter()).isEqualTo(compactionThreadLimiter);
    }
  }

  @Test
  public void oldDefaults() {
    try (final Options options = new Options()) {
      options.oldDefaults(4, 6);
      assertThat(options.writeBufferSize()).isEqualTo(4 << 20);
      assertThat(options.compactionPriority()).isEqualTo(CompactionPriority.ByCompensatedSize);
      assertThat(options.targetFileSizeBase()).isEqualTo(2 * 1048576);
      assertThat(options.maxBytesForLevelBase()).isEqualTo(10 * 1048576);
      assertThat(options.softPendingCompactionBytesLimit()).isEqualTo(0);
      assertThat(options.hardPendingCompactionBytesLimit()).isEqualTo(0);
      assertThat(options.level0StopWritesTrigger()).isEqualTo(24);
    }
  }

  @Test
  public void optimizeForSmallDbWithCache() {
    try (final Options options = new Options(); final Cache cache = new LRUCache(1024)) {
      assertThat(options.optimizeForSmallDb(cache)).isEqualTo(options);
    }
  }

  @Test
  public void cfPaths() {
    try (final Options options = new Options()) {
      final List<DbPath> paths = Arrays.asList(
          new DbPath(Paths.get("test1"), 2 << 25), new DbPath(Paths.get("/test2/path"), 2 << 25));
      assertThat(options.cfPaths()).isEqualTo(Collections.emptyList());
      assertThat(options.setCfPaths(paths)).isEqualTo(options);
      assertThat(options.cfPaths()).isEqualTo(paths);
    }
  }

  @Test
  public void avoidUnnecessaryBlockingIO() {
    try (final Options options = new Options()) {
      assertThat(options.avoidUnnecessaryBlockingIO()).isEqualTo(false);
      assertThat(options.setAvoidUnnecessaryBlockingIO(true)).isEqualTo(options);
      assertThat(options.avoidUnnecessaryBlockingIO()).isEqualTo(true);
    }
  }

  @Test
  public void persistStatsToDisk() {
    try (final Options options = new Options()) {
      assertThat(options.persistStatsToDisk()).isEqualTo(false);
      assertThat(options.setPersistStatsToDisk(true)).isEqualTo(options);
      assertThat(options.persistStatsToDisk()).isEqualTo(true);
    }
  }

  @Test
  public void writeDbidToManifest() {
    try (final Options options = new Options()) {
      assertThat(options.writeDbidToManifest()).isEqualTo(true);
      assertThat(options.setWriteDbidToManifest(false)).isEqualTo(options);
      assertThat(options.writeDbidToManifest()).isEqualTo(false);
    }
  }

  @Test
  public void trackOptionsFileNumberInManifest() {
    try (final Options options = new Options()) {
      assertThat(options.trackOptionsFileNumberInManifest()).isFalse();
      assertThat(options.setTrackOptionsFileNumberInManifest(true)).isEqualTo(options);
      assertThat(options.trackOptionsFileNumberInManifest()).isTrue();
    }
  }

  @Test
  public void logReadaheadSize() {
    try (final Options options = new Options()) {
      assertThat(options.logReadaheadSize()).isEqualTo(0);
      final int size = 1024 * 1024 * 100;
      assertThat(options.setLogReadaheadSize(size)).isEqualTo(options);
      assertThat(options.logReadaheadSize()).isEqualTo(size);
    }
  }

  @Test
  public void bestEffortsRecovery() {
    try (final Options options = new Options()) {
      assertThat(options.bestEffortsRecovery()).isEqualTo(false);
      assertThat(options.setBestEffortsRecovery(true)).isEqualTo(options);
      assertThat(options.bestEffortsRecovery()).isEqualTo(true);
    }
  }

  @Test
  public void maxBgerrorResumeCount() {
    try (final Options options = new Options()) {
      final int INT_MAX = 2147483647;
      assertThat(options.maxBgerrorResumeCount()).isEqualTo(INT_MAX);
      assertThat(options.setMaxBgErrorResumeCount(-1)).isEqualTo(options);
      assertThat(options.maxBgerrorResumeCount()).isEqualTo(-1);
    }
  }

  @Test
  public void bgerrorResumeRetryInterval() {
    try (final Options options = new Options()) {
      assertThat(options.bgerrorResumeRetryInterval()).isEqualTo(1000000);
      final long newRetryInterval = 24 * 3600 * 1000000L;
      assertThat(options.setBgerrorResumeRetryInterval(newRetryInterval)).isEqualTo(options);
      assertThat(options.bgerrorResumeRetryInterval()).isEqualTo(newRetryInterval);
    }
  }

  @Test
  public void maxWriteBatchGroupSizeBytes() {
    try (final Options options = new Options()) {
      assertThat(options.maxWriteBatchGroupSizeBytes()).isEqualTo(1024 * 1024);
      final long size = 1024 * 1024 * 1024 * 10L;
      assertThat(options.setMaxWriteBatchGroupSizeBytes(size)).isEqualTo(options);
      assertThat(options.maxWriteBatchGroupSizeBytes()).isEqualTo(size);
    }
  }

  @Test
  public void memtableMaxRangeDeletions() {
    try (final Options options = new Options()) {
      assertThat(options.memtableMaxRangeDeletions()).isEqualTo(0);
      final int val = 32;
      assertThat(options.setMemtableMaxRangeDeletions(val)).isEqualTo(options);
      assertThat(options.memtableMaxRangeDeletions()).isEqualTo(val);
    }
  }

  @Test
  public void minTombstonesForRangeConversion() {
    try (final Options options = new Options()) {
      assertThat(options.minTombstonesForRangeConversion()).isEqualTo(0);
      final int val = 100;
      assertThat(options.setMinTombstonesForRangeConversion(val)).isEqualTo(options);
      assertThat(options.minTombstonesForRangeConversion()).isEqualTo(val);
    }
  }

  @Test
  public void dailyOffpeakTimeUTC() {
    try (final Options options = new Options()) {
      assertThat(options.dailyOffpeakTimeUTC()).isEqualTo("");
      final String offPeak = "03:45-20:15";
      assertThat(options.setDailyOffpeakTimeUTC(offPeak)).isEqualTo(options);
      assertThat(options.dailyOffpeakTimeUTC()).isEqualTo(offPeak);
    }
  }

  @Test
  public void eventListeners() {
    final AtomicBoolean wasCalled1 = new AtomicBoolean();
    final AtomicBoolean wasCalled2 = new AtomicBoolean();
    try (final Options options = new Options();
         final AbstractEventListener el1 =
             new AbstractEventListener() {
               @Override
               public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
                 wasCalled1.set(true);
               }
             };
         final AbstractEventListener el2 =
             new AbstractEventListener() {
               @Override
               public void onMemTableSealed(final MemTableInfo memTableInfo) {
                 wasCalled2.set(true);
               }
             }) {
      assertThat(options.setListeners(Arrays.asList(el1, el2))).isEqualTo(options);
      final List<AbstractEventListener> listeners = options.listeners();
      assertEquals(el1, listeners.get(0));
      assertEquals(el2, listeners.get(1));
      options.setListeners(Collections.emptyList());
      listeners.get(0).onTableFileDeleted(null);
      assertTrue(wasCalled1.get());
      listeners.get(1).onMemTableSealed(null);
      assertTrue(wasCalled2.get());
      final List<AbstractEventListener> listeners2 = options.listeners();
      assertNotNull(listeners2);
      assertEquals(0, listeners2.size());
    }
  }
  @Test
  public void tablePropertiesCollectorFactory() {
    try (final Options options = new Options()) {
      try (TablePropertiesCollectorFactory collectorFactory =
               TablePropertiesCollectorFactory.NewCompactOnDeletionCollectorFactory(10, 10, 1.0)) {
        List<TablePropertiesCollectorFactory> factories = Arrays.asList(collectorFactory);
        options.setTablePropertiesCollectorFactory(factories);
      }
      List<TablePropertiesCollectorFactory> factories = options.tablePropertiesCollectorFactory();
      try {
        assertThat(factories).hasSize(1);
      } finally {
        factories.stream().forEach(TablePropertiesCollectorFactory::close);
      }
    }
  }
}
