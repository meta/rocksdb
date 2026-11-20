// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.file.Paths;
import java.util.*;

/**
 * DBOptions to control the behavior of a database.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 * <p>
 * As a descendent of {@link AbstractNativeReference}, this class is {@link AutoCloseable}
 * and will be automatically released if opened in the preamble of a try with resources block.
 */
public class DBOptions extends RocksObject
    implements DBOptionsInterface<DBOptions>, MutableDBOptionsInterface<DBOptions> {
  /**
   * Construct DBOptions.
   * <p>
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::DBOptions} in the c++ side.
   */
  public DBOptions() {
    super(newDBOptionsInstance());
    numShardBits_ = DEFAULT_NUM_SHARD_BITS;
    env_ = Env.getDefault();
  }

  /**
   * Copy constructor for DBOptions.
   * <p>
   * NOTE: This does a shallow copy, which means env, rate_limiter, sst_file_manager,
   * info_log and other pointers will be cloned!
   *
   * @param other The DBOptions to copy.
   */
  public DBOptions(final DBOptions other) {
    super(copyDBOptions(other.nativeHandle_));
    this.env_ = other.env_;
    this.numShardBits_ = other.numShardBits_;
    this.rateLimiter_ = other.rateLimiter_;
    this.rowCache_ = other.rowCache_;
    this.walFilter_ = other.walFilter_;
    this.writeBufferManager_ = other.writeBufferManager_;
  }

  /**
   * Constructor from Options
   *
   * @param options The options.
   */
  public DBOptions(final Options options) {
    super(newDBOptionsFromOptions(options.nativeHandle_));
  }

  /**
   * <p>Method to get a options instance by using pre-configured
   * property values. If one or many values are undefined in
   * the context of RocksDB the method will return a null
   * value.</p>
   *
   * <p><strong>Note</strong>: Property keys can be derived from
   * getter methods within the options class. Example: the method
   * {@code allowMmapReads()} has a property key:
   * {@code allow_mmap_reads}.</p>
   *
   * @param cfgOpts The ConfigOptions to control how the string is processed.
   * @param properties {@link java.util.Properties} instance.
   *
   * @return {@link org.rocksdb.DBOptions instance}
   *     or null.
   *
   * @throws java.lang.IllegalArgumentException if null or empty
   *     {@link java.util.Properties} instance is passed to the method call.
   */
  public static DBOptions getDBOptionsFromProps(
      final ConfigOptions cfgOpts, final Properties properties) {
    DBOptions dbOptions = null;
    final String optionsString = Options.getOptionStringFromProps(properties);
    final long handle = getDBOptionsFromProps(cfgOpts.nativeHandle_, optionsString);
    if (handle != 0) {
      dbOptions = new DBOptions(handle);
    }
    return dbOptions;
  }

  /**
   * <p>Method to get a options instance by using pre-configured
   * property values. If one or many values are undefined in
   * the context of RocksDB the method will return a null
   * value.</p>
   *
   * <p><strong>Note</strong>: Property keys can be derived from
   * getter methods within the options class. Example: the method
   * {@code allowMmapReads()} has a property key:
   * {@code allow_mmap_reads}.</p>
   *
   * @param properties {@link java.util.Properties} instance.
   *
   * @return {@link org.rocksdb.DBOptions instance}
   *     or null.
   *
   * @throws java.lang.IllegalArgumentException if null or empty
   *     {@link java.util.Properties} instance is passed to the method call.
   */
  public static DBOptions getDBOptionsFromProps(final Properties properties) {
    DBOptions dbOptions = null;
    final String optionsString = Options.getOptionStringFromProps(properties);
    final long handle = getDBOptionsFromProps(optionsString);
    if (handle != 0) {
      dbOptions = new DBOptions(handle);
    }
    return dbOptions;
  }

  @Override
  public DBOptions optimizeForSmallDb() {
    optimizeForSmallDb(nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setIncreaseParallelism(
      final int totalThreads) {
    assert(isOwningHandle());
    setIncreaseParallelism(nativeHandle_, totalThreads);
    return this;
  }

  @Override
  public DBOptions setCreateIfMissing(final boolean flag) {
    assert(isOwningHandle());
    setCreateIfMissing(nativeHandle_, flag);
    return this;
  }

  @Override
  public boolean createIfMissing() {
    assert(isOwningHandle());
    return createIfMissing(nativeHandle_);
  }

  @Override
  public DBOptions setCreateMissingColumnFamilies(
      final boolean flag) {
    assert(isOwningHandle());
    setCreateMissingColumnFamilies(nativeHandle_, flag);
    return this;
  }

  @Override
  public boolean createMissingColumnFamilies() {
    assert(isOwningHandle());
    return createMissingColumnFamilies(nativeHandle_);
  }

  @Override
  public DBOptions setErrorIfExists(
      final boolean errorIfExists) {
    assert(isOwningHandle());
    setErrorIfExists(nativeHandle_, errorIfExists);
    return this;
  }

  @Override
  public boolean errorIfExists() {
    assert(isOwningHandle());
    return errorIfExists(nativeHandle_);
  }

  @Override
  public DBOptions setParanoidChecks(
      final boolean paranoidChecks) {
    assert(isOwningHandle());
    setParanoidChecks(nativeHandle_, paranoidChecks);
    return this;
  }

  @Override
  public boolean paranoidChecks() {
    assert(isOwningHandle());
    return paranoidChecks(nativeHandle_);
  }

  @Override
  public DBOptions setOpenFilesAsync(final boolean openFilesAsync) {
    assert (isOwningHandle());
    setOpenFilesAsync(nativeHandle_, openFilesAsync);
    return this;
  }

  @Override
  public boolean openFilesAsync() {
    assert (isOwningHandle());
    return openFilesAsync(nativeHandle_);
  }

  @Override
  public DBOptions setEnv(final Env env) {
    setEnv(nativeHandle_, env.nativeHandle_);
    this.env_ = env;
    return this;
  }

  @Override
  public Env getEnv() {
    return env_;
  }

  @Override
  public DBOptions setRateLimiter(final RateLimiter rateLimiter) {
    assert(isOwningHandle());
    rateLimiter_ = rateLimiter;
    setRateLimiter(nativeHandle_, rateLimiter.nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setSstFileManager(final SstFileManager sstFileManager) {
    assert(isOwningHandle());
    setSstFileManager(nativeHandle_, sstFileManager.nativeHandle_);
    return this;
  }

  @Override
  public DBOptions setLogger(final LoggerInterface logger) {
    assert(isOwningHandle());
    setLogger(nativeHandle_, logger.getNativeHandle(), logger.getLoggerType().getValue());
    return this;
  }

  @Override
  public DBOptions setInfoLogLevel(
      final InfoLogLevel infoLogLevel) {
    assert(isOwningHandle());
    setInfoLogLevel(nativeHandle_, infoLogLevel.getValue());
    return this;
  }

  @Override
  public InfoLogLevel infoLogLevel() {
    assert(isOwningHandle());
    return InfoLogLevel.getInfoLogLevel(
        infoLogLevel(nativeHandle_));
  }

  @Override
  public DBOptions setMaxOpenFiles(
      final int maxOpenFiles) {
    assert(isOwningHandle());
    setMaxOpenFiles(nativeHandle_, maxOpenFiles);
    return this;
  }

  @Override
  public int maxOpenFiles() {
    assert(isOwningHandle());
    return maxOpenFiles(nativeHandle_);
  }

  @Override
  public DBOptions setMaxFileOpeningThreads(final int maxFileOpeningThreads) {
    assert(isOwningHandle());
    setMaxFileOpeningThreads(nativeHandle_, maxFileOpeningThreads);
    return this;
  }

  @Override
  public int maxFileOpeningThreads() {
    assert(isOwningHandle());
    return maxFileOpeningThreads(nativeHandle_);
  }

  @Override
  public DBOptions setMaxTotalWalSize(
      final long maxTotalWalSize) {
    assert(isOwningHandle());
    setMaxTotalWalSize(nativeHandle_, maxTotalWalSize);
    return this;
  }

  @Override
  public long maxTotalWalSize() {
    assert(isOwningHandle());
    return maxTotalWalSize(nativeHandle_);
  }

  @Override
  public DBOptions setStatistics(final Statistics statistics) {
    assert(isOwningHandle());
    setStatistics(nativeHandle_, statistics.nativeHandle_);
    return this;
  }

  @Override
  public Statistics statistics() {
    assert(isOwningHandle());
    final long statisticsNativeHandle = statistics(nativeHandle_);
    if(statisticsNativeHandle == 0) {
      return null;
    } else {
      return new Statistics(statisticsNativeHandle);
    }
  }

  @Override
  public DBOptions setUseFsync(
      final boolean useFsync) {
    assert(isOwningHandle());
    setUseFsync(nativeHandle_, useFsync);
    return this;
  }

  @Override
  public boolean useFsync() {
    assert(isOwningHandle());
    return useFsync(nativeHandle_);
  }

  @Override
  public DBOptions setDbPaths(final Collection<DbPath> dbPaths) {
    assert(isOwningHandle());

    final int len = dbPaths.size();
    final String[] paths = new String[len];
    final long[] targetSizes = new long[len];

    int i = 0;
    for(final DbPath dbPath : dbPaths) {
      paths[i] = dbPath.path.toString();
      targetSizes[i] = dbPath.targetSize;
      i++;
    }
    setDbPaths(nativeHandle_, paths, targetSizes);
    return this;
  }

  @Override
  public List<DbPath> dbPaths() {
    final int len = (int)dbPathsLen(nativeHandle_);
    if(len == 0) {
      return Collections.emptyList();
    } else {
      final String[] paths = new String[len];
      final long[] targetSizes = new long[len];

      dbPaths(nativeHandle_, paths, targetSizes);

      final List<DbPath> dbPaths = new ArrayList<>();
      for(int i = 0; i < len; i++) {
        dbPaths.add(new DbPath(Paths.get(paths[i]), targetSizes[i]));
      }
      return dbPaths;
    }
  }

  @Override
  public DBOptions setDbLogDir(
      final String dbLogDir) {
    assert(isOwningHandle());
    setDbLogDir(nativeHandle_, dbLogDir);
    return this;
  }

  @Override
  public String dbLogDir() {
    assert(isOwningHandle());
    return dbLogDir(nativeHandle_);
  }

  @Override
  public DBOptions setWalDir(
      final String walDir) {
    assert(isOwningHandle());
    setWalDir(nativeHandle_, walDir);
    return this;
  }

  @Override
  public String walDir() {
    assert(isOwningHandle());
    return walDir(nativeHandle_);
  }

  @Override
  public DBOptions setDeleteObsoleteFilesPeriodMicros(
      final long micros) {
    assert(isOwningHandle());
    setDeleteObsoleteFilesPeriodMicros(nativeHandle_, micros);
    return this;
  }

  @Override
  public long deleteObsoleteFilesPeriodMicros() {
    assert(isOwningHandle());
    return deleteObsoleteFilesPeriodMicros(nativeHandle_);
  }

  @Override
  public DBOptions setMaxBackgroundJobs(final int maxBackgroundJobs) {
    assert(isOwningHandle());
    setMaxBackgroundJobs(nativeHandle_, maxBackgroundJobs);
    return this;
  }

  @Override
  public int maxBackgroundJobs() {
    assert(isOwningHandle());
    return maxBackgroundJobs(nativeHandle_);
  }

  @Override
  @Deprecated
  public DBOptions setMaxBackgroundCompactions(
      final int maxBackgroundCompactions) {
    assert(isOwningHandle());
    setMaxBackgroundCompactions(nativeHandle_, maxBackgroundCompactions);
    return this;
  }

  @Override
  @Deprecated
  public int maxBackgroundCompactions() {
    assert(isOwningHandle());
    return maxBackgroundCompactions(nativeHandle_);
  }

  @Override
  public DBOptions setMaxSubcompactions(final int maxSubcompactions) {
    assert(isOwningHandle());
    setMaxSubcompactions(nativeHandle_, maxSubcompactions);
    return this;
  }

  @Override
  public int maxSubcompactions() {
    assert(isOwningHandle());
    return maxSubcompactions(nativeHandle_);
  }

  @Override
  @Deprecated
  public DBOptions setMaxBackgroundFlushes(
      final int maxBackgroundFlushes) {
    assert(isOwningHandle());
    setMaxBackgroundFlushes(nativeHandle_, maxBackgroundFlushes);
    return this;
  }

  @Override
  @Deprecated
  public int maxBackgroundFlushes() {
    assert(isOwningHandle());
    return maxBackgroundFlushes(nativeHandle_);
  }

  @Override
  public DBOptions setMaxLogFileSize(final long maxLogFileSize) {
    assert(isOwningHandle());
    setMaxLogFileSize(nativeHandle_, maxLogFileSize);
    return this;
  }

  @Override
  public long maxLogFileSize() {
    assert(isOwningHandle());
    return maxLogFileSize(nativeHandle_);
  }

  @Override
  public DBOptions setLogFileTimeToRoll(
      final long logFileTimeToRoll) {
    assert(isOwningHandle());
    setLogFileTimeToRoll(nativeHandle_, logFileTimeToRoll);
    return this;
  }

  @Override
  public long logFileTimeToRoll() {
    assert(isOwningHandle());
    return logFileTimeToRoll(nativeHandle_);
  }

  @Override
  public DBOptions setKeepLogFileNum(
      final long keepLogFileNum) {
    assert(isOwningHandle());
    setKeepLogFileNum(nativeHandle_, keepLogFileNum);
    return this;
  }

  @Override
  public long keepLogFileNum() {
    assert(isOwningHandle());
    return keepLogFileNum(nativeHandle_);
  }

  @Override
  public DBOptions setRecycleLogFileNum(final long recycleLogFileNum) {
    assert(isOwningHandle());
    setRecycleLogFileNum(nativeHandle_, recycleLogFileNum);
    return this;
  }

  @Override
  public long recycleLogFileNum() {
    assert(isOwningHandle());
    return recycleLogFileNum(nativeHandle_);
  }

  @Override
  public DBOptions setMaxManifestFileSize(
      final long maxManifestFileSize) {
    assert(isOwningHandle());
    setMaxManifestFileSize(nativeHandle_, maxManifestFileSize);
    return this;
  }

  @Override
  public long maxManifestFileSize() {
    assert(isOwningHandle());
    return maxManifestFileSize(nativeHandle_);
  }

  @Override
  public DBOptions setTableCacheNumshardbits(
      final int tableCacheNumshardbits) {
    assert(isOwningHandle());
    setTableCacheNumshardbits(nativeHandle_, tableCacheNumshardbits);
    return this;
  }

  @Override
  public int tableCacheNumshardbits() {
    assert(isOwningHandle());
    return tableCacheNumshardbits(nativeHandle_);
  }

  @Override
  public DBOptions setWalTtlSeconds(
      final long walTtlSeconds) {
    assert(isOwningHandle());
    setWalTtlSeconds(nativeHandle_, walTtlSeconds);
    return this;
  }

  @Override
  public long walTtlSeconds() {
    assert(isOwningHandle());
    return walTtlSeconds(nativeHandle_);
  }

  @Override
  public DBOptions setWalSizeLimitMB(
      final long sizeLimitMB) {
    assert(isOwningHandle());
    setWalSizeLimitMB(nativeHandle_, sizeLimitMB);
    return this;
  }

  @Override
  public long walSizeLimitMB() {
    assert(isOwningHandle());
    return walSizeLimitMB(nativeHandle_);
  }

  @Override
  public DBOptions setMaxWriteBatchGroupSizeBytes(final long maxWriteBatchGroupSizeBytes) {
    setMaxWriteBatchGroupSizeBytes(nativeHandle_, maxWriteBatchGroupSizeBytes);
    return this;
  }

  @Override
  public long maxWriteBatchGroupSizeBytes() {
    assert (isOwningHandle());
    return maxWriteBatchGroupSizeBytes(nativeHandle_);
  }

  @Override
  public DBOptions setManifestPreallocationSize(
      final long size) {
    assert(isOwningHandle());
    setManifestPreallocationSize(nativeHandle_, size);
    return this;
  }

  @Override
  public long manifestPreallocationSize() {
    assert(isOwningHandle());
    return manifestPreallocationSize(nativeHandle_);
  }

  @Override
  public DBOptions setAllowMmapReads(
      final boolean allowMmapReads) {
    assert(isOwningHandle());
    setAllowMmapReads(nativeHandle_, allowMmapReads);
    return this;
  }

  @Override
  public boolean allowMmapReads() {
    assert(isOwningHandle());
    return allowMmapReads(nativeHandle_);
  }

  @Override
  public DBOptions setAllowMmapWrites(
      final boolean allowMmapWrites) {
    assert(isOwningHandle());
    setAllowMmapWrites(nativeHandle_, allowMmapWrites);
    return this;
  }

  @Override
  public boolean allowMmapWrites() {
    assert(isOwningHandle());
    return allowMmapWrites(nativeHandle_);
  }

  @Override
  public DBOptions setUseDirectReads(
      final boolean useDirectReads) {
    assert(isOwningHandle());
    setUseDirectReads(nativeHandle_, useDirectReads);
    return this;
  }

  @Override
  public boolean useDirectReads() {
    assert(isOwningHandle());
    return useDirectReads(nativeHandle_);
  }

  @Override
  public DBOptions setUseDirectIoForFlushAndCompaction(
      final boolean useDirectIoForFlushAndCompaction) {
    assert(isOwningHandle());
    setUseDirectIoForFlushAndCompaction(nativeHandle_,
        useDirectIoForFlushAndCompaction);
    return this;
  }

  @Override
  public boolean useDirectIoForFlushAndCompaction() {
    assert(isOwningHandle());
    return useDirectIoForFlushAndCompaction(nativeHandle_);
  }

  @Override
  public DBOptions setAllowFAllocate(final boolean allowFAllocate) {
    assert(isOwningHandle());
    setAllowFAllocate(nativeHandle_, allowFAllocate);
    return this;
  }

  @Override
  public boolean allowFAllocate() {
    assert(isOwningHandle());
    return allowFAllocate(nativeHandle_);
  }

  @Override
  public DBOptions setIsFdCloseOnExec(
      final boolean isFdCloseOnExec) {
    assert(isOwningHandle());
    setIsFdCloseOnExec(nativeHandle_, isFdCloseOnExec);
    return this;
  }

  @Override
  public boolean isFdCloseOnExec() {
    assert(isOwningHandle());
    return isFdCloseOnExec(nativeHandle_);
  }

  @Override
  public DBOptions setStatsDumpPeriodSec(
      final int statsDumpPeriodSec) {
    assert(isOwningHandle());
    setStatsDumpPeriodSec(nativeHandle_, statsDumpPeriodSec);
    return this;
  }

  @Override
  public int statsDumpPeriodSec() {
    assert(isOwningHandle());
    return statsDumpPeriodSec(nativeHandle_);
  }

  @Override
  public DBOptions setMaxCompactionTriggerWakeupSeconds(
      final long maxCompactionTriggerWakeupSeconds) {
    assert (isOwningHandle());
    setMaxCompactionTriggerWakeupSeconds(nativeHandle_, maxCompactionTriggerWakeupSeconds);
    return this;
  }

  @Override
  public long maxCompactionTriggerWakeupSeconds() {
    assert (isOwningHandle());
    return maxCompactionTriggerWakeupSeconds(nativeHandle_);
  }

  @Override
  public DBOptions setStatsPersistPeriodSec(
      final int statsPersistPeriodSec) {
    assert(isOwningHandle());
    setStatsPersistPeriodSec(nativeHandle_, statsPersistPeriodSec);
    return this;
  }

  @Override
  public int statsPersistPeriodSec() {
    assert(isOwningHandle());
    return statsPersistPeriodSec(nativeHandle_);
  }

  @Override
  public DBOptions setStatsHistoryBufferSize(
      final long statsHistoryBufferSize) {
    assert(isOwningHandle());
    setStatsHistoryBufferSize(nativeHandle_, statsHistoryBufferSize);
    return this;
  }

  @Override
  public long statsHistoryBufferSize() {
    assert(isOwningHandle());
    return statsHistoryBufferSize(nativeHandle_);
  }

  @Override
  public DBOptions setAdviseRandomOnOpen(
      final boolean adviseRandomOnOpen) {
    assert(isOwningHandle());
    setAdviseRandomOnOpen(nativeHandle_, adviseRandomOnOpen);
    return this;
  }

  @Override
  public boolean adviseRandomOnOpen() {
    return adviseRandomOnOpen(nativeHandle_);
  }

  @Override
  public DBOptions setDbWriteBufferSize(final long dbWriteBufferSize) {
    assert(isOwningHandle());
    setDbWriteBufferSize(nativeHandle_, dbWriteBufferSize);
    return this;
  }

  @Override
  public DBOptions setWriteBufferManager(final WriteBufferManager writeBufferManager) {
    assert(isOwningHandle());
    setWriteBufferManager(nativeHandle_, writeBufferManager.nativeHandle_);
    this.writeBufferManager_ = writeBufferManager;
    return this;
  }

  @Override
  public WriteBufferManager writeBufferManager() {
    assert(isOwningHandle());
    return this.writeBufferManager_;
  }

  @Override
  public long dbWriteBufferSize() {
    assert(isOwningHandle());
    return dbWriteBufferSize(nativeHandle_);
  }

  @Override
  public DBOptions setCompactionReadaheadSize(final long compactionReadaheadSize) {
    assert(isOwningHandle());
    setCompactionReadaheadSize(nativeHandle_, compactionReadaheadSize);
    return this;
  }

  @Override
  public long compactionReadaheadSize() {
    assert(isOwningHandle());
    return compactionReadaheadSize(nativeHandle_);
  }

  @Override
  public DBOptions setDailyOffpeakTimeUTC(String offpeakTimeUTC) {
    assert (isOwningHandle());
    setDailyOffpeakTimeUTC(nativeHandle_, offpeakTimeUTC);
    return this;
  }

  @Override
  public String dailyOffpeakTimeUTC() {
    assert (isOwningHandle());
    return dailyOffpeakTimeUTC(nativeHandle_);
  }

  @Override
  public DBOptions setWritableFileMaxBufferSize(final long writableFileMaxBufferSize) {
    assert(isOwningHandle());
    setWritableFileMaxBufferSize(nativeHandle_, writableFileMaxBufferSize);
    return this;
  }

  @Override
  public long writableFileMaxBufferSize() {
    assert(isOwningHandle());
    return writableFileMaxBufferSize(nativeHandle_);
  }

  @Override
  public DBOptions setUseAdaptiveMutex(
      final boolean useAdaptiveMutex) {
    assert(isOwningHandle());
    setUseAdaptiveMutex(nativeHandle_, useAdaptiveMutex);
    return this;
  }

  @Override
  public boolean useAdaptiveMutex() {
    assert(isOwningHandle());
    return useAdaptiveMutex(nativeHandle_);
  }

  @Override
  public DBOptions setBytesPerSync(
      final long bytesPerSync) {
    assert(isOwningHandle());
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }

  @Override
  public long bytesPerSync() {
    return bytesPerSync(nativeHandle_);
  }

  @Override
  public DBOptions setWalBytesPerSync(final long walBytesPerSync) {
    assert(isOwningHandle());
    setWalBytesPerSync(nativeHandle_, walBytesPerSync);
    return this;
  }

  @Override
  public long walBytesPerSync() {
    assert(isOwningHandle());
    return walBytesPerSync(nativeHandle_);
  }

  @Override
  public DBOptions setStrictBytesPerSync(final boolean strictBytesPerSync) {
    assert(isOwningHandle());
    setStrictBytesPerSync(nativeHandle_, strictBytesPerSync);
    return this;
  }

  @Override
  public boolean strictBytesPerSync() {
    assert(isOwningHandle());
    return strictBytesPerSync(nativeHandle_);
  }

  @Override
  public DBOptions setListeners(final List<AbstractEventListener> listeners) {
    assert (isOwningHandle());
    setEventListeners(nativeHandle_, RocksCallbackObject.toNativeHandleList(listeners));
    return this;
  }

  @Override
  public List<AbstractEventListener> listeners() {
    assert (isOwningHandle());
    return Arrays.asList(eventListeners(nativeHandle_));
  }

  @Override
  public DBOptions setEnableThreadTracking(final boolean enableThreadTracking) {
    assert(isOwningHandle());
    setEnableThreadTracking(nativeHandle_, enableThreadTracking);
    return this;
  }

  @Override
  public boolean enableThreadTracking() {
    assert(isOwningHandle());
    return enableThreadTracking(nativeHandle_);
  }

  @Override
  public DBOptions setDelayedWriteRate(final long delayedWriteRate) {
    assert(isOwningHandle());
    setDelayedWriteRate(nativeHandle_, delayedWriteRate);
    return this;
  }

  @Override
  public long delayedWriteRate(){
    return delayedWriteRate(nativeHandle_);
  }

  @Override
  public DBOptions setEnablePipelinedWrite(final boolean enablePipelinedWrite) {
    assert(isOwningHandle());
    setEnablePipelinedWrite(nativeHandle_, enablePipelinedWrite);
    return this;
  }

  @Override
  public boolean enablePipelinedWrite() {
    assert(isOwningHandle());
    return enablePipelinedWrite(nativeHandle_);
  }

  @Override
  public DBOptions setUnorderedWrite(final boolean unorderedWrite) {
    setUnorderedWrite(nativeHandle_, unorderedWrite);
    return this;
  }

  @Override
  public boolean unorderedWrite() {
    return unorderedWrite(nativeHandle_);
  }


  @Override
  public DBOptions setAllowConcurrentMemtableWrite(
      final boolean allowConcurrentMemtableWrite) {
    setAllowConcurrentMemtableWrite(nativeHandle_,
        allowConcurrentMemtableWrite);
    return this;
  }

  @Override
  public boolean allowConcurrentMemtableWrite() {
    return allowConcurrentMemtableWrite(nativeHandle_);
  }

  @Override
  public DBOptions setEnableWriteThreadAdaptiveYield(
      final boolean enableWriteThreadAdaptiveYield) {
    setEnableWriteThreadAdaptiveYield(nativeHandle_,
        enableWriteThreadAdaptiveYield);
    return this;
  }

  @Override
  public boolean enableWriteThreadAdaptiveYield() {
    return enableWriteThreadAdaptiveYield(nativeHandle_);
  }

  @Override
  public DBOptions setWriteThreadMaxYieldUsec(final long writeThreadMaxYieldUsec) {
    setWriteThreadMaxYieldUsec(nativeHandle_, writeThreadMaxYieldUsec);
    return this;
  }

  @Override
  public long writeThreadMaxYieldUsec() {
    return writeThreadMaxYieldUsec(nativeHandle_);
  }

  @Override
  public DBOptions setWriteThreadSlowYieldUsec(final long writeThreadSlowYieldUsec) {
    setWriteThreadSlowYieldUsec(nativeHandle_, writeThreadSlowYieldUsec);
    return this;
  }

  @Override
  public long writeThreadSlowYieldUsec() {
    return writeThreadSlowYieldUsec(nativeHandle_);
  }

  @Override
  public DBOptions setSkipStatsUpdateOnDbOpen(final boolean skipStatsUpdateOnDbOpen) {
    assert(isOwningHandle());
    setSkipStatsUpdateOnDbOpen(nativeHandle_, skipStatsUpdateOnDbOpen);
    return this;
  }

  @Override
  public boolean skipStatsUpdateOnDbOpen() {
    assert(isOwningHandle());
    return skipStatsUpdateOnDbOpen(nativeHandle_);
  }

  @Override
  public DBOptions setWalRecoveryMode(final WALRecoveryMode walRecoveryMode) {
    assert(isOwningHandle());
    setWalRecoveryMode(nativeHandle_, walRecoveryMode.getValue());
    return this;
  }

  @Override
  public WALRecoveryMode walRecoveryMode() {
    assert(isOwningHandle());
    return WALRecoveryMode.getWALRecoveryMode(walRecoveryMode(nativeHandle_));
  }

  @Override
  public DBOptions setAllow2pc(final boolean allow2pc) {
    assert(isOwningHandle());
    setAllow2pc(nativeHandle_, allow2pc);
    return this;
  }

  @Override
  public boolean allow2pc() {
    assert(isOwningHandle());
    return allow2pc(nativeHandle_);
  }

  @Override
  public DBOptions setRowCache(final Cache rowCache) {
    assert(isOwningHandle());
    setRowCache(nativeHandle_, rowCache.nativeHandle_);
    this.rowCache_ = rowCache;
    return this;
  }

  @Override
  public Cache rowCache() {
    assert(isOwningHandle());
    return this.rowCache_;
  }

  @Override
  public DBOptions setWalFilter(final AbstractWalFilter walFilter) {
    assert(isOwningHandle());
    setWalFilter(nativeHandle_, walFilter.nativeHandle_);
    this.walFilter_ = walFilter;
    return this;
  }

  @Override
  public WalFilter walFilter() {
    assert(isOwningHandle());
    return this.walFilter_;
  }

  @Override
  public DBOptions setFailIfOptionsFileError(final boolean failIfOptionsFileError) {
    assert(isOwningHandle());
    setFailIfOptionsFileError(nativeHandle_, failIfOptionsFileError);
    return this;
  }

  @Override
  public boolean failIfOptionsFileError() {
    assert(isOwningHandle());
    return failIfOptionsFileError(nativeHandle_);
  }

  @Override
  public DBOptions setDumpMallocStats(final boolean dumpMallocStats) {
    assert(isOwningHandle());
    setDumpMallocStats(nativeHandle_, dumpMallocStats);
    return this;
  }

  @Override
  public boolean dumpMallocStats() {
    assert(isOwningHandle());
    return dumpMallocStats(nativeHandle_);
  }

  @Override
  public DBOptions setAvoidFlushDuringRecovery(final boolean avoidFlushDuringRecovery) {
    assert(isOwningHandle());
    setAvoidFlushDuringRecovery(nativeHandle_, avoidFlushDuringRecovery);
    return this;
  }

  @Override
  public boolean avoidFlushDuringRecovery() {
    assert(isOwningHandle());
    return avoidFlushDuringRecovery(nativeHandle_);
  }

  @Override
  public DBOptions setAvoidFlushDuringShutdown(final boolean avoidFlushDuringShutdown) {
    assert(isOwningHandle());
    setAvoidFlushDuringShutdown(nativeHandle_, avoidFlushDuringShutdown);
    return this;
  }

  @Override
  public boolean avoidFlushDuringShutdown() {
    assert(isOwningHandle());
    return avoidFlushDuringShutdown(nativeHandle_);
  }

  @Override
  public DBOptions setAllowIngestBehind(final boolean allowIngestBehind) {
    assert(isOwningHandle());
    setAllowIngestBehind(nativeHandle_, allowIngestBehind);
    return this;
  }

  @Override
  public boolean allowIngestBehind() {
    assert(isOwningHandle());
    return allowIngestBehind(nativeHandle_);
  }

  @Override
  public DBOptions setTwoWriteQueues(final boolean twoWriteQueues) {
    assert(isOwningHandle());
    setTwoWriteQueues(nativeHandle_, twoWriteQueues);
    return this;
  }

  @Override
  public boolean twoWriteQueues() {
    assert(isOwningHandle());
    return twoWriteQueues(nativeHandle_);
  }

  @Override
  public DBOptions setManualWalFlush(final boolean manualWalFlush) {
    assert(isOwningHandle());
    setManualWalFlush(nativeHandle_, manualWalFlush);
    return this;
  }

  @Override
  public boolean manualWalFlush() {
    assert(isOwningHandle());
    return manualWalFlush(nativeHandle_);
  }

  @Override
  public DBOptions setAtomicFlush(final boolean atomicFlush) {
    setAtomicFlush(nativeHandle_, atomicFlush);
    return this;
  }

  @Override
  public boolean atomicFlush() {
    return atomicFlush(nativeHandle_);
  }

  @Override
  public DBOptions setAvoidUnnecessaryBlockingIO(final boolean avoidUnnecessaryBlockingIO) {
    setAvoidUnnecessaryBlockingIO(nativeHandle_, avoidUnnecessaryBlockingIO);
    return this;
  }

  @Override
  public boolean avoidUnnecessaryBlockingIO() {
    assert (isOwningHandle());
    return avoidUnnecessaryBlockingIO(nativeHandle_);
  }

  @Override
  public DBOptions setPersistStatsToDisk(final boolean persistStatsToDisk) {
    setPersistStatsToDisk(nativeHandle_, persistStatsToDisk);
    return this;
  }

  @Override
  public boolean persistStatsToDisk() {
    assert (isOwningHandle());
    return persistStatsToDisk(nativeHandle_);
  }

  @Override
  public DBOptions setWriteDbidToManifest(final boolean writeDbidToManifest) {
    setWriteDbidToManifest(nativeHandle_, writeDbidToManifest);
    return this;
  }

  @Override
  public boolean writeDbidToManifest() {
    assert (isOwningHandle());
    return writeDbidToManifest(nativeHandle_);
  }

  @Override
  public DBOptions setTrackOptionsFileNumberInManifest(final boolean track) {
    setTrackOptionsFileNumberInManifest(nativeHandle_, track);
    return this;
  }

  @Override
  public boolean trackOptionsFileNumberInManifest() {
    assert (isOwningHandle());
    return trackOptionsFileNumberInManifest(nativeHandle_);
  }

  @Override
  public DBOptions setLogReadaheadSize(final long logReadaheadSize) {
    setLogReadaheadSize(nativeHandle_, logReadaheadSize);
    return this;
  }

  @Override
  public long logReadaheadSize() {
    assert (isOwningHandle());
    return logReadaheadSize(nativeHandle_);
  }

  @Override
  public DBOptions setBestEffortsRecovery(final boolean bestEffortsRecovery) {
    setBestEffortsRecovery(nativeHandle_, bestEffortsRecovery);
    return this;
  }

  @Override
  public boolean bestEffortsRecovery() {
    assert (isOwningHandle());
    return bestEffortsRecovery(nativeHandle_);
  }

  @Override
  public DBOptions setMaxBgErrorResumeCount(final int maxBgerrorResumeCount) {
    setMaxBgErrorResumeCount(nativeHandle_, maxBgerrorResumeCount);
    return this;
  }

  @Override
  public int maxBgerrorResumeCount() {
    assert (isOwningHandle());
    return maxBgerrorResumeCount(nativeHandle_);
  }

  @Override
  public DBOptions setBgerrorResumeRetryInterval(final long bgerrorResumeRetryInterval) {
    setBgerrorResumeRetryInterval(nativeHandle_, bgerrorResumeRetryInterval);
    return this;
  }

  @Override
  public long bgerrorResumeRetryInterval() {
    assert (isOwningHandle());
    return bgerrorResumeRetryInterval(nativeHandle_);
  }

  static final int DEFAULT_NUM_SHARD_BITS = -1;




  /**
   * <p>Private constructor to be used by
   * {@link #getDBOptionsFromProps(java.util.Properties)}</p>
   *
   * @param nativeHandle native handle to DBOptions instance.
   */
  private DBOptions(final long nativeHandle) {
    super(nativeHandle);
  }

  private static native long getDBOptionsFromProps(long cfgHandle, String optString);
  private static native long getDBOptionsFromProps(String optString);

  private static long newDBOptionsInstance() {
    RocksDB.loadLibrary();
    return newDBOptions();
  }
  private static native long newDBOptions();

  private static native long copyDBOptions(final long handle);
  private static native long newDBOptionsFromOptions(final long optionsHandle);
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);
  private static native void optimizeForSmallDb(final long handle);
  private static native void setIncreaseParallelism(long handle, int totalThreads);
  private static native void setCreateIfMissing(long handle, boolean flag);
  private static native boolean createIfMissing(long handle);
  private static native void setCreateMissingColumnFamilies(long handle, boolean flag);
  private static native boolean createMissingColumnFamilies(long handle);
  private static native void setEnv(long handle, long envHandle);
  private static native void setErrorIfExists(long handle, boolean errorIfExists);
  private static native boolean errorIfExists(long handle);
  private static native void setParanoidChecks(long handle, boolean paranoidChecks);
  private static native boolean paranoidChecks(long handle);
  private static native void setOpenFilesAsync(long handle, boolean openFilesAsync);
  private static native boolean openFilesAsync(long handle);
  private static native void setRateLimiter(long handle, long rateLimiterHandle);
  private static native void setSstFileManager(final long handle, final long sstFileManagerHandle);
  private static native void setLogger(
      final long handle, final long loggerHandle, final byte loggerType);
  private static native void setInfoLogLevel(long handle, byte logLevel);
  private static native byte infoLogLevel(long handle);
  private static native void setMaxOpenFiles(long handle, int maxOpenFiles);
  private static native int maxOpenFiles(long handle);
  private static native void setMaxFileOpeningThreads(
      final long handle, final int maxFileOpeningThreads);
  private static native int maxFileOpeningThreads(final long handle);
  private static native void setMaxTotalWalSize(long handle, long maxTotalWalSize);
  private static native long maxTotalWalSize(long handle);
  private static native void setStatistics(final long handle, final long statisticsHandle);
  private static native long statistics(final long handle);
  private static native boolean useFsync(long handle);
  private static native void setUseFsync(long handle, boolean useFsync);
  private static native void setDbPaths(
      final long handle, final String[] paths, final long[] targetSizes);
  private static native long dbPathsLen(final long handle);
  private static native void dbPaths(
      final long handle, final String[] paths, final long[] targetSizes);
  private static native void setDbLogDir(long handle, String dbLogDir);
  private static native String dbLogDir(long handle);
  private static native void setWalDir(long handle, String walDir);
  private static native String walDir(long handle);
  private static native void setDeleteObsoleteFilesPeriodMicros(long handle, long micros);
  private static native long deleteObsoleteFilesPeriodMicros(long handle);
  private static native void setMaxBackgroundCompactions(long handle, int maxBackgroundCompactions);
  private static native int maxBackgroundCompactions(long handle);
  private static native void setMaxSubcompactions(long handle, int maxSubcompactions);
  private static native int maxSubcompactions(long handle);
  private static native void setMaxBackgroundFlushes(long handle, int maxBackgroundFlushes);
  private static native int maxBackgroundFlushes(long handle);
  private static native void setMaxBackgroundJobs(long handle, int maxBackgroundJobs);
  private static native int maxBackgroundJobs(long handle);
  private static native void setMaxLogFileSize(long handle, long maxLogFileSize)
      throws IllegalArgumentException;
  private static native long maxLogFileSize(long handle);
  private static native void setLogFileTimeToRoll(long handle, long logFileTimeToRoll)
      throws IllegalArgumentException;
  private static native long logFileTimeToRoll(long handle);
  private static native void setKeepLogFileNum(long handle, long keepLogFileNum)
      throws IllegalArgumentException;
  private static native long keepLogFileNum(long handle);
  private static native void setRecycleLogFileNum(long handle, long recycleLogFileNum);
  private static native long recycleLogFileNum(long handle);
  private static native void setMaxManifestFileSize(long handle, long maxManifestFileSize);
  private static native long maxManifestFileSize(long handle);
  private static native void setTableCacheNumshardbits(long handle, int tableCacheNumshardbits);
  private static native int tableCacheNumshardbits(long handle);
  private static native void setWalTtlSeconds(long handle, long walTtlSeconds);
  private static native long walTtlSeconds(long handle);
  private static native void setWalSizeLimitMB(long handle, long sizeLimitMB);
  private static native long walSizeLimitMB(long handle);
  private static native void setMaxWriteBatchGroupSizeBytes(
      final long handle, final long maxWriteBatchGroupSizeBytes);
  private static native long maxWriteBatchGroupSizeBytes(final long handle);
  private static native void setManifestPreallocationSize(long handle, long size)
      throws IllegalArgumentException;
  private static native long manifestPreallocationSize(long handle);
  private static native void setUseDirectReads(long handle, boolean useDirectReads);
  private static native boolean useDirectReads(long handle);
  private static native void setUseDirectIoForFlushAndCompaction(
      long handle, boolean useDirectIoForFlushAndCompaction);
  private static native boolean useDirectIoForFlushAndCompaction(long handle);
  private static native void setAllowFAllocate(final long handle, final boolean allowFAllocate);
  private static native boolean allowFAllocate(final long handle);
  private static native void setAllowMmapReads(long handle, boolean allowMmapReads);
  private static native boolean allowMmapReads(long handle);
  private static native void setAllowMmapWrites(long handle, boolean allowMmapWrites);
  private static native boolean allowMmapWrites(long handle);
  private static native void setIsFdCloseOnExec(long handle, boolean isFdCloseOnExec);
  private static native boolean isFdCloseOnExec(long handle);
  private static native void setStatsDumpPeriodSec(long handle, int statsDumpPeriodSec);
  private static native int statsDumpPeriodSec(long handle);
  private static native void setMaxCompactionTriggerWakeupSeconds(
      long handle, long maxCompactionTriggerWakeupSeconds);
  private static native long maxCompactionTriggerWakeupSeconds(long handle);
  private static native void setStatsPersistPeriodSec(
      final long handle, final int statsPersistPeriodSec);
  private static native int statsPersistPeriodSec(final long handle);
  private static native void setStatsHistoryBufferSize(
      final long handle, final long statsHistoryBufferSize);
  private static native long statsHistoryBufferSize(final long handle);
  private static native void setAdviseRandomOnOpen(long handle, boolean adviseRandomOnOpen);
  private static native boolean adviseRandomOnOpen(long handle);
  private static native void setDbWriteBufferSize(final long handle, final long dbWriteBufferSize);
  private static native void setWriteBufferManager(
      final long dbOptionsHandle, final long writeBufferManagerHandle);
  private static native long dbWriteBufferSize(final long handle);
  private static native void setCompactionReadaheadSize(
      final long handle, final long compactionReadaheadSize);
  private static native long compactionReadaheadSize(final long handle);
  private static native void setDailyOffpeakTimeUTC(
      final long handle, final String dailyOffpeakTimeUTC);
  private static native String dailyOffpeakTimeUTC(final long handle);
  private static native void setWritableFileMaxBufferSize(
      final long handle, final long writableFileMaxBufferSize);
  private static native long writableFileMaxBufferSize(final long handle);
  private static native void setUseAdaptiveMutex(long handle, boolean useAdaptiveMutex);
  private static native boolean useAdaptiveMutex(long handle);
  private static native void setBytesPerSync(long handle, long bytesPerSync);
  private static native long bytesPerSync(long handle);
  private static native void setWalBytesPerSync(long handle, long walBytesPerSync);
  private static native long walBytesPerSync(long handle);
  private static native void setStrictBytesPerSync(
      final long handle, final boolean strictBytesPerSync);
  private static native boolean strictBytesPerSync(final long handle);
  private static native void setEventListeners(
      final long handle, final long[] eventListenerHandles);
  private static native AbstractEventListener[] eventListeners(final long handle);
  private static native void setEnableThreadTracking(long handle, boolean enableThreadTracking);
  private static native boolean enableThreadTracking(long handle);
  private static native void setDelayedWriteRate(long handle, long delayedWriteRate);
  private static native long delayedWriteRate(long handle);
  private static native void setEnablePipelinedWrite(
      final long handle, final boolean enablePipelinedWrite);
  private static native boolean enablePipelinedWrite(final long handle);
  private static native void setUnorderedWrite(final long handle, final boolean unorderedWrite);
  private static native boolean unorderedWrite(final long handle);
  private static native void setAllowConcurrentMemtableWrite(
      long handle, boolean allowConcurrentMemtableWrite);
  private static native boolean allowConcurrentMemtableWrite(long handle);
  private static native void setEnableWriteThreadAdaptiveYield(
      long handle, boolean enableWriteThreadAdaptiveYield);
  private static native boolean enableWriteThreadAdaptiveYield(long handle);
  private static native void setWriteThreadMaxYieldUsec(long handle, long writeThreadMaxYieldUsec);
  private static native long writeThreadMaxYieldUsec(long handle);
  private static native void setWriteThreadSlowYieldUsec(
      long handle, long writeThreadSlowYieldUsec);
  private static native long writeThreadSlowYieldUsec(long handle);
  private static native void setSkipStatsUpdateOnDbOpen(
      final long handle, final boolean skipStatsUpdateOnDbOpen);
  private static native boolean skipStatsUpdateOnDbOpen(final long handle);
  private static native void setWalRecoveryMode(final long handle, final byte walRecoveryMode);
  private static native byte walRecoveryMode(final long handle);
  private static native void setAllow2pc(final long handle, final boolean allow2pc);
  private static native boolean allow2pc(final long handle);
  private static native void setRowCache(final long handle, final long rowCacheHandle);
  private static native void setWalFilter(final long handle, final long walFilterHandle);
  private static native void setFailIfOptionsFileError(
      final long handle, final boolean failIfOptionsFileError);
  private static native boolean failIfOptionsFileError(final long handle);
  private static native void setDumpMallocStats(final long handle, final boolean dumpMallocStats);
  private static native boolean dumpMallocStats(final long handle);
  private static native void setAvoidFlushDuringRecovery(
      final long handle, final boolean avoidFlushDuringRecovery);
  private static native boolean avoidFlushDuringRecovery(final long handle);
  private static native void setAvoidFlushDuringShutdown(
      final long handle, final boolean avoidFlushDuringShutdown);
  private static native boolean avoidFlushDuringShutdown(final long handle);
  private static native void setAllowIngestBehind(
      final long handle, final boolean allowIngestBehind);
  private static native boolean allowIngestBehind(final long handle);
  private static native void setTwoWriteQueues(final long handle, final boolean twoWriteQueues);
  private static native boolean twoWriteQueues(final long handle);
  private static native void setManualWalFlush(final long handle, final boolean manualWalFlush);
  private static native boolean manualWalFlush(final long handle);
  private static native void setAtomicFlush(final long handle, final boolean atomicFlush);
  private static native boolean atomicFlush(final long handle);
  private static native void setAvoidUnnecessaryBlockingIO(
      final long handle, final boolean avoidBlockingIO);
  private static native boolean avoidUnnecessaryBlockingIO(final long handle);
  private static native void setPersistStatsToDisk(
      final long handle, final boolean persistStatsToDisk);
  private static native boolean persistStatsToDisk(final long handle);
  private static native void setWriteDbidToManifest(
      final long handle, final boolean writeDbidToManifest);
  private static native boolean writeDbidToManifest(final long handle);
  private static native void setTrackOptionsFileNumberInManifest(
      final long handle, final boolean track);
  private static native boolean trackOptionsFileNumberInManifest(final long handle);
  private static native void setLogReadaheadSize(final long handle, final long logReadaheadSize);
  private static native long logReadaheadSize(final long handle);
  private static native void setBestEffortsRecovery(
      final long handle, final boolean bestEffortsRecovery);
  private static native boolean bestEffortsRecovery(final long handle);
  private static native void setMaxBgErrorResumeCount(
      final long handle, final int maxBgerrorRecumeCount);
  private static native int maxBgerrorResumeCount(final long handle);
  private static native void setBgerrorResumeRetryInterval(
      final long handle, final long bgerrorResumeRetryInterval);
  private static native long bgerrorResumeRetryInterval(final long handle);

  // instance variables
  // NOTE: If you add new member variables, please update the copy constructor above!
  private Env env_;
  private int numShardBits_;
  private RateLimiter rateLimiter_;
  private Cache rowCache_;
  private WalFilter walFilter_;
  private WriteBufferManager writeBufferManager_;
}
