// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Options to control the behavior of a database.  It will be used
 * during the creation of a {@link org.rocksdb.RocksDB} (i.e., RocksDB.open()).
 * <p>
 * As a descendent of {@link AbstractNativeReference}, this class is {@link AutoCloseable}
 * and will be automatically released if opened in the preamble of a try with resources block.
 */
public class Options extends RocksObject
    implements DBOptionsInterface<Options>, MutableDBOptionsInterface<Options>,
               ColumnFamilyOptionsInterface<Options>, MutableColumnFamilyOptionsInterface<Options> {
  /**
   * Converts the input properties into a Options-style formatted string
   * @param properties   The set of properties to convert
   * @return The Options-style representation of those properties.
   */
  public static String getOptionStringFromProps(final Properties properties) {
    if (properties == null || properties.size() == 0) {
      throw new IllegalArgumentException("Properties value must contain at least one value.");
    }
    final StringBuilder stringBuilder = new StringBuilder();
    for (final String name : properties.stringPropertyNames()) {
      stringBuilder.append(name);
      stringBuilder.append("=");
      stringBuilder.append(properties.getProperty(name));
      stringBuilder.append(";");
    }
    return stringBuilder.toString();
  }

  /**
   * Construct options for opening a RocksDB.
   * <p>
   * This constructor will create (by allocating a block of memory)
   * an {@code rocksdb::Options} in the c++ side.
   */
  public Options() {
    super(newOptionsInstance());
    env_ = Env.getDefault();
  }

  /**
   * Construct options for opening a RocksDB. Reusing database options
   * and column family options.
   *
   * @param dbOptions {@link org.rocksdb.DBOptions} instance
   * @param columnFamilyOptions {@link org.rocksdb.ColumnFamilyOptions}
   *     instance
   */
  public Options(final DBOptions dbOptions,
      final ColumnFamilyOptions columnFamilyOptions) {
    super(newOptions(dbOptions.nativeHandle_,
        columnFamilyOptions.nativeHandle_));
    env_ = dbOptions.getEnv() != null ? dbOptions.getEnv() : Env.getDefault();
  }

  /**
   * Copy constructor for ColumnFamilyOptions.
   * <p>
   * NOTE: This does a shallow copy, which means comparator, merge_operator
   * and other pointers will be cloned!
   *
   * @param other The Options to copy.
   */
  public Options(final Options other) {
    super(copyOptions(other.nativeHandle_));
    this.env_ = other.env_;
    this.memTableConfig_ = other.memTableConfig_;
    this.tableFormatConfig_ = other.tableFormatConfig_;
    this.rateLimiter_ = other.rateLimiter_;
    this.comparator_ = other.comparator_;
    this.compactionFilter_ = other.compactionFilter_;
    this.compactionFilterFactory_ = other.compactionFilterFactory_;
    this.compactionOptionsUniversal_ = other.compactionOptionsUniversal_;
    this.compactionOptionsFIFO_ = other.compactionOptionsFIFO_;
    this.compressionOptions_ = other.compressionOptions_;
    this.rowCache_ = other.rowCache_;
    this.writeBufferManager_ = other.writeBufferManager_;
    this.compactionThreadLimiter_ = other.compactionThreadLimiter_;
    this.bottommostCompressionOptions_ = other.bottommostCompressionOptions_;
    this.walFilter_ = other.walFilter_;
    this.sstPartitionerFactory_ = other.sstPartitionerFactory_;
  }

  @Override
  public Options setIncreaseParallelism(final int totalThreads) {
    assert(isOwningHandle());
    setIncreaseParallelism(nativeHandle_, totalThreads);
    return this;
  }

  @Override
  public Options setCreateIfMissing(final boolean flag) {
    assert(isOwningHandle());
    setCreateIfMissing(nativeHandle_, flag);
    return this;
  }

  @Override
  public Options setCreateMissingColumnFamilies(final boolean flag) {
    assert(isOwningHandle());
    setCreateMissingColumnFamilies(nativeHandle_, flag);
    return this;
  }

  @Override
  public Options setEnv(final Env env) {
    assert(isOwningHandle());
    setEnv(nativeHandle_, env.nativeHandle_);
    env_ = env;
    return this;
  }

  @Override
  public Env getEnv() {
    return env_;
  }

  /**
   * <p>Set appropriate parameters for bulk loading.
   * The reason that this is a function that returns "this" instead of a
   * constructor is to enable chaining of multiple similar calls in the future.
   * </p>
   *
   * <p>All data will be in level 0 without any automatic compaction.
   * It's recommended to manually call CompactRange(NULL, NULL) before reading
   * from the database, because otherwise the read can be very slow.</p>
   *
   * @return the instance of the current Options.
   */
  public Options prepareForBulkLoad() {
    prepareForBulkLoad(nativeHandle_);
    return this;
  }

  @Override
  public boolean createIfMissing() {
    assert(isOwningHandle());
    return createIfMissing(nativeHandle_);
  }

  @Override
  public boolean createMissingColumnFamilies() {
    assert(isOwningHandle());
    return createMissingColumnFamilies(nativeHandle_);
  }

  @Override
  public Options oldDefaults(final int majorVersion, final int minorVersion) {
    oldDefaults(nativeHandle_, majorVersion, minorVersion);
    return this;
  }

  @Override
  public Options optimizeForSmallDb() {
    optimizeForSmallDb(nativeHandle_);
    return this;
  }

  @Override
  public Options optimizeForSmallDb(final Cache cache) {
    optimizeForSmallDb(nativeHandle_, cache.getNativeHandle());
    return this;
  }

  @Override
  public Options optimizeForPointLookup(final long blockCacheSizeMb) {
    optimizeForPointLookup(nativeHandle_,
        blockCacheSizeMb);
    return this;
  }

  @Override
  public Options optimizeLevelStyleCompaction() {
    optimizeLevelStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public Options optimizeLevelStyleCompaction(final long memtableMemoryBudget) {
    optimizeLevelStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public Options optimizeUniversalStyleCompaction() {
    optimizeUniversalStyleCompaction(nativeHandle_,
        DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET);
    return this;
  }

  @Override
  public Options optimizeUniversalStyleCompaction(
      final long memtableMemoryBudget) {
    optimizeUniversalStyleCompaction(nativeHandle_,
        memtableMemoryBudget);
    return this;
  }

  @Override
  public Options setComparator(final BuiltinComparator builtinComparator) {
    assert(isOwningHandle());
    setComparatorHandle(nativeHandle_, builtinComparator.ordinal());
    return this;
  }

  @Override
  public Options setComparator(
      final AbstractComparator comparator) {
    assert(isOwningHandle());
    setComparatorHandle(nativeHandle_, comparator.nativeHandle_,
            comparator.getComparatorType().getValue());
    comparator_ = comparator;
    return this;
  }

  @Override
  public Options setMergeOperatorName(final String name) {
    assert(isOwningHandle());
    if (name == null) {
      throw new IllegalArgumentException(
          "Merge operator name must not be null.");
    }
    setMergeOperatorName(nativeHandle_, name);
    return this;
  }

  @Override
  public Options setMergeOperator(final MergeOperator mergeOperator) {
    setMergeOperator(nativeHandle_, mergeOperator.nativeHandle_);
    return this;
  }

  @Override
  public Options setCompactionFilter(
          final AbstractCompactionFilter<? extends AbstractSlice<?>>
                  compactionFilter) {
    setCompactionFilterHandle(nativeHandle_, compactionFilter.nativeHandle_);
    compactionFilter_ = compactionFilter;
    return this;
  }

  @Override
  public AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter() {
    assert (isOwningHandle());
    return compactionFilter_;
  }

  @Override
  public Options setCompactionFilterFactory(final AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory) {
    assert (isOwningHandle());
    setCompactionFilterFactoryHandle(nativeHandle_, compactionFilterFactory.nativeHandle_);
    compactionFilterFactory_ = compactionFilterFactory;
    return this;
  }

  @Override
  public AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>> compactionFilterFactory() {
    assert (isOwningHandle());
    return compactionFilterFactory_;
  }

  @Override
  public Options setWriteBufferSize(final long writeBufferSize) {
    assert(isOwningHandle());
    setWriteBufferSize(nativeHandle_, writeBufferSize);
    return this;
  }

  @Override
  public long writeBufferSize()  {
    assert(isOwningHandle());
    return writeBufferSize(nativeHandle_);
  }

  @Override
  public Options setMaxWriteBufferNumber(final int maxWriteBufferNumber) {
    assert(isOwningHandle());
    setMaxWriteBufferNumber(nativeHandle_, maxWriteBufferNumber);
    return this;
  }

  @Override
  public int maxWriteBufferNumber() {
    assert(isOwningHandle());
    return maxWriteBufferNumber(nativeHandle_);
  }

  @Override
  public boolean errorIfExists() {
    assert(isOwningHandle());
    return errorIfExists(nativeHandle_);
  }

  @Override
  public Options setErrorIfExists(final boolean errorIfExists) {
    assert(isOwningHandle());
    setErrorIfExists(nativeHandle_, errorIfExists);
    return this;
  }

  @Override
  public boolean paranoidChecks() {
    assert(isOwningHandle());
    return paranoidChecks(nativeHandle_);
  }

  @Override
  public Options setParanoidChecks(final boolean paranoidChecks) {
    assert(isOwningHandle());
    setParanoidChecks(nativeHandle_, paranoidChecks);
    return this;
  }

  @Override
  public boolean openFilesAsync() {
    assert (isOwningHandle());
    return openFilesAsync(nativeHandle_);
  }

  @Override
  public Options setOpenFilesAsync(final boolean openFilesAsync) {
    assert (isOwningHandle());
    setOpenFilesAsync(nativeHandle_, openFilesAsync);
    return this;
  }

  @Override
  public int maxOpenFiles() {
    assert(isOwningHandle());
    return maxOpenFiles(nativeHandle_);
  }

  @Override
  public Options setMaxFileOpeningThreads(final int maxFileOpeningThreads) {
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
  public Options setMaxTotalWalSize(final long maxTotalWalSize) {
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
  public Options setMaxOpenFiles(final int maxOpenFiles) {
    assert(isOwningHandle());
    setMaxOpenFiles(nativeHandle_, maxOpenFiles);
    return this;
  }

  @Override
  public boolean useFsync() {
    assert(isOwningHandle());
    return useFsync(nativeHandle_);
  }

  @Override
  public Options setUseFsync(final boolean useFsync) {
    assert(isOwningHandle());
    setUseFsync(nativeHandle_, useFsync);
    return this;
  }

  @Override
  public Options setDbPaths(final Collection<DbPath> dbPaths) {
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
  public String dbLogDir() {
    assert(isOwningHandle());
    return dbLogDir(nativeHandle_);
  }

  @Override
  public Options setDbLogDir(final String dbLogDir) {
    assert(isOwningHandle());
    setDbLogDir(nativeHandle_, dbLogDir);
    return this;
  }

  @Override
  public String walDir() {
    assert(isOwningHandle());
    return walDir(nativeHandle_);
  }

  @Override
  public Options setWalDir(final String walDir) {
    assert(isOwningHandle());
    setWalDir(nativeHandle_, walDir);
    return this;
  }

  @Override
  public long deleteObsoleteFilesPeriodMicros() {
    assert(isOwningHandle());
    return deleteObsoleteFilesPeriodMicros(nativeHandle_);
  }

  @Override
  public Options setDeleteObsoleteFilesPeriodMicros(
      final long micros) {
    assert(isOwningHandle());
    setDeleteObsoleteFilesPeriodMicros(nativeHandle_, micros);
    return this;
  }

  @Override
  @Deprecated
  public int maxBackgroundCompactions() {
    assert(isOwningHandle());
    return maxBackgroundCompactions(nativeHandle_);
  }

  @Override
  public Options setStatistics(final Statistics statistics) {
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
  @Deprecated
  public Options setMaxBackgroundCompactions(
      final int maxBackgroundCompactions) {
    assert(isOwningHandle());
    setMaxBackgroundCompactions(nativeHandle_, maxBackgroundCompactions);
    return this;
  }

  @Override
  public Options setMaxSubcompactions(final int maxSubcompactions) {
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
  public int maxBackgroundFlushes() {
    assert(isOwningHandle());
    return maxBackgroundFlushes(nativeHandle_);
  }

  @Override
  @Deprecated
  public Options setMaxBackgroundFlushes(
      final int maxBackgroundFlushes) {
    assert(isOwningHandle());
    setMaxBackgroundFlushes(nativeHandle_, maxBackgroundFlushes);
    return this;
  }

  @Override
  public int maxBackgroundJobs() {
    assert(isOwningHandle());
    return maxBackgroundJobs(nativeHandle_);
  }

  @Override
  public Options setMaxBackgroundJobs(final int maxBackgroundJobs) {
    assert(isOwningHandle());
    setMaxBackgroundJobs(nativeHandle_, maxBackgroundJobs);
    return this;
  }

  @Override
  public long maxLogFileSize() {
    assert(isOwningHandle());
    return maxLogFileSize(nativeHandle_);
  }

  @Override
  public Options setMaxLogFileSize(final long maxLogFileSize) {
    assert(isOwningHandle());
    setMaxLogFileSize(nativeHandle_, maxLogFileSize);
    return this;
  }

  @Override
  public long logFileTimeToRoll() {
    assert(isOwningHandle());
    return logFileTimeToRoll(nativeHandle_);
  }

  @Override
  public Options setLogFileTimeToRoll(final long logFileTimeToRoll) {
    assert(isOwningHandle());
    setLogFileTimeToRoll(nativeHandle_, logFileTimeToRoll);
    return this;
  }

  @Override
  public long keepLogFileNum() {
    assert(isOwningHandle());
    return keepLogFileNum(nativeHandle_);
  }

  @Override
  public Options setKeepLogFileNum(final long keepLogFileNum) {
    assert(isOwningHandle());
    setKeepLogFileNum(nativeHandle_, keepLogFileNum);
    return this;
  }


  @Override
  public Options setRecycleLogFileNum(final long recycleLogFileNum) {
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
  public long maxManifestFileSize() {
    assert(isOwningHandle());
    return maxManifestFileSize(nativeHandle_);
  }

  @Override
  public Options setMaxManifestFileSize(
      final long maxManifestFileSize) {
    assert(isOwningHandle());
    setMaxManifestFileSize(nativeHandle_, maxManifestFileSize);
    return this;
  }

  @Override
  public Options setMaxTableFilesSizeFIFO(
    final long maxTableFilesSize) {
    assert(maxTableFilesSize > 0); // unsigned native type
    assert(isOwningHandle());
    setMaxTableFilesSizeFIFO(nativeHandle_, maxTableFilesSize);
    return this;
  }

  @Override
  public long maxTableFilesSizeFIFO() {
    return maxTableFilesSizeFIFO(nativeHandle_);
  }

  @Override
  public int tableCacheNumshardbits() {
    assert(isOwningHandle());
    return tableCacheNumshardbits(nativeHandle_);
  }

  @Override
  public Options setTableCacheNumshardbits(
      final int tableCacheNumshardbits) {
    assert(isOwningHandle());
    setTableCacheNumshardbits(nativeHandle_, tableCacheNumshardbits);
    return this;
  }

  @Override
  public long walTtlSeconds() {
    assert(isOwningHandle());
    return walTtlSeconds(nativeHandle_);
  }

  @Override
  public Options setWalTtlSeconds(final long walTtlSeconds) {
    assert(isOwningHandle());
    setWalTtlSeconds(nativeHandle_, walTtlSeconds);
    return this;
  }

  @Override
  public long walSizeLimitMB() {
    assert(isOwningHandle());
    return walSizeLimitMB(nativeHandle_);
  }

  @Override
  public Options setMaxWriteBatchGroupSizeBytes(final long maxWriteBatchGroupSizeBytes) {
    setMaxWriteBatchGroupSizeBytes(nativeHandle_, maxWriteBatchGroupSizeBytes);
    return this;
  }

  @Override
  public long maxWriteBatchGroupSizeBytes() {
    assert (isOwningHandle());
    return maxWriteBatchGroupSizeBytes(nativeHandle_);
  }

  @Override
  public Options setWalSizeLimitMB(final long sizeLimitMB) {
    assert(isOwningHandle());
    setWalSizeLimitMB(nativeHandle_, sizeLimitMB);
    return this;
  }

  @Override
  public long manifestPreallocationSize() {
    assert(isOwningHandle());
    return manifestPreallocationSize(nativeHandle_);
  }

  @Override
  public Options setManifestPreallocationSize(final long size) {
    assert(isOwningHandle());
    setManifestPreallocationSize(nativeHandle_, size);
    return this;
  }

  @Override
  public Options setUseDirectReads(final boolean useDirectReads) {
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
  public Options setUseDirectIoForFlushAndCompaction(
      final boolean useDirectIoForFlushAndCompaction) {
    assert(isOwningHandle());
    setUseDirectIoForFlushAndCompaction(nativeHandle_, useDirectIoForFlushAndCompaction);
    return this;
  }

  @Override
  public boolean useDirectIoForFlushAndCompaction() {
    assert(isOwningHandle());
    return useDirectIoForFlushAndCompaction(nativeHandle_);
  }

  @Override
  public Options setAllowFAllocate(final boolean allowFAllocate) {
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
  public boolean allowMmapReads() {
    assert(isOwningHandle());
    return allowMmapReads(nativeHandle_);
  }

  @Override
  public Options setAllowMmapReads(final boolean allowMmapReads) {
    assert(isOwningHandle());
    setAllowMmapReads(nativeHandle_, allowMmapReads);
    return this;
  }

  @Override
  public boolean allowMmapWrites() {
    assert(isOwningHandle());
    return allowMmapWrites(nativeHandle_);
  }

  @Override
  public Options setAllowMmapWrites(final boolean allowMmapWrites) {
    assert(isOwningHandle());
    setAllowMmapWrites(nativeHandle_, allowMmapWrites);
    return this;
  }

  @Override
  public boolean isFdCloseOnExec() {
    assert(isOwningHandle());
    return isFdCloseOnExec(nativeHandle_);
  }

  @Override
  public Options setIsFdCloseOnExec(final boolean isFdCloseOnExec) {
    assert(isOwningHandle());
    setIsFdCloseOnExec(nativeHandle_, isFdCloseOnExec);
    return this;
  }

  @Override
  public int statsDumpPeriodSec() {
    assert(isOwningHandle());
    return statsDumpPeriodSec(nativeHandle_);
  }

  @Override
  public Options setStatsDumpPeriodSec(final int statsDumpPeriodSec) {
    assert(isOwningHandle());
    setStatsDumpPeriodSec(nativeHandle_, statsDumpPeriodSec);
    return this;
  }

  @Override
  public Options setMaxCompactionTriggerWakeupSeconds(
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
  public Options setStatsPersistPeriodSec(
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
  public Options setStatsHistoryBufferSize(
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
  public boolean adviseRandomOnOpen() {
    return adviseRandomOnOpen(nativeHandle_);
  }

  @Override
  public Options setAdviseRandomOnOpen(final boolean adviseRandomOnOpen) {
    assert(isOwningHandle());
    setAdviseRandomOnOpen(nativeHandle_, adviseRandomOnOpen);
    return this;
  }

  @Override
  public Options setDbWriteBufferSize(final long dbWriteBufferSize) {
    assert(isOwningHandle());
    setDbWriteBufferSize(nativeHandle_, dbWriteBufferSize);
    return this;
  }

  @Override
  public Options setWriteBufferManager(final WriteBufferManager writeBufferManager) {
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
  public Options setCompactionReadaheadSize(final long compactionReadaheadSize) {
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
  public Options setDailyOffpeakTimeUTC(String offpeakTimeUTC) {
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
  public Options setWritableFileMaxBufferSize(final long writableFileMaxBufferSize) {
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
  public boolean useAdaptiveMutex() {
    assert(isOwningHandle());
    return useAdaptiveMutex(nativeHandle_);
  }

  @Override
  public Options setUseAdaptiveMutex(final boolean useAdaptiveMutex) {
    assert(isOwningHandle());
    setUseAdaptiveMutex(nativeHandle_, useAdaptiveMutex);
    return this;
  }

  @Override
  public long bytesPerSync() {
    return bytesPerSync(nativeHandle_);
  }

  @Override
  public Options setBytesPerSync(final long bytesPerSync) {
    assert(isOwningHandle());
    setBytesPerSync(nativeHandle_, bytesPerSync);
    return this;
  }

  @Override
  public Options setWalBytesPerSync(final long walBytesPerSync) {
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
  public Options setStrictBytesPerSync(final boolean strictBytesPerSync) {
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
  public Options setListeners(final List<AbstractEventListener> listeners) {
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
  public Options setEnableThreadTracking(final boolean enableThreadTracking) {
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
  public Options setDelayedWriteRate(final long delayedWriteRate) {
    assert(isOwningHandle());
    setDelayedWriteRate(nativeHandle_, delayedWriteRate);
    return this;
  }

  @Override
  public long delayedWriteRate(){
    return delayedWriteRate(nativeHandle_);
  }

  @Override
  public Options setEnablePipelinedWrite(final boolean enablePipelinedWrite) {
    setEnablePipelinedWrite(nativeHandle_, enablePipelinedWrite);
    return this;
  }

  @Override
  public boolean enablePipelinedWrite() {
    return enablePipelinedWrite(nativeHandle_);
  }

  @Override
  public Options setUnorderedWrite(final boolean unorderedWrite) {
    setUnorderedWrite(nativeHandle_, unorderedWrite);
    return this;
  }

  @Override
  public boolean unorderedWrite() {
    return unorderedWrite(nativeHandle_);
  }

  @Override
  public Options setAllowConcurrentMemtableWrite(
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
  public Options setEnableWriteThreadAdaptiveYield(
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
  public Options setWriteThreadMaxYieldUsec(final long writeThreadMaxYieldUsec) {
    setWriteThreadMaxYieldUsec(nativeHandle_, writeThreadMaxYieldUsec);
    return this;
  }

  @Override
  public long writeThreadMaxYieldUsec() {
    return writeThreadMaxYieldUsec(nativeHandle_);
  }

  @Override
  public Options setWriteThreadSlowYieldUsec(final long writeThreadSlowYieldUsec) {
    setWriteThreadSlowYieldUsec(nativeHandle_, writeThreadSlowYieldUsec);
    return this;
  }

  @Override
  public long writeThreadSlowYieldUsec() {
    return writeThreadSlowYieldUsec(nativeHandle_);
  }

  @Override
  public Options setSkipStatsUpdateOnDbOpen(final boolean skipStatsUpdateOnDbOpen) {
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
  public Options setWalRecoveryMode(final WALRecoveryMode walRecoveryMode) {
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
  public Options setAllow2pc(final boolean allow2pc) {
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
  public Options setRowCache(final Cache rowCache) {
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
  public Options setWalFilter(final AbstractWalFilter walFilter) {
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
  public Options setFailIfOptionsFileError(final boolean failIfOptionsFileError) {
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
  public Options setDumpMallocStats(final boolean dumpMallocStats) {
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
  public Options setAvoidFlushDuringRecovery(final boolean avoidFlushDuringRecovery) {
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
  public Options setAvoidFlushDuringShutdown(final boolean avoidFlushDuringShutdown) {
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
  public Options setAllowIngestBehind(final boolean allowIngestBehind) {
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
  public Options setTwoWriteQueues(final boolean twoWriteQueues) {
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
  public Options setManualWalFlush(final boolean manualWalFlush) {
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
  public MemTableConfig memTableConfig() {
    return this.memTableConfig_;
  }

  @Override
  public Options setMemTableConfig(final MemTableConfig config) {
    memTableConfig_ = config;
    setMemTableFactory(nativeHandle_, config.newMemTableFactoryHandle());
    return this;
  }

  @Override
  public Options setRateLimiter(final RateLimiter rateLimiter) {
    assert(isOwningHandle());
    rateLimiter_ = rateLimiter;
    setRateLimiter(nativeHandle_, rateLimiter.nativeHandle_);
    return this;
  }

  @Override
  public Options setSstFileManager(final SstFileManager sstFileManager) {
    assert(isOwningHandle());
    setSstFileManager(nativeHandle_, sstFileManager.nativeHandle_);
    return this;
  }

  @Override
  public Options setLogger(final LoggerInterface logger) {
    assert(isOwningHandle());
    setLogger(nativeHandle_, logger.getNativeHandle(), logger.getLoggerType().getValue());
    return this;
  }

  @Override
  public Options setInfoLogLevel(final InfoLogLevel infoLogLevel) {
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
  public String memTableFactoryName() {
    assert(isOwningHandle());
    return memTableFactoryName(nativeHandle_);
  }

  @Override
  public TableFormatConfig tableFormatConfig() {
    return this.tableFormatConfig_;
  }

  @Override
  public Options setTableFormatConfig(final TableFormatConfig config) {
    tableFormatConfig_ = config;
    setTableFactory(nativeHandle_, config.newTableFactoryHandle());
    return this;
  }

  @Override
  public String tableFactoryName() {
    assert(isOwningHandle());
    return tableFactoryName(nativeHandle_);
  }

  @Override
  public Options setCfPaths(final Collection<DbPath> cfPaths) {
    assert (isOwningHandle());

    final int len = cfPaths.size();
    final String[] paths = new String[len];
    final long[] targetSizes = new long[len];

    int i = 0;
    for (final DbPath dbPath : cfPaths) {
      paths[i] = dbPath.path.toString();
      targetSizes[i] = dbPath.targetSize;
      i++;
    }
    setCfPaths(nativeHandle_, paths, targetSizes);
    return this;
  }

  @Override
  public List<DbPath> cfPaths() {
    final int len = (int) cfPathsLen(nativeHandle_);

    if (len == 0) {
      return Collections.emptyList();
    }

    final String[] paths = new String[len];
    final long[] targetSizes = new long[len];

    cfPaths(nativeHandle_, paths, targetSizes);

    final List<DbPath> cfPaths = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      cfPaths.add(new DbPath(Paths.get(paths[i]), targetSizes[i]));
    }

    return cfPaths;
  }

  @Override
  public Options useFixedLengthPrefixExtractor(final int n) {
    assert(isOwningHandle());
    useFixedLengthPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public Options useCappedPrefixExtractor(final int n) {
    assert(isOwningHandle());
    useCappedPrefixExtractor(nativeHandle_, n);
    return this;
  }

  @Override
  public CompressionType compressionType() {
    return CompressionType.getCompressionType(compressionType(nativeHandle_));
  }

  @Override
  public Options setCompressionPerLevel(
      final List<CompressionType> compressionLevels) {
    final byte[] byteCompressionTypes = new byte[
        compressionLevels.size()];
    for (int i = 0; i < compressionLevels.size(); i++) {
      byteCompressionTypes[i] = compressionLevels.get(i).getValue();
    }
    setCompressionPerLevel(nativeHandle_, byteCompressionTypes);
    return this;
  }

  @Override
  public List<CompressionType> compressionPerLevel() {
    final byte[] byteCompressionTypes =
        compressionPerLevel(nativeHandle_);
    final List<CompressionType> compressionLevels = new ArrayList<>();
    for (final byte byteCompressionType : byteCompressionTypes) {
      compressionLevels.add(CompressionType.getCompressionType(
          byteCompressionType));
    }
    return compressionLevels;
  }

  @Override
  public Options setCompressionType(final CompressionType compressionType) {
    setCompressionType(nativeHandle_, compressionType.getValue());
    return this;
  }

  @Override
  public Options setBottommostCompressionType(
      final CompressionType bottommostCompressionType) {
    setBottommostCompressionType(nativeHandle_,
        bottommostCompressionType.getValue());
    return this;
  }

  @Override
  public CompressionType bottommostCompressionType() {
    return CompressionType.getCompressionType(
        bottommostCompressionType(nativeHandle_));
  }

  @Override
  public Options setBottommostCompressionOptions(
      final CompressionOptions bottommostCompressionOptions) {
    setBottommostCompressionOptions(nativeHandle_,
        bottommostCompressionOptions.nativeHandle_);
    this.bottommostCompressionOptions_ = bottommostCompressionOptions;
    return this;
  }

  @Override
  public CompressionOptions bottommostCompressionOptions() {
    return this.bottommostCompressionOptions_;
  }

  @Override
  public Options setCompressionOptions(
      final CompressionOptions compressionOptions) {
    setCompressionOptions(nativeHandle_, compressionOptions.nativeHandle_);
    this.compressionOptions_ = compressionOptions;
    return this;
  }

  @Override
  public CompressionOptions compressionOptions() {
    return this.compressionOptions_;
  }

  @Override
  public CompactionStyle compactionStyle() {
    return CompactionStyle.fromValue(compactionStyle(nativeHandle_));
  }

  @Override
  public Options setCompactionStyle(
      final CompactionStyle compactionStyle) {
    setCompactionStyle(nativeHandle_, compactionStyle.getValue());
    return this;
  }

  @Override
  public int numLevels() {
    return numLevels(nativeHandle_);
  }

  @Override
  public Options setNumLevels(final int numLevels) {
    setNumLevels(nativeHandle_, numLevels);
    return this;
  }

  @Override
  public int levelZeroFileNumCompactionTrigger() {
    return levelZeroFileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroFileNumCompactionTrigger(
      final int numFiles) {
    setLevelZeroFileNumCompactionTrigger(
        nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroSlowdownWritesTrigger() {
    return levelZeroSlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroSlowdownWritesTrigger(
      final int numFiles) {
    setLevelZeroSlowdownWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public int levelZeroStopWritesTrigger() {
    return levelZeroStopWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setLevelZeroStopWritesTrigger(
      final int numFiles) {
    setLevelZeroStopWritesTrigger(nativeHandle_, numFiles);
    return this;
  }

  @Override
  public long targetFileSizeBase() {
    return targetFileSizeBase(nativeHandle_);
  }

  @Override
  public Options setTargetFileSizeBase(final long targetFileSizeBase) {
    setTargetFileSizeBase(nativeHandle_, targetFileSizeBase);
    return this;
  }

  @Override
  public int targetFileSizeMultiplier() {
    return targetFileSizeMultiplier(nativeHandle_);
  }

  @Override
  public Options setTargetFileSizeMultiplier(final int multiplier) {
    setTargetFileSizeMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public Options setMaxBytesForLevelBase(final long maxBytesForLevelBase) {
    setMaxBytesForLevelBase(nativeHandle_, maxBytesForLevelBase);
    return this;
  }

  @Override
  public long maxBytesForLevelBase() {
    return maxBytesForLevelBase(nativeHandle_);
  }

  @Override
  public Options setLevelCompactionDynamicLevelBytes(
      final boolean enableLevelCompactionDynamicLevelBytes) {
    setLevelCompactionDynamicLevelBytes(nativeHandle_,
        enableLevelCompactionDynamicLevelBytes);
    return this;
  }

  @Override
  public boolean levelCompactionDynamicLevelBytes() {
    return levelCompactionDynamicLevelBytes(nativeHandle_);
  }

  @Override
  public double maxBytesForLevelMultiplier() {
    return maxBytesForLevelMultiplier(nativeHandle_);
  }

  @Override
  public Options setMaxBytesForLevelMultiplier(final double multiplier) {
    setMaxBytesForLevelMultiplier(nativeHandle_, multiplier);
    return this;
  }

  @Override
  public long maxCompactionBytes() {
    return maxCompactionBytes(nativeHandle_);
  }

  @Override
  public Options setMaxCompactionBytes(final long maxCompactionBytes) {
    setMaxCompactionBytes(nativeHandle_, maxCompactionBytes);
    return this;
  }

  @Override
  public long arenaBlockSize() {
    return arenaBlockSize(nativeHandle_);
  }

  @Override
  public Options setArenaBlockSize(final long arenaBlockSize) {
    setArenaBlockSize(nativeHandle_, arenaBlockSize);
    return this;
  }

  @Override
  public boolean disableAutoCompactions() {
    return disableAutoCompactions(nativeHandle_);
  }

  @Override
  public Options setDisableAutoCompactions(
      final boolean disableAutoCompactions) {
    setDisableAutoCompactions(nativeHandle_, disableAutoCompactions);
    return this;
  }

  @Override
  public long maxSequentialSkipInIterations() {
    return maxSequentialSkipInIterations(nativeHandle_);
  }

  @Override
  public Options setMaxSequentialSkipInIterations(
      final long maxSequentialSkipInIterations) {
    setMaxSequentialSkipInIterations(nativeHandle_,
        maxSequentialSkipInIterations);
    return this;
  }

  @Override
  public boolean inplaceUpdateSupport() {
    return inplaceUpdateSupport(nativeHandle_);
  }

  @Override
  public Options setInplaceUpdateSupport(
      final boolean inplaceUpdateSupport) {
    setInplaceUpdateSupport(nativeHandle_, inplaceUpdateSupport);
    return this;
  }

  @Override
  public long inplaceUpdateNumLocks() {
    return inplaceUpdateNumLocks(nativeHandle_);
  }

  @Override
  public Options setInplaceUpdateNumLocks(
      final long inplaceUpdateNumLocks) {
    setInplaceUpdateNumLocks(nativeHandle_, inplaceUpdateNumLocks);
    return this;
  }

  @Override
  public double memtablePrefixBloomSizeRatio() {
    return memtablePrefixBloomSizeRatio(nativeHandle_);
  }

  @Override
  public Options setMemtablePrefixBloomSizeRatio(final double memtablePrefixBloomSizeRatio) {
    setMemtablePrefixBloomSizeRatio(nativeHandle_, memtablePrefixBloomSizeRatio);
    return this;
  }

  @Override
  public double experimentalMempurgeThreshold() {
    return experimentalMempurgeThreshold(nativeHandle_);
  }

  @Override
  public Options setExperimentalMempurgeThreshold(final double experimentalMempurgeThreshold) {
    setExperimentalMempurgeThreshold(nativeHandle_, experimentalMempurgeThreshold);
    return this;
  }

  @Override
  public boolean memtableWholeKeyFiltering() {
    return memtableWholeKeyFiltering(nativeHandle_);
  }

  @Override
  public Options setMemtableWholeKeyFiltering(final boolean memtableWholeKeyFiltering) {
    setMemtableWholeKeyFiltering(nativeHandle_, memtableWholeKeyFiltering);
    return this;
  }

  @Override
  public int bloomLocality() {
    return bloomLocality(nativeHandle_);
  }

  @Override
  public Options setBloomLocality(final int bloomLocality) {
    setBloomLocality(nativeHandle_, bloomLocality);
    return this;
  }

  @Override
  public long maxSuccessiveMerges() {
    return maxSuccessiveMerges(nativeHandle_);
  }

  @Override
  public Options setMaxSuccessiveMerges(final long maxSuccessiveMerges) {
    setMaxSuccessiveMerges(nativeHandle_, maxSuccessiveMerges);
    return this;
  }

  @Override
  public int minWriteBufferNumberToMerge() {
    return minWriteBufferNumberToMerge(nativeHandle_);
  }

  @Override
  public Options setMinWriteBufferNumberToMerge(
      final int minWriteBufferNumberToMerge) {
    setMinWriteBufferNumberToMerge(nativeHandle_, minWriteBufferNumberToMerge);
    return this;
  }

  @Override
  public Options setOptimizeFiltersForHits(
      final boolean optimizeFiltersForHits) {
    setOptimizeFiltersForHits(nativeHandle_, optimizeFiltersForHits);
    return this;
  }

  @Override
  public boolean optimizeFiltersForHits() {
    return optimizeFiltersForHits(nativeHandle_);
  }

  @Override
  public Options setMemtableHugePageSize(final long memtableHugePageSize) {
    setMemtableHugePageSize(nativeHandle_,
        memtableHugePageSize);
    return this;
  }

  @Override
  public long memtableHugePageSize() {
    return memtableHugePageSize(nativeHandle_);
  }

  @Override
  public Options setSoftPendingCompactionBytesLimit(final long softPendingCompactionBytesLimit) {
    setSoftPendingCompactionBytesLimit(nativeHandle_,
        softPendingCompactionBytesLimit);
    return this;
  }

  @Override
  public long softPendingCompactionBytesLimit() {
    return softPendingCompactionBytesLimit(nativeHandle_);
  }

  @Override
  public Options setHardPendingCompactionBytesLimit(final long hardPendingCompactionBytesLimit) {
    setHardPendingCompactionBytesLimit(nativeHandle_, hardPendingCompactionBytesLimit);
    return this;
  }

  @Override
  public long hardPendingCompactionBytesLimit() {
    return hardPendingCompactionBytesLimit(nativeHandle_);
  }

  @Override
  public Options setLevel0FileNumCompactionTrigger(final int level0FileNumCompactionTrigger) {
    setLevel0FileNumCompactionTrigger(nativeHandle_, level0FileNumCompactionTrigger);
    return this;
  }

  @Override
  public int level0FileNumCompactionTrigger() {
    return level0FileNumCompactionTrigger(nativeHandle_);
  }

  @Override
  public Options setLevel0SlowdownWritesTrigger(final int level0SlowdownWritesTrigger) {
    setLevel0SlowdownWritesTrigger(nativeHandle_, level0SlowdownWritesTrigger);
    return this;
  }

  @Override
  public int level0SlowdownWritesTrigger() {
    return level0SlowdownWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setLevel0StopWritesTrigger(final int level0StopWritesTrigger) {
    setLevel0StopWritesTrigger(nativeHandle_, level0StopWritesTrigger);
    return this;
  }

  @Override
  public int level0StopWritesTrigger() {
    return level0StopWritesTrigger(nativeHandle_);
  }

  @Override
  public Options setMaxBytesForLevelMultiplierAdditional(
      final int[] maxBytesForLevelMultiplierAdditional) {
    setMaxBytesForLevelMultiplierAdditional(nativeHandle_, maxBytesForLevelMultiplierAdditional);
    return this;
  }

  @Override
  public int[] maxBytesForLevelMultiplierAdditional() {
    return maxBytesForLevelMultiplierAdditional(nativeHandle_);
  }

  @Override
  public Options setParanoidFileChecks(final boolean paranoidFileChecks) {
    setParanoidFileChecks(nativeHandle_, paranoidFileChecks);
    return this;
  }

  @Override
  public boolean paranoidFileChecks() {
    return paranoidFileChecks(nativeHandle_);
  }

  @Override
  public Options setCompactionPriority(
      final CompactionPriority compactionPriority) {
    setCompactionPriority(nativeHandle_, compactionPriority.getValue());
    return this;
  }

  @Override
  public CompactionPriority compactionPriority() {
    return CompactionPriority.getCompactionPriority(
        compactionPriority(nativeHandle_));
  }

  @Override
  public Options setReportBgIoStats(final boolean reportBgIoStats) {
    setReportBgIoStats(nativeHandle_, reportBgIoStats);
    return this;
  }

  @Override
  public boolean reportBgIoStats() {
    return reportBgIoStats(nativeHandle_);
  }

  @Override
  public Options setTtl(final long ttl) {
    setTtl(nativeHandle_, ttl);
    return this;
  }

  @Override
  public long ttl() {
    return ttl(nativeHandle_);
  }

  @Override
  public Options setPeriodicCompactionSeconds(final long periodicCompactionSeconds) {
    setPeriodicCompactionSeconds(nativeHandle_, periodicCompactionSeconds);
    return this;
  }

  @Override
  public long periodicCompactionSeconds() {
    return periodicCompactionSeconds(nativeHandle_);
  }

  @Override
  public Options setCompactionOptionsUniversal(
      final CompactionOptionsUniversal compactionOptionsUniversal) {
    setCompactionOptionsUniversal(nativeHandle_,
        compactionOptionsUniversal.nativeHandle_);
    this.compactionOptionsUniversal_ = compactionOptionsUniversal;
    return this;
  }

  @Override
  public CompactionOptionsUniversal compactionOptionsUniversal() {
    return this.compactionOptionsUniversal_;
  }

  @Override
  public Options setCompactionOptionsFIFO(final CompactionOptionsFIFO compactionOptionsFIFO) {
    setCompactionOptionsFIFO(nativeHandle_,
        compactionOptionsFIFO.nativeHandle_);
    this.compactionOptionsFIFO_ = compactionOptionsFIFO;
    return this;
  }

  @Override
  public CompactionOptionsFIFO compactionOptionsFIFO() {
    return this.compactionOptionsFIFO_;
  }

  @Override
  public Options setForceConsistencyChecks(final boolean forceConsistencyChecks) {
    setForceConsistencyChecks(nativeHandle_, forceConsistencyChecks);
    return this;
  }

  @Override
  public boolean forceConsistencyChecks() {
    return forceConsistencyChecks(nativeHandle_);
  }

  @Override
  public Options setAtomicFlush(final boolean atomicFlush) {
    setAtomicFlush(nativeHandle_, atomicFlush);
    return this;
  }

  @Override
  public boolean atomicFlush() {
    return atomicFlush(nativeHandle_);
  }

  @Override
  public Options setAvoidUnnecessaryBlockingIO(final boolean avoidUnnecessaryBlockingIO) {
    setAvoidUnnecessaryBlockingIO(nativeHandle_, avoidUnnecessaryBlockingIO);
    return this;
  }

  @Override
  public boolean avoidUnnecessaryBlockingIO() {
    assert (isOwningHandle());
    return avoidUnnecessaryBlockingIO(nativeHandle_);
  }

  @Override
  public Options setPersistStatsToDisk(final boolean persistStatsToDisk) {
    setPersistStatsToDisk(nativeHandle_, persistStatsToDisk);
    return this;
  }

  @Override
  public boolean persistStatsToDisk() {
    assert (isOwningHandle());
    return persistStatsToDisk(nativeHandle_);
  }

  @Override
  public Options setWriteDbidToManifest(final boolean writeDbidToManifest) {
    setWriteDbidToManifest(nativeHandle_, writeDbidToManifest);
    return this;
  }

  @Override
  public boolean writeDbidToManifest() {
    assert (isOwningHandle());
    return writeDbidToManifest(nativeHandle_);
  }

  @Override
  public Options setTrackOptionsFileNumberInManifest(final boolean track) {
    setTrackOptionsFileNumberInManifest(nativeHandle_, track);
    return this;
  }

  @Override
  public boolean trackOptionsFileNumberInManifest() {
    assert (isOwningHandle());
    return trackOptionsFileNumberInManifest(nativeHandle_);
  }

  @Override
  public Options setLogReadaheadSize(final long logReadaheadSize) {
    setLogReadaheadSize(nativeHandle_, logReadaheadSize);
    return this;
  }

  @Override
  public long logReadaheadSize() {
    assert (isOwningHandle());
    return logReadaheadSize(nativeHandle_);
  }

  @Override
  public Options setBestEffortsRecovery(final boolean bestEffortsRecovery) {
    setBestEffortsRecovery(nativeHandle_, bestEffortsRecovery);
    return this;
  }

  @Override
  public boolean bestEffortsRecovery() {
    assert (isOwningHandle());
    return bestEffortsRecovery(nativeHandle_);
  }

  @Override
  public Options setMaxBgErrorResumeCount(final int maxBgerrorResumeCount) {
    setMaxBgErrorResumeCount(nativeHandle_, maxBgerrorResumeCount);
    return this;
  }

  @Override
  public int maxBgerrorResumeCount() {
    assert (isOwningHandle());
    return maxBgerrorResumeCount(nativeHandle_);
  }

  @Override
  public Options setBgerrorResumeRetryInterval(final long bgerrorResumeRetryInterval) {
    setBgerrorResumeRetryInterval(nativeHandle_, bgerrorResumeRetryInterval);
    return this;
  }

  @Override
  public long bgerrorResumeRetryInterval() {
    assert (isOwningHandle());
    return bgerrorResumeRetryInterval(nativeHandle_);
  }

  @Override
  public Options setSstPartitionerFactory(final SstPartitionerFactory sstPartitionerFactory) {
    setSstPartitionerFactory(nativeHandle_, sstPartitionerFactory.nativeHandle_);
    this.sstPartitionerFactory_ = sstPartitionerFactory;
    return this;
  }

  @Override
  public SstPartitionerFactory sstPartitionerFactory() {
    return sstPartitionerFactory_;
  }

  @Override
  public Options setMemtableMaxRangeDeletions(final int count) {
    setMemtableMaxRangeDeletions(nativeHandle_, count);
    return this;
  }

  @Override
  public int memtableMaxRangeDeletions() {
    return memtableMaxRangeDeletions(nativeHandle_);
  }

  @Override
  public Options setMinTombstonesForRangeConversion(final int minTombstonesForRangeConversion) {
    setMinTombstonesForRangeConversion(nativeHandle_, minTombstonesForRangeConversion);
    return this;
  }

  @Override
  public int minTombstonesForRangeConversion() {
    return minTombstonesForRangeConversion(nativeHandle_);
  }

  @Override
  public Options setCompactionThreadLimiter(final ConcurrentTaskLimiter compactionThreadLimiter) {
    setCompactionThreadLimiter(nativeHandle_, compactionThreadLimiter.nativeHandle_);
    this.compactionThreadLimiter_ = compactionThreadLimiter;
    return this;
  }

  @Override
  public ConcurrentTaskLimiter compactionThreadLimiter() {
    assert (isOwningHandle());
    return this.compactionThreadLimiter_;
  }

  //
  // BEGIN options for blobs (integrated BlobDB)
  //

  @Override
  public Options setEnableBlobFiles(final boolean enableBlobFiles) {
    setEnableBlobFiles(nativeHandle_, enableBlobFiles);
    return this;
  }

  @Override
  public boolean enableBlobFiles() {
    return enableBlobFiles(nativeHandle_);
  }

  @Override
  public Options setMinBlobSize(final long minBlobSize) {
    setMinBlobSize(nativeHandle_, minBlobSize);
    return this;
  }

  @Override
  public long minBlobSize() {
    return minBlobSize(nativeHandle_);
  }

  @Override
  public Options setBlobFileSize(final long blobFileSize) {
    setBlobFileSize(nativeHandle_, blobFileSize);
    return this;
  }

  @Override
  public long blobFileSize() {
    return blobFileSize(nativeHandle_);
  }

  @Override
  public Options setBlobCompressionType(final CompressionType compressionType) {
    setBlobCompressionType(nativeHandle_, compressionType.getValue());
    return this;
  }

  @Override
  public CompressionType blobCompressionType() {
    return CompressionType.values()[blobCompressionType(nativeHandle_)];
  }

  @Override
  public Options setEnableBlobGarbageCollection(final boolean enableBlobGarbageCollection) {
    setEnableBlobGarbageCollection(nativeHandle_, enableBlobGarbageCollection);
    return this;
  }

  @Override
  public boolean enableBlobGarbageCollection() {
    return enableBlobGarbageCollection(nativeHandle_);
  }

  @Override
  public Options setBlobGarbageCollectionAgeCutoff(final double blobGarbageCollectionAgeCutoff) {
    setBlobGarbageCollectionAgeCutoff(nativeHandle_, blobGarbageCollectionAgeCutoff);
    return this;
  }

  @Override
  public double blobGarbageCollectionAgeCutoff() {
    return blobGarbageCollectionAgeCutoff(nativeHandle_);
  }

  @Override
  public Options setBlobGarbageCollectionForceThreshold(
      final double blobGarbageCollectionForceThreshold) {
    setBlobGarbageCollectionForceThreshold(nativeHandle_, blobGarbageCollectionForceThreshold);
    return this;
  }

  @Override
  public double blobGarbageCollectionForceThreshold() {
    return blobGarbageCollectionForceThreshold(nativeHandle_);
  }

  @Override
  public Options setReadTriggeredCompactionThreshold(
      final double readTriggeredCompactionThreshold) {
    setReadTriggeredCompactionThreshold(nativeHandle_, readTriggeredCompactionThreshold);
    return this;
  }

  @Override
  public double readTriggeredCompactionThreshold() {
    return readTriggeredCompactionThreshold(nativeHandle_);
  }

  @Override
  public Options setBlobCompactionReadaheadSize(final long blobCompactionReadaheadSize) {
    setBlobCompactionReadaheadSize(nativeHandle_, blobCompactionReadaheadSize);
    return this;
  }

  @Override
  public long blobCompactionReadaheadSize() {
    return blobCompactionReadaheadSize(nativeHandle_);
  }

  @Override
  public Options setBlobFileStartingLevel(final int blobFileStartingLevel) {
    setBlobFileStartingLevel(nativeHandle_, blobFileStartingLevel);
    return this;
  }

  @Override
  public int blobFileStartingLevel() {
    return blobFileStartingLevel(nativeHandle_);
  }

  @Override
  public Options setPrepopulateBlobCache(final PrepopulateBlobCache prepopulateBlobCache) {
    setPrepopulateBlobCache(nativeHandle_, prepopulateBlobCache.getValue());
    return this;
  }

  @Override
  public PrepopulateBlobCache prepopulateBlobCache() {
    return PrepopulateBlobCache.getPrepopulateBlobCache(prepopulateBlobCache(nativeHandle_));
  }

  //
  // END options for blobs (integrated BlobDB)
  //

  /**
   * Return copy of TablePropertiesCollectorFactory list. Modifying this list will not change
   * underlying options C++ object. {@link #setTablePropertiesCollectorFactory(List)
   * setTablePropertiesCollectorFactory} must be called to propagate changes. All instance must be
   * properly closed to prevent memory leaks.
   * @return copy of TablePropertiesCollectorFactory list.
   */
  public List<TablePropertiesCollectorFactory> tablePropertiesCollectorFactory() {
    long[] factoryHandlers = tablePropertiesCollectorFactory(nativeHandle_);

    return Arrays.stream(factoryHandlers)
        .mapToObj(factoryHandle -> TablePropertiesCollectorFactory.newWrapper(factoryHandle))
        .collect(Collectors.toList());
  }

  /**
   * Set TablePropertiesCollectorFactory in underlying C++ object.
   * This method create its own copy of the list. Caller is responsible for
   * closing all the instances in the list.
   * @param factories
   */
  public void setTablePropertiesCollectorFactory(List<TablePropertiesCollectorFactory> factories) {
    long[] factoryHandlers = new long[factories.size()];
    for (int i = 0; i < factoryHandlers.length; i++) {
      factoryHandlers[i] = factories.get(i).getNativeHandle();
    }
    setTablePropertiesCollectorFactory(nativeHandle_, factoryHandlers);
  }

  private static long newOptionsInstance() {
    RocksDB.loadLibrary();
    return newOptions();
  }
  private static native long newOptions();
  private static native long newOptions(long dbOptHandle, long cfOptHandle);
  private static native long copyOptions(long handle);
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);
  private static native void setEnv(long optHandle, long envHandle);
  private static native void prepareForBulkLoad(long handle);

  // DB native handles
  private static native void setIncreaseParallelism(long handle, int totalThreads);
  private static native void setCreateIfMissing(long handle, boolean flag);
  private static native boolean createIfMissing(long handle);
  private static native void setCreateMissingColumnFamilies(long handle, boolean flag);
  private static native boolean createMissingColumnFamilies(long handle);
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
  private static native void setMaxTotalWalSize(long handle, long maxTotalWalSize);
  private static native void setMaxFileOpeningThreads(
      final long handle, final int maxFileOpeningThreads);
  private static native int maxFileOpeningThreads(final long handle);
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
  private static native void setMaxBackgroundJobs(long handle, int maxMaxBackgroundJobs);
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
  private static native void setMaxTableFilesSizeFIFO(long handle, long maxTableFilesSize);
  private static native long maxTableFilesSizeFIFO(long handle);
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
      final long handle, final long writeBufferManagerHandle);
  private static native long dbWriteBufferSize(final long handle);
  private static native void setCompactionReadaheadSize(
      final long handle, final long compactionReadaheadSize);
  private static native long compactionReadaheadSize(final long handle);
  private static native void setDailyOffpeakTimeUTC(final long handle, final String offpeakTimeUTC);
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
      final long handle, final boolean pipelinedWrite);
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

  // CF native handles
  private static native void oldDefaults(
      final long handle, final int majorVersion, final int minorVersion);
  private static native void optimizeForSmallDb(final long handle);
  private static native void optimizeForSmallDb(final long handle, final long cacheHandle);
  private static native void optimizeForPointLookup(long handle, long blockCacheSizeMb);
  private static native void optimizeLevelStyleCompaction(long handle, long memtableMemoryBudget);
  private static native void optimizeUniversalStyleCompaction(
      long handle, long memtableMemoryBudget);
  private static native void setComparatorHandle(long handle, int builtinComparator);
  private static native void setComparatorHandle(
      long optHandle, long comparatorHandle, byte comparatorType);
  private static native void setMergeOperatorName(long handle, String name);
  private static native void setMergeOperator(long handle, long mergeOperatorHandle);
  private static native void setCompactionFilterHandle(long handle, long compactionFilterHandle);
  private static native void setCompactionFilterFactoryHandle(
      long handle, long compactionFilterFactoryHandle);
  private static native void setWriteBufferSize(long handle, long writeBufferSize)
      throws IllegalArgumentException;
  private static native long writeBufferSize(long handle);
  private static native void setMaxWriteBufferNumber(long handle, int maxWriteBufferNumber);
  private static native int maxWriteBufferNumber(long handle);
  private static native void setMinWriteBufferNumberToMerge(
      long handle, int minWriteBufferNumberToMerge);
  private static native int minWriteBufferNumberToMerge(long handle);
  private static native void setCompressionType(long handle, byte compressionType);
  private static native byte compressionType(long handle);
  private static native void setCompressionPerLevel(long handle, byte[] compressionLevels);
  private static native byte[] compressionPerLevel(long handle);
  private static native void setBottommostCompressionType(
      long handle, byte bottommostCompressionType);
  private static native byte bottommostCompressionType(long handle);
  private static native void setBottommostCompressionOptions(
      final long handle, final long bottommostCompressionOptionsHandle);
  private static native void setCompressionOptions(long handle, long compressionOptionsHandle);
  private static native void useFixedLengthPrefixExtractor(long handle, int prefixLength);
  private static native void useCappedPrefixExtractor(long handle, int prefixLength);
  private static native void setNumLevels(long handle, int numLevels);
  private static native int numLevels(long handle);
  private static native void setLevelZeroFileNumCompactionTrigger(long handle, int numFiles);
  private static native int levelZeroFileNumCompactionTrigger(long handle);
  private static native void setLevelZeroSlowdownWritesTrigger(long handle, int numFiles);
  private static native int levelZeroSlowdownWritesTrigger(long handle);
  private static native void setLevelZeroStopWritesTrigger(long handle, int numFiles);
  private static native int levelZeroStopWritesTrigger(long handle);
  private static native void setTargetFileSizeBase(long handle, long targetFileSizeBase);
  private static native long targetFileSizeBase(long handle);
  private static native void setTargetFileSizeMultiplier(long handle, int multiplier);
  private static native int targetFileSizeMultiplier(long handle);
  private static native void setMaxBytesForLevelBase(long handle, long maxBytesForLevelBase);
  private static native long maxBytesForLevelBase(long handle);
  private static native void setLevelCompactionDynamicLevelBytes(
      long handle, boolean enableLevelCompactionDynamicLevelBytes);
  private static native boolean levelCompactionDynamicLevelBytes(long handle);
  private static native void setMaxBytesForLevelMultiplier(long handle, double multiplier);
  private static native double maxBytesForLevelMultiplier(long handle);
  private static native void setMaxCompactionBytes(long handle, long maxCompactionBytes);
  private static native long maxCompactionBytes(long handle);
  private static native void setArenaBlockSize(long handle, long arenaBlockSize)
      throws IllegalArgumentException;
  private static native long arenaBlockSize(long handle);
  private static native void setDisableAutoCompactions(long handle, boolean disableAutoCompactions);
  private static native boolean disableAutoCompactions(long handle);
  private static native void setCompactionStyle(long handle, byte compactionStyle);
  private static native byte compactionStyle(long handle);
  private static native void setMaxSequentialSkipInIterations(
      long handle, long maxSequentialSkipInIterations);
  private static native long maxSequentialSkipInIterations(long handle);
  private static native void setMemTableFactory(long handle, long factoryHandle);
  private static native String memTableFactoryName(long handle);
  private static native void setTableFactory(long handle, long factoryHandle);
  private static native String tableFactoryName(long handle);
  private static native void setCfPaths(
      final long handle, final String[] paths, final long[] targetSizes);
  private static native long cfPathsLen(final long handle);
  private static native void cfPaths(
      final long handle, final String[] paths, final long[] targetSizes);
  private static native void setInplaceUpdateSupport(long handle, boolean inplaceUpdateSupport);
  private static native boolean inplaceUpdateSupport(long handle);
  private static native void setInplaceUpdateNumLocks(long handle, long inplaceUpdateNumLocks)
      throws IllegalArgumentException;
  private static native long inplaceUpdateNumLocks(long handle);
  private static native void setMemtablePrefixBloomSizeRatio(
      long handle, double memtablePrefixBloomSizeRatio);
  private static native double memtablePrefixBloomSizeRatio(long handle);
  private static native void setExperimentalMempurgeThreshold(
      long handle, double experimentalMempurgeThreshold);
  private static native double experimentalMempurgeThreshold(long handle);
  private static native void setMemtableWholeKeyFiltering(
      long handle, boolean memtableWholeKeyFiltering);
  private static native boolean memtableWholeKeyFiltering(long handle);
  private static native void setBloomLocality(long handle, int bloomLocality);
  private static native int bloomLocality(long handle);
  private static native void setMaxSuccessiveMerges(long handle, long maxSuccessiveMerges)
      throws IllegalArgumentException;
  private static native long maxSuccessiveMerges(long handle);
  private static native void setOptimizeFiltersForHits(long handle, boolean optimizeFiltersForHits);
  private static native boolean optimizeFiltersForHits(long handle);
  private static native void setMemtableHugePageSize(long handle, long memtableHugePageSize);
  private static native long memtableHugePageSize(long handle);
  private static native void setSoftPendingCompactionBytesLimit(
      long handle, long softPendingCompactionBytesLimit);
  private static native long softPendingCompactionBytesLimit(long handle);
  private static native void setHardPendingCompactionBytesLimit(
      long handle, long hardPendingCompactionBytesLimit);
  private static native long hardPendingCompactionBytesLimit(long handle);
  private static native void setLevel0FileNumCompactionTrigger(
      long handle, int level0FileNumCompactionTrigger);
  private static native int level0FileNumCompactionTrigger(long handle);
  private static native void setLevel0SlowdownWritesTrigger(
      long handle, int level0SlowdownWritesTrigger);
  private static native int level0SlowdownWritesTrigger(long handle);
  private static native void setLevel0StopWritesTrigger(long handle, int level0StopWritesTrigger);
  private static native int level0StopWritesTrigger(long handle);
  private static native void setMaxBytesForLevelMultiplierAdditional(
      long handle, int[] maxBytesForLevelMultiplierAdditional);
  private static native int[] maxBytesForLevelMultiplierAdditional(long handle);
  private static native void setParanoidFileChecks(long handle, boolean paranoidFileChecks);
  private static native boolean paranoidFileChecks(long handle);
  private static native void setCompactionPriority(
      final long handle, final byte compactionPriority);
  private static native byte compactionPriority(final long handle);
  private static native void setReportBgIoStats(final long handle, final boolean reportBgIoStats);
  private static native boolean reportBgIoStats(final long handle);
  private static native void setTtl(final long handle, final long ttl);
  private static native long ttl(final long handle);
  private static native void setPeriodicCompactionSeconds(
      final long handle, final long periodicCompactionSeconds);
  private static native long periodicCompactionSeconds(final long handle);
  private static native void setCompactionOptionsUniversal(
      final long handle, final long compactionOptionsUniversalHandle);
  private static native void setCompactionOptionsFIFO(
      final long handle, final long compactionOptionsFIFOHandle);
  private static native void setForceConsistencyChecks(
      final long handle, final boolean forceConsistencyChecks);
  private static native boolean forceConsistencyChecks(final long handle);
  private static native void setAtomicFlush(final long handle, final boolean atomicFlush);
  private static native boolean atomicFlush(final long handle);
  private static native void setSstPartitionerFactory(long nativeHandle_, long newFactoryHandle);
  private static native void setMemtableMaxRangeDeletions(final long handle, final int count);
  private static native int memtableMaxRangeDeletions(final long handle);
  private static native void setMinTombstonesForRangeConversion(
      final long handle, final int minTombstonesForRangeConversion);
  private static native int minTombstonesForRangeConversion(final long handle);
  private static native void setCompactionThreadLimiter(
      final long nativeHandle_, final long newLimiterHandle);
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

  private static native void setEnableBlobFiles(
      final long nativeHandle_, final boolean enableBlobFiles);
  private static native boolean enableBlobFiles(final long nativeHandle_);
  private static native void setMinBlobSize(final long nativeHandle_, final long minBlobSize);
  private static native long minBlobSize(final long nativeHandle_);
  private static native void setBlobFileSize(final long nativeHandle_, final long blobFileSize);
  private static native long blobFileSize(final long nativeHandle_);
  private static native void setBlobCompressionType(
      final long nativeHandle_, final byte compressionType);
  private static native byte blobCompressionType(final long nativeHandle_);
  private static native void setEnableBlobGarbageCollection(
      final long nativeHandle_, final boolean enableBlobGarbageCollection);
  private static native boolean enableBlobGarbageCollection(final long nativeHandle_);
  private static native void setBlobGarbageCollectionAgeCutoff(
      final long nativeHandle_, final double blobGarbageCollectionAgeCutoff);
  private static native double blobGarbageCollectionAgeCutoff(final long nativeHandle_);
  private static native void setBlobGarbageCollectionForceThreshold(
      final long nativeHandle_, final double blobGarbageCollectionForceThreshold);
  private static native double blobGarbageCollectionForceThreshold(final long nativeHandle_);
  private static native void setReadTriggeredCompactionThreshold(
      final long nativeHandle_, final double readTriggeredCompactionThreshold);
  private static native double readTriggeredCompactionThreshold(final long nativeHandle_);
  private static native void setBlobCompactionReadaheadSize(
      final long nativeHandle_, final long blobCompactionReadaheadSize);
  private static native long blobCompactionReadaheadSize(final long nativeHandle_);
  private static native void setBlobFileStartingLevel(
      final long nativeHandle_, final int blobFileStartingLevel);
  private static native int blobFileStartingLevel(final long nativeHandle_);
  private static native void setPrepopulateBlobCache(
      final long nativeHandle_, final byte prepopulateBlobCache);
  private static native byte prepopulateBlobCache(final long nativeHandle_);
  private static native long[] tablePropertiesCollectorFactory(long nativeHandle);
  private static native void setTablePropertiesCollectorFactory(
      long nativeHandle, long[] factoryHandlers);

  // instance variables
  // NOTE: If you add new member variables, please update the copy constructor above!
  private Env env_;
  private MemTableConfig memTableConfig_;
  private TableFormatConfig tableFormatConfig_;
  private RateLimiter rateLimiter_;
  private AbstractComparator comparator_;
  private AbstractCompactionFilter<? extends AbstractSlice<?>> compactionFilter_;
  private AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
          compactionFilterFactory_;
  private CompactionOptionsUniversal compactionOptionsUniversal_;
  private CompactionOptionsFIFO compactionOptionsFIFO_;
  private CompressionOptions bottommostCompressionOptions_;
  private CompressionOptions compressionOptions_;
  private Cache rowCache_;
  private WalFilter walFilter_;
  private WriteBufferManager writeBufferManager_;
  private SstPartitionerFactory sstPartitionerFactory_;
  private ConcurrentTaskLimiter compactionThreadLimiter_;
}
