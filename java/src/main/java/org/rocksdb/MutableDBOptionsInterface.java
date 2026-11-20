// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.rocksdb;

public interface MutableDBOptionsInterface<T extends MutableDBOptionsInterface<T>> {
  /**
   * Specifies the maximum number of concurrent background jobs (both flushes
   * and compactions combined).
   * Default: 2
   *
   * @param maxBackgroundJobs number of max concurrent background jobs
   * @return the instance of the current object.
   */
  T setMaxBackgroundJobs(int maxBackgroundJobs);

  /**
   * Returns the maximum number of concurrent background jobs (both flushes
   * and compactions combined).
   * Default: 2
   *
   * @return the maximum number of concurrent background jobs.
   */
  int maxBackgroundJobs();

  /**
   * NOT SUPPORTED ANYMORE: RocksDB automatically decides this based on the
   * value of max_background_jobs. For backwards compatibility we will set
   * `max_background_jobs = max_background_compactions + max_background_flushes`
   * in the case where user sets at least one of `max_background_compactions` or
   * `max_background_flushes` (we replace -1 by 1 in case one option is unset).
   * <p>
   * Specifies the maximum number of concurrent background compaction jobs,
   * submitted to the default LOW priority thread pool.
   * If you're increasing this, also consider increasing number of threads in
   * LOW priority thread pool. For more information, see
   * Default: -1
   *
   * @param maxBackgroundCompactions the maximum number of background
   *     compaction jobs.
   * @return the instance of the current object.
   *
   * @see RocksEnv#setBackgroundThreads(int)
   * @see RocksEnv#setBackgroundThreads(int, Priority)
   * @see DBOptionsInterface#maxBackgroundFlushes()
   * @deprecated Use {@link #setMaxBackgroundJobs(int)}
   */
  @Deprecated
  T setMaxBackgroundCompactions(int maxBackgroundCompactions);

  /**
   * NOT SUPPORTED ANYMORE: RocksDB automatically decides this based on the
   * value of max_background_jobs. For backwards compatibility we will set
   * `max_background_jobs = max_background_compactions + max_background_flushes`
   * in the case where user sets at least one of `max_background_compactions` or
   * `max_background_flushes` (we replace -1 by 1 in case one option is unset).
   * <p>
   * Returns the maximum number of concurrent background compaction jobs,
   * submitted to the default LOW priority thread pool.
   * When increasing this number, we may also want to consider increasing
   * number of threads in LOW priority thread pool.
   * Default: -1
   *
   * @return the maximum number of concurrent background compaction jobs.
   * @see RocksEnv#setBackgroundThreads(int)
   * @see RocksEnv#setBackgroundThreads(int, Priority)
   *
   * @deprecated Use {@link #setMaxBackgroundJobs(int)}
   */
  @Deprecated
  int maxBackgroundCompactions();

  /**
   * By default RocksDB will flush all memtables on DB close if there are
   * unpersisted data (i.e. with WAL disabled) The flush can be skip to speedup
   * DB close. Unpersisted data WILL BE LOST.
   * <p>
   * DEFAULT: false
   * <p>
   * Dynamically changeable through
   *     {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}
   *     API.
   *
   * @param avoidFlushDuringShutdown true if we should avoid flush during
   *     shutdown
   *
   * @return the reference to the current options.
   */
  T setAvoidFlushDuringShutdown(boolean avoidFlushDuringShutdown);

  /**
   * By default RocksDB will flush all memtables on DB close if there are
   * unpersisted data (i.e. with WAL disabled) The flush can be skip to speedup
   * DB close. Unpersisted data WILL BE LOST.
   * <p>
   * DEFAULT: false
   * <p>
   * Dynamically changeable through
   *     {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}
   *     API.
   *
   * @return true if we should avoid flush during shutdown
   */
  boolean avoidFlushDuringShutdown();

  /**
   * This is the maximum buffer size that is used by WritableFileWriter.
   * On Windows, we need to maintain an aligned buffer for writes.
   * We allow the buffer to grow until it's size hits the limit.
   * <p>
   * Default: 1024 * 1024 (1 MB)
   *
   * @param writableFileMaxBufferSize the maximum buffer size
   *
   * @return the reference to the current options.
   */
  T setWritableFileMaxBufferSize(long writableFileMaxBufferSize);

  /**
   * This is the maximum buffer size that is used by WritableFileWriter.
   * On Windows, we need to maintain an aligned buffer for writes.
   * We allow the buffer to grow until it's size hits the limit.
   * <p>
   * Default: 1024 * 1024 (1 MB)
   *
   * @return the maximum buffer size
   */
  long writableFileMaxBufferSize();

  /**
   * The limited write rate to DB if
   * {@link ColumnFamilyOptions#softPendingCompactionBytesLimit()} or
   * {@link ColumnFamilyOptions#level0SlowdownWritesTrigger()} is triggered,
   * or we are writing to the last mem table allowed and we allow more than 3
   * mem tables. It is calculated using size of user write requests before
   * compression. RocksDB may decide to slow down more if the compaction still
   * gets behind further.
   * If the value is 0, we will infer a value from `rater_limiter` value
   * if it is not empty, or 16MB if `rater_limiter` is empty. Note that
   * if users change the rate in `rate_limiter` after DB is opened,
   * `delayed_write_rate` won't be adjusted.
   * <p>
   * Unit: bytes per second.
   * <p>
   * Default: 0
   * <p>
   * Dynamically changeable through {@link RocksDB#setDBOptions(MutableDBOptions)}.
   *
   * @param delayedWriteRate the rate in bytes per second
   *
   * @return the reference to the current options.
   */
  T setDelayedWriteRate(long delayedWriteRate);

  /**
   * The limited write rate to DB if
   * {@link ColumnFamilyOptions#softPendingCompactionBytesLimit()} or
   * {@link ColumnFamilyOptions#level0SlowdownWritesTrigger()} is triggered,
   * or we are writing to the last mem table allowed and we allow more than 3
   * mem tables. It is calculated using size of user write requests before
   * compression. RocksDB may decide to slow down more if the compaction still
   * gets behind further.
   * If the value is 0, we will infer a value from `rater_limiter` value
   * if it is not empty, or 16MB if `rater_limiter` is empty. Note that
   * if users change the rate in `rate_limiter` after DB is opened,
   * `delayed_write_rate` won't be adjusted.
   * <p>
   * Unit: bytes per second.
   * <p>
   * Default: 0
   * <p>
   * Dynamically changeable through {@link RocksDB#setDBOptions(MutableDBOptions)}.
   *
   * @return the rate in bytes per second
   */
  long delayedWriteRate();

  /**
   * <p>Set the max total write-ahead log size. Once write-ahead logs exceed this size, we will
   * start forcing the flush of column families whose memtables are backed by the oldest live WAL
   * file
   * </p>
   * <p>The oldest WAL files are the ones that are causing all the space amplification.
   * </p>
   *  For example, with 15 column families, each with
   *  <code>write_buffer_size = 128 MB</code>
   *  <code>max_write_buffer_number = 6</code>
   *  <code>max_total_wal_size</code> will be calculated to be <code>[15 * 128MB * 6] * 4 =
   * 45GB</code>
   * <p>
   *  The RocksDB wiki has some discussion about how the WAL interacts
   *  with memtables and flushing of column families, at
   * <a href="https://github.com/meta/rocksdb/wiki/Column-Families">...</a>
   *  </p>
   * <p>If set to 0 (default), we will dynamically choose the WAL size limit to
   * be [sum of all write_buffer_size * max_write_buffer_number] * 4</p>
   * <p>This option takes effect only when there are more than one column family as
   * otherwise the wal size is dictated by the write_buffer_size.</p>
   * <p>Default: 0</p>
   *
   * @param maxTotalWalSize max total wal size.
   * @return the instance of the current object.
   */
  T setMaxTotalWalSize(long maxTotalWalSize);

  /**
   * <p>Returns the max total write-ahead log size. Once write-ahead logs exceed this size,
   * we will start forcing the flush of column families whose memtables are
   * backed by the oldest live WAL file.</p>
   * <p>The oldest WAL files are the ones that are causing all the space amplification.
   * </p>
   *  For example, with 15 column families, each with
   *  <code>write_buffer_size = 128 MB</code>
   *  <code>max_write_buffer_number = 6</code>
   *  <code>max_total_wal_size</code> will be calculated to be <code>[15 * 128MB * 6] * 4 =
   * 45GB</code>
   * <p>
   *  The RocksDB wiki has some discussion about how the WAL interacts
   *  with memtables and flushing of column families, at
   * <a href="https://github.com/meta/rocksdb/wiki/Column-Families">...</a>
   *  </p>
   * <p>If set to 0 (default), we will dynamically choose the WAL size limit to
   * be [sum of all write_buffer_size * max_write_buffer_number] * 4</p>
   * <p>This option takes effect only when there are more than one column family as
   * otherwise the wal size is dictated by the write_buffer_size.</p>
   * <p>Default: 0</p>
   *
   *
   * <p>If set to 0 (default), we will dynamically choose the WAL size limit
   * to be [sum of all write_buffer_size * max_write_buffer_number] * 4
   * </p>
   *
   * @return max total wal size
   */
  long maxTotalWalSize();

  /**
   * The periodicity when obsolete files get deleted. The default
   * value is 6 hours. The files that get out of scope by compaction
   * process will still get automatically delete on every compaction,
   * regardless of this setting
   *
   * @param micros the time interval in micros
   * @return the instance of the current object.
   */
  T setDeleteObsoleteFilesPeriodMicros(long micros);

  /**
   * The periodicity when obsolete files get deleted. The default
   * value is 6 hours. The files that get out of scope by compaction
   * process will still get automatically delete on every compaction,
   * regardless of this setting
   *
   * @return the time interval in micros when obsolete files will be deleted.
   */
  long deleteObsoleteFilesPeriodMicros();

  /**
   * if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
   * Default: 600 (10 minutes)
   *
   * @param statsDumpPeriodSec time interval in seconds.
   * @return the instance of the current object.
   */
  T setStatsDumpPeriodSec(int statsDumpPeriodSec);

  /**
   * If not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
   * Default: 600 (10 minutes)
   *
   * @return time interval in seconds.
   */
  int statsDumpPeriodSec();

  /**
   * Maximum interval in seconds between periodic compaction trigger checks.
   * Default: 43200 (12 hours)
   *
   * @param maxCompactionTriggerWakeupSeconds time interval in seconds.
   * @return the instance of the current object.
   */
  T setMaxCompactionTriggerWakeupSeconds(long maxCompactionTriggerWakeupSeconds);

  /**
   * Maximum interval in seconds between periodic compaction trigger checks.
   * Default: 43200 (12 hours)
   *
   * @return time interval in seconds.
   */
  long maxCompactionTriggerWakeupSeconds();

  /**
   * If not zero, dump rocksdb.stats to RocksDB every
   * {@code statsPersistPeriodSec}
   *
   * Default: 600
   *
   * @param statsPersistPeriodSec time interval in seconds.
   * @return the instance of the current object.
   */
  T setStatsPersistPeriodSec(int statsPersistPeriodSec);

  /**
   * If not zero, dump rocksdb.stats to RocksDB every
   * {@code statsPersistPeriodSec}
   *
   * @return time interval in seconds.
   */
  int statsPersistPeriodSec();

  /**
   * If not zero, periodically take stats snapshots and store in memory, the
   * memory size for stats snapshots is capped at {@code statsHistoryBufferSize}
   *
   * Default: 1MB
   *
   * @param statsHistoryBufferSize the size of the buffer.
   * @return the instance of the current object.
   */
  T setStatsHistoryBufferSize(long statsHistoryBufferSize);

  /**
   * If not zero, periodically take stats snapshots and store in memory, the
   * memory size for stats snapshots is capped at {@code statsHistoryBufferSize}
   *
   * @return the size of the buffer.
   */
  long statsHistoryBufferSize();

  /**
   * Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set. Value -1 means
   * files opened are always kept open. You can estimate number of files based
   * on {@code target_file_size_base} and {@code target_file_size_multiplier}
   * for level-based compaction. For universal-style compaction, you can usually
   * set it to -1.
   * Default: -1
   *
   * @param maxOpenFiles the maximum number of open files.
   * @return the instance of the current object.
   */
  T setMaxOpenFiles(int maxOpenFiles);

  /**
   * Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set. Value -1 means
   * files opened are always kept open. You can estimate number of files based
   * on {@code target_file_size_base} and {@code target_file_size_multiplier}
   * for level-based compaction. For universal-style compaction, you can usually
   * set it to -1.
   * Default: -1
   *
   * @return the maximum number of open files.
   */
  int maxOpenFiles();

  /**
   * Allows OS to incrementally sync files to disk while they are being
   * written, asynchronously, in the background.
   * Issue one request for every bytes_per_sync written. 0 turns it off.
   * Default: 0
   *
   * @param bytesPerSync size in bytes
   * @return the instance of the current object.
   */
  T setBytesPerSync(long bytesPerSync);

  /**
   * Allows OS to incrementally sync files to disk while they are being
   * written, asynchronously, in the background.
   * Issue one request for every bytes_per_sync written. 0 turns it off.
   * Default: 0
   *
   * @return size in bytes
   */
  long bytesPerSync();

  /**
   * Same as {@link #setBytesPerSync(long)} , but applies to WAL files
   * <p>
   * Default: 0, turned off
   *
   * @param walBytesPerSync size in bytes
   * @return the instance of the current object.
   */
  T setWalBytesPerSync(long walBytesPerSync);

  /**
   * Same as {@link #bytesPerSync()} , but applies to WAL files
   * <p>
   * Default: 0, turned off
   *
   * @return size in bytes
   */
  long walBytesPerSync();

  /**
   * When true, guarantees WAL files have at most {@link #walBytesPerSync()}
   * bytes submitted for writeback at any given time, and SST files have at most
   * {@link #bytesPerSync()} bytes pending writeback at any given time. This
   * can be used to handle cases where processing speed exceeds I/O speed
   * during file generation, which can lead to a huge sync when the file is
   * finished, even with {@link #bytesPerSync()} / {@link #walBytesPerSync()}
   * properly configured.
   * <p>
   * - If `sync_file_range` is supported it achieves this by waiting for any
   *   prior `sync_file_range`s to finish before proceeding. In this way,
   *   processing (compression, etc.) can proceed uninhibited in the gap
   *   between `sync_file_range`s, and we block only when I/O falls
   *   behind.
   * - Otherwise the `WritableFile::Sync` method is used. Note this mechanism
   *   always blocks, thus preventing the interleaving of I/O and processing.
   * <p>
   * Note: Enabling this option does not provide any additional persistence
   * guarantees, as it may use `sync_file_range`, which does not write out
   * metadata.
   * <p>
   * Default: false
   *
   * @param strictBytesPerSync the bytes per sync
   * @return the instance of the current object.
   */
  T setStrictBytesPerSync(boolean strictBytesPerSync);

  /**
   * Return the strict byte limit per sync.
   * <p>
   * See {@link #setStrictBytesPerSync(boolean)}
   *
   * @return the limit in bytes.
   */
  boolean strictBytesPerSync();

  /**
   * If non-zero, we perform bigger reads when doing compaction. If you're
   * running RocksDB on spinning disks, you should set this to at least 2MB.
   * <p>
   * That way RocksDB's compaction is doing sequential instead of random reads.
   * <p>
   * Default: 2MB
   *
   * @param compactionReadaheadSize The compaction read-ahead size
   *
   * @return the reference to the current options.
   */
  T setCompactionReadaheadSize(final long compactionReadaheadSize);

  /**
   * If non-zero, we perform bigger reads when doing compaction. If you're
   * running RocksDB on spinning disks, you should set this to at least 2MB.
   * <p>
   * That way RocksDB's compaction is doing sequential instead of random reads.
   * <p>
   * Default: 0
   *
   * @return The compaction read-ahead size
   */
  long compactionReadaheadSize();

  /**
   * Implementing off-peak duration awareness in RocksDB. In this context,
   * "off-peak time" signifies periods characterized by significantly less read
   * and write activity compared to other times. By leveraging this knowledge,
   * we can prevent low-priority tasks, such as TTL-based compactions, from
   * competing with read and write operations during peak hours. Essentially, we
   * preprocess these tasks during the preceding off-peak period, just before
   * the next peak cycle begins. For example, if the TTL is configured for 25
   * days, we may compact the files during the off-peak hours of the 24th day.
   *
   * Time of the day in UTC, start_time-end_time inclusive.
   * Format - HH:mm-HH:mm (00:00-23:59)
   * If the start time exceeds the end time, it will be considered that the time period
   * spans to the next day (e.g., 23:30-04:00). To make an entire day off-peak,
   * use "0:00-23:59". To make an entire day have no offpeak period, leave
   * this field blank. Default: Empty string (no offpeak).
   *
   * @param offpeakTimeUTC String value from which to parse offpeak time range
   */
  T setDailyOffpeakTimeUTC(final String offpeakTimeUTC);

  /**
   *
   * Implementing off-peak duration awareness in RocksDB. In this context,
   * "off-peak time" signifies periods characterized by significantly less read
   * and write activity compared to other times. By leveraging this knowledge,
   * we can prevent low-priority tasks, such as TTL-based compactions, from
   * competing with read and write operations during peak hours. Essentially, we
   * preprocess these tasks during the preceding off-peak period, just before
   * the next peak cycle begins. For example, if the TTL is configured for 25
   * days, we may compact the files during the off-peak hours of the 24th day.
   *
   * Time of the day in UTC, start_time-end_time inclusive.
   * Format - HH:mm-HH:mm (00:00-23:59)
   * If the start time exceeds the end time, it will be considered that the time period
   * spans to the next day (e.g., 23:30-04:00). To make an entire day off-peak,
   * use "0:00-23:59". To make an entire day have no offpeak period, leave
   * this field blank. Default: Empty string (no offpeak).
   *
   * @return String value of current offpeak time range, "" if none is set.
   */
  String dailyOffpeakTimeUTC();
}
