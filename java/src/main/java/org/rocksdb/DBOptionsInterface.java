// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Collection;
import java.util.List;

public interface DBOptionsInterface<T extends DBOptionsInterface<T>> {
  /**
   * Use this if your DB is very small (like under 1GB) and you don't want to
   * spend lots of memory for memtables.
   *
   * @return the instance of the current object.
   */
  T optimizeForSmallDb();

  /**
   * Use the specified object to interact with the environment,
   * e.g. to read/write files, schedule background work, etc.
   * Default: {@link Env#getDefault()}
   *
   * @param env {@link Env} instance.
   * @return the instance of the current Options.
   */
  T setEnv(final Env env);

  /**
   * Returns the set RocksEnv instance.
   *
   * @return {@link RocksEnv} instance set in the options.
   */
  Env getEnv();

  /**
   * <p>By default, RocksDB uses only one background thread for flush and
   * compaction. Calling this function will set it up such that total of
   * `total_threads` is used.</p>
   *
   * <p>You almost definitely want to call this function if your system is
   * bottlenecked by RocksDB.</p>
   *
   * @param totalThreads The total number of threads to be used by RocksDB.
   *     A good value is the number of cores.
   *
   * @return the instance of the current Options
   */
  T setIncreaseParallelism(int totalThreads);

  /**
   * If this value is set to true, then the database will be created
   * if it is missing during {@code RocksDB.open()}.
   * Default: false
   *
   * @param flag a flag indicating whether to create a database the
   *     specified database in {@link RocksDB#open(org.rocksdb.Options, String)} operation
   *     is missing.
   * @return the instance of the current Options
   * @see RocksDB#open(org.rocksdb.Options, String)
   */
  T setCreateIfMissing(boolean flag);

  /**
   * Return true if the create_if_missing flag is set to true.
   * If true, the database will be created if it is missing.
   *
   * @return true if the createIfMissing option is set to true.
   * @see #setCreateIfMissing(boolean)
   */
  boolean createIfMissing();

  /**
   * <p>If true, missing column families will be automatically created</p>
   *
   * <p>Default: false</p>
   *
   * @param flag a flag indicating if missing column families shall be
   *     created automatically.
   * @return true if missing column families shall be created automatically
   *     on open.
   */
  T setCreateMissingColumnFamilies(boolean flag);

  /**
   * Return true if the create_missing_column_families flag is set
   * to true. If true column families be created if missing.
   *
   * @return true if the createMissingColumnFamilies is set to
   *     true.
   * @see #setCreateMissingColumnFamilies(boolean)
   */
  boolean createMissingColumnFamilies();

  /**
   * If true, an error will be thrown during RocksDB.open() if the
   * database already exists.
   * Default: false
   *
   * @param errorIfExists if true, an exception will be thrown
   *     during {@code RocksDB.open()} if the database already exists.
   * @return the reference to the current option.
   * @see RocksDB#open(org.rocksdb.Options, String)
   */
  T setErrorIfExists(boolean errorIfExists);

  /**
   * If true, an error will be thrown during RocksDB.open() if the
   * database already exists.
   *
   * @return if true, an error is raised when the specified database
   *    already exists before open.
   */
  boolean errorIfExists();

  /**
   * If true, the implementation will do aggressive checking of the
   * data it is processing and will stop early if it detects any
   * errors.  This may have unforeseen ramifications: for example, a
   * corruption of one DB entry may cause a large number of entries to
   * become unreadable or for the entire DB to become unopenable.
   * If any of the  writes to the database fails (Put, Delete, Merge, Write),
   * the database will switch to read-only mode and fail all other
   * Write operations.
   * Default: true
   *
   * @param paranoidChecks a flag to indicate whether paranoid-check
   *     is on.
   * @return the reference to the current option.
   */
  T setParanoidChecks(boolean paranoidChecks);

  /**
   * If true, the implementation will do aggressive checking of the
   * data it is processing and will stop early if it detects any
   * errors.  This may have unforeseen ramifications: for example, a
   * corruption of one DB entry may cause a large number of entries to
   * become unreadable or for the entire DB to become unopenable.
   * If any of the  writes to the database fails (Put, Delete, Merge, Write),
   * the database will switch to read-only mode and fail all other
   * Write operations.
   *
   * @return a boolean indicating whether paranoid-check is on.
   */
  boolean paranoidChecks();

  /**
   * If true, SST files are opened and validated asynchronously in the
   * background after DB::Open returns. This reduces DB open time for
   * databases with many SST files.
   *
   * Default: false
   *
   * @param openFilesAsync true to enable async file opening.
   * @return the reference to the current option.
   */
  T setOpenFilesAsync(boolean openFilesAsync);

  /**
   * If true, SST files are opened and validated asynchronously in the
   * background after DB::Open returns.
   *
   * @return a boolean indicating whether async file opening is enabled.
   */
  boolean openFilesAsync();

  /**
   * Use to control write rate of flush and compaction. Flush has higher
   * priority than compaction. Rate limiting is disabled if nullptr.
   * Default: nullptr
   *
   * @param rateLimiter {@link org.rocksdb.RateLimiter} instance.
   * @return the instance of the current object.
   *
   * @since 3.10.0
   */
  T setRateLimiter(RateLimiter rateLimiter);

  /**
   * Use to track SST files and control their file deletion rate.
   *
   * Features:
   *  - Throttle the deletion rate of the SST files.
   *  - Keep track the total size of all SST files.
   *  - Set a maximum allowed space limit for SST files that when reached
   *    the DB wont do any further flushes or compactions and will set the
   *    background error.
   *  - Can be shared between multiple dbs.
   *
   *  Limitations:
   *  - Only track and throttle deletes of SST files in
   *    first db_path (db_name if db_paths is empty).
   *
   * @param sstFileManager The SST File Manager for the db.
   * @return the instance of the current object.
   */
  T setSstFileManager(SstFileManager sstFileManager);

  /**
   * <p>Any internal progress/error information generated by
   * the db will be written to the Logger if it is non-nullptr,
   * or to a file stored in the same directory as the DB
   * contents if info_log is nullptr.</p>
   *
   * <p>Default: nullptr</p>
   *
   * @param logger {@link LoggerInterface} instance.
   * @return the instance of the current object.
   */
  T setLogger(LoggerInterface logger);

  /**
   * <p>Sets the RocksDB log level. Default level is INFO</p>
   *
   * @param infoLogLevel log level to set.
   * @return the instance of the current object.
   */
  T setInfoLogLevel(InfoLogLevel infoLogLevel);

  /**
   * <p>Returns currently set log level.</p>
   * @return {@link org.rocksdb.InfoLogLevel} instance.
   */
  InfoLogLevel infoLogLevel();

  /**
   * If {@link MutableDBOptionsInterface#maxOpenFiles()} is -1, DB will open
   * all files on DB::Open(). You can use this option to increase the number
   * of threads used to open the files.
   *
   * Default: 16
   *
   * @param maxFileOpeningThreads the maximum number of threads to use to
   *     open files
   *
   * @return the reference to the current options.
   */
  T setMaxFileOpeningThreads(int maxFileOpeningThreads);

  /**
   * If {@link MutableDBOptionsInterface#maxOpenFiles()} is -1, DB will open all
   * files on DB::Open(). You can use this option to increase the number of
   * threads used to open the files.
   *
   * Default: 16
   *
   * @return the maximum number of threads to use to open files
   */
  int maxFileOpeningThreads();

  /**
   * <p>Sets the statistics object which collects metrics about database operations.
   * Statistics objects should not be shared between DB instances as
   * it does not use any locks to prevent concurrent updates.</p>
   *
   * @param statistics The statistics to set
   *
   * @return the instance of the current object.
   *
   * @see RocksDB#open(org.rocksdb.Options, String)
   */
  T setStatistics(final Statistics statistics);

  /**
   * <p>Returns statistics object.</p>
   *
   * @return the instance of the statistics object or null if there is no
   * statistics object.
   *
   * @see #setStatistics(Statistics)
   */
  Statistics statistics();

  /**
   * <p>If true, then every store to stable storage will issue a fsync.</p>
   * <p>If false, then every store to stable storage will issue a fdatasync.
   * This parameter should be set to true while storing data to
   * filesystem like ext3 that can lose files after a reboot.</p>
   * <p>Default: false</p>
   *
   * @param useFsync a boolean flag to specify whether to use fsync
   * @return the instance of the current object.
   */
  T setUseFsync(boolean useFsync);

  /**
   * <p>If true, then every store to stable storage will issue a fsync.</p>
   * <p>If false, then every store to stable storage will issue a fdatasync.
   * This parameter should be set to true while storing data to
   * filesystem like ext3 that can lose files after a reboot.</p>
   *
   * @return boolean value indicating if fsync is used.
   */
  boolean useFsync();

  /**
   * A list of paths where SST files can be put into, with its target size.
   * Newer data is placed into paths specified earlier in the vector while
   * older data gradually moves to paths specified later in the vector.
   *
   * For example, you have a flash device with 10GB allocated for the DB,
   * as well as a hard drive of 2TB, you should config it to be:
   *    [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
   *
   * The system will try to guarantee data under each path is close to but
   * not larger than the target size. But current and future file sizes used
   * by determining where to place a file are based on best-effort estimation,
   * which means there is a chance that the actual size under the directory
   * is slightly more than target size under some workloads. User should give
   * some buffer room for those cases.
   *
   * If none of the paths has sufficient room to place a file, the file will
   * be placed to the last path anyway, despite to the target size.
   *
   * Placing newer data to earlier paths is also best-efforts. User should
   * expect user files to be placed in higher levels in some extreme cases.
   *
   * If left empty, only one path will be used, which is db_name passed when
   * opening the DB.
   *
   * Default: empty
   *
   * @param dbPaths the paths and target sizes
   *
   * @return the reference to the current options
   */
  T setDbPaths(final Collection<DbPath> dbPaths);

  /**
   * A list of paths where SST files can be put into, with its target size.
   * Newer data is placed into paths specified earlier in the vector while
   * older data gradually moves to paths specified later in the vector.
   *
   * For example, you have a flash device with 10GB allocated for the DB,
   * as well as a hard drive of 2TB, you should config it to be:
   *    [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
   *
   * The system will try to guarantee data under each path is close to but
   * not larger than the target size. But current and future file sizes used
   * by determining where to place a file are based on best-effort estimation,
   * which means there is a chance that the actual size under the directory
   * is slightly more than target size under some workloads. User should give
   * some buffer room for those cases.
   *
   * If none of the paths has sufficient room to place a file, the file will
   * be placed to the last path anyway, despite to the target size.
   *
   * Placing newer data to earlier paths is also best-efforts. User should
   * expect user files to be placed in higher levels in some extreme cases.
   *
   * If left empty, only one path will be used, which is db_name passed when
   * opening the DB.
   *
   * Default: {@link java.util.Collections#emptyList()}
   *
   * @return dbPaths the paths and target sizes
   */
  List<DbPath> dbPaths();

  /**
   * This specifies the info LOG dir.
   * If it is empty, the log files will be in the same dir as data.
   * If it is non empty, the log files will be in the specified dir,
   * and the db data dir's absolute path will be used as the log file
   * name's prefix.
   *
   * @param dbLogDir the path to the info log directory
   * @return the instance of the current object.
   */
  T setDbLogDir(String dbLogDir);

  /**
   * Returns the directory of info log.
   *
   * If it is empty, the log files will be in the same dir as data.
   * If it is non empty, the log files will be in the specified dir,
   * and the db data dir's absolute path will be used as the log file
   * name's prefix.
   *
   * @return the path to the info log directory
   */
  String dbLogDir();

  /**
   * This specifies the absolute dir path for write-ahead logs (WAL).
   * If it is empty, the log files will be in the same dir as data,
   *   dbname is used as the data dir by default
   * If it is non empty, the log files will be in kept the specified dir.
   * When destroying the db,
   *   all log files in wal_dir and the dir itself is deleted
   *
   * @param walDir the path to the write-ahead-log directory.
   * @return the instance of the current object.
   */
  T setWalDir(String walDir);

  /**
   * Returns the path to the write-ahead-logs (WAL) directory.
   *
   * If it is empty, the log files will be in the same dir as data,
   *   dbname is used as the data dir by default
   * If it is non empty, the log files will be in kept the specified dir.
   * When destroying the db,
   *   all log files in wal_dir and the dir itself is deleted
   *
   * @return the path to the write-ahead-logs (WAL) directory.
   */
  String walDir();

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
   * This value represents the maximum number of threads that will
   * concurrently perform a compaction job by breaking it into multiple,
   * smaller ones that are run simultaneously.
   * Default: 1 (i.e. no subcompactions)
   *
   * @param maxSubcompactions The maximum number of threads that will
   *     concurrently perform a compaction job
   *
   * @return the instance of the current object.
   */
  T setMaxSubcompactions(int maxSubcompactions);

  /**
   * This value represents the maximum number of threads that will
   * concurrently perform a compaction job by breaking it into multiple,
   * smaller ones that are run simultaneously.
   * Default: 1 (i.e. no subcompactions)
   *
   * @return The maximum number of threads that will concurrently perform a
   *     compaction job
   */
  int maxSubcompactions();

  /**
   * NOT SUPPORTED ANYMORE: RocksDB automatically decides this based on the
   * value of max_background_jobs. For backwards compatibility we will set
   * `max_background_jobs = max_background_compactions + max_background_flushes`
   * in the case where user sets at least one of `max_background_compactions` or
   * `max_background_flushes`.
   *
   * Specifies the maximum number of concurrent background flush jobs.
   * If you're increasing this, also consider increasing number of threads in
   * HIGH priority thread pool. For more information, see
   * Default: -1
   *
   * @param maxBackgroundFlushes number of max concurrent flush jobs
   * @return the instance of the current object.
   *
   * @see RocksEnv#setBackgroundThreads(int)
   * @see RocksEnv#setBackgroundThreads(int, Priority)
   * @see MutableDBOptionsInterface#maxBackgroundCompactions()
   *
   * @deprecated Use {@link MutableDBOptionsInterface#setMaxBackgroundJobs(int)}
   */
  @Deprecated
  T setMaxBackgroundFlushes(int maxBackgroundFlushes);

  /**
   * NOT SUPPORTED ANYMORE: RocksDB automatically decides this based on the
   * value of max_background_jobs. For backwards compatibility we will set
   * `max_background_jobs = max_background_compactions + max_background_flushes`
   * in the case where user sets at least one of `max_background_compactions` or
   * `max_background_flushes`.
   *
   * Returns the maximum number of concurrent background flush jobs.
   * If you're increasing this, also consider increasing number of threads in
   * HIGH priority thread pool. For more information, see
   * Default: -1
   *
   * @return the maximum number of concurrent background flush jobs.
   * @see RocksEnv#setBackgroundThreads(int)
   * @see RocksEnv#setBackgroundThreads(int, Priority)
   */
  @Deprecated
  int maxBackgroundFlushes();

  /**
   * Specifies the maximum size of a info log file. If the current log file
   * is larger than `max_log_file_size`, a new info log file will
   * be created.
   * If 0, all logs will be written to one log file.
   *
   * @param maxLogFileSize the maximum size of a info log file.
   * @return the instance of the current object.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  T setMaxLogFileSize(long maxLogFileSize);

  /**
   * Returns the maximum size of a info log file. If the current log file
   * is larger than this size, a new info log file will be created.
   * If 0, all logs will be written to one log file.
   *
   * @return the maximum size of the info log file.
   */
  long maxLogFileSize();

  /**
   * Specifies the time interval for the info log file to roll (in seconds).
   * If specified with non-zero value, log file will be rolled
   * if it has been active longer than `log_file_time_to_roll`.
   * Default: 0 (disabled)
   *
   * @param logFileTimeToRoll the time interval in seconds.
   * @return the instance of the current object.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  T setLogFileTimeToRoll(long logFileTimeToRoll);

  /**
   * Returns the time interval for the info log file to roll (in seconds).
   * If specified with non-zero value, log file will be rolled
   * if it has been active longer than `log_file_time_to_roll`.
   * Default: 0 (disabled)
   *
   * @return the time interval in seconds.
   */
  long logFileTimeToRoll();

  /**
   * Specifies the maximum number of info log files to be kept.
   * Default: 1000
   *
   * @param keepLogFileNum the maximum number of info log files to be kept.
   * @return the instance of the current object.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  T setKeepLogFileNum(long keepLogFileNum);

  /**
   * Returns the maximum number of info log files to be kept.
   * Default: 1000
   *
   * @return the maximum number of info log files to be kept.
   */
  long keepLogFileNum();

  /**
   * Recycle log files.
   *
   * If non-zero, we will reuse previously written log files for new
   * logs, overwriting the old data.  The value indicates how many
   * such files we will keep around at any point in time for later
   * use.
   *
   * This is more efficient because the blocks are already
   * allocated and fdatasync does not need to update the inode after
   * each write.
   *
   * Default: 0
   *
   * @param recycleLogFileNum the number of log files to keep for recycling
   *
   * @return the reference to the current options
   */
  T setRecycleLogFileNum(long recycleLogFileNum);

  /**
   * Recycle log files.
   *
   * If non-zero, we will reuse previously written log files for new
   * logs, overwriting the old data.  The value indicates how many
   * such files we will keep around at any point in time for later
   * use.
   *
   * This is more efficient because the blocks are already
   * allocated and fdatasync does not need to update the inode after
   * each write.
   *
   * Default: 0
   *
   * @return the number of log files kept for recycling
   */
  long recycleLogFileNum();

  /**
   * Manifest file is rolled over on reaching this limit.
   * The older manifest file be deleted.
   * The default value is 1GB so that the manifest file can grow, but not
   * reach the limit of storage capacity.
   *
   * @param maxManifestFileSize the size limit of a manifest file.
   * @return the instance of the current object.
   */
  T setMaxManifestFileSize(long maxManifestFileSize);

  /**
   * Manifest file is rolled over on reaching this limit.
   * The older manifest file be deleted.
   * The default value is 1GB so that the manifest file can grow, but not
   * reach the limit of storage capacity.
   *
   * @return the size limit of a manifest file.
   */
  long maxManifestFileSize();

  /**
   * Number of shards used for table cache.
   *
   * @param tableCacheNumshardbits the number of chards
   * @return the instance of the current object.
   */
  T setTableCacheNumshardbits(int tableCacheNumshardbits);

  /**
   * Number of shards used for table cache.
   *
   * @return the number of shards used for table cache.
   */
  int tableCacheNumshardbits();

  /**
   * {@link #walTtlSeconds()} and {@link #walSizeLimitMB()} affect when WALs
   * will be archived and deleted.
   *
   * When both are zero, obsolete WALs will not be archived and will be deleted
   * immediately. Otherwise, obsolete WALs will be archived prior to deletion.
   *
   * When `WAL_size_limit_MB` is nonzero, archived WALs starting with the
   * earliest will be deleted until the total size of the archive falls below
   * this limit. All empty WALs will be deleted.
   *
   * When `WAL_ttl_seconds` is nonzero, archived WALs older than
   * `WAL_ttl_seconds` will be deleted.
   *
   * When only `WAL_ttl_seconds` is nonzero, the frequency at which archived
   * WALs are deleted is every `WAL_ttl_seconds / 2` seconds. When only
   * `WAL_size_limit_MB` is nonzero, the deletion frequency is every ten
   * minutes. When both are nonzero, the deletion frequency is the minimum of
   * those two values.
   *
   * @param walTtlSeconds the ttl seconds
   * @return the instance of the current object.
   * @see #setWalSizeLimitMB(long)
   */
  T setWalTtlSeconds(long walTtlSeconds);

  /**
   * WalTtlSeconds() and walSizeLimitMB() affect when WALs will be archived and
   * deleted.
   *
   * When both are zero, obsolete WALs will not be archived and will be deleted
   * immediately. Otherwise, obsolete WALs will be archived prior to deletion.
   *
   * When `WAL_size_limit_MB` is nonzero, archived WALs starting with the
   * earliest will be deleted until the total size of the archive falls below
   * this limit. All empty WALs will be deleted.
   *
   * When `WAL_ttl_seconds` is nonzero, archived WALs older than
   * `WAL_ttl_seconds` will be deleted.
   *
   * When only `WAL_ttl_seconds` is nonzero, the frequency at which archived
   * WALs are deleted is every `WAL_ttl_seconds / 2` seconds. When only
   * `WAL_size_limit_MB` is nonzero, the deletion frequency is every ten
   * minutes. When both are nonzero, the deletion frequency is the minimum of
   * those two values.
   *
   * @return the wal-ttl seconds
   * @see #walSizeLimitMB()
   */
  long walTtlSeconds();

  /**
   * WalTtlSeconds() and walSizeLimitMB() affect how archived logs
   * will be deleted.
   *
   * When both are zero, obsolete WALs will not be archived and will be deleted
   * immediately. Otherwise, obsolete WALs will be archived prior to deletion.
   *
   * When `WAL_size_limit_MB` is nonzero, archived WALs starting with the
   * earliest will be deleted until the total size of the archive falls below
   * this limit. All empty WALs will be deleted.
   *
   * When `WAL_ttl_seconds` is nonzero, archived WALs older than
   * `WAL_ttl_seconds` will be deleted.
   *
   * When only `WAL_ttl_seconds` is nonzero, the frequency at which archived
   * WALs are deleted is every `WAL_ttl_seconds / 2` seconds. When only
   * `WAL_size_limit_MB` is nonzero, the deletion frequency is every ten
   * minutes. When both are nonzero, the deletion frequency is the minimum of
   * those two values.
   *
   * @param sizeLimitMB size limit in mega-bytes.
   * @return the instance of the current object.
   * @see #setWalSizeLimitMB(long)
   */
  T setWalSizeLimitMB(long sizeLimitMB);

  /**
   * WalTtlSeconds() and walSizeLimitMB() affect when WALs will be archived and
   * deleted.
   *
   * When both are zero, obsolete WALs will not be archived and will be deleted
   * immediately. Otherwise, obsolete WALs will be archived prior to deletion.
   *
   * When `WAL_size_limit_MB` is nonzero, archived WALs starting with the
   * earliest will be deleted until the total size of the archive falls below
   * this limit. All empty WALs will be deleted.
   *
   * When `WAL_ttl_seconds` is nonzero, archived WALs older than
   * `WAL_ttl_seconds` will be deleted.
   *
   * When only `WAL_ttl_seconds` is nonzero, the frequency at which archived
   * WALs are deleted is every `WAL_ttl_seconds / 2` seconds. When only
   * `WAL_size_limit_MB` is nonzero, the deletion frequency is every ten
   * minutes. When both are nonzero, the deletion frequency is the minimum of
   * those two values.
   *
   * @return size limit in mega-bytes.
   * @see #walSizeLimitMB()
   */
  long walSizeLimitMB();

  /**
   * The maximum limit of number of bytes that are written in a single batch
   * of WAL or memtable write. It is followed when the leader write size
   * is larger than 1/8 of this limit.
   *
   * Default: 1 MB
   *
   * @param maxWriteBatchGroupSizeBytes the maximum limit of number of bytes, see description.
   * @return the instance of the current object.
   */
  T setMaxWriteBatchGroupSizeBytes(final long maxWriteBatchGroupSizeBytes);

  /**
   * The maximum limit of number of bytes that are written in a single batch
   * of WAL or memtable write. It is followed when the leader write size
   * is larger than 1/8 of this limit.
   *
   * Default: 1 MB
   *
   * @return the maximum limit of number of bytes, see description.
   */
  long maxWriteBatchGroupSizeBytes();

  /**
   * Number of bytes to preallocate (via fallocate) the manifest
   * files.  Default is 4mb, which is reasonable to reduce random IO
   * as well as prevent overallocation for mounts that preallocate
   * large amounts of data (such as xfs's allocsize option).
   *
   * @param size the size in byte
   * @return the instance of the current object.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  T setManifestPreallocationSize(long size);

  /**
   * Number of bytes to preallocate (via fallocate) the manifest
   * files.  Default is 4mb, which is reasonable to reduce random IO
   * as well as prevent overallocation for mounts that preallocate
   * large amounts of data (such as xfs's allocsize option).
   *
   * @return size in bytes.
   */
  long manifestPreallocationSize();

  /**
   * Enable the OS to use direct I/O for reading sst tables.
   * Default: false
   *
   * @param useDirectReads if true, then direct read is enabled
   * @return the instance of the current object.
   */
  T setUseDirectReads(boolean useDirectReads);

  /**
   * Enable the OS to use direct I/O for reading sst tables.
   * Default: false
   *
   * @return if true, then direct reads are enabled
   */
  boolean useDirectReads();

  /**
   * Enable the OS to use direct reads and writes in flush and
   * compaction
   * Default: false
   *
   * @param useDirectIoForFlushAndCompaction if true, then direct
   *        I/O will be enabled for background flush and compactions
   * @return the instance of the current object.
   */
  T setUseDirectIoForFlushAndCompaction(boolean useDirectIoForFlushAndCompaction);

  /**
   * Enable the OS to use direct reads and writes in flush and
   * compaction
   *
   * @return if true, then direct I/O is enabled for flush and
   *         compaction
   */
  boolean useDirectIoForFlushAndCompaction();

  /**
   * Whether fallocate calls are allowed
   *
   * @param allowFAllocate false if fallocate() calls are bypassed
   *
   * @return the reference to the current options.
   */
  T setAllowFAllocate(boolean allowFAllocate);

  /**
   * Whether fallocate calls are allowed
   *
   * @return false if fallocate() calls are bypassed
   */
  boolean allowFAllocate();

  /**
   * Allow the OS to mmap file for reading sst tables.
   * Default: false
   *
   * @param allowMmapReads true if mmap reads are allowed.
   * @return the instance of the current object.
   */
  T setAllowMmapReads(boolean allowMmapReads);

  /**
   * Allow the OS to mmap file for reading sst tables.
   * Default: false
   *
   * @return true if mmap reads are allowed.
   */
  boolean allowMmapReads();

  /**
   * Allow the OS to mmap file for writing. Default: false
   *
   * @param allowMmapWrites true if mmap writes are allowd.
   * @return the instance of the current object.
   */
  T setAllowMmapWrites(boolean allowMmapWrites);

  /**
   * Allow the OS to mmap file for writing. Default: false
   *
   * @return true if mmap writes are allowed.
   */
  boolean allowMmapWrites();

  /**
   * Disable child process inherit open files. Default: true
   *
   * @param isFdCloseOnExec true if child process inheriting open
   *     files is disabled.
   * @return the instance of the current object.
   */
  T setIsFdCloseOnExec(boolean isFdCloseOnExec);

  /**
   * Disable child process inherit open files. Default: true
   *
   * @return true if child process inheriting open files is disabled.
   */
  boolean isFdCloseOnExec();

  /**
   * If set true, will hint the underlying file system that the file
   * access pattern is random, when a sst file is opened.
   * Default: true
   *
   * @param adviseRandomOnOpen true if hinting random access is on.
   * @return the instance of the current object.
   */
  T setAdviseRandomOnOpen(boolean adviseRandomOnOpen);

  /**
   * If set true, will hint the underlying file system that the file
   * access pattern is random, when a sst file is opened.
   * Default: true
   *
   * @return true if hinting random access is on.
   */
  boolean adviseRandomOnOpen();

  /**
   * Amount of data to build up in memtables across all column
   * families before writing to disk.
   *
   * This is distinct from {@link ColumnFamilyOptions#writeBufferSize()},
   * which enforces a limit for a single memtable.
   *
   * This feature is disabled by default. Specify a non-zero value
   * to enable it.
   *
   * Default: 0 (disabled)
   *
   * @param dbWriteBufferSize the size of the write buffer
   *
   * @return the reference to the current options.
   */
  T setDbWriteBufferSize(long dbWriteBufferSize);

  /**
   * Use passed {@link WriteBufferManager} to control memory usage across
   * multiple column families and/or DB instances.
   *
   * Check <a href="https://github.com/meta/rocksdb/wiki/Write-Buffer-Manager">
   *     https://github.com/meta/rocksdb/wiki/Write-Buffer-Manager</a>
   * for more details on when to use it
   *
   * @param writeBufferManager The WriteBufferManager to use
   * @return the reference of the current options.
   */
  T setWriteBufferManager(final WriteBufferManager writeBufferManager);

  /**
   * Reference to {@link WriteBufferManager} used by it. <br>
   *
   * Default: null (Disabled)
   *
   * @return a reference to WriteBufferManager
   */
  WriteBufferManager writeBufferManager();

  /**
   * Amount of data to build up in memtables across all column
   * families before writing to disk.
   *
   * This is distinct from {@link ColumnFamilyOptions#writeBufferSize()},
   * which enforces a limit for a single memtable.
   *
   * This feature is disabled by default. Specify a non-zero value
   * to enable it.
   *
   * Default: 0 (disabled)
   *
   * @return the size of the write buffer
   */
  long dbWriteBufferSize();

  /**
   * Use adaptive mutex, which spins in the user space before resorting
   * to kernel. This could reduce context switch when the mutex is not
   * heavily contended. However, if the mutex is hot, we could end up
   * wasting spin time.
   * Default: false
   *
   * @param useAdaptiveMutex true if adaptive mutex is used.
   * @return the instance of the current object.
   */
  T setUseAdaptiveMutex(boolean useAdaptiveMutex);

  /**
   * Use adaptive mutex, which spins in the user space before resorting
   * to kernel. This could reduce context switch when the mutex is not
   * heavily contended. However, if the mutex is hot, we could end up
   * wasting spin time.
   * Default: false
   *
   * @return true if adaptive mutex is used.
   */
  boolean useAdaptiveMutex();

  /**
   * Sets the {@link EventListener}s whose callback functions
   * will be called when specific RocksDB event happens.
   *
   * Note: the RocksJava API currently only supports EventListeners implemented in Java.
   * It could be extended in future to also support adding/removing EventListeners implemented in
   * C++.
   *
   * @param listeners the listeners who should be notified on various events.
   *
   * @return the instance of the current object.
   */
  T setListeners(final List<AbstractEventListener> listeners);

  /**
   * Sets the {@link EventListener}s whose callback functions
   * will be called when specific RocksDB event happens.
   *
   * Note: the RocksJava API currently only supports EventListeners implemented in Java.
   * It could be extended in future to also support adding/removing EventListeners implemented in
   * C++.
   *
   * @return the instance of the current object.
   */
  List<AbstractEventListener> listeners();

  /**
   * If true, then the status of the threads involved in this DB will
   * be tracked and available via GetThreadList() API.
   *
   * Default: false
   *
   * @param enableThreadTracking true to enable tracking
   *
   * @return the reference to the current options.
   */
  T setEnableThreadTracking(boolean enableThreadTracking);

  /**
   * If true, then the status of the threads involved in this DB will
   * be tracked and available via GetThreadList() API.
   *
   * Default: false
   *
   * @return true if tracking is enabled
   */
  boolean enableThreadTracking();

  /**
   * By default, a single write thread queue is maintained. The thread gets
   * to the head of the queue becomes write batch group leader and responsible
   * for writing to WAL and memtable for the batch group.
   *
   * If {@link #enablePipelinedWrite()} is true, separate write thread queue is
   * maintained for WAL write and memtable write. A write thread first enter WAL
   * writer queue and then memtable writer queue. Pending thread on the WAL
   * writer queue thus only have to wait for previous writers to finish their
   * WAL writing but not the memtable writing. Enabling the feature may improve
   * write throughput and reduce latency of the prepare phase of two-phase
   * commit.
   *
   * Default: false
   *
   * @param enablePipelinedWrite true to enabled pipelined writes
   *
   * @return the reference to the current options.
   */
  T setEnablePipelinedWrite(final boolean enablePipelinedWrite);

  /**
   * Returns true if pipelined writes are enabled.
   * See {@link #setEnablePipelinedWrite(boolean)}.
   *
   * @return true if pipelined writes are enabled, false otherwise.
   */
  boolean enablePipelinedWrite();

  /**
   * Setting {@link #unorderedWrite()} to true trades higher write throughput with
   * relaxing the immutability guarantee of snapshots. This violates the
   * repeatability one expects from ::Get from a snapshot, as well as
   * ::MultiGet and Iterator's consistent-point-in-time view property.
   * If the application cannot tolerate the relaxed guarantees, it can implement
   * its own mechanisms to work around that and yet benefit from the higher
   * throughput. Using TransactionDB with WRITE_PREPARED write policy and
   * {@link #twoWriteQueues()} true is one way to achieve immutable snapshots despite
   * unordered_write.
   *
   * By default, i.e., when it is false, rocksdb does not advance the sequence
   * number for new snapshots unless all the writes with lower sequence numbers
   * are already finished. This provides the immutability that we except from
   * snapshots. Moreover, since Iterator and MultiGet internally depend on
   * snapshots, the snapshot immutability results into Iterator and MultiGet
   * offering consistent-point-in-time view. If set to true, although
   * Read-Your-Own-Write property is still provided, the snapshot immutability
   * property is relaxed: the writes issued after the snapshot is obtained (with
   * larger sequence numbers) will be still not visible to the reads from that
   * snapshot, however, there still might be pending writes (with lower sequence
   * number) that will change the state visible to the snapshot after they are
   * landed to the memtable.
   *
   * @param unorderedWrite true to enabled unordered write
   *
   * @return the reference to the current options.
   */
  T setUnorderedWrite(final boolean unorderedWrite);

  /**
   * Returns true if unordered write are enabled.
   * See {@link #setUnorderedWrite(boolean)}.
   *
   * @return true if unordered write are enabled, false otherwise.
   */
  boolean unorderedWrite();

  /**
   * If true, allow multi-writers to update mem tables in parallel.
   * Only some memtable factorys support concurrent writes; currently it
   * is implemented only for SkipListFactory.  Concurrent memtable writes
   * are not compatible with inplace_update_support or filter_deletes.
   * It is strongly recommended to set
   * {@link #setEnableWriteThreadAdaptiveYield(boolean)} if you are going to use
   * this feature.
   * Default: true
   *
   * @param allowConcurrentMemtableWrite true to enable concurrent writes
   *     for the memtable
   *
   * @return the reference to the current options.
   */
  T setAllowConcurrentMemtableWrite(boolean allowConcurrentMemtableWrite);

  /**
   * If true, allow multi-writers to update mem tables in parallel.
   * Only some memtable factorys support concurrent writes; currently it
   * is implemented only for SkipListFactory.  Concurrent memtable writes
   * are not compatible with inplace_update_support or filter_deletes.
   * It is strongly recommended to set
   * {@link #setEnableWriteThreadAdaptiveYield(boolean)} if you are going to use
   * this feature.
   * Default: true
   *
   * @return true if concurrent writes are enabled for the memtable
   */
  boolean allowConcurrentMemtableWrite();

  /**
   * If true, threads synchronizing with the write batch group leader will
   * wait for up to {@link #writeThreadMaxYieldUsec()} before blocking on a
   * mutex. This can substantially improve throughput for concurrent workloads,
   * regardless of whether {@link #allowConcurrentMemtableWrite()} is enabled.
   * Default: true
   *
   * @param enableWriteThreadAdaptiveYield true to enable adaptive yield for the
   *     write threads
   *
   * @return the reference to the current options.
   */
  T setEnableWriteThreadAdaptiveYield(
      boolean enableWriteThreadAdaptiveYield);

  /**
   * If true, threads synchronizing with the write batch group leader will
   * wait for up to {@link #writeThreadMaxYieldUsec()} before blocking on a
   * mutex. This can substantially improve throughput for concurrent workloads,
   * regardless of whether {@link #allowConcurrentMemtableWrite()} is enabled.
   * Default: true
   *
   * @return true if adaptive yield is enabled
   *    for the writing threads
   */
  boolean enableWriteThreadAdaptiveYield();

  /**
   * The maximum number of microseconds that a write operation will use
   * a yielding spin loop to coordinate with other write threads before
   * blocking on a mutex.  (Assuming {@link #writeThreadSlowYieldUsec()} is
   * set properly) increasing this value is likely to increase RocksDB
   * throughput at the expense of increased CPU usage.
   * Default: 100
   *
   * @param writeThreadMaxYieldUsec maximum number of microseconds
   *
   * @return the reference to the current options.
   */
  T setWriteThreadMaxYieldUsec(long writeThreadMaxYieldUsec);

  /**
   * The maximum number of microseconds that a write operation will use
   * a yielding spin loop to coordinate with other write threads before
   * blocking on a mutex.  (Assuming {@link #writeThreadSlowYieldUsec()} is
   * set properly) increasing this value is likely to increase RocksDB
   * throughput at the expense of increased CPU usage.
   * Default: 100
   *
   * @return the maximum number of microseconds
   */
  long writeThreadMaxYieldUsec();

  /**
   * The latency in microseconds after which a std::this_thread::yield
   * call (sched_yield on Linux) is considered to be a signal that
   * other processes or threads would like to use the current core.
   * Increasing this makes writer threads more likely to take CPU
   * by spinning, which will show up as an increase in the number of
   * involuntary context switches.
   * Default: 3
   *
   * @param writeThreadSlowYieldUsec the latency in microseconds
   *
   * @return the reference to the current options.
   */
  T setWriteThreadSlowYieldUsec(long writeThreadSlowYieldUsec);

  /**
   * The latency in microseconds after which a std::this_thread::yield
   * call (sched_yield on Linux) is considered to be a signal that
   * other processes or threads would like to use the current core.
   * Increasing this makes writer threads more likely to take CPU
   * by spinning, which will show up as an increase in the number of
   * involuntary context switches.
   * Default: 3
   *
   * @return writeThreadSlowYieldUsec the latency in microseconds
   */
  long writeThreadSlowYieldUsec();

  /**
   * If true, then DB::Open() will not update the statistics used to optimize
   * compaction decision by loading table properties from many files.
   * Turning off this feature will improve DBOpen time especially in
   * disk environment.
   *
   * Default: false
   *
   * @param skipStatsUpdateOnDbOpen true if updating stats will be skipped
   *
   * @return the reference to the current options.
   */
  T setSkipStatsUpdateOnDbOpen(boolean skipStatsUpdateOnDbOpen);

  /**
   * If true, then DB::Open() will not update the statistics used to optimize
   * compaction decision by loading table properties from many files.
   * Turning off this feature will improve DBOpen time especially in
   * disk environment.
   *
   * Default: false
   *
   * @return true if updating stats will be skipped
   */
  boolean skipStatsUpdateOnDbOpen();

  /**
   * Recovery mode to control the consistency while replaying WAL
   *
   * Default: {@link WALRecoveryMode#PointInTimeRecovery}
   *
   * @param walRecoveryMode The WAL recover mode
   *
   * @return the reference to the current options.
   */
  T setWalRecoveryMode(WALRecoveryMode walRecoveryMode);

  /**
   * Recovery mode to control the consistency while replaying WAL
   *
   * Default: {@link WALRecoveryMode#PointInTimeRecovery}
   *
   * @return The WAL recover mode
   */
  WALRecoveryMode walRecoveryMode();

  /**
   * if set to false then recovery will fail when a prepared
   * transaction is encountered in the WAL
   *
   * Default: false
   *
   * @param allow2pc true if two-phase-commit is enabled
   *
   * @return the reference to the current options.
   */
  T setAllow2pc(boolean allow2pc);

  /**
   * if set to false then recovery will fail when a prepared
   * transaction is encountered in the WAL
   *
   * Default: false
   *
   * @return true if two-phase-commit is enabled
   */
  boolean allow2pc();

  /**
   * A global cache for table-level rows.
   *
   * Default: null (disabled)
   *
   * @param rowCache The global row cache
   *
   * @return the reference to the current options.
   */
  T setRowCache(final Cache rowCache);

  /**
   * A global cache for table-level rows.
   *
   * Default: null (disabled)
   *
   * @return The global row cache
   */
  Cache rowCache();

  /**
   * A filter object supplied to be invoked while processing write-ahead-logs
   * (WALs) during recovery. The filter provides a way to inspect log
   * records, ignoring a particular record or skipping replay.
   * The filter is invoked at startup and is invoked from a single-thread
   * currently.
   *
   * @param walFilter the filter for processing WALs during recovery.
   *
   * @return the reference to the current options.
   */
  T setWalFilter(final AbstractWalFilter walFilter);

  /**
   * Get's the filter for processing WALs during recovery.
   * See {@link #setWalFilter(AbstractWalFilter)}.
   *
   * @return the filter used for processing WALs during recovery.
   */
  WalFilter walFilter();

  /**
   * If true, then DB::Open / CreateColumnFamily / DropColumnFamily
   * / SetOptions will fail if options file is not detected or properly
   * persisted.
   *
   * DEFAULT: false
   *
   * @param failIfOptionsFileError true if we should fail if there is an error
   *     in the options file
   *
   * @return the reference to the current options.
   */
  T setFailIfOptionsFileError(boolean failIfOptionsFileError);

  /**
   * If true, then DB::Open / CreateColumnFamily / DropColumnFamily
   * / SetOptions will fail if options file is not detected or properly
   * persisted.
   *
   * DEFAULT: false
   *
   * @return true if we should fail if there is an error in the options file
   */
  boolean failIfOptionsFileError();

  /**
   * If true, then print malloc stats together with rocksdb.stats
   * when printing to LOG.
   *
   * DEFAULT: false
   *
   * @param dumpMallocStats true if malloc stats should be printed to LOG
   *
   * @return the reference to the current options.
   */
  T setDumpMallocStats(boolean dumpMallocStats);

  /**
   * If true, then print malloc stats together with rocksdb.stats
   * when printing to LOG.
   *
   * DEFAULT: false
   *
   * @return true if malloc stats should be printed to LOG
   */
  boolean dumpMallocStats();

  /**
   * By default RocksDB replay WAL logs and flush them on DB open, which may
   * create very small SST files. If this option is enabled, RocksDB will try
   * to avoid (but not guarantee not to) flush during recovery. Also, existing
   * WAL logs will be kept, so that if crash happened before flush, we still
   * have logs to recover from.
   *
   * DEFAULT: false
   *
   * @param avoidFlushDuringRecovery true to try to avoid (but not guarantee
   *     not to) flush during recovery
   *
   * @return the reference to the current options.
   */
  T setAvoidFlushDuringRecovery(boolean avoidFlushDuringRecovery);

  /**
   * By default RocksDB replay WAL logs and flush them on DB open, which may
   * create very small SST files. If this option is enabled, RocksDB will try
   * to avoid (but not guarantee not to) flush during recovery. Also, existing
   * WAL logs will be kept, so that if crash happened before flush, we still
   * have logs to recover from.
   *
   * DEFAULT: false
   *
   * @return true to try to avoid (but not guarantee not to) flush during
   *     recovery
   */
  boolean avoidFlushDuringRecovery();

  /**
   * Set this option to true during creation of database if you want
   * to be able to ingest behind (call IngestExternalFile() skipping keys
   * that already exist, rather than overwriting matching keys).
   * Setting this option to true will affect 2 things:
   *     1) Disable some internal optimizations around SST file compression
   *     2) Reserve bottom-most level for ingested files only.
   *     3) Note that num_levels should be &gt;= 3 if this option is turned on.
   *
   * DEFAULT: false
   *
   * @param allowIngestBehind true to allow ingest behind, false to disallow.
   *
   * @return the reference to the current options.
   */
  T setAllowIngestBehind(final boolean allowIngestBehind);

  /**
   * Returns true if ingest behind is allowed.
   * See {@link #setAllowIngestBehind(boolean)}.
   *
   * @return true if ingest behind is allowed, false otherwise.
   */
  boolean allowIngestBehind();

  /**
   * If enabled it uses two queues for writes, one for the ones with
   * disable_memtable and one for the ones that also write to memtable. This
   * allows the memtable writes not to lag behind other writes. It can be used
   * to optimize MySQL 2PC in which only the commits, which are serial, write to
   * memtable.
   *
   * DEFAULT: false
   *
   * @param twoWriteQueues true to enable two write queues, false otherwise.
   *
   * @return the reference to the current options.
   */
  T setTwoWriteQueues(final boolean twoWriteQueues);

  /**
   * Returns true if two write queues are enabled.
   *
   * @return true if two write queues are enabled, false otherwise.
   */
  boolean twoWriteQueues();

  /**
   * If true WAL is not flushed automatically after each write. Instead it
   * relies on manual invocation of FlushWAL to write the WAL buffer to its
   * file.
   *
   * DEFAULT: false
   *
   * @param manualWalFlush true to set disable automatic WAL flushing,
   *     false otherwise.
   *
   * @return the reference to the current options.
   */
  T setManualWalFlush(final boolean manualWalFlush);

  /**
   * Returns true if automatic WAL flushing is disabled.
   * See {@link #setManualWalFlush(boolean)}.
   *
   * @return true if automatic WAL flushing is disabled, false otherwise.
   */
  boolean manualWalFlush();

  /**
   * If true, RocksDB supports flushing multiple column families and committing
   * their results atomically to MANIFEST. Note that it is not
   * necessary to set atomic_flush to true if WAL is always enabled since WAL
   * allows the database to be restored to the last persistent state in WAL.
   * This option is useful when there are column families with writes NOT
   * protected by WAL.
   * For manual flush, application has to specify which column families to
   * flush atomically in {@link RocksDB#flush(FlushOptions, List)}.
   * For auto-triggered flush, RocksDB atomically flushes ALL column families.
   *
   * Currently, any WAL-enabled writes after atomic flush may be replayed
   * independently if the process crashes later and tries to recover.
   *
   * @param atomicFlush true to enable atomic flush of multiple column families.
   *
   * @return the reference to the current options.
   */
  T setAtomicFlush(final boolean atomicFlush);

  /**
   * Determine if atomic flush of multiple column families is enabled.
   *
   * See {@link #setAtomicFlush(boolean)}.
   *
   * @return true if atomic flush is enabled.
   */
  boolean atomicFlush();

  /**
   * If true, working thread may avoid doing unnecessary and long-latency
   * operation (such as deleting obsolete files directly or deleting memtable)
   * and will instead schedule a background job to do it.
   * Use it if you're latency-sensitive.
   * If set to true, takes precedence over
   * {@link ReadOptions#setBackgroundPurgeOnIteratorCleanup(boolean)}.
   *
   * @param avoidUnnecessaryBlockingIO If true, working thread may avoid doing unnecessary
   *     operation.
   * @return the reference to the current options.
   */
  T setAvoidUnnecessaryBlockingIO(final boolean avoidUnnecessaryBlockingIO);

  /**
   * If true, working thread may avoid doing unnecessary and long-latency
   * operation (such as deleting obsolete files directly or deleting memtable)
   * and will instead schedule a background job to do it.
   * Use it if you're latency-sensitive.
   * If set to true, takes precedence over
   * {@link ReadOptions#setBackgroundPurgeOnIteratorCleanup(boolean)}.
   *
   * @return true, if working thread may avoid doing unnecessary operation.
   */
  boolean avoidUnnecessaryBlockingIO();

  /**
   * If true, automatically persist stats to a hidden column family (column
   * family name: ___rocksdb_stats_history___) every
   * stats_persist_period_sec seconds; otherwise, write to an in-memory
   * struct. User can query through `GetStatsHistory` API.
   * If user attempts to create a column family with the same name on a DB
   * which have previously set persist_stats_to_disk to true, the column family
   * creation will fail, but the hidden column family will survive, as well as
   * the previously persisted statistics.
   * When peristing stats to disk, the stat name will be limited at 100 bytes.
   * Default: false
   *
   * @param persistStatsToDisk true if stats should be persisted to hidden column family.
   * @return the instance of the current object.
   */
  T setPersistStatsToDisk(final boolean persistStatsToDisk);

  /**
   * If true, automatically persist stats to a hidden column family (column
   * family name: ___rocksdb_stats_history___) every
   * stats_persist_period_sec seconds; otherwise, write to an in-memory
   * struct. User can query through `GetStatsHistory` API.
   * If user attempts to create a column family with the same name on a DB
   * which have previously set persist_stats_to_disk to true, the column family
   * creation will fail, but the hidden column family will survive, as well as
   * the previously persisted statistics.
   * When peristing stats to disk, the stat name will be limited at 100 bytes.
   * Default: false
   *
   * @return true if stats should be persisted to hidden column family.
   */
  boolean persistStatsToDisk();

  /**
   * Historically DB ID has always been stored in Identity File in DB folder.
   * If this flag is true, the DB ID is written to Manifest file in addition
   * to the Identity file. By doing this 2 problems are solved
   * 1. We don't checksum the Identity file where as Manifest file is.
   * 2. Since the source of truth for DB is Manifest file DB ID will sit with
   *    the source of truth. Previously the Identity file could be copied
   *    independent of Manifest and that can result in wrong DB ID.
   * We recommend setting this flag to true.
   * Default: false
   *
   * @param writeDbidToManifest if true, then DB ID will be written to Manifest file.
   * @return the instance of the current object.
   */
  T setWriteDbidToManifest(final boolean writeDbidToManifest);

  /**
   * Historically DB ID has always been stored in Identity File in DB folder.
   * If this flag is true, the DB ID is written to Manifest file in addition
   * to the Identity file. By doing this 2 problems are solved
   * 1. We don't checksum the Identity file where as Manifest file is.
   * 2. Since the source of truth for DB is Manifest file DB ID will sit with
   *    the source of truth. Previously the Identity file could be copied
   *    independent of Manifest and that can result in wrong DB ID.
   * We recommend setting this flag to true.
   * Default: false
   *
   * @return true, if DB ID will be written to Manifest file.
   */
  boolean writeDbidToManifest();

  /**
   * Commits each durable OPTIONS file number in the MANIFEST for this writable
   * open. If false, writes use legacy highest-numbered-OPTIONS behavior even
   * when the MANIFEST contains an older committed number.
   *
   * @param track whether this writable open should commit OPTIONS file numbers
   * @return the instance of the current object
   */
  T setTrackOptionsFileNumberInManifest(final boolean track);

  /**
   * @return whether this open commits OPTIONS file numbers in MANIFEST
   */
  boolean trackOptionsFileNumberInManifest();

  /**
   * The number of bytes to prefetch when reading the log. This is mostly useful
   * for reading a remotely located log, as it can save the number of
   * round-trips. If 0, then the prefetching is disabled.
   *
   * Default: 0
   *
   * @param logReadaheadSize the number of bytes to prefetch when reading the log.
   * @return the instance of the current object.
   */
  T setLogReadaheadSize(final long logReadaheadSize);

  /**
   * The number of bytes to prefetch when reading the log. This is mostly useful
   * for reading a remotely located log, as it can save the number of
   * round-trips. If 0, then the prefetching is disabled.
   *
   * Default: 0
   *
   * @return the number of bytes to prefetch when reading the log.
   */
  long logReadaheadSize();

  /**
   * By default, RocksDB recovery fails if any table file referenced in
   * MANIFEST are missing after scanning the MANIFEST.
   * Best-efforts recovery is another recovery mode that
   * tries to restore the database to the most recent point in time without
   * missing file.
   * Currently not compatible with atomic flush. Furthermore, WAL files will
   * not be used for recovery if best_efforts_recovery is true.
   * Default: false
   *
   * @param bestEffortsRecovery if true, RocksDB will use best-efforts mode when recovering.
   * @return the instance of the current object.
   */
  T setBestEffortsRecovery(final boolean bestEffortsRecovery);

  /**
   * By default, RocksDB recovery fails if any table file referenced in
   * MANIFEST are missing after scanning the MANIFEST.
   * Best-efforts recovery is another recovery mode that
   * tries to restore the database to the most recent point in time without
   * missing file.
   * Currently not compatible with atomic flush. Furthermore, WAL files will
   * not be used for recovery if best_efforts_recovery is true.
   * Default: false
   *
   * @return true, if RocksDB uses best-efforts mode when recovering.
   */
  boolean bestEffortsRecovery();

  /**
   * It defines how many times db resume is called by a separate thread when
   * background retryable IO Error happens. When background retryable IO
   * Error happens, SetBGError is called to deal with the error. If the error
   * can be auto-recovered (e.g., retryable IO Error during Flush or WAL write),
   * then db resume is called in background to recover from the error. If this
   * value is 0 or negative, db resume will not be called.
   *
   * Default: INT_MAX
   *
   * @param maxBgerrorResumeCount maximum number of times db resume should be called when IO Error
   *     happens.
   * @return the instance of the current object.
   */
  T setMaxBgErrorResumeCount(final int maxBgerrorResumeCount);

  /**
   * It defines how many times db resume is called by a separate thread when
   * background retryable IO Error happens. When background retryable IO
   * Error happens, SetBGError is called to deal with the error. If the error
   * can be auto-recovered (e.g., retryable IO Error during Flush or WAL write),
   * then db resume is called in background to recover from the error. If this
   * value is 0 or negative, db resume will not be called.
   *
   * Default: INT_MAX
   *
   * @return maximum number of times db resume should be called when IO Error happens.
   */
  int maxBgerrorResumeCount();

  /**
   * If max_bgerror_resume_count is &ge; 2, db resume is called multiple times.
   * This option decides how long to wait to retry the next resume if the
   * previous resume fails and satisfy redo resume conditions.
   *
   * Default: 1000000 (microseconds).
   *
   * @param bgerrorResumeRetryInterval how many microseconds to wait between DB resume attempts.
   * @return the instance of the current object.
   */
  T setBgerrorResumeRetryInterval(final long bgerrorResumeRetryInterval);

  /**
   * If max_bgerror_resume_count is &ge; 2, db resume is called multiple times.
   * This option decides how long to wait to retry the next resume if the
   * previous resume fails and satisfy redo resume conditions.
   *
   * Default: 1000000 (microseconds).
   *
   * @return the instance of the current object.
   */
  long bgerrorResumeRetryInterval();

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
