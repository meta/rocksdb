// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Advanced Column Family Options which are mutable
 * <p>
 * Taken from include/rocksdb/advanced_options.h
 * and MutableCFOptions in util/cf_options.h
 */
public interface AdvancedMutableColumnFamilyOptionsInterface<
    T extends AdvancedMutableColumnFamilyOptionsInterface<T>> {
  /**
   * The maximum number of write buffers that are built up in memory.
   * The default is 2, so that when 1 write buffer is being flushed to
   * storage, new writes can continue to the other write buffer.
   * Default: 2
   *
   * @param maxWriteBufferNumber maximum number of write buffers.
   * @return the instance of the current options.
   */
  T setMaxWriteBufferNumber(
      int maxWriteBufferNumber);

  /**
   * Returns maximum number of write buffers.
   *
   * @return maximum number of write buffers.
   * @see #setMaxWriteBufferNumber(int)
   */
  int maxWriteBufferNumber();

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @param inplaceUpdateNumLocks the number of locks used for
   *     inplace updates.
   * @return the reference to the current options.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *     while overflowing the underlying platform specific value.
   */
  T setInplaceUpdateNumLocks(
      long inplaceUpdateNumLocks);

  /**
   * Number of locks used for inplace update
   * Default: 10000, if inplace_update_support = true, else 0.
   *
   * @return the number of locks used for inplace update.
   */
  long inplaceUpdateNumLocks();

  /**
   * if prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
   * create prefix bloom for memtable with the size of
   * write_buffer_size * memtable_prefix_bloom_size_ratio.
   * If it is larger than 0.25, it is sanitized to 0.25.
   * <p>
   * Default: 0 (disabled)
   *
   * @param memtablePrefixBloomSizeRatio the ratio of memtable used by the
   *     bloom filter, 0 means no bloom filter
   * @return the reference to the current options.
   */
  T setMemtablePrefixBloomSizeRatio(
      double memtablePrefixBloomSizeRatio);

  /**
   * if prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
   * create prefix bloom for memtable with the size of
   * write_buffer_size * memtable_prefix_bloom_size_ratio.
   * If it is larger than 0.25, it is sanitized to 0.25.
   * <p>
   * Default: 0 (disabled)
   *
   * @return the ratio of memtable used by the bloom filter
   */
  double memtablePrefixBloomSizeRatio();

  /**
   * Threshold used in the MemPurge (memtable garbage collection)
   * feature. A value of 0.0 corresponds to no MemPurge,
   * a value of 1.0 will trigger a MemPurge as often as possible.
   * <p>
   * Default: 0.0 (disabled)
   *
   * @param experimentalMempurgeThreshold the threshold used by
   *     the MemPurge decider.
   * @return the reference to the current options.
   */
  T setExperimentalMempurgeThreshold(double experimentalMempurgeThreshold);

  /**
   * Threshold used in the MemPurge (memtable garbage collection)
   * feature. A value of 0.0 corresponds to no MemPurge,
   * a value of 1.0 will trigger a MemPurge as often as possible.
   * <p>
   * Default: 0 (disabled)
   *
   * @return the threshold used by the MemPurge decider
   */
  double experimentalMempurgeThreshold();

  /**
   * During forward or reverse iteration, when this many or more
   * strictly contiguous point tombstones are encountered with no live
   * keys between them, a range tombstone is inserted into the current
   * mutable memtable.
   *
   * Set to 0 to disable.
   * Default: 0 (disabled)
   *
   * @param minTombstonesForRangeConversion the minimum number of contiguous
   *     point tombstones to trigger range conversion
   * @return the reference to the current options.
   */
  T setMinTombstonesForRangeConversion(int minTombstonesForRangeConversion);

  /**
   * Gets the current setting for minimum tombstones for range conversion.
   * 0 (default) indicates that the feature is disabled.
   *
   * @return current value of min_tombstones_for_range_conversion
   */
  int minTombstonesForRangeConversion();

  /**
   * Enable whole key bloom filter in memtable. Note this will only take effect
   * if memtable_prefix_bloom_size_ratio is not 0. Enabling whole key filtering
   * can potentially reduce CPU usage for point-look-ups.
   * <p>
   * Default: false (disabled)
   *
   * @param memtableWholeKeyFiltering true if whole key bloom filter is enabled
   *     in memtable
   * @return the reference to the current options.
   */
  T setMemtableWholeKeyFiltering(boolean memtableWholeKeyFiltering);

  /**
   * Returns whether whole key bloom filter is enabled in memtable
   *
   * @return true if whole key bloom filter is enabled in memtable
   */
  boolean memtableWholeKeyFiltering();

  /**
   * Page size for huge page TLB for bloom in memtable. If &le; 0, not allocate
   * from huge page TLB but from malloc.
   * Need to reserve huge pages for it to be allocated. For example:
   *     sysctl -w vm.nr_hugepages=20
   * See linux doc Documentation/vm/hugetlbpage.txt
   *
   * @param memtableHugePageSize The page size of the huge
   *     page tlb
   * @return the reference to the current options.
   */
  T setMemtableHugePageSize(
      long memtableHugePageSize);

  /**
   * Page size for huge page TLB for bloom in memtable. If &le; 0, not allocate
   * from huge page TLB but from malloc.
   * Need to reserve huge pages for it to be allocated. For example:
   *     sysctl -w vm.nr_hugepages=20
   * See linux doc Documentation/vm/hugetlbpage.txt
   *
   * @return The page size of the huge page tlb
   */
  long memtableHugePageSize();

  /**
   * The size of one block in arena memory allocation.
   * If &le; 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   * <p>
   * There are two additional restriction of the specified size:
   * (1) size should be in the range of [4096, 2 &lt;&lt; 30] and
   * (2) be the multiple of the CPU word (which helps with the memory
   * alignment).
   * <p>
   * We'll automatically check and adjust the size number to make sure it
   * conforms to the restrictions.
   * Default: 0
   *
   * @param arenaBlockSize the size of an arena block
   * @return the reference to the current options.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  T setArenaBlockSize(long arenaBlockSize);

  /**
   * The size of one block in arena memory allocation.
   * If &le; 0, a proper value is automatically calculated (usually 1/10 of
   * writer_buffer_size).
   * <p>
   * There are two additional restriction of the specified size:
   * (1) size should be in the range of [4096, 2 &lt;&lt; 30] and
   * (2) be the multiple of the CPU word (which helps with the memory
   * alignment).
   * <p>
   * We'll automatically check and adjust the size number to make sure it
   * conforms to the restrictions.
   * Default: 0
   *
   * @return the size of an arena block
   */
  long arenaBlockSize();

  /**
   * Soft limit on number of level-0 files. We start slowing down writes at this
   * point. A value &lt; 0 means that no writing slow down will be triggered by
   * number of files in level-0.
   *
   * @param level0SlowdownWritesTrigger The soft limit on the number of
   *   level-0 files
   * @return the reference to the current options.
   */
  T setLevel0SlowdownWritesTrigger(
      int level0SlowdownWritesTrigger);

  /**
   * Soft limit on number of level-0 files. We start slowing down writes at this
   * point. A value &lt; 0 means that no writing slow down will be triggered by
   * number of files in level-0.
   *
   * @return The soft limit on the number of
   *   level-0 files
   */
  int level0SlowdownWritesTrigger();

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @param level0StopWritesTrigger The maximum number of level-0 files
   * @return the reference to the current options.
   */
  T setLevel0StopWritesTrigger(
      int level0StopWritesTrigger);

  /**
   * Maximum number of level-0 files.  We stop writes at this point.
   *
   * @return The maximum number of level-0 files
   */
  int level0StopWritesTrigger();

  /**
   * The target file size for compaction.
   * This targetFileSizeBase determines a level-1 file size.
   * Target file size for level L can be calculated by
   * targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1))
   * For example, if targetFileSizeBase is 2MB and
   * target_file_size_multiplier is 10, then each file on level-1 will
   * be 2MB, and each file on level 2 will be 20MB,
   * and each file on level-3 will be 200MB.
   * by default targetFileSizeBase is 64MB.
   *
   * @param targetFileSizeBase the target size of a level-0 file.
   * @return the reference to the current options.
   *
   * @see #setTargetFileSizeMultiplier(int)
   */
  T setTargetFileSizeBase(
      long targetFileSizeBase);

  /**
   * The target file size for compaction.
   * This targetFileSizeBase determines a level-1 file size.
   * Target file size for level L can be calculated by
   * targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1))
   * For example, if targetFileSizeBase is 2MB and
   * target_file_size_multiplier is 10, then each file on level-1 will
   * be 2MB, and each file on level 2 will be 20MB,
   * and each file on level-3 will be 200MB.
   * by default targetFileSizeBase is 64MB.
   *
   * @return the target size of a level-0 file.
   *
   * @see #targetFileSizeMultiplier()
   */
  long targetFileSizeBase();

  /**
   * targetFileSizeMultiplier defines the size ratio between a
   * level-L file and level-(L+1) file.
   * By default target_file_size_multiplier is 1, meaning
   * files in different levels have the same target.
   *
   * @param multiplier the size ratio between a level-(L+1) file
   *     and level-L file.
   * @return the reference to the current options.
   */
  T setTargetFileSizeMultiplier(
      int multiplier);

  /**
   * targetFileSizeMultiplier defines the size ratio between a
   * level-(L+1) file and level-L file.
   * By default targetFileSizeMultiplier is 1, meaning
   * files in different levels have the same target.
   *
   * @return the size ratio between a level-(L+1) file and level-L file.
   */
  int targetFileSizeMultiplier();

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @param multiplier the ratio between the total size of level-(L+1)
   *     files and the total size of level-L files for all L.
   * @return the reference to the current options.
   * <p>
   * See {@link MutableColumnFamilyOptionsInterface#setMaxBytesForLevelBase(long)}
   */
  T setMaxBytesForLevelMultiplier(double multiplier);

  /**
   * The ratio between the total size of level-(L+1) files and the total
   * size of level-L files for all L.
   * DEFAULT: 10
   *
   * @return the ratio between the total size of level-(L+1) files and
   *     the total size of level-L files for all L.
   * <p>
   * See {@link MutableColumnFamilyOptionsInterface#maxBytesForLevelBase()}
   */
  double maxBytesForLevelMultiplier();

  /**
   * Different max-size multipliers for different levels.
   * These are multiplied by max_bytes_for_level_multiplier to arrive
   * at the max-size of each level.
   * <p>
   * Default: 1
   *
   * @param maxBytesForLevelMultiplierAdditional The max-size multipliers
   *   for each level
   * @return the reference to the current options.
   */
  T setMaxBytesForLevelMultiplierAdditional(
      int[] maxBytesForLevelMultiplierAdditional);

  /**
   * Different max-size multipliers for different levels.
   * These are multiplied by max_bytes_for_level_multiplier to arrive
   * at the max-size of each level.
   * <p>
   * Default: 1
   *
   * @return The max-size multipliers for each level
   */
  int[] maxBytesForLevelMultiplierAdditional();

  /**
   * All writes will be slowed down to at least delayed_write_rate if estimated
   * bytes needed to be compaction exceed this threshold.
   * <p>
   * Default: 64GB
   *
   * @param softPendingCompactionBytesLimit The soft limit to impose on
   *   compaction
   * @return the reference to the current options.
   */
  T setSoftPendingCompactionBytesLimit(
      long softPendingCompactionBytesLimit);

  /**
   * All writes will be slowed down to at least delayed_write_rate if estimated
   * bytes needed to be compaction exceed this threshold.
   * <p>
   * Default: 64GB
   *
   * @return The soft limit to impose on compaction
   */
  long softPendingCompactionBytesLimit();

  /**
   * All writes are stopped if estimated bytes needed to be compaction exceed
   * this threshold.
   * <p>
   * Default: 256GB
   *
   * @param hardPendingCompactionBytesLimit The hard limit to impose on
   *   compaction
   * @return the reference to the current options.
   */
  T setHardPendingCompactionBytesLimit(
      long hardPendingCompactionBytesLimit);

  /**
   * All writes are stopped if estimated bytes needed to be compaction exceed
   * this threshold.
   * <p>
   * Default: 256GB
   *
   * @return The hard limit to impose on compaction
   */
  long hardPendingCompactionBytesLimit();

  /**
   * An iteration-&gt;Next() sequentially skips over keys with the same
   * user-key unless this option is set. This number specifies the number
   * of keys (with the same userkey) that will be sequentially
   * skipped before a reseek is issued.
   * Default: 8
   *
   * @param maxSequentialSkipInIterations the number of keys could
   *     be skipped in an iteration.
   * @return the reference to the current options.
   */
  T setMaxSequentialSkipInIterations(
      long maxSequentialSkipInIterations);

  /**
   * An iteration-&gt;Next() sequentially skips over keys with the same
   * user-key unless this option is set. This number specifies the number
   * of keys (with the same userkey) that will be sequentially
   * skipped before a reseek is issued.
   * Default: 8
   *
   * @return the number of keys could be skipped in an iteration.
   */
  long maxSequentialSkipInIterations();

  /**
   * Maximum number of successive merge operations on a key in the memtable.
   * <p>
   * When a merge operation is added to the memtable and the maximum number of
   * successive merges is reached, the value of the key will be calculated and
   * inserted into the memtable instead of the merge operation. This will
   * ensure that there are never more than max_successive_merges merge
   * operations in the memtable.
   * <p>
   * Default: 0 (disabled)
   *
   * @param maxSuccessiveMerges the maximum number of successive merges.
   * @return the reference to the current options.
   * @throws java.lang.IllegalArgumentException thrown on 32-Bit platforms
   *   while overflowing the underlying platform specific value.
   */
  T setMaxSuccessiveMerges(
      long maxSuccessiveMerges);

  /**
   * Maximum number of successive merge operations on a key in the memtable.
   * <p>
   * When a merge operation is added to the memtable and the maximum number of
   * successive merges is reached, the value of the key will be calculated and
   * inserted into the memtable instead of the merge operation. This will
   * ensure that there are never more than max_successive_merges merge
   * operations in the memtable.
   * <p>
   * Default: 0 (disabled)
   *
   * @return the maximum number of successive merges.
   */
  long maxSuccessiveMerges();

  /**
   * After writing every SST file, reopen it and read all the keys.
   * <p>
   * Default: false
   *
   * @param paranoidFileChecks true to enable paranoid file checks
   * @return the reference to the current options.
   */
  T setParanoidFileChecks(
      boolean paranoidFileChecks);

  /**
   * After writing every SST file, reopen it and read all the keys.
   * <p>
   * Default: false
   *
   * @return true if paranoid file checks are enabled
   */
  boolean paranoidFileChecks();

  /**
   * Measure IO stats in compactions and flushes, if true.
   * <p>
   * Default: false
   *
   * @param reportBgIoStats true to enable reporting
   * @return the reference to the current options.
   */
  T setReportBgIoStats(
      boolean reportBgIoStats);

  /**
   * Determine whether IO stats in compactions and flushes are being measured
   *
   * @return true if reporting is enabled
   */
  boolean reportBgIoStats();

  /**
   * Non-bottom-level files older than TTL will go through the compaction
   * process. This needs {@link MutableDBOptionsInterface#maxOpenFiles()} to be
   * set to -1.
   * <p>
   * Enabled only for level compaction for now.
   * <p>
   * Default: 0 (disabled)
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param ttl the time-to-live.
   *
   * @return the reference to the current options.
   */
  T setTtl(final long ttl);

  /**
   * Get the TTL for Non-bottom-level files that will go through the compaction
   * process.
   * <p>
   * See {@link #setTtl(long)}.
   *
   * @return the time-to-live.
   */
  long ttl();

  /**
   * Files older than this value will be picked up for compaction, and
   * re-written to the same level as they were before.
   * One main use of the feature is to make sure a file goes through compaction
   * filters periodically. Users can also use the feature to clear up SST
   * files using old format.
   * <p>
   * A file's age is computed by looking at file_creation_time or
   * oldest_ancestor_time table properties in order, if they have valid
   * non-zero values; if not, the age is based on the file's last modified time
   * (given by the underlying Env).
   * <p>
   * Supported in Level and FIFO compaction.
   * In FIFO compaction, this option has the same meaning as TTL and whichever
   * stricter will be used.
   * Pre-req: max_open_file == -1.
   * unit: seconds. Ex: 7 days = 7 * 24 * 60 * 60
   * <p>
   * Values:
   * 0: Turn off Periodic compactions.
   * UINT64_MAX - 1 (i.e 0xfffffffffffffffe): Let RocksDB control this feature
   *     as needed. For now, RocksDB will change this value to 30 days
   *     (i.e 30 * 24 * 60 * 60) so that every file goes through the compaction
   *     process at least once every 30 days if not compacted sooner.
   *     In FIFO compaction, since the option has the same meaning as ttl,
   *     when this value is left default, and ttl is left to 0, 30 days will be
   *     used. Otherwise, min(ttl, periodic_compaction_seconds) will be used.
   * <p>
   * Default: 0xfffffffffffffffe (allow RocksDB to auto-tune)
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param periodicCompactionSeconds the periodic compaction in seconds.
   *
   * @return the reference to the current options.
   */
  T setPeriodicCompactionSeconds(final long periodicCompactionSeconds);

  /**
   * Get the periodicCompactionSeconds.
   * <p>
   * See {@link #setPeriodicCompactionSeconds(long)}.
   *
   * @return the periodic compaction in seconds.
   */
  long periodicCompactionSeconds();

  //
  // BEGIN options for blobs (integrated BlobDB)
  //

  /**
   * When set, large values (blobs) are written to separate blob files, and only
   * pointers to them are stored in SST files. This can reduce write amplification
   * for large-value use cases at the cost of introducing a level of indirection
   * for reads. See also the options min_blob_size, blob_file_size,
   * blob_compression_type, enable_blob_garbage_collection, and
   * blob_garbage_collection_age_cutoff below.
   * <p>
   * Default: false
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param enableBlobFiles true iff blob files should be enabled
   *
   * @return the reference to the current options.
   */
  T setEnableBlobFiles(final boolean enableBlobFiles);

  /**
   * When set, large values (blobs) are written to separate blob files, and only
   * pointers to them are stored in SST files. This can reduce write amplification
   * for large-value use cases at the cost of introducing a level of indirection
   * for reads. See also the options min_blob_size, blob_file_size,
   * blob_compression_type, enable_blob_garbage_collection, and
   * blob_garbage_collection_age_cutoff below.
   * <p>
   * Default: false
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @return true if blob files are enabled
   */
  boolean enableBlobFiles();

  /**
   * Set the size of the smallest value to be stored separately in a blob file. Values
   * which have an uncompressed size smaller than this threshold are stored
   * alongside the keys in SST files in the usual fashion. A value of zero for
   * this option means that all values are stored in blob files. Note that
   * enable_blob_files has to be set in order for this option to have any effect.
   * <p>
   * Default: 0
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param minBlobSize the size of the smallest value to be stored separately in a blob file
   * @return the reference to the current options.
   */
  T setMinBlobSize(final long minBlobSize);

  /**
   * Get the size of the smallest value to be stored separately in a blob file. Values
   * which have an uncompressed size smaller than this threshold are stored
   * alongside the keys in SST files in the usual fashion. A value of zero for
   * this option means that all values are stored in blob files. Note that
   * enable_blob_files has to be set in order for this option to have any effect.
   * <p>
   * Default: 0
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @return the current minimum size of value which is stored separately in a blob
   */
  long minBlobSize();

  /**
   * Set the size limit for blob files. When writing blob files, a new file is opened
   * once this limit is reached. Note that enable_blob_files has to be set in
   * order for this option to have any effect.
   * <p>
   * Default: 256 MB
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param blobFileSize the size limit for blob files
   *
   * @return the reference to the current options.
   */
  T setBlobFileSize(final long blobFileSize);

  /**
   * The size limit for blob files. When writing blob files, a new file is opened
   * once this limit is reached.
   *
   * @return the current size limit for blob files
   */
  long blobFileSize();

  /**
   * Set the compression algorithm to use for large values stored in blob files. Note
   * that enable_blob_files has to be set in order for this option to have any
   * effect.
   * <p>
   * Default: no compression
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param compressionType the compression algorithm to use.
   *
   * @return the reference to the current options.
   */
  T setBlobCompressionType(CompressionType compressionType);

  /**
   * Get the compression algorithm in use for large values stored in blob files.
   * Note that enable_blob_files has to be set in order for this option to have any
   * effect.
   *
   * @return the current compression algorithm
   */
  CompressionType blobCompressionType();

  /**
   * Enable/disable garbage collection of blobs. Blob GC is performed as part of
   * compaction. Valid blobs residing in blob files older than a cutoff get
   * relocated to new files as they are encountered during compaction, which makes
   * it possible to clean up blob files once they contain nothing but
   * obsolete/garbage blobs. See also blob_garbage_collection_age_cutoff below.
   * <p>
   * Default: false
   *
   * @param enableBlobGarbageCollection the new enabled/disabled state of blob garbage collection
   *
   * @return the reference to the current options.
   */
  T setEnableBlobGarbageCollection(final boolean enableBlobGarbageCollection);

  /**
   * Query whether garbage collection of blobs is enabled.Blob GC is performed as part of
   * compaction. Valid blobs residing in blob files older than a cutoff get
   * relocated to new files as they are encountered during compaction, which makes
   * it possible to clean up blob files once they contain nothing but
   * obsolete/garbage blobs. See also blob_garbage_collection_age_cutoff below.
   * <p>
   * Default: false
   *
   * @return true if blob garbage collection is currently enabled.
   */
  boolean enableBlobGarbageCollection();

  /**
   * Set cutoff in terms of blob file age for garbage collection. Blobs in the
   * oldest N blob files will be relocated when encountered during compaction,
   * where N = garbage_collection_cutoff * number_of_blob_files. Note that
   * enable_blob_garbage_collection has to be set in order for this option to have
   * any effect.
   * <p>
   * Default: 0.25
   *
   * @param blobGarbageCollectionAgeCutoff the new age cutoff
   *
   * @return the reference to the current options.
   */
  T setBlobGarbageCollectionAgeCutoff(double blobGarbageCollectionAgeCutoff);
  /**
   * Get cutoff in terms of blob file age for garbage collection. Blobs in the
   * oldest N blob files will be relocated when encountered during compaction,
   * where N = garbage_collection_cutoff * number_of_blob_files. Note that
   * enable_blob_garbage_collection has to be set in order for this option to have
   * any effect.
   * <p>
   * Default: 0.25
   *
   * @return the current age cutoff for garbage collection
   */
  double blobGarbageCollectionAgeCutoff();

  /**
   * If the ratio of garbage in the blob files currently eligible for garbage
   * collection exceeds this threshold, targeted compactions are scheduled in
   * order to force garbage collecting the oldest blob files. This option is
   * currently only supported with leveled compactions.
   * <p>
   *  Note that {@link #enableBlobGarbageCollection} has to be set in order for this
   *  option to have any effect.
   * <p>
   *  Default: 1.0
   * <p>
   * Dynamically changeable through the SetOptions() API
   *
   * @param blobGarbageCollectionForceThreshold new value for the threshold
   * @return the reference to the current options
   */
  T setBlobGarbageCollectionForceThreshold(double blobGarbageCollectionForceThreshold);

  /**
   * Get the current value for the {@code #blobGarbageCollectionForceThreshold}
   * @return the current threshold at which garbage collection of blobs is forced
   */
  double blobGarbageCollectionForceThreshold();

  /**
   * Set threshold for read-triggered compaction. An SST file is marked for
   * compaction when its sampled read frequency (sampled_reads / file_size)
   * exceeds this value. Use 0.0 to disable.
   *
   * @param readTriggeredCompactionThreshold the threshold value
   * @return the reference to the current options
   */
  T setReadTriggeredCompactionThreshold(double readTriggeredCompactionThreshold);

  /**
   * Get the current value for read-triggered compaction threshold.
   *
   * @return the current threshold
   */
  double readTriggeredCompactionThreshold();

  /**
   * Set compaction readahead for blob files.
   * <p>
   * Default: 0
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param blobCompactionReadaheadSize the compaction readahead for blob files
   *
   * @return the reference to the current options.
   */
  T setBlobCompactionReadaheadSize(final long blobCompactionReadaheadSize);

  /**
   * Get compaction readahead for blob files.
   *
   * @return the current compaction readahead for blob files
   */
  long blobCompactionReadaheadSize();

  /**
   * Set a certain LSM tree level to enable blob files.
   * <p>
   * Default: 0
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param blobFileStartingLevel the starting level to enable blob files
   *
   * @return the reference to the current options.
   */
  T setBlobFileStartingLevel(final int blobFileStartingLevel);

  /**
   * Get the starting LSM tree level to enable blob files.
   * <p>
   * Default: 0
   *
   * @return the current LSM tree level to enable blob files.
   */
  int blobFileStartingLevel();

  /**
   * Set the number of bytes per key-value checksum for memtables.
   * Enables per-KV checksum protection in memtables to detect silent data corruption
   * <p>
   * Default: 0 (disabled)
   * <p>
   * Supported values: 0, 1, 2, 4, 8 - 8 bytes per key
   *
   * @param memtableProtectionBytesPerKey number of checksum bytes per key-value
   *
   * @return the reference to the current options
   */
  T setMemtableProtectionBytesPerKey(final int memtableProtectionBytesPerKey);

  /**
   * Get the number of bytes per key-value checksum for memtables
   * <p>
   * Default: 0
   *
   * @return the number of checksum bytes per key-value
   */
  int memtableProtectionBytesPerKey();

  /**
   * Set a certain prepopulate blob cache option.
   * <p>
   * Default: 0
   * <p>
   * Dynamically changeable through
   * {@link RocksDB#setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}.
   *
   * @param prepopulateBlobCache prepopulate the blob cache option
   *
   * @return the reference to the current options.
   */
  T setPrepopulateBlobCache(final PrepopulateBlobCache prepopulateBlobCache);

  /**
   * Get the prepopulate blob cache option.
   * <p>
   * Default: 0
   *
   * @return the current prepopulate blob cache option.
   */
  PrepopulateBlobCache prepopulateBlobCache();

  //
  // END options for blobs (integrated BlobDB)
  //
}
