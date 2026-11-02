//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class PerfContext extends RocksObject {
  protected PerfContext(final long nativeHandle) {
    super(nativeHandle);
  }

  public void reset() {
    reset(nativeHandle_);
  }

  /**
   * @return total number of user key comparisons
   */
  public long getUserKeyComparisonCount() {
    return getUserKeyComparisonCount(nativeHandle_);
  }

  /**
   * @return total number of block cache hits
   */
  public long getBlockCacheHitCount() {
    return getBlockCacheHitCount(nativeHandle_);
  }

  /**
   * @return total number of block reads (with IO)
   */
  public long getBlockReadCount() {
    return getBlockReadCount(nativeHandle_);
  }

  /**
   * @return total number of bytes from block reads
   */
  public long getBlockReadByte() {
    return getBlockReadByte(nativeHandle_);
  }

  /*
      @return total nanos spent on block reads
   */
  public long getBlockReadTime() {
    return getBlockReadTime(nativeHandle_);
  }

  /**
   * @return total cpu time in nanos spent on block reads
   */
  public long getBlockReadCpuTime() {
    return getBlockReadCpuTime(nativeHandle_);
  }

  /**
   * @return total number of index block hits
   */
  public long getBlockCacheIndexHitCount() {
    return getBlockCacheIndexHitCount(nativeHandle_);
  }

  /**
   * @return total number of standalone handles lookup from secondary cache
   */
  public long getBlockCacheStandaloneHandleCount() {
    return getBlockCacheStandaloneHandleCount(nativeHandle_);
  }

  /**
   * @return total number of real handles lookup from secondary cache that are inserted  into
   *     primary cache
   */
  public long getBlockCacheRealHandleCount() {
    return getBlockCacheRealHandleCount(nativeHandle_);
  }

  /**
   * @return total number of index block reads
   */
  public long getIndexBlockReadCount() {
    return getIndexBlockReadCount(nativeHandle_);
  }

  /**
   * @return total number of filter block hits
   */
  public long getBlockCacheFilterHitCount() {
    return getBlockCacheFilterHitCount(nativeHandle_);
  }

  /**
   * @return total number of filter block reads
   */
  public long getFilterBlockReadCount() {
    return getFilterBlockReadCount(nativeHandle_);
  }

  /**
   * @return total number of compression dictionary block reads
   */
  public long getCompressionDictBlockReadCount() {
    return getCompressionDictBlockReadCount(nativeHandle_);
  }

  /**
   * @return total number of secondary cache hits
   */
  public long getSecondaryCacheHitCount() {
    return getSecondaryCacheHitCount(nativeHandle_);
  }

  /**
   * @return total number of real handles inserted into secondary cache
   */
  public long getCompressedSecCacheInsertRealCount() {
    return getCompressedSecCacheInsertRealCount(nativeHandle_);
  }

  /**
   * @return total number of dummy handles inserted into secondary cache
   */
  public long getCompressedSecCacheInsertDummyCount() {
    return getCompressedSecCacheInsertDummyCount(nativeHandle_);
  }

  /**
   * @return bytes for vals before compression in secondary cache
   */
  public long getCompressedSecCacheUncompressedBytes() {
    return getCompressedSecCacheUncompressedBytes(nativeHandle_);
  }

  /**
   * @return bytes for vals after compression in secondary cache
   */
  public long getCompressedSecCacheCompressedBytes() {
    return getCompressedSecCacheCompressedBytes(nativeHandle_);
  }

  /**
   * @return total nanos spent on block checksum
   */
  public long getBlockChecksumTime() {
    return getBlockChecksumTime(nativeHandle_);
  }

  /**
   *
   * @return total nanos spent on block decompression
   */
  public long getBlockDecompressTime() {
    return getBlockDecompressTime(nativeHandle_);
  }

  /**
   * @return bytes for vals returned by Get
   */
  public long getReadBytes() {
    return getReadBytes(nativeHandle_);
  }

  /**
   * @return bytes for vals returned by MultiGet
   */
  public long getMultigetReadBytes() {
    return getMultigetReadBytes(nativeHandle_);
  }

  /**
   * @return bytes for keys/vals decoded by iterator
   */
  public long getIterReadBytes() {
    return getIterReadBytes(nativeHandle_);
  }

  /**
   * @return total number of blob cache hits
   */
  public long getBlobCacheHitCount() {
    return getBlobCacheHitCount(nativeHandle_);
  }

  /**
   * @return total number of bytes read from blob cache
   */
  public long getBlobCacheReadByte() {
    return getBlobCacheReadByte(nativeHandle_);
  }

  /**
   * @return total number of blob reads (with IO)
   */
  public long getBlobReadCount() {
    return getBlobReadCount(nativeHandle_);
  }

  /**
   * @return total number of bytes from blob reads
   */
  public long getBlobReadByte() {
    return getBlobReadByte(nativeHandle_);
  }

  /**
   * @return total nanos spent on blob reads
   */
  public long getBlobReadTime() {
    return getBlobReadTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on blob checksum
   */
  public long getBlobChecksumTime() {
    return getBlobChecksumTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on blob decompression
   */
  public long getBlobDecompressTime() {
    return getBlobDecompressTime(nativeHandle_);
  }

  /**
   * total number of internal keys skipped over during iteration.
   * There are several reasons for it:
   * 1. when calling Next(), the iterator is in the position of the previous
   *    key, so that we'll need to skip it. It means this counter will always
   *    be incremented in Next().
   * 2. when calling Next(), we need to skip internal entries for the previous
   *    keys that are overwritten.
   * 3. when calling Next(), Seek() or SeekToFirst(), after previous key
   *    before calling Next(), the seek key in Seek() or the beginning for
   *    SeekToFirst(), there may be one or more deleted keys before the next
   *    valid key that the operation should place the iterator to. We need
   *    to skip both of the tombstone and updates hidden by the tombstones. The
   *    tombstones are not included in this counter, while previous updates
   *    hidden by the tombstones will be included here.
   * 4. symmetric cases for Prev() and SeekToLast()
   * internal_recent_skipped_count is not included in this counter.
   *
   * @return total number of internal keys skipped over during iteration
   */
  public long getInternalKeySkippedCount() {
    return getInternalKeySkippedCount(nativeHandle_);
  }

  /**
   * Total number of deletes and single deletes skipped over during iteration
   * When calling Next(), Seek() or SeekToFirst(), after previous position
   * before calling Next(), the seek key in Seek() or the beginning for
   * SeekToFirst(), there may be one or more deleted keys before the next valid
   * key. Every deleted key is counted once. We don't recount here if there are
   * still older updates invalidated by the tombstones.
   *
   * @return Total number of deletes and single deletes skipped over during iteration
   */
  public long getInternalDeleteSkippedCount() {
    return getInternalDeleteSkippedCount(nativeHandle_);
  }

  /**
   * How many times iterators skipped over internal keys that are more recent
   * than the snapshot that iterator is using.
   *
   * @return number of skipped recent internal keys
   */
  public long getInternalRecentSkippedCount() {
    return getInternalRecentSkippedCount(nativeHandle_);
  }

  /**
   * How many merge operands were fed into the merge operator by iterators.
   * Note: base values are not included in the count.
   *
   * @return How many merge operands were fed into the merge operator by iterator
   */
  public long getInternalMergeCount() {
    return getInternalMergeCount(nativeHandle_);
  }

  /**
   * How many merge operands were fed into the merge operator by point lookups.
   * Note: base values are not included in the count.
   *
   * @return How many merge operands were fed into the merge operator by point lookups
   */
  public long getInternalMergePointLookupCount() {
    return getInternalMergePointLookupCount(nativeHandle_);
  }

  /**
   * Number of times we reseeked inside a merging iterator, specifically to skip
   * after or before a range of keys covered by a range deletion in a newer LSM
   * component.
   *
   * @return Number of times we reseeked inside a merging iterator
   */
  public long getInternalRangeDelReseekCount() {
    return getInternalRangeDelReseekCount(nativeHandle_);
  }

  /**
   * @return total nanos spent on getting snapshot
   */
  public long getSnapshotTime() {
    return getSnapshotTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on querying memtables
   */
  public long getFromMemtableTime() {
    return getFromMemtableTime(nativeHandle_);
  }

  /**
   * @return number of mem tables queried
   */
  public long getFromMemtableCount() {
    return getFromMemtableCount(nativeHandle_);
  }

  /**
   * @return total nanos spent after Get() finds a key
   */
  public long getPostProcessTime() {
    return getPostProcessTime(nativeHandle_);
  }

  /**
   * @return total nanos reading from output files
   */
  public long getFromOutputFilesTime() {
    return getFromOutputFilesTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on seeking memtable
   */
  public long getSeekOnMemtableTime() {
    return getSeekOnMemtableTime(nativeHandle_);
  }

  /**
   * number of seeks issued on memtable
   * (including SeekForPrev but not SeekToFirst and SeekToLast)
   * @return number of seeks issued on memtable
   */
  public long getSeekOnMemtableCount() {
    return getSeekOnMemtableCount(nativeHandle_);
  }

  /**
   * @return  number of Next()s issued on memtable
   */
  public long getNextOnMemtableCount() {
    return getNextOnMemtableCount(nativeHandle_);
  }

  /**
   * @return number of Prev()s issued on memtable
   */
  public long getPrevOnMemtableCount() {
    return getPrevOnMemtableCount(nativeHandle_);
  }

  /**
   * @return total nanos spent on seeking child iters
   */
  public long getSeekChildSeekTime() {
    return getSeekChildSeekTime(nativeHandle_);
  }

  /**
   * @return number of seek issued in child iterators
   */
  public long getSeekChildSeekCount() {
    return getSeekChildSeekCount(nativeHandle_);
  }

  /**
   * @return total nanos spent on the merge min heap
   */
  public long getSeekMinHeapTime() {
    return getSeekMinHeapTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on the merge max heap
   */
  public long getSeekMaxHeapTime() {
    return getSeekMaxHeapTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on seeking the internal entries
   */
  public long getSeekInternalSeekTime() {
    return getSeekInternalSeekTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on iterating internal entries to find the next user entry
   */
  public long getFindNextUserEntryTime() {
    return getFindNextUserEntryTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on writing to WAL
   */
  public long getWriteWalTime() {
    return getWriteWalTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on writing to mem tables
   */
  public long getWriteMemtableTime() {
    return getWriteMemtableTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on delaying or throttling write
   */
  public long getWriteDelayTime() {
    return getWriteDelayTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on switching memtable/wal and scheduling flushes/compactions.
   */
  public long getWriteSchedulingFlushesCompactionsTime() {
    return getWriteSchedulingFlushesCompactionsTime(nativeHandle_);
  }

  /**
   * @return total nanos spent on writing a record, excluding the above four things
   */
  public long getWritePreAndPostProcessTime() {
    return getWritePreAndPostProcessTime(nativeHandle_);
  }

  /**
   * @return time spent waiting for other threads of the batch group
   */
  public long getWriteThreadWaitNanos() {
    return getWriteThreadWaitNanos(nativeHandle_);
  }

  /**
   * @return time spent on acquiring DB mutex.
   */
  public long getDbMutexLockNanos() {
    return getDbMutexLockNanos(nativeHandle_);
  }

  /**
   * @return Time spent on waiting with a condition variable created with DB mutex.
   */
  public long getDbConditionWaitNanos() {
    return getDbConditionWaitNanos(nativeHandle_);
  }

  /**
   * @return Time spent on merge operator.
   */
  public long getMergeOperatorTimeNanos() {
    return getMergeOperatorTimeNanos(nativeHandle_);
  }

  /**
   * @return Time spent on reading index block from block cache or SST file
   */
  public long getReadIndexBlockNanos() {
    return getReadIndexBlockNanos(nativeHandle_);
  }

  /**
   * @return Time spent on reading filter block from block cache or SST file
   */
  public long getReadFilterBlockNanos() {
    return getReadFilterBlockNanos(nativeHandle_);
  }

  /**
   * @return  Time spent on creating data block iterator
   */
  public long getNewTableBlockIterNanos() {
    return getNewTableBlockIterNanos(nativeHandle_);
  }

  /**
   * @return Time spent on creating a iterator of an SST file.
   */
  public long getNewTableIteratorNanos() {
    return getNewTableIteratorNanos(nativeHandle_);
  }

  /**
   * @return Time spent on seeking a key in data/index blocks
   */
  public long getBlockSeekNanos() {
    return getBlockSeekNanos(nativeHandle_);
  }
  /**
   * @return  Time spent on finding or creating a table reader
   */
  public long getFindTableNanos() {
    return getFindTableNanos(nativeHandle_);
  }

  /**
   * @return total number of mem table bloom hits
   */
  public long getBloomMemtableHitCount() {
    return getBloomMemtableHitCount(nativeHandle_);
  }

  // total number of mem table bloom misses
  public long getBloomMemtableMissCount() {
    return getBloomMemtableMissCount(nativeHandle_);
  }

  /**
   * @return total number of SST bloom hits
   */
  public long getBloomSstHitCount() {
    return getBloomSstHitCount(nativeHandle_);
  }

  /**
   * @return total number of SST bloom misses
   */
  public long getBloomSstMissCount() {
    return getBloomSstMissCount(nativeHandle_);
  }

  /**
   * @return Time spent waiting on key locks in transaction lock manager.
   */
  public long getKeyLockWaitTime() {
    return getKeyLockWaitTime(nativeHandle_);
  }
  /**
   * @return number of times acquiring a lock was blocked by another transaction.
   */
  public long getKeyLockWaitCount() {
    return getKeyLockWaitCount(nativeHandle_);
  }

  /**
   * @return Total time spent in Env filesystem operations. These are only populated when TimedEnv
   *     is used.
   */
  public long getEnvNewSequentialFileNanos() {
    return getEnvNewSequentialFileNanos(nativeHandle_);
  }

  public long getEnvNewRandomAccessFileNanos() {
    return getEnvNewRandomAccessFileNanos(nativeHandle_);
  }

  public long getEnvNewWritableFileNanos() {
    return getEnvNewWritableFileNanos(nativeHandle_);
  }

  public long getEnvReuseWritableFileNanos() {
    return getEnvReuseWritableFileNanos(nativeHandle_);
  }

  public long getEnvNewRandomRwFileNanos() {
    return getEnvNewRandomRwFileNanos(nativeHandle_);
  }

  public long getEnvNewDirectoryNanos() {
    return getEnvNewDirectoryNanos(nativeHandle_);
  }

  public long getEnvFileExistsNanos() {
    return getEnvFileExistsNanos(nativeHandle_);
  }
  public long getEnvGetChildrenNanos() {
    return getEnvGetChildrenNanos(nativeHandle_);
  }

  public long getEnvGetChildrenFileAttributesNanos() {
    return getEnvGetChildrenFileAttributesNanos(nativeHandle_);
  }

  public long getEnvDeleteFileNanos() {
    return getEnvDeleteFileNanos(nativeHandle_);
  }

  public long getEnvCreateDirNanos() {
    return getEnvCreateDirNanos(nativeHandle_);
  }
  public long getEnvCreateDirIfMissingNanos() {
    return getEnvCreateDirIfMissingNanos(nativeHandle_);
  }

  public long getEnvDeleteDirNanos() {
    return getEnvDeleteDirNanos(nativeHandle_);
  }

  public long getEnvGetFileSizeNanos() {
    return getEnvGetFileSizeNanos(nativeHandle_);
  }

  public long getEnvGetFileModificationTimeNanos() {
    return getEnvGetFileModificationTimeNanos(nativeHandle_);
  }

  public long getEnvRenameFileNanos() {
    return getEnvRenameFileNanos(nativeHandle_);
  }

  public long getEnvLinkFileNanos() {
    return getEnvLinkFileNanos(nativeHandle_);
  }

  public long getEnvLockFileNanos() {
    return getEnvLockFileNanos(nativeHandle_);
  }

  public long getEnvUnlockFileNanos() {
    return getEnvUnlockFileNanos(nativeHandle_);
  }

  public long getEnvNewLoggerNanos() {
    return getEnvNewLoggerNanos(nativeHandle_);
  }

  public long getGetCpuNanos() {
    return getGetCpuNanos(nativeHandle_);
  }

  public long getIterNextCpuNanos() {
    return getIterNextCpuNanos(nativeHandle_);
  }
  public long getIterPrevCpuNanos() {
    return getIterPrevCpuNanos(nativeHandle_);
  }

  public long getIterSeekCpuNanos() {
    return getIterSeekCpuNanos(nativeHandle_);
  }

  /**
   * @return Time spent in encrypting data. Populated when EncryptedEnv is used.
   */
  public long getEncryptDataNanos() {
    return getEncryptDataNanos(nativeHandle_);
  }

  /**
   * @return Time spent in decrypting data. Populated when EncryptedEnv is used.
   */
  public long getDecryptDataNanos() {
    return getDecryptDataNanos(nativeHandle_);
  }

  public long getNumberAsyncSeek() {
    return getNumberAsyncSeek(nativeHandle_);
  }

  @Override
  public String toString() {
    return toString(true);
  }

  public String toString(final boolean excludeZeroCounters) {
    return toString(nativeHandle_, excludeZeroCounters);
  }

  @Override
  protected void disposeInternal(long handle) {
    // Nothing to do. Perf context is valid for all the time of application is running.
  }

  private native void reset(final long nativeHandle);

  private native long getUserKeyComparisonCount(final long handle);
  private native long getBlockCacheHitCount(final long handle);
  private native long getBlockReadCount(final long handle);
  private native long getBlockReadByte(final long handle);
  private native long getBlockReadTime(final long handle);
  private native long getBlockReadCpuTime(final long handle);
  private native long getBlockCacheIndexHitCount(final long handle);
  private native long getBlockCacheStandaloneHandleCount(final long handle);
  private native long getBlockCacheRealHandleCount(final long handle);
  private native long getIndexBlockReadCount(final long handle);
  private native long getBlockCacheFilterHitCount(final long handle);
  private native long getFilterBlockReadCount(final long handle);
  private native long getCompressionDictBlockReadCount(final long handle);

  private native long getSecondaryCacheHitCount(long handle);
  private native long getCompressedSecCacheInsertRealCount(long handle);

  private native long getCompressedSecCacheInsertDummyCount(final long handle);
  private native long getCompressedSecCacheUncompressedBytes(final long handle);
  private native long getCompressedSecCacheCompressedBytes(final long handle);
  private native long getBlockChecksumTime(final long handle);
  private native long getBlockDecompressTime(final long handle);
  private native long getReadBytes(final long handle);
  private native long getMultigetReadBytes(final long handle);
  private native long getIterReadBytes(final long handle);
  private native long getBlobCacheHitCount(final long handle);
  private native long getBlobCacheReadByte(final long handle);
  private native long getBlobReadCount(final long handle);
  private native long getBlobReadByte(final long handle);
  private native long getBlobReadTime(final long handle);
  private native long getBlobChecksumTime(final long handle);
  private native long getBlobDecompressTime(final long handle);
  private native long getInternalKeySkippedCount(final long handle);
  private native long getInternalDeleteSkippedCount(final long handle);
  private native long getInternalRecentSkippedCount(final long handle);
  private native long getInternalMergeCount(final long handle);
  private native long getInternalMergePointLookupCount(final long handle);
  private native long getInternalRangeDelReseekCount(final long handle);
  private native long getSnapshotTime(final long handle);
  private native long getFromMemtableTime(final long handle);
  private native long getFromMemtableCount(final long handle);
  private native long getPostProcessTime(final long handle);
  private native long getFromOutputFilesTime(final long handle);
  private native long getSeekOnMemtableTime(final long handle);
  private native long getSeekOnMemtableCount(final long handle);
  private native long getNextOnMemtableCount(final long handle);
  private native long getPrevOnMemtableCount(final long handle);
  private native long getSeekChildSeekTime(final long handle);
  private native long getSeekChildSeekCount(final long handle);
  private native long getSeekMinHeapTime(final long handle);
  private native long getSeekMaxHeapTime(final long handle);
  private native long getSeekInternalSeekTime(final long handle);
  private native long getFindNextUserEntryTime(final long handle);
  private native long getWriteWalTime(long handle);
  private native long getWriteMemtableTime(long handle);
  private native long getWriteDelayTime(long handle);
  private native long getWriteSchedulingFlushesCompactionsTime(long handle);
  private native long getWritePreAndPostProcessTime(long handle);
  private native long getWriteThreadWaitNanos(long handle);
  private native long getDbMutexLockNanos(long handle);
  private native long getDbConditionWaitNanos(long handle);
  private native long getMergeOperatorTimeNanos(long handle);
  private native long getReadIndexBlockNanos(long handle);
  private native long getReadFilterBlockNanos(long handle);
  private native long getNewTableBlockIterNanos(long handle);
  private native long getNewTableIteratorNanos(long handle);
  private native long getBlockSeekNanos(long handle);
  private native long getFindTableNanos(long handle);
  private native long getBloomMemtableHitCount(long handle);
  private native long getBloomMemtableMissCount(long handle);
  private native long getBloomSstHitCount(long handle);
  private native long getBloomSstMissCount(long handle);
  private native long getKeyLockWaitTime(long handle);
  private native long getKeyLockWaitCount(long handle);
  private native long getEnvNewSequentialFileNanos(long handle);
  private native long getEnvNewRandomAccessFileNanos(long handle);
  private native long getEnvNewWritableFileNanos(long handle);
  private native long getEnvReuseWritableFileNanos(long handle);
  private native long getEnvNewRandomRwFileNanos(long handle);
  private native long getEnvNewDirectoryNanos(long handle);
  private native long getEnvFileExistsNanos(long handle);
  private native long getEnvGetChildrenNanos(long handle);
  private native long getEnvGetChildrenFileAttributesNanos(long handle);
  private native long getEnvDeleteFileNanos(long handle);
  private native long getEnvCreateDirNanos(long handle);
  private native long getEnvCreateDirIfMissingNanos(long handle);
  private native long getEnvDeleteDirNanos(long handle);
  private native long getEnvGetFileSizeNanos(long handle);
  private native long getEnvGetFileModificationTimeNanos(long handle);
  private native long getEnvRenameFileNanos(long handle);
  private native long getEnvLinkFileNanos(long handle);
  private native long getEnvLockFileNanos(long handle);
  private native long getEnvUnlockFileNanos(long handle);
  private native long getEnvNewLoggerNanos(long handle);
  private native long getGetCpuNanos(long nativeHandle_);
  private native long getIterNextCpuNanos(long nativeHandle_);
  private native long getIterPrevCpuNanos(long nativeHandle_);
  private native long getIterSeekCpuNanos(long nativeHandle_);
  private native long getEncryptDataNanos(long nativeHandle_);
  private native long getDecryptDataNanos(long nativeHandle_);
  private native long getNumberAsyncSeek(long nativeHandle_);

  private native String toString(final long nativeHandle, final boolean excludeZeroCounters);
}
