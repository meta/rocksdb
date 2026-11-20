// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Options that control write operations.
 * <p>
 * Note that developers should call WriteOptions.dispose() to release the
 * c++ side memory before a WriteOptions instance runs out of scope.
 */
public class WriteOptions extends RocksObject {
  /**
   * Construct WriteOptions instance.
   */
  public WriteOptions() {
    super(newWriteOptions());

  }

  // TODO(AR) consider ownership
  WriteOptions(final long nativeHandle) {
    super(nativeHandle);
    disOwnNativeHandle();
  }

  /**
   * Copy constructor for WriteOptions.
   * <p>
   * NOTE: This does a shallow copy, which means comparator, merge_operator, compaction_filter,
   * compaction_filter_factory and other pointers will be cloned!
   *
   * @param other The ColumnFamilyOptions to copy.
   */
  public WriteOptions(final WriteOptions other) {
    super(copyWriteOptions(other.nativeHandle_));
  }

  /**
   * If true, the write will be flushed from the operating system
   * buffer cache (by calling WritableFile::Sync()) before the write
   * is considered complete.  If this flag is true, writes will be
   * slower.
   * <p>
   * If this flag is false, and the machine crashes, some recent
   * writes may be lost.  Note that if it is just the process that
   * crashes (i.e., the machine does not reboot), no writes will be
   * lost even if sync==false.
   * <p>
   * In other words, a DB write with sync==false has similar
   * crash semantics as the "write()" system call.  A DB write
   * with sync==true has similar crash semantics to a "write()"
   * system call followed by "fdatasync()".
   * <p>
   * Default: false
   *
   * @param flag a boolean flag to indicate whether a write
   *     should be synchronized.
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setSync(final boolean flag) {
    setSync(nativeHandle_, flag);
    return this;
  }

  /**
   * If true, the write will be flushed from the operating system
   * buffer cache (by calling WritableFile::Sync()) before the write
   * is considered complete.  If this flag is true, writes will be
   * slower.
   * <p>
   * If this flag is false, and the machine crashes, some recent
   * writes may be lost.  Note that if it is just the process that
   * crashes (i.e., the machine does not reboot), no writes will be
   * lost even if sync==false.
   * <p>
   * In other words, a DB write with sync==false has similar
   * crash semantics as the "write()" system call.  A DB write
   * with sync==true has similar crash semantics to a "write()"
   * system call followed by "fdatasync()".
   *
   * @return boolean value indicating if sync is active.
   */
  public boolean sync() {
    return sync(nativeHandle_);
  }

  /**
   * If true, writes will not first go to the write ahead log,
   * and the write may got lost after a crash. The backup engine
   * relies on write-ahead logs to back up the memtable, so if
   * you disable write-ahead logs, you must create backups with
   * flush_before_backup=true to avoid losing unflushed memtable data.
   *
   * @param flag a boolean flag to specify whether to disable
   *     write-ahead-log on writes.
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setDisableWAL(final boolean flag) {
    setDisableWAL(nativeHandle_, flag);
    return this;
  }

  /**
   * If true, writes will not first go to the write ahead log,
   * and the write may got lost after a crash. The backup engine
   * relies on write-ahead logs to back up the memtable, so if
   * you disable write-ahead logs, you must create backups with
   * flush_before_backup=true to avoid losing unflushed memtable data.
   *
   * @return boolean value indicating if WAL is disabled.
   */
  public boolean disableWAL() {
    return disableWAL(nativeHandle_);
  }

  /**
   * If true and if user is trying to write to column families that don't exist
   * (they were dropped), ignore the write (don't return an error). If there
   * are multiple writes in a WriteBatch, other writes will succeed.
   * <p>
   * Default: false
   *
   * @param ignoreMissingColumnFamilies true to ignore writes to column families
   *     which don't exist
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setIgnoreMissingColumnFamilies(
      final boolean ignoreMissingColumnFamilies) {
    setIgnoreMissingColumnFamilies(nativeHandle_, ignoreMissingColumnFamilies);
    return this;
  }

  /**
   * If true and if user is trying to write to column families that don't exist
   * (they were dropped), ignore the write (don't return an error). If there
   * are multiple writes in a WriteBatch, other writes will succeed.
   * <p>
   * Default: false
   *
   * @return true if writes to column families which don't exist are ignored
   */
  public boolean ignoreMissingColumnFamilies() {
    return ignoreMissingColumnFamilies(nativeHandle_);
  }

  /**
   * If true and we need to wait or sleep for the write request, fails
   * immediately with {@link Status.Code#Incomplete}.
   *
   * @param noSlowdown true to fail write requests if we need to wait or sleep
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setNoSlowdown(final boolean noSlowdown) {
    setNoSlowdown(nativeHandle_, noSlowdown);
    return this;
  }

  /**
   * If true and we need to wait or sleep for the write request, fails
   * immediately with {@link Status.Code#Incomplete}.
   *
   * @return true when write requests are failed if we need to wait or sleep
   */
  public boolean noSlowdown() {
    return noSlowdown(nativeHandle_);
  }

  /**
   * If true, this write request is of lower priority if compaction is
   * behind. In the case that, {@link #noSlowdown()} == true, the request
   * will be cancelled immediately with {@link Status.Code#Incomplete} returned.
   * Otherwise, it will be slowed down. The slowdown value is determined by
   * RocksDB to guarantee it introduces minimum impacts to high priority writes.
   * <p>
   * Default: false
   *
   * @param lowPri true if the write request should be of lower priority than
   *     compactions which are behind.
   *
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setLowPri(final boolean lowPri) {
    setLowPri(nativeHandle_, lowPri);
    return this;
  }

  /**
   * Returns true if this write request is of lower priority if compaction is
   * behind.
   * <p>
   * See {@link #setLowPri(boolean)}.
   *
   * @return true if this write request is of lower priority, false otherwise.
   */
  public boolean lowPri() {
    return lowPri(nativeHandle_);
  }

  /**
   * If true, this writebatch will maintain the last insert positions of each
   * memtable as hints in concurrent write. It can improve write performance
   * in concurrent writes if keys in one writebatch are sequential. In
   * non-concurrent writes (when {@code concurrent_memtable_writes} is false) this
   * option will be ignored.
   * <p>
   * Default: false
   *
   * @return true if writebatch will maintain the last insert positions of each memtable as hints in
   *     concurrent write.
   */
  public boolean memtableInsertHintPerBatch() {
    return memtableInsertHintPerBatch(nativeHandle_);
  }

  /**
   * If true, this writebatch will maintain the last insert positions of each
   * memtable as hints in concurrent write. It can improve write performance
   * in concurrent writes if keys in one writebatch are sequential. In
   * non-concurrent writes (when {@code concurrent_memtable_writes} is false) this
   * option will be ignored.
   * <p>
   * Default: false
   *
   * @param memtableInsertHintPerBatch true if writebatch should maintain the last insert positions
   *     of each memtable as hints in concurrent write.
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setMemtableInsertHintPerBatch(final boolean memtableInsertHintPerBatch) {
    setMemtableInsertHintPerBatch(nativeHandle_, memtableInsertHintPerBatch);
    return this;
  }

  /**
   * Set the number of bytes per key-value checksum for write batches.
   * Enables per-KV checksum protection in write batches to detect silent data corruption.
   * <p>
   * Default: 0
   * <p>
   * Supported values: 0, 8
   *
   * @param protectionBytesPerKey number of checksum bytes per key-value
   *
   * @return the instance of the current WriteOptions.
   */
  public WriteOptions setProtectionBytesPerKey(final long protectionBytesPerKey) {
    setProtectionBytesPerKey(nativeHandle_, protectionBytesPerKey);
    return this;
  }

  /**
   * Get the number of bytes per key-value checksum for write batches.
   * <p>
   * Default: 0
   *
   * @return the number of checksum bytes per key-value
   */
  public long protectionBytesPerKey() {
    return protectionBytesPerKey(nativeHandle_);
  }

  private static native long newWriteOptions();
  private static native long copyWriteOptions(long handle);
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  private static native void setSync(long handle, boolean flag);
  private static native boolean sync(long handle);
  private static native void setDisableWAL(long handle, boolean flag);
  private static native boolean disableWAL(long handle);
  private static native void setIgnoreMissingColumnFamilies(
      final long handle, final boolean ignoreMissingColumnFamilies);
  private static native boolean ignoreMissingColumnFamilies(final long handle);
  private static native void setNoSlowdown(final long handle, final boolean noSlowdown);
  private static native boolean noSlowdown(final long handle);
  private static native void setLowPri(final long handle, final boolean lowPri);
  private static native boolean lowPri(final long handle);
  private static native boolean memtableInsertHintPerBatch(final long handle);
  private static native void setMemtableInsertHintPerBatch(
      final long handle, final boolean memtableInsertHintPerBatch);
  private static native void setProtectionBytesPerKey(
      final long handle, final long protectionBytesPerKey);
  private static native long protectionBytesPerKey(final long handle);
}
