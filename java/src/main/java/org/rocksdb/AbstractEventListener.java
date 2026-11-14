// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.rocksdb.AbstractEventListener.EnabledEventCallback.*; //NOPMD

/**
 * Base class for Event Listeners.
 */
@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "PMD.UnnecessaryImport"})
public abstract class AbstractEventListener extends RocksCallbackObject implements EventListener {
  public enum EnabledEventCallback {
    ON_FLUSH_COMPLETED((byte) 0x0),
    ON_FLUSH_BEGIN((byte) 0x1),
    ON_TABLE_FILE_DELETED((byte) 0x2),
    ON_COMPACTION_BEGIN((byte) 0x3),
    ON_COMPACTION_COMPLETED((byte) 0x4),
    ON_TABLE_FILE_CREATED((byte) 0x5),
    ON_TABLE_FILE_CREATION_STARTED((byte) 0x6),
    ON_MEMTABLE_SEALED((byte) 0x7),
    ON_COLUMN_FAMILY_HANDLE_DELETION_STARTED((byte) 0x8),
    ON_EXTERNAL_FILE_INGESTED((byte) 0x9),
    ON_BACKGROUND_ERROR((byte) 0xA),
    ON_STALL_CONDITIONS_CHANGED((byte) 0xB),
    ON_FILE_READ_FINISH((byte) 0xC),
    ON_FILE_WRITE_FINISH((byte) 0xD),
    ON_FILE_FLUSH_FINISH((byte) 0xE),
    ON_FILE_SYNC_FINISH((byte) 0xF),
    ON_FILE_RANGE_SYNC_FINISH((byte) 0x10),
    ON_FILE_TRUNCATE_FINISH((byte) 0x11),
    ON_FILE_CLOSE_FINISH((byte) 0x12),
    SHOULD_BE_NOTIFIED_ON_FILE_IO((byte) 0x13),
    ON_ERROR_RECOVERY_BEGIN((byte) 0x14),
    ON_ERROR_RECOVERY_COMPLETED((byte) 0x15);

    private final byte value;

    EnabledEventCallback(final byte value) {
      this.value = value;
    }

    /**
     * Get the internal representation value.
     *
     * @return the internal representation value
     */
    byte getValue() {
      return value;
    }

    /**
     * Get the EnabledEventCallbacks from the internal representation value.
     *
     * @return the enabled event callback.
     *
     * @throws IllegalArgumentException if the value is unknown.
     */
    static EnabledEventCallback fromValue(final byte value) {
      for (final EnabledEventCallback enabledEventCallback : values()) {
        if (enabledEventCallback.value == value) {
          return enabledEventCallback;
        }
      }

      throw new IllegalArgumentException(
          "Illegal value provided for EnabledEventCallback: " + value);
    }
  }

  /**
   * Creates an Event Listener that will
   * receive all callbacks from C++.
   * <p>
   * If you don't need all callbacks, it is much more efficient to
   * just register for the ones you need by calling
   * {@link #AbstractEventListener(EnabledEventCallback...)} instead.
   */
  protected AbstractEventListener() {
    this(ON_FLUSH_COMPLETED, ON_FLUSH_BEGIN, ON_TABLE_FILE_DELETED, ON_COMPACTION_BEGIN,
        ON_COMPACTION_COMPLETED, ON_TABLE_FILE_CREATED, ON_TABLE_FILE_CREATION_STARTED,
        ON_MEMTABLE_SEALED, ON_COLUMN_FAMILY_HANDLE_DELETION_STARTED, ON_EXTERNAL_FILE_INGESTED,
        ON_BACKGROUND_ERROR, ON_STALL_CONDITIONS_CHANGED, ON_FILE_READ_FINISH, ON_FILE_WRITE_FINISH,
        ON_FILE_FLUSH_FINISH, ON_FILE_SYNC_FINISH, ON_FILE_RANGE_SYNC_FINISH,
        ON_FILE_TRUNCATE_FINISH, ON_FILE_CLOSE_FINISH, SHOULD_BE_NOTIFIED_ON_FILE_IO,
        ON_ERROR_RECOVERY_BEGIN, ON_ERROR_RECOVERY_COMPLETED);
  }

  /**
   * Creates an Event Listener that will
   * receive only certain callbacks from C++.
   *
   * @param enabledEventCallbacks callbacks to enable in Java.
   */
  protected AbstractEventListener(final EnabledEventCallback... enabledEventCallbacks) {
    super(packToLong(enabledEventCallbacks));
  }

  /**
   * Pack EnabledEventCallbacks to a long.
   *
   * @param enabledEventCallbacks the flags
   *
   * @return a long
   */
  private static long packToLong(final EnabledEventCallback... enabledEventCallbacks) {
    long l = 0;
    for (final EnabledEventCallback enabledEventCallback : enabledEventCallbacks) {
      l |= 1L << enabledEventCallback.getValue();
    }
    return l;
  }

  @Override
  public void onFlushCompleted(final RocksDB db, final FlushJobInfo flushJobInfo) {
    // no-op
  }

  /**
   * Called from JNI, proxy for
   *     {@link #onFlushCompleted(RocksDB, FlushJobInfo)}.
   *
   * @param dbHandle native handle of the database
   * @param flushJobInfo the flush job info
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private void onFlushCompletedProxy(final long dbHandle, final FlushJobInfo flushJobInfo) {
    final RocksDB db = new RocksDB(dbHandle); // NOPMD - CloseResource
    db.disOwnNativeHandle(); // we don't own this!
    onFlushCompleted(db, flushJobInfo);
  }

  @Override
  public void onFlushBegin(final RocksDB db, final FlushJobInfo flushJobInfo) {
    // no-op
  }

  /**
   * Called from JNI, proxy for
   *     {@link #onFlushBegin(RocksDB, FlushJobInfo)}.
   *
   * @param dbHandle native handle of the database
   * @param flushJobInfo the flush job info
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private void onFlushBeginProxy(final long dbHandle, final FlushJobInfo flushJobInfo) {
    final RocksDB db = new RocksDB(dbHandle); // NOPMD - CloseResource
    db.disOwnNativeHandle(); // we don't own this!
    onFlushBegin(db, flushJobInfo);
  }

  @Override
  public void onTableFileDeleted(final TableFileDeletionInfo tableFileDeletionInfo) {
    // no-op
  }

  @Override
  public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
    // no-op
  }

  /**
   * Called from JNI, proxy for
   *     {@link #onCompactionBegin(RocksDB, CompactionJobInfo)}.
   *
   * @param dbHandle native handle of the database
   * @param compactionJobInfo the flush job info
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private void onCompactionBeginProxy(
      final long dbHandle, final CompactionJobInfo compactionJobInfo) {
    final RocksDB db = new RocksDB(dbHandle); // NOPMD - CloseResource
    db.disOwnNativeHandle(); // we don't own this!
    onCompactionBegin(db, compactionJobInfo);
  }

  @Override
  public void onCompactionCompleted(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
    // no-op
  }

  /**
   * Called from JNI, proxy for
   *     {@link #onCompactionCompleted(RocksDB, CompactionJobInfo)}.
   *
   * @param dbHandle native handle of the database
   * @param compactionJobInfo the flush job info
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private void onCompactionCompletedProxy(
      final long dbHandle, final CompactionJobInfo compactionJobInfo) {
    final RocksDB db = new RocksDB(dbHandle); // NOPMD - CloseResource
    db.disOwnNativeHandle(); // we don't own this!
    onCompactionCompleted(db, compactionJobInfo);
  }

  @Override
  public void onTableFileCreated(final TableFileCreationInfo tableFileCreationInfo) {
    // no-op
  }

  @Override
  public void onTableFileCreationStarted(
      final TableFileCreationBriefInfo tableFileCreationBriefInfo) {
    // no-op
  }

  @Override
  public void onMemTableSealed(final MemTableInfo memTableInfo) {
    // no-op
  }

  @Override
  public void onColumnFamilyHandleDeletionStarted(final ColumnFamilyHandle columnFamilyHandle) {
    // no-op
  }

  @Override
  public void onExternalFileIngested(
      final RocksDB db, final ExternalFileIngestionInfo externalFileIngestionInfo) {
    // no-op
  }

  /**
   * Called from JNI, proxy for
   *     {@link #onExternalFileIngested(RocksDB, ExternalFileIngestionInfo)}.
   *
   * @param dbHandle native handle of the database
   * @param externalFileIngestionInfo the flush job info
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private void onExternalFileIngestedProxy(
      final long dbHandle, final ExternalFileIngestionInfo externalFileIngestionInfo) {
    final RocksDB db = new RocksDB(dbHandle); // NOPMD - CloseResource
    db.disOwnNativeHandle(); // we don't own this!
    onExternalFileIngested(db, externalFileIngestionInfo);
  }

  @Override
  public void onBackgroundError(
      final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
    // no-op
  }

  /**
   * Called from JNI, proxy for
   *     {@link #onBackgroundError(BackgroundErrorReason, Status)}.
   *
   * @param reasonByte byte value representing error reason
   * @param backgroundError status with error code
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private void onBackgroundErrorProxy(final byte reasonByte, final Status backgroundError) {
    onBackgroundError(BackgroundErrorReason.fromValue(reasonByte), backgroundError);
  }

  @Override
  public void onStallConditionsChanged(final WriteStallInfo writeStallInfo) {
    // no-op
  }

  @Override
  public void onFileReadFinish(final FileOperationInfo fileOperationInfo) {
    // no-op
  }

  @Override
  public void onFileWriteFinish(final FileOperationInfo fileOperationInfo) {
    // no-op
  }

  @Override
  public void onFileFlushFinish(final FileOperationInfo fileOperationInfo) {
    // no-op
  }

  @Override
  public void onFileSyncFinish(final FileOperationInfo fileOperationInfo) {
    // no-op
  }

  @Override
  public void onFileRangeSyncFinish(final FileOperationInfo fileOperationInfo) {
    // no-op
  }

  @Override
  public void onFileTruncateFinish(final FileOperationInfo fileOperationInfo) {
    // no-op
  }

  @Override
  public void onFileCloseFinish(final FileOperationInfo fileOperationInfo) {
    // no-op
  }

  @Override
  public boolean shouldBeNotifiedOnFileIO() {
    return false;
  }

  @Override
  public boolean onErrorRecoveryBegin(
      final BackgroundErrorReason backgroundErrorReason, final Status backgroundError) {
    return true;
  }

  /**
   * Called from JNI, proxy for
   *     {@link #onErrorRecoveryBegin(BackgroundErrorReason, Status)}.
   *
   * @param reasonByte byte value representing error reason
   * @param backgroundError status with error code
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private boolean onErrorRecoveryBeginProxy(final byte reasonByte, final Status backgroundError) {
    return onErrorRecoveryBegin(BackgroundErrorReason.fromValue(reasonByte), backgroundError);
  }

  @Override
  public void onErrorRecoveryCompleted(final Status oldBackgroundError) {
    // no-op
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewEventListener(nativeParameterHandles[0]);
  }

  /**
   * Deletes underlying C++ native callback object pointer
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native long createNewEventListener(final long enabledEventCallbackValues);
  private native void disposeInternal(final long handle);
}
