// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * WaitForCompactOptions is used to configure the behavior of
 * {@link RocksDB#waitForCompact(WaitForCompactOptions)}.
 * It allows waiting for flush and compaction operations to complete
 * with optional timeout and abort-on-pause settings.
 */
public class WaitForCompactOptions extends RocksObject {
  /**
   * Creates a new WaitForCompactOptions with default settings.
   * <p>
   * Default values:
   * - abortOnPause: false
   * - flush: false
   * - waitForPurge: false
   * - closeDb: false
   * - timeout: 0 (no timeout)
   */
  public WaitForCompactOptions() {
    super(newWaitForCompactOptions());
  }

  /**
   * Sets whether to abort waiting when background work is paused.
   *
   * @param abortOnPause if true, waiting will be aborted if background
   *                     work is paused via PauseBackgroundWork()
   * @return this WaitForCompactOptions instance
   */
  public WaitForCompactOptions setAbortOnPause(final boolean abortOnPause) {
    setAbortOnPause(nativeHandle_, abortOnPause);
    return this;
  }

  /**
   * Returns whether abort-on-pause is enabled.
   *
   * @return true if abort-on-pause is enabled
   */
  public boolean abortOnPause() {
    return abortOnPause(nativeHandle_);
  }

  /**
   * Sets whether to wait for flush operations to complete in addition
   * to compaction operations.
   *
   * @param flush if true, also waits for flush operations
   * @return this WaitForCompactOptions instance
   */
  public WaitForCompactOptions setFlush(final boolean flush) {
    setFlush(nativeHandle_, flush);
    return this;
  }

  /**
   * Returns whether waiting for flush is enabled.
   *
   * @return true if waiting for flush is enabled
   */
  public boolean flush() {
    return flush(nativeHandle_);
  }

  /**
   * Sets whether to wait for purge to complete.
   *
   * @param waitForPurge if true, wait for purge to complete
   * @return this WaitForCompactOptions instance
   */
  public WaitForCompactOptions setWaitForPurge(final boolean waitForPurge) {
    setWaitForPurge(nativeHandle_, waitForPurge);
    return this;
  }

  /**
   * Returns whether waiting for purge is enabled.
   *
   * @return true if waiting for purge is enabled
   */
  public boolean waitForPurge() {
    return waitForPurge(nativeHandle_);
  }

  /**
   * Sets whether to call Close() after waiting is done.
   * By the time Close() is called here, there should be no background jobs
   * in progress and no new background jobs should be added.
   *
   * @param closeDb if true, close the database after waiting
   * @return this WaitForCompactOptions instance
   */
  public WaitForCompactOptions setCloseDb(final boolean closeDb) {
    setCloseDb(nativeHandle_, closeDb);
    return this;
  }

  /**
   * Returns whether the database will be closed after waiting.
   *
   * @return true if close is enabled
   */
  public boolean closeDb() {
    return closeDb(nativeHandle_);
  }

  /**
   * Sets a timeout for waiting.
   *
   * @param timeoutMicros timeout in microseconds. 0 means no timeout.
   * @return this WaitForCompactOptions instance
   */
  public WaitForCompactOptions setTimeout(final long timeoutMicros) {
    setTimeout(nativeHandle_, timeoutMicros);
    return this;
  }

  /**
   * Returns the timeout setting in microseconds.
   *
   * @return timeout in microseconds, 0 if no timeout
   */
  public long timeout() {
    return timeout(nativeHandle_);
  }

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native long newWaitForCompactOptions();
  private static native void setAbortOnPause(final long handle, final boolean abortOnPause);
  private static native boolean abortOnPause(final long handle);
  private static native void setFlush(final long handle, final boolean flush);
  private static native boolean flush(final long handle);
  private static native void setWaitForPurge(final long handle, final boolean waitForPurge);
  private static native boolean waitForPurge(final long handle);
  private static native void setCloseDb(final long handle, final boolean closeDb);
  private static native boolean closeDb(final long handle);
  private static native void setTimeout(final long handle, final long timeoutMicros);
  private static native long timeout(final long handle);
  private static native void disposeInternalJni(final long handle);
}