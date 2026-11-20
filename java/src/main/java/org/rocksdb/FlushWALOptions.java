// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * FlushWALOptions is used to configure flushing of Write-Ahead Log (WAL) data.
 * It provides options for controlling sync behavior and rate limiting priority
 * when manually flushing the WAL.
 */
public class FlushWALOptions extends RocksObject {
  /**
   * Creates a new FlushWALOptions with default settings.
   * <p>
   * Default values:
   * - sync: false
   * - rateLimiterPriority: Env.TOTAL
   */
  public FlushWALOptions() {
    super(newFlushWALOptions());
  }

  /**
   * Creates a new FlushWALOptions with specified sync behavior.
   *
   * @param sync if true, the WAL will be synced to storage after flush
   */
  public FlushWALOptions(final boolean sync) {
    super(newFlushWALOptions());
    setSync(sync);
  }

  /**
   * Sets whether to sync the WAL to storage after flushing.
   *
   * @param sync if true, ensures data is persisted to storage
   * @return this FlushWALOptions instance
   */
  public FlushWALOptions setSync(final boolean sync) {
    setSync(nativeHandle_, sync);
    return this;
  }

  /**
   * Returns whether sync is enabled.
   *
   * @return true if sync is enabled
   */
  public boolean sync() {
    return sync(nativeHandle_);
  }

  /**
   * Sets the rate limiter priority for WAL flush I/O operations.
   * This allows controlling the priority of manual WAL flush operations
   * when rate limiting is enabled.
   *
   * @param rateLimiterPriority the priority level from {@link IOPriority}
   * @return this FlushWALOptions instance
   */
  public FlushWALOptions setRateLimiterPriority(final IOPriority rateLimiterPriority) {
    setRateLimiterPriority(nativeHandle_, rateLimiterPriority.getValue());
    return this;
  }

  /**
   * Returns the current rate limiter priority setting.
   *
   * @return the rate limiter priority
   */
  public IOPriority rateLimiterPriority() {
    return IOPriority.getIOPriority(rateLimiterPriority(nativeHandle_));
  }

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native long newFlushWALOptions();
  private static native void setSync(final long handle, final boolean sync);
  private static native boolean sync(final long handle);
  private static native void setRateLimiterPriority(
      final long handle, final byte rateLimiterPriority);
  private static native byte rateLimiterPriority(final long handle);
  private static native void disposeInternalJni(final long handle);
}
