// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Java wrapper over native write_buffer_manager class
 */
public class WriteBufferManager extends RocksObject {
  /**
   * Construct a new instance of WriteBufferManager.
   * <p>
   * Check <a href="https://github.com/meta/rocksdb/wiki/Write-Buffer-Manager">
   *     https://github.com/meta/rocksdb/wiki/Write-Buffer-Manager</a>
   * for more details on when to use it
   *
   * @param bufferSizeBytes buffer size(in bytes) to use for native write_buffer_manager
   * @param cache cache whose memory should be bounded by this write buffer manager
   * @param allowStall if set true, it will enable stalling of writes when memory_usage() exceeds
   *     buffer_size.
   *        It will wait for flush to complete and memory usage to drop down.
   */
  public WriteBufferManager(
      final long bufferSizeBytes, final Cache cache, final boolean allowStall) {
    super(newWriteBufferManagerInstance(bufferSizeBytes, cache.nativeHandle_, allowStall));
    this.allowStall_ = allowStall;
  }

  /**
   * Construct a new instance with an explicit flush policy.
   *
   * @param bufferSizeBytes buffer size in bytes
   * @param cache cache whose memory should be bounded by this manager
   * @param allowStall whether writes may stall when memory exceeds the limit
   * @param flushPolicy policy used to choose mutable memtables to flush
   */
  public WriteBufferManager(final long bufferSizeBytes, final Cache cache, final boolean allowStall,
      final WriteBufferManagerFlushPolicy flushPolicy) {
    super(newWriteBufferManagerWithFlushPolicyInstance(
        bufferSizeBytes, cache.nativeHandle_, allowStall, flushPolicy.getValue()));
    this.allowStall_ = allowStall;
  }

  public WriteBufferManager(final long bufferSizeBytes, final Cache cache){
    this(bufferSizeBytes, cache, false);
  }

  public boolean allowStall() {
    return allowStall_;
  }

  /**
   * Returns the policy used for write-buffer-manager-triggered flushes.
   *
   * @return the current flush policy
   */
  public WriteBufferManagerFlushPolicy flushPolicy() {
    assert (isOwningHandle());
    return WriteBufferManagerFlushPolicy.fromValue(flushPolicy(nativeHandle_));
  }

  /**
   * Changes the policy used for write-buffer-manager-triggered flushes.
   *
   * @param flushPolicy the new flush policy
   */
  public void setFlushPolicy(final WriteBufferManagerFlushPolicy flushPolicy) {
    assert (isOwningHandle());
    setFlushPolicy(nativeHandle_, flushPolicy.getValue());
  }

  private static long newWriteBufferManagerInstance(
      final long bufferSizeBytes, final long cacheHandle, final boolean allowStall) {
    RocksDB.loadLibrary();
    return newWriteBufferManager(bufferSizeBytes, cacheHandle, allowStall);
  }
  private static native long newWriteBufferManager(
      final long bufferSizeBytes, final long cacheHandle, final boolean allowStall);

  private static long newWriteBufferManagerWithFlushPolicyInstance(final long bufferSizeBytes,
      final long cacheHandle, final boolean allowStall, final byte flushPolicy) {
    RocksDB.loadLibrary();
    return newWriteBufferManagerWithFlushPolicy(
        bufferSizeBytes, cacheHandle, allowStall, flushPolicy);
  }

  private static native long newWriteBufferManagerWithFlushPolicy(final long bufferSizeBytes,
      final long cacheHandle, final boolean allowStall, final byte flushPolicy);
  private static native byte flushPolicy(final long handle);
  private static native void setFlushPolicy(final long handle, final byte flushPolicy);
  @Override
  protected void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native void disposeInternalJni(final long handle);

  private final boolean allowStall_;
}
