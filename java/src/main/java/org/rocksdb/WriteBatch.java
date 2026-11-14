// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * WriteBatch holds a collection of updates to apply atomically to a DB.
 * <p>
 * The updates are applied in the order in which they are added
 * to the WriteBatch.  For example, the value of "key" will be "v3"
 * after the following batch is written:
 * <p>
 *    batch.put("key", "v1");
 *    batch.remove("key");
 *    batch.put("key", "v2");
 *    batch.put("key", "v3");
 * <p>
 * Multiple threads can invoke const methods on a WriteBatch without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same WriteBatch must use
 * external synchronization.
 */
public class WriteBatch extends AbstractWriteBatch {
  /**
   * Constructs a WriteBatch instance.
   */
  public WriteBatch() {
    this(0);
  }

  /**
   * Constructs a WriteBatch instance with a given size.
   *
   * @param reserved_bytes reserved size for WriteBatch
   */
  public WriteBatch(final int reserved_bytes) {
    super(newWriteBatch(reserved_bytes));
  }

  /**
   * Constructs a WriteBatch instance with a given size and per-KV checksum protection.
   * <p>
   * Supported values: 0, 8
   *
   * @param reserved_bytes reserved size for WriteBatch
   * @param protectionBytesPerKey number of checksum bytes per key-value
   */
  public WriteBatch(final int reserved_bytes, final long protectionBytesPerKey) {
    super(newWriteBatch(reserved_bytes, protectionBytesPerKey));
  }

  /**
   * Constructs a WriteBatch instance from a serialized representation
   * as returned by {@link #data()}.
   *
   * @param serialized the serialized representation.
   */
  public WriteBatch(final byte[] serialized) {
    super(newWriteBatch(serialized, serialized.length));
  }

  /**
   * Support for iterating over the contents of a batch.
   *
   * @param handler A handler that is called back for each
   *                update present in the batch
   *
   * @throws RocksDBException If we cannot iterate over the batch
   */
  public void iterate(final Handler handler) throws RocksDBException {
    iterate(nativeHandle_, handler.nativeHandle_);
  }

  /**
   * Retrieve the serialized version of this batch.
   *
   * @return the serialized representation of this write batch.
   *
   * @throws RocksDBException if an error occurs whilst retrieving
   *   the serialized batch data.
   */
  public byte[] data() throws RocksDBException {
    return data(nativeHandle_);
  }

  /**
   * Retrieve data size of the batch.
   *
   * @return the serialized data size of the batch.
   */
  public long getDataSize() {
    return getDataSize(nativeHandle_);
  }

  /**
   * Returns true if Put will be called during Iterate.
   *
   * @return true if Put will be called during Iterate.
   */
  public boolean hasPut() {
    return hasPut(nativeHandle_);
  }

  /**
   * Returns true if Delete will be called during Iterate.
   *
   * @return true if Delete will be called during Iterate.
   */
  public boolean hasDelete() {
    return hasDelete(nativeHandle_);
  }

  /**
   * Returns true if SingleDelete will be called during Iterate.
   *
   * @return true if SingleDelete will be called during Iterate.
   */
  public boolean hasSingleDelete() {
    return hasSingleDelete(nativeHandle_);
  }

  /**
   * Returns true if DeleteRange will be called during Iterate.
   *
   * @return true if DeleteRange will be called during Iterate.
   */
  public boolean hasDeleteRange() {
    return hasDeleteRange(nativeHandle_);
  }

  /**
   * Returns true if Merge will be called during Iterate.
   *
   * @return true if Merge will be called during Iterate.
   */
  public boolean hasMerge() {
    return hasMerge(nativeHandle_);
  }

  /**
   * Returns true if MarkBeginPrepare will be called during Iterate.
   *
   * @return true if MarkBeginPrepare will be called during Iterate.
   */
  public boolean hasBeginPrepare() {
    return hasBeginPrepare(nativeHandle_);
  }

  /**
   * Returns true if MarkEndPrepare will be called during Iterate.
   *
   * @return true if MarkEndPrepare will be called during Iterate.
   */
  public boolean hasEndPrepare() {
    return hasEndPrepare(nativeHandle_);
  }

  /**
   * Returns true if MarkCommit will be called during Iterate.
   *
   * @return true if MarkCommit will be called during Iterate.
   */
  public boolean hasCommit() {
    return hasCommit(nativeHandle_);
  }

  /**
   * Returns true if MarkRollback will be called during Iterate.
   *
   * @return true if MarkRollback will be called during Iterate.
   */
  public boolean hasRollback() {
    return hasRollback(nativeHandle_);
  }

  @Override
  public WriteBatch getWriteBatch() {
    return this;
  }

  /**
   * Marks this point in the WriteBatch as the last record to
   * be inserted into the WAL, provided the WAL is enabled.
   */
  public void markWalTerminationPoint() {
    markWalTerminationPoint(nativeHandle_);
  }

  /**
   * Gets the WAL termination point.
   * <p>
   * See {@link #markWalTerminationPoint()}
   *
   * @return the WAL termination point
   */
  public SavePoint getWalTerminationPoint() {
    return getWalTerminationPoint(nativeHandle_);
  }

  @Override
  WriteBatch getWriteBatch(final long handle) {
    return this;
  }

  /**
   * <p>Private WriteBatch constructor which is used to construct
   * WriteBatch instances from C++ side. As the reference to this
   * object is also managed from C++ side the handle will be disowned.</p>
   *
   * @param nativeHandle address of native instance.
   */
  WriteBatch(final long nativeHandle) {
    this(nativeHandle, false);
  }

  /**
   * <p>Private WriteBatch constructor which is used to construct
   * WriteBatch instances. </p>
   *
   * @param nativeHandle address of native instance.
   * @param owningNativeHandle whether to own this reference from the C++ side or not
   */
  WriteBatch(final long nativeHandle, final boolean owningNativeHandle) {
    super(nativeHandle);
    if(!owningNativeHandle)
      disOwnNativeHandle();
  }

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  @Override
  final int count0(final long handle) {
    return count0Jni(handle);
  }

  private static native int count0Jni(final long handle);

  @Override
  final void put(final long handle, final byte[] key, final int keyLen, final byte[] value,
      final int valueLen) {
    putJni(handle, key, keyLen, value, valueLen);
  }
  private static native void putJni(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen);

  @Override
  final void put(final long handle, final byte[] key, final int keyLen, final byte[] value,
      final int valueLen, final long cfHandle) {
    putJni(handle, key, keyLen, value, valueLen, cfHandle);
  }

  private static native void putJni(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle);

  @Override
  final void putDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final ByteBuffer value, final int valueOffset, final int valueLength,
      final long cfHandle) {
    putDirectJni(handle, key, keyOffset, keyLength, value, valueOffset, valueLength, cfHandle);
  }

  private static native void putDirectJni(final long handle, final ByteBuffer key,
      final int keyOffset, final int keyLength, final ByteBuffer value, final int valueOffset,
      final int valueLength, final long cfHandle);

  @Override
  final void merge(final long handle, final byte[] key, final int keyLen, final byte[] value,
      final int valueLen) {
    mergeJni(handle, key, keyLen, value, valueLen);
  }

  private static native void mergeJni(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen);

  @Override
  final void merge(final long handle, final byte[] key, final int keyLen, final byte[] value,
      final int valueLen, final long cfHandle) {
    mergeJni(handle, key, keyLen, value, valueLen, cfHandle);
  }
  private static native void mergeJni(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle);

  @Override
  final void delete(final long handle, final byte[] key, final int keyLen) throws RocksDBException {
    deleteJni(handle, key, keyLen);
  }

  private static native void deleteJni(final long handle, final byte[] key, final int keyLen)
      throws RocksDBException;

  @Override
  final void delete(final long handle, final byte[] key, final int keyLen, final long cfHandle)
      throws RocksDBException {
    deleteJni(handle, key, keyLen, cfHandle);
  }
  private static native void deleteJni(final long handle, final byte[] key, final int keyLen,
      final long cfHandle) throws RocksDBException;
  @Override
  final void singleDelete(final long handle, final byte[] key, final int keyLen)
      throws RocksDBException {
    singleDeleteJni(handle, key, keyLen);
  }

  private static native void singleDeleteJni(final long handle, final byte[] key, final int keyLen)
      throws RocksDBException;

  @Override
  final void singleDelete(final long handle, final byte[] key, final int keyLen,
      final long cfHandle) throws RocksDBException {
    singleDeleteJni(handle, key, keyLen, cfHandle);
  }
  private static native void singleDeleteJni(final long handle, final byte[] key, final int keyLen,
      final long cfHandle) throws RocksDBException;

  @Override
  final void deleteDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final long cfHandle) throws RocksDBException {
    deleteDirectJni(handle, key, keyOffset, keyLength, cfHandle);
  }

  private static native void deleteDirectJni(final long handle, final ByteBuffer key,
      final int keyOffset, final int keyLength, final long cfHandle) throws RocksDBException;

  @Override
  final void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen) {
    deleteRangeJni(handle, beginKey, beginKeyLen, endKey, endKeyLen);
  }
  private static native void deleteRangeJni(final long handle, final byte[] beginKey,
      final int beginKeyLen, final byte[] endKey, final int endKeyLen);
  @Override
  final void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen, final long cfHandle) {
    deleteRangeJni(handle, beginKey, beginKeyLen, endKey, endKeyLen, cfHandle);
  }
  private static native void deleteRangeJni(final long handle, final byte[] beginKey,
      final int beginKeyLen, final byte[] endKey, final int endKeyLen, final long cfHandle);

  @Override
  final void putLogData(final long handle, final byte[] blob, final int blobLen)
      throws RocksDBException {
    putLogDataJni(handle, blob, blobLen);
  }

  private static native void putLogDataJni(final long handle, final byte[] blob, final int blobLen)
      throws RocksDBException;

  @Override
  final void clear0(final long handle) {
    clear0Jni(handle);
  }

  private static native void clear0Jni(final long handle);

  @Override
  final void setSavePoint0(final long handle) {
    setSavePoint0Jni(handle);
  }
  private static native void setSavePoint0Jni(final long handle);
  @Override
  final void rollbackToSavePoint0(final long handle) {
    rollbackToSavePoint0Jni(handle);
  }
  private static native void rollbackToSavePoint0Jni(final long handle);
  @Override
  final void popSavePoint(final long handle) throws RocksDBException {
    popSavePointJni(handle);
  }
  private static native void popSavePointJni(final long handle) throws RocksDBException;
  @Override
  final void setMaxBytes(final long nativeHandle, final long maxBytes) {
    setMaxBytesJni(nativeHandle, maxBytes);
  }

  private static native void setMaxBytesJni(final long nativeHandle, final long maxBytes);

  private static native long newWriteBatch(final int reserved_bytes);
  private static native long newWriteBatch(
      final int reserved_bytes, final long protectionBytesPerKey);
  private static native long newWriteBatch(final byte[] serialized, final int serializedLength);
  private static native void iterate(final long handle, final long handlerHandle)
      throws RocksDBException;
  private static native byte[] data(final long nativeHandle) throws RocksDBException;
  private static native long getDataSize(final long nativeHandle);
  private static native boolean hasPut(final long nativeHandle);
  private static native boolean hasDelete(final long nativeHandle);
  private static native boolean hasSingleDelete(final long nativeHandle);
  private static native boolean hasDeleteRange(final long nativeHandle);
  private static native boolean hasMerge(final long nativeHandle);
  private static native boolean hasBeginPrepare(final long nativeHandle);
  private static native boolean hasEndPrepare(final long nativeHandle);
  private static native boolean hasCommit(final long nativeHandle);
  private static native boolean hasRollback(final long nativeHandle);
  private static native void markWalTerminationPoint(final long nativeHandle);
  private static native SavePoint getWalTerminationPoint(final long nativeHandle);

  /**
   * Handler callback for iterating over the contents of a batch.
   */
  public abstract static class Handler extends RocksCallbackObject {
    public Handler() {
      super(0L);
    }

    @Override
    protected long initializeNative(final long... nativeParameterHandles) {
      return createNewHandler0();
    }

    public abstract void put(final int columnFamilyId, final byte[] key,
        final byte[] value) throws RocksDBException;
    public abstract void put(final byte[] key, final byte[] value);
    public abstract void merge(final int columnFamilyId, final byte[] key,
        final byte[] value) throws RocksDBException;
    public abstract void merge(final byte[] key, final byte[] value);
    public abstract void delete(final int columnFamilyId, final byte[] key)
        throws RocksDBException;
    public abstract void delete(final byte[] key);
    public abstract void singleDelete(final int columnFamilyId,
        final byte[] key) throws RocksDBException;
    public abstract void singleDelete(final byte[] key);
    public abstract void deleteRange(final int columnFamilyId,
        final byte[] beginKey, final byte[] endKey) throws RocksDBException;
    public abstract void deleteRange(final byte[] beginKey,
        final byte[] endKey);
    public abstract void logData(final byte[] blob);
    public abstract void putBlobIndex(final int columnFamilyId,
        final byte[] key, final byte[] value) throws RocksDBException;
    public abstract void markBeginPrepare() throws RocksDBException;
    public abstract void markEndPrepare(final byte[] xid)
        throws RocksDBException;
    public abstract void markNoop(final boolean emptyBatch)
        throws RocksDBException;
    public abstract void markRollback(final byte[] xid)
        throws RocksDBException;
    public abstract void markCommit(final byte[] xid)
        throws RocksDBException;
    public abstract void markCommitWithTimestamp(final byte[] xid, final byte[] ts)
        throws RocksDBException;

    /**
     * shouldContinue is called by the underlying iterator
     * {@link WriteBatch#iterate(Handler)}. If it returns false,
     * iteration is halted. Otherwise, it continues
     * iterating. The default implementation always
     * returns true.
     *
     * @return boolean value indicating if the
     *     iteration is halted.
     */
    public boolean shouldContinue() {
      return true;
    }

    private native long createNewHandler0();
  }

  /**
   * A structure for describing the save point in the Write Batch.
   */
  public static class SavePoint {
    private long size;
    private long count;
    private long contentFlags;

    public SavePoint(final long size, final long count,
        final long contentFlags) {
      this.size = size;
      this.count = count;
      this.contentFlags = contentFlags;
    }

    public void clear() {
      this.size = 0;
      this.count = 0;
      this.contentFlags = 0;
    }

    /**
     * Get the size of the serialized representation.
     *
     * @return the size of the serialized representation.
     */
    public long getSize() {
      return size;
    }

    /**
     * Get the number of elements.
     *
     * @return the number of elements.
     */
    public long getCount() {
      return count;
    }

    /**
     * Get the content flags.
     *
     * @return the content flags.
     */
    public long getContentFlags() {
      return contentFlags;
    }

    public boolean isCleared() {
      return (size | count | contentFlags) == 0;
    }
  }
}
