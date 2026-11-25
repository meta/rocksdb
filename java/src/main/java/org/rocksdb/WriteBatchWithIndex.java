// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.rocksdb.util.BufferUtil.CheckBounds;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Similar to {@link org.rocksdb.WriteBatch} but with a binary searchable
 * index built for all the keys inserted.
 * <p>
 * Calling put, merge, remove or putLogData calls the same function
 * as with {@link org.rocksdb.WriteBatch} whilst also building an index.
 * <p>
 * A user can call {@link org.rocksdb.WriteBatchWithIndex#newIterator()} to
 * create an iterator over the write batch or
 * {@link org.rocksdb.WriteBatchWithIndex#newIteratorWithBase(org.rocksdb.RocksIterator)}
 * to get an iterator for the database with Read-Your-Own-Writes like capability
 */
public class WriteBatchWithIndex extends AbstractWriteBatch {
  /**
   * Creates a WriteBatchWithIndex where no bytes
   * are reserved up-front, byte wise comparison is
   * used for fallback key comparisons,
   * and duplicate keys operations are retained
   */
  public WriteBatchWithIndex() {
    super(newWriteBatchWithIndex());
  }

  /**
   * Creates a WriteBatchWithIndex where no bytes
   * are reserved up-front, byte wise comparison is
   * used for fallback key comparisons, and duplicate key
   * assignment is determined by the constructor argument
   *
   * @param overwriteKey if true, overwrite the key in the index when
   *   inserting a duplicate key, in this way an iterator will never
   *   show two entries with the same key.
   */
  public WriteBatchWithIndex(final boolean overwriteKey) {
    super(newWriteBatchWithIndex(overwriteKey));
  }

  /**
   * Creates a WriteBatchWithIndex
   *
   * @param fallbackIndexComparator We fall back to this comparator
   *  to compare keys within a column family if we cannot determine
   *  the column family and so look up its comparator.
   *
   * @param reservedBytes reserved bytes in underlying WriteBatch
   *
   * @param overwriteKey if true, overwrite the key in the index when
   *   inserting a duplicate key, in this way an iterator will never
   *   show two entries with the same key.
   */
  public WriteBatchWithIndex(
      final AbstractComparator
          fallbackIndexComparator, final int reservedBytes,
      final boolean overwriteKey) {
    super(newWriteBatchWithIndex(fallbackIndexComparator.nativeHandle_,
        fallbackIndexComparator.getComparatorType().getValue(), reservedBytes,
        overwriteKey));
  }

  /**
   * <p>Private WriteBatchWithIndex constructor which is used to construct
   * WriteBatchWithIndex instances from C++ side. As the reference to this
   * object is also managed from C++ side the handle will be disowned.</p>
   *
   * @param nativeHandle address of native instance.
   */
  WriteBatchWithIndex(final long nativeHandle) {
    super(nativeHandle);
    disOwnNativeHandle();
  }

  /**
   * Create an iterator of a column family. User can call
   * {@link org.rocksdb.RocksIteratorInterface#seek(byte[])} to
   * search to the next entry of or after a key. Keys will be iterated in the
   * order given by index_comparator. For multiple updates on the same key,
   * each update will be returned as a separate entry, in the order of update
   * time.
   *
   * @param columnFamilyHandle The column family to iterate over
   * @return An iterator for the Write Batch contents, restricted to the column
   * family
   */
  public WBWIRocksIterator newIterator(
      final ColumnFamilyHandle columnFamilyHandle) {
    return new WBWIRocksIterator(this, iterator1(nativeHandle_,
            columnFamilyHandle.nativeHandle_));
  }

  /**
   * Create an iterator of the default column family. User can call
   * {@link org.rocksdb.RocksIteratorInterface#seek(byte[])} to
   * search to the next entry of or after a key. Keys will be iterated in the
   * order given by index_comparator. For multiple updates on the same key,
   * each update will be returned as a separate entry, in the order of update
   * time.
   *
   * @return An iterator for the Write Batch contents
   */
  public WBWIRocksIterator newIterator() {
    return new WBWIRocksIterator(this, iterator0(nativeHandle_));
  }

  /**
   * Provides Read-Your-Own-Writes like functionality by
   * creating a new Iterator that will use {@link org.rocksdb.WBWIRocksIterator}
   * as a delta and baseIterator as a base
   * <p>
   * Updating write batch with the current key of the iterator is not safe.
   * We strongly recommend users not to do it. It will invalidate the current
   * key() and value() of the iterator. This invalidation happens even before
   * the write batch update finishes. The state may recover after Next() is
   * called.
   *
   * @param columnFamilyHandle The column family to iterate over
   * @param baseIterator The base iterator,
   *   e.g. {@link org.rocksdb.RocksDB#newIterator()}
   * @return An iterator which shows a view comprised of both the database
   * point-in-time from baseIterator and modifications made in this write batch.
   */
  public RocksIterator newIteratorWithBase(
      final ColumnFamilyHandle columnFamilyHandle,
      final RocksIterator baseIterator) {
    return newIteratorWithBase(columnFamilyHandle, baseIterator, null);
  }

  /**
   * Provides Read-Your-Own-Writes like functionality by
   * creating a new Iterator that will use {@link org.rocksdb.WBWIRocksIterator}
   * as a delta and baseIterator as a base
   * <p>
   * Updating write batch with the current key of the iterator is not safe.
   * We strongly recommend users not to do it. It will invalidate the current
   * key() and value() of the iterator. This invalidation happens even before
   * the write batch update finishes. The state may recover after Next() is
   * called.
   *
   * @param columnFamilyHandle The column family to iterate over
   * @param baseIterator The base iterator,
   *   e.g. {@link org.rocksdb.RocksDB#newIterator()}
   * @param readOptions the read options, or null
   * @return An iterator which shows a view comprised of both the database
   * point-in-time from baseIterator and modifications made in this write batch.
   */
  public RocksIterator newIteratorWithBase(final ColumnFamilyHandle columnFamilyHandle,
      final RocksIterator baseIterator, /* @Nullable */ final ReadOptions readOptions) {
    final RocksIterator iterator = new RocksIterator(baseIterator.parent_,
        iteratorWithBase(nativeHandle_, columnFamilyHandle.nativeHandle_,
            baseIterator.nativeHandle_, readOptions == null ? 0 : readOptions.nativeHandle_));

    // when the iterator is deleted it will also delete the baseIterator
    baseIterator.disOwnNativeHandle();

    return iterator;
  }

  /**
   * Provides Read-Your-Own-Writes like functionality by
   * creating a new Iterator that will use {@link org.rocksdb.WBWIRocksIterator}
   * as a delta and baseIterator as a base. Operates on the default column
   * family.
   *
   * @param baseIterator The base iterator,
   *   e.g. {@link org.rocksdb.RocksDB#newIterator()}
   * @return An iterator which shows a view comprised of both the database
   * point-in-time from baseIterator and modifications made in this write batch.
   */
  public RocksIterator newIteratorWithBase(final RocksIterator baseIterator) {
    return newIteratorWithBase(baseIterator.parent_.getDefaultColumnFamily(), baseIterator, null);
  }

  /**
   * Provides Read-Your-Own-Writes like functionality by
   * creating a new Iterator that will use {@link org.rocksdb.WBWIRocksIterator}
   * as a delta and baseIterator as a base. Operates on the default column
   * family.
   *
   * @param baseIterator The base iterator,
   *   e.g. {@link org.rocksdb.RocksDB#newIterator()}
   * @param readOptions the read options, or null
   * @return An iterator which shows a view comprised of both the database
   * point-in-time from baseIterator and modifications made in this write batch.
   */
  public RocksIterator newIteratorWithBase(final RocksIterator baseIterator,
      /* @Nullable */ final ReadOptions readOptions) {
    return newIteratorWithBase(
        baseIterator.parent_.getDefaultColumnFamily(), baseIterator, readOptions);
  }

  /**
   * Similar to {@link RocksDB#get(ColumnFamilyHandle, byte[])} but will only
   * read the key from this batch.
   *
   * @param columnFamilyHandle The column family to retrieve the value from
   * @param options The database options to use
   * @param key The key to read the value for
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException if the batch does not have enough data to resolve
   * Merge operations, MergeInProgress status may be returned.
   */
  public byte[] getFromBatch(final ColumnFamilyHandle columnFamilyHandle,
      final DBOptions options, final byte[] key) throws RocksDBException {
    return getFromBatch(nativeHandle_, options.nativeHandle_,
        key, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Similar to {@link RocksDB#get(byte[])} but will only
   * read the key from this batch.
   *
   * @param options The database options to use
   * @param key The key to read the value for
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException if the batch does not have enough data to resolve
   * Merge operations, MergeInProgress status may be returned.
   */
  public byte[] getFromBatch(final DBOptions options, final byte[] key)
      throws RocksDBException {
    return getFromBatch(nativeHandle_, options.nativeHandle_, key, key.length);
  }

  /**
   * Similar to {@link RocksDB#get(ColumnFamilyHandle, byte[])} but will also
   * read writes from this batch.
   * <p>
   * This function will query both this batch and the DB and then merge
   * the results using the DB's merge operator (if the batch contains any
   * merge requests).
   * <p>
   * Setting {@link ReadOptions#setSnapshot(Snapshot)} will affect what is
   * read from the DB but will NOT change which keys are read from the batch
   * (the keys in this batch do not yet belong to any snapshot and will be
   * fetched regardless).
   *
   * @param db The Rocks database
   * @param columnFamilyHandle The column family to retrieve the value from
   * @param options The read options to use
   * @param key The key to read the value for
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException if the value for the key cannot be read
   */
  public byte[] getFromBatchAndDB(final RocksDB db, final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions options, final byte[] key) throws RocksDBException {
    return getFromBatchAndDB(nativeHandle_, db.nativeHandle_,
        options.nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Similar to {@link RocksDB#get(byte[])} but will also
   * read writes from this batch.
   * <p>
   * This function will query both this batch and the DB and then merge
   * the results using the DB's merge operator (if the batch contains any
   * merge requests).
   * <p>
   * Setting {@link ReadOptions#setSnapshot(Snapshot)} will affect what is
   * read from the DB but will NOT change which keys are read from the batch
   * (the keys in this batch do not yet belong to any snapshot and will be
   * fetched regardless).
   *
   * @param db The Rocks database
   * @param options The read options to use
   * @param key The key to read the value for
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException if the value for the key cannot be read
   */
  public byte[] getFromBatchAndDB(final RocksDB db, final ReadOptions options,
      final byte[] key) throws RocksDBException {
    return getFromBatchAndDB(nativeHandle_, db.nativeHandle_,
        options.nativeHandle_, key, key.length);
  }

  public Status getEntityFromBatch(final ColumnFamilyHandle columnFamily, final ByteBuffer key,
      final List<WideColumn.ByteBufferWideColumn> columns) throws RocksDBException {
    ByteBuffer[] names = new ByteBuffer[columns.size()];
    ByteBuffer[] values = new ByteBuffer[columns.size()];
    int[] namesRequiredSize = new int[columns.size()];
    int[] valuesRequiredSize = new int[columns.size()];

    int[] namesOffset = new int[columns.size()];
    int[] namesRemaining = new int[columns.size()];
    int[] valuesOffset = new int[columns.size()];
    int[] valuesRemaining = new int[columns.size()];

    for (int i = 0; i < names.length; i++) {
      names[i] = columns.get(i).getName();
      namesOffset[i] = names[i].position();
      namesRemaining[i] = names[i].remaining();

      values[i] = columns.get(i).getValue();
      valuesOffset[i] = values[i].position();
      valuesRemaining[i] = values[i].remaining();
    }

    Status status = getEntityFromBatchDirect(nativeHandle_, key, key.position(), key.remaining(),
        names, namesOffset, namesRemaining, namesRequiredSize, values, valuesOffset,
        valuesRemaining, valuesRequiredSize, columnFamily.nativeHandle_);

    if (status != null && status.getCode() == Status.Code.Ok) {
      for (int i = 0; i < namesRequiredSize.length; i++) {
        columns.get(i).setNameRequiredSize(namesRequiredSize[i]);

        if (namesRequiredSize[i] > names[i].remaining()) {
          names[i].position(names[i].position() + names[i].remaining()); // Fully filled buffer
        } else {
          names[i].position(names[i].position() + namesRequiredSize[i]);
        }

        columns.get(i).setValueRequiredSize(valuesRequiredSize[i]);
        if (valuesRequiredSize[i] > values[i].remaining()) {
          values[i].position(values[i].position() + values[i].remaining());
        } else {
          values[i].position(values[i].position() + valuesRequiredSize[i]);
        }
      }
    }
    return status;
  }
  public Status getEntityFromBatch(final ColumnFamilyHandle columnFamily, final byte[] key,
      int keyOffset, int keyLength, final List<WideColumn<byte[]>> values) throws RocksDBException {
    CheckBounds(keyOffset, keyLength, key.length);

    RocksDB.GetEntityResult result = new RocksDB.GetEntityResult();
    getEntityFromBatch(nativeHandle_, key, keyOffset, keyLength, result,
        columnFamily == null ? 0 : columnFamily.nativeHandle_);

    if (result.status.getCode() == Status.Code.Ok) {
      for (int i = 0; i < result.names.length; i++) {
        values.add(new WideColumn<>(result.names[i], result.values[i]));
      }
    }
    return result.status;
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

  @Override
  void putEntity(long nativeHandle, byte[] key, int keyOffset, int keyLength, byte[][] names,
      byte[][] values, long cfHandle) throws RocksDBException {
    putEntityJni(nativeHandle, key, keyOffset, keyLength, names, values, cfHandle);
  }
  private static native void putEntityJni(long nativeHandle, byte[] key, int keyOffset,
      int keyLength, byte[][] names, byte[][] values, long cfHandle) throws RocksDBException;

  private static native void putJni(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle);

  @Override
  final void putDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final ByteBuffer value, final int valueOffset, final int valueLength,
      final long cfHandle) {
    putDirectJni(handle, key, keyOffset, keyLength, value, valueOffset, valueLength, cfHandle);
  }

  @Override
  void putEntityDirect(long nativeHandle, ByteBuffer key, int keyOffset, int keyLength,
      ByteBuffer[] names, int[] namesOffset, int[] namesLength, ByteBuffer[] values,
      int[] valuesOffset, int[] valuesLength, long cfHandle) throws RocksDBException {
    putEntityDirectJni(nativeHandle, key, keyOffset, keyLength, names, namesOffset, namesLength,
        values, valuesOffset, valuesLength, cfHandle);
  }

  private static native void putEntityDirectJni(long nativeHandle, ByteBuffer key, int keyOffset,
      int keyLength, ByteBuffer[] names, int[] namesOffset, int[] namesLength, ByteBuffer[] values,
      int[] valuesOffset, int[] valuesLength, long cfHandle) throws RocksDBException;

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

  // DO NOT USE - `WriteBatchWithIndex::deleteRange` is not yet supported
  @Override
  final void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen) {
    deleteRangeJni(handle, beginKey, beginKeyLen, endKey, endKeyLen);
  }

  private static native void deleteRangeJni(final long handle, final byte[] beginKey,
      final int beginKeyLen, final byte[] endKey, final int endKeyLen);

  // DO NOT USE - `WriteBatchWithIndex::deleteRange` is not yet supported
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

  @Override
  final WriteBatch getWriteBatch(final long handle) {
    return getWriteBatchJni(handle);
  }

  private static native WriteBatch getWriteBatchJni(final long handle);

  private static native long newWriteBatchWithIndex();
  private static native long newWriteBatchWithIndex(final boolean overwriteKey);
  private static native long newWriteBatchWithIndex(final long fallbackIndexComparatorHandle,
      final byte comparatorType, final int reservedBytes, final boolean overwriteKey);
  private static native long iterator0(final long handle);
  private static native long iterator1(final long handle, final long cfHandle);
  private static native long iteratorWithBase(final long handle, final long cfHandle,
      final long baseIteratorHandle, final long readOptionsHandle);
  private static native byte[] getFromBatch(
      final long handle, final long optHandle, final byte[] key, final int keyLen);
  private static native byte[] getFromBatch(final long handle, final long optHandle,
      final byte[] key, final int keyLen, final long cfHandle);
  private static native byte[] getFromBatchAndDB(final long handle, final long dbHandle,
      final long readOptHandle, final byte[] key, final int keyLen);
  private static native byte[] getFromBatchAndDB(final long handle, final long dbHandle,
      final long readOptHandle, final byte[] key, final int keyLen, final long cfHandle);

  private static native void getEntityFromBatch(final long handle, final byte[] key, int keyOffset,
      int keyLength, final RocksDB.GetEntityResult result, final long cfHandle)
      throws RocksDBException;

  private static native Status getEntityFromBatchDirect(long nativeHandle, ByteBuffer key,
      int position, int remaining, ByteBuffer[] names, int[] namesOffset, int[] namesRemaining,
      int[] namesRequiredSize, ByteBuffer[] values, int[] valuesOffset, int[] valuesRemaining,
      int[] valuesRequiredSize, long columnFamilyHandle) throws RocksDBException;
}
