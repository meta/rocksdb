// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.rocksdb.RocksDB.PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Provides BEGIN/COMMIT/ROLLBACK transactions.
 * <p>
 * To use transactions, you must first create either an
 * {@link OptimisticTransactionDB} or a {@link TransactionDB}
 *
 * To create a transaction, use
 * {@link OptimisticTransactionDB#beginTransaction(org.rocksdb.WriteOptions)} or
 * {@link TransactionDB#beginTransaction(org.rocksdb.WriteOptions)}
 *
 * It is up to the caller to synchronize access to this object.
 * <p>
 * See samples/src/main/java/OptimisticTransactionSample.java and
 * samples/src/main/java/TransactionSample.java for some simple
 * examples.
 */
public class Transaction extends RocksObject {
  private static final String FOR_EACH_KEY_THERE_MUST_BE_A_COLUMNFAMILYHANDLE =
      "For each key there must be a ColumnFamilyHandle.";

  private static final String BB_ALL_DIRECT_OR_INDIRECT =
      "ByteBuffer parameters must all be direct, or must all be indirect";
  private final RocksDB parent;
  private final ColumnFamilyHandle defaultColumnFamilyHandle;

  /**
   * Intentionally package private
   * as this is called from
   * {@link OptimisticTransactionDB#beginTransaction(org.rocksdb.WriteOptions)}
   * or {@link TransactionDB#beginTransaction(org.rocksdb.WriteOptions)}
   *
   * @param parent This must be either {@link TransactionDB} or
   *     {@link OptimisticTransactionDB}
   * @param transactionHandle The native handle to the underlying C++
   *     transaction object
   */
  Transaction(final RocksDB parent, final long transactionHandle) {
    super(transactionHandle);
    this.parent = parent;
    this.defaultColumnFamilyHandle = parent.getDefaultColumnFamily();
  }

  /**
   * If a transaction has a snapshot set, the transaction will ensure that
   * any keys successfully written (or fetched via {@link #getForUpdate}) have
   * not been modified outside of this transaction since the time the snapshot
   * was set.
   * <p>
   * If a snapshot has not been set, the transaction guarantees that keys have
   * not been modified since the time each key was first written (or fetched via
   * {@link #getForUpdate}).
   * <p>
   * Using {@code #setSnapshot()} will provide stricter isolation guarantees
   * at the expense of potentially more transaction failures due to conflicts
   * with other writes.
   * <p>
   * Calling {@code #setSnapshot()} has no effect on keys written before this
   * function has been called.
   * <p>
   * {@code #setSnapshot()} may be called multiple times if you would like to
   * change the snapshot used for different operations in this transaction.
   * <p>
   * Calling {@code #setSnapshot()} will not affect the version of Data returned
   * by get(...) methods. See {@link #get} for more details.
   */
  public void setSnapshot() {
    assert(isOwningHandle());
    setSnapshot(nativeHandle_);
  }

  /**
   * Similar to {@link #setSnapshot()}, but will not change the current snapshot
   * until put/merge/delete/getForUpdate/multiGetForUpdate is called.
   * By calling this function, the transaction will essentially call
   * {@link #setSnapshot()} for you right before performing the next
   * write/getForUpdate.
   * <p>
   * Calling {@code #setSnapshotOnNextOperation()} will not affect what
   * snapshot is returned by {@link #getSnapshot} until the next
   * write/getForUpdate is executed.
   * <p>
   * When the snapshot is created the notifier's snapshotCreated method will
   * be called so that the caller can get access to the snapshot.
   * <p>
   * This is an optimization to reduce the likelihood of conflicts that
   * could occur in between the time {@link #setSnapshot()} is called and the
   * first write/getForUpdate operation. i.e. this prevents the following
   * race-condition:
   * <p>
   *   txn1-&gt;setSnapshot();
   *                             txn2-&gt;put("A", ...);
   *                             txn2-&gt;commit();
   *   txn1-&gt;getForUpdate(opts, "A", ...);  * FAIL!
   */
  public void setSnapshotOnNextOperation() {
    assert(isOwningHandle());
    setSnapshotOnNextOperation(nativeHandle_);
  }

  /**
   * Similar to {@link #setSnapshot()}, but will not change the current snapshot
   * until put/merge/delete/getForUpdate/multiGetForUpdate is called.
   * By calling this function, the transaction will essentially call
   * {@link #setSnapshot()} for you right before performing the next
   * write/getForUpdate.
   * <p>
   * Calling {@link #setSnapshotOnNextOperation()} will not affect what
   * snapshot is returned by {@link #getSnapshot} until the next
   * write/getForUpdate is executed.
   * <p>
   * When the snapshot is created the
   * {@link AbstractTransactionNotifier#snapshotCreated(Snapshot)} method will
   * be called so that the caller can get access to the snapshot.
   * <p>
   * This is an optimization to reduce the likelihood of conflicts that
   * could occur in between the time {@link #setSnapshot()} is called and the
   * first write/getForUpdate operation. i.e. this prevents the following
   * race-condition:
   * <p>
   *   txn1-&gt;setSnapshot();
   *                             txn2-&gt;put("A", ...);
   *                             txn2-&gt;commit();
   *   txn1-&gt;getForUpdate(opts, "A", ...);  * FAIL!
   *
   * @param transactionNotifier A handler for receiving snapshot notifications
   *     for the transaction
   *
   */
  public void setSnapshotOnNextOperation(
      final AbstractTransactionNotifier transactionNotifier) {
    assert(isOwningHandle());
    setSnapshotOnNextOperation(nativeHandle_, transactionNotifier.nativeHandle_);
  }

  /**
   * Returns the Snapshot created by the last call to {@link #setSnapshot()}.
   * <p>
   * REQUIRED: The returned Snapshot is only valid up until the next time
   * {@link #setSnapshot()}/{@link #setSnapshotOnNextOperation()} is called,
   * {@link #clearSnapshot()} is called, or the Transaction is deleted.
   *
   * @return The snapshot or null if there is no snapshot
   */
  public Snapshot getSnapshot() {
    assert(isOwningHandle());
    final long snapshotNativeHandle = getSnapshot(nativeHandle_);
    if(snapshotNativeHandle == 0) {
      return null;
    } else {
      return new Snapshot(snapshotNativeHandle);
    }
  }

  /**
   * Clears the current snapshot (i.e. no snapshot will be 'set')
   * <p>
   * This removes any snapshot that currently exists or is set to be created
   * on the next update operation ({@link #setSnapshotOnNextOperation()}).
   * <p>
   * Calling {@code #clearSnapshot()} has no effect on keys written before this
   * function has been called.
   * <p>
   * If a reference to a snapshot was retrieved via {@link #getSnapshot()}, it
   * will no longer be valid and should be discarded after a call to
   * {@code #clearSnapshot()}.
   */
  public void clearSnapshot() {
    assert(isOwningHandle());
    clearSnapshot(nativeHandle_);
  }

  /**
   * Prepare the current transaction for 2PC
   * @throws RocksDBException if an error occurs when preparing the transaction
   */
  public void prepare() throws RocksDBException {
    //TODO(AR) consider a Java'ish version of this function, which returns an AutoCloseable (commit)
    assert(isOwningHandle());
    prepare(nativeHandle_);
  }

  /**
   * Write all batched keys to the db atomically.
   * <p>
   * Returns OK on success.
   * <p>
   * May return any error status that could be returned by DB:Write().
   * <p>
   * If this transaction was created by an {@link OptimisticTransactionDB}
   * Status::Busy() may be returned if the transaction could not guarantee
   * that there are no write conflicts. Status::TryAgain() may be returned
   * if the memtable history size is not large enough
   *  (See max_write_buffer_size_to_maintain).
   * <p>
   * If this transaction was created by a {@link TransactionDB},
   * Status::Expired() may be returned if this transaction has lived for
   * longer than {@link TransactionOptions#getExpiration()}.
   *
   * @throws RocksDBException if an error occurs when committing the transaction
   */
  public void commit() throws RocksDBException {
    assert(isOwningHandle());
    commit(nativeHandle_);
  }

  /**
   * Discard all batched writes in this transaction.
   *
   * @throws RocksDBException if an error occurs when rolling back the transaction
   */
  public void rollback() throws RocksDBException {
    assert(isOwningHandle());
    rollback(nativeHandle_);
  }

  /**
   * Records the state of the transaction for future calls to
   * {@link #rollbackToSavePoint()}.
   * <p>
   * May be called multiple times to set multiple save points.
   *
   * @throws RocksDBException if an error occurs whilst setting a save point
   */
  public void setSavePoint() throws RocksDBException {
    assert(isOwningHandle());
    setSavePoint(nativeHandle_);
  }

  /**
   * Undo all operations in this transaction (put, merge, delete, putLogData)
   * since the most recent call to {@link #setSavePoint()} and removes the most
   * recent {@link #setSavePoint()}.
   * <p>
   * If there is no previous call to {@link #setSavePoint()},
   * returns Status::NotFound()
   *
   * @throws RocksDBException if an error occurs when rolling back to a save point
   */
  public void rollbackToSavePoint() throws RocksDBException {
    assert(isOwningHandle());
    rollbackToSavePoint(nativeHandle_);
  }

  /**
   * This function has an inconsistent parameter order compared to other {@code get()}
   * methods and is deprecated in favour of one with a consistent order.
   *
   * This function is similar to
   * {@link RocksDB#get(ColumnFamilyHandle, ReadOptions, byte[])} except it will
   * also read pending changes in this transaction.
   * Currently, this function will return Status::MergeInProgress if the most
   * recent write to the queried key in this batch is a Merge.
   * <p>
   * If {@link ReadOptions#snapshot()} is not set, the current version of the
   * key will be read. Calling {@link #setSnapshot()} does not affect the
   * version of the data returned.
   * <p>
   * Note that setting {@link ReadOptions#setSnapshot(Snapshot)} will affect
   * what is read from the DB but will NOT change which keys are read from this
   * transaction (the keys in this transaction do not yet belong to any snapshot
   * and will be fetched regardless).
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance
   * @param readOptions Read options.
   * @param key the key to retrieve the value for.
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  @Deprecated
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle, final ReadOptions readOptions,
      final byte[] key) throws RocksDBException {
    assert (isOwningHandle());
    return get(nativeHandle_, readOptions.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * This function is similar to
   * {@link RocksDB#get(ColumnFamilyHandle, ReadOptions, byte[])} except it will
   * also read pending changes in this transaction.
   * Currently, this function will return Status::MergeInProgress if the most
   * recent write to the queried key in this batch is a Merge.
   *
   * If {@link ReadOptions#snapshot()} is not set, the current version of the
   * key will be read. Calling {@link #setSnapshot()} does not affect the
   * version of the data returned.
   *
   * Note that setting {@link ReadOptions#setSnapshot(Snapshot)} will affect
   * what is read from the DB but will NOT change which keys are read from this
   * transaction (the keys in this transaction do not yet belong to any snapshot
   * and will be fetched regardless).
   *
   * @param readOptions Read options.
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance
   * @param key the key to retrieve the value for.
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  public byte[] get(final ReadOptions readOptions, final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    assert (isOwningHandle());
    return get(nativeHandle_, readOptions.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * This function is similar to
   * {@link RocksDB#get(ReadOptions, byte[])} except it will
   * also read pending changes in this transaction.
   * Currently, this function will return Status::MergeInProgress if the most
   * recent write to the queried key in this batch is a Merge.
   * <p>
   * If {@link ReadOptions#snapshot()} is not set, the current version of the
   * key will be read. Calling {@link #setSnapshot()} does not affect the
   * version of the data returned.
   * <p>
   * Note that setting {@link ReadOptions#setSnapshot(Snapshot)} will affect
   * what is read from the DB but will NOT change which keys are read from this
   * transaction (the keys in this transaction do not yet belong to any snapshot
   * and will be fetched regardless).
   *
   * @param readOptions Read options.
   * @param key the key to retrieve the value for.
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  public byte[] get(final ReadOptions readOptions, final byte[] key)
      throws RocksDBException {
    assert(isOwningHandle());
    return get(nativeHandle_, readOptions.nativeHandle_, key, 0, key.length,
        defaultColumnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key in the default column family
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return A {@link GetStatus} wrapping the result status and the return value size.
   *     If {@code GetStatus.status} is {@code Ok} then {@code GetStatus.requiredSize} contains
   *     the size of the actual value that matches the specified
   *     {@code key} in byte. If {@code GetStatus.requiredSize} is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and a partial result was
   *     returned. If {@code GetStatus.status} is {@code NotFound} this indicates that
   *     the value was not found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public GetStatus get(final ReadOptions opt, final byte[] key, final byte[] value)
      throws RocksDBException {
    final int result = get(nativeHandle_, opt.nativeHandle_, key, 0, key.length, value, 0,
        value.length, defaultColumnFamilyHandle.nativeHandle_);
    if (result < 0) {
      return GetStatus.fromStatusCode(Status.Code.NotFound, 0);
    } else {
      return GetStatus.fromStatusCode(Status.Code.Ok, result);
    }
  }

  /**
   * Get the value associated with the specified key in a specified column family
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param columnFamilyHandle the column family to find the key in
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return A {@link GetStatus} wrapping the result status and the return value size.
   *     If {@code GetStatus.status} is {@code Ok} then {@code GetStatus.requiredSize} contains
   *     the size of the actual value that matches the specified
   *     {@code key} in byte. If {@code GetStatus.requiredSize} is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and a partial result was
   *     returned. If {@code GetStatus.status} is {@code NotFound} this indicates that
   *     the value was not found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public GetStatus get(final ReadOptions opt, final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    final int result = get(nativeHandle_, opt.nativeHandle_, key, 0, key.length, value, 0,
        value.length, columnFamilyHandle.nativeHandle_);
    if (result < 0) {
      return GetStatus.fromStatusCode(Status.Code.NotFound, 0);
    } else {
      return GetStatus.fromStatusCode(Status.Code.Ok, result);
    }
  }

  /**
   * Get the value associated with the specified key within the specified column family.
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param columnFamilyHandle the column family in which to find the key.
   * @param key the key to retrieve the value. It is using position and limit.
   *     Supports direct buffer only.
   * @param value the out-value to receive the retrieved value.
   *     It is using position and limit. Limit is set according to value size.
   *     Supports direct buffer only.
   * @return A {@link GetStatus} wrapping the result status and the return value size.
   *     If {@code GetStatus.status} is {@code Ok} then {@code GetStatus.requiredSize} contains
   *     the size of the actual value that matches the specified
   *     {@code key} in byte. If {@code GetStatus.requiredSize} is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and a partial result was
   *     returned. If {@code GetStatus.status} is {@code NotFound} this indicates that
   *     the value was not found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public GetStatus get(final ReadOptions opt, final ColumnFamilyHandle columnFamilyHandle,
      final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    final int result;
    if (key.isDirect() && value.isDirect()) {
      result = getDirect(nativeHandle_, opt.nativeHandle_, key, key.position(), key.remaining(),
          value, value.position(), value.remaining(), columnFamilyHandle.nativeHandle_);
    } else if (!key.isDirect() && !value.isDirect()) {
      assert key.hasArray();
      assert value.hasArray();
      result =
          get(nativeHandle_, opt.nativeHandle_, key.array(), key.arrayOffset() + key.position(),
              key.remaining(), value.array(), value.arrayOffset() + value.position(),
              value.remaining(), columnFamilyHandle.nativeHandle_);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }

    key.position(key.limit());
    if (result < 0) {
      return GetStatus.fromStatusCode(Status.Code.NotFound, 0);
    } else {
      value.position(Math.min(value.limit(), value.position() + result));
      return GetStatus.fromStatusCode(Status.Code.Ok, result);
    }
  }

  /**
   * Get the value associated with the specified key within the default column family.
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value. It is using position and limit.
   *     Supports direct buffer only.
   * @param value the out-value to receive the retrieved value.
   *     It is using position and limit. Limit is set according to value size.
   *     Supports direct buffer only.
   * @return A {@link GetStatus} wrapping the result status and the return value size.
   *     If {@code GetStatus.status} is {@code Ok} then {@code GetStatus.requiredSize} contains
   *     the size of the actual value that matches the specified
   *     {@code key} in byte. If {@code GetStatus.requiredSize} is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and a partial result was
   *     returned. If {@code GetStatus.status} is {@code NotFound} this indicates that
   *     the value was not found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public GetStatus get(final ReadOptions opt, final ByteBuffer key, final ByteBuffer value)
      throws RocksDBException {
    return get(opt, this.defaultColumnFamilyHandle, key, value);
  }

  /**
   * This function is similar to
   * {@link RocksDB#multiGetAsList} except it will
   * also read pending changes in this transaction.
   * Currently, this function will return Status::MergeInProgress if the most
   * recent write to the queried key in this batch is a Merge.
   * <p>
   * If {@link ReadOptions#snapshot()} is not set, the current version of the
   * key will be read. Calling {@link #setSnapshot()} does not affect the
   * version of the data returned.
   * <p>
   * Note that setting {@link ReadOptions#setSnapshot(Snapshot)} will affect
   * what is read from the DB but will NOT change which keys are read from this
   * transaction (the keys in this transaction do not yet belong to any snapshot
   * and will be fetched regardless).
   *
   * @param readOptions Read options.
   * @param columnFamilyHandles {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys of keys for which values need to be retrieved.
   *
   * @return Array of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   */
  @Deprecated
  public byte[][] multiGet(final ReadOptions readOptions,
      final List<ColumnFamilyHandle> columnFamilyHandles, final byte[][] keys)
      throws RocksDBException {
    assert(isOwningHandle());
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.length != columnFamilyHandles.size()) {
      throw new IllegalArgumentException(FOR_EACH_KEY_THERE_MUST_BE_A_COLUMNFAMILYHANDLE);
    }
    if(keys.length == 0) {
      return new byte[0][0];
    }
    final long[] cfHandles = new long[columnFamilyHandles.size()];
    for (int i = 0; i < columnFamilyHandles.size(); i++) {
      cfHandles[i] = columnFamilyHandles.get(i).nativeHandle_;
    }

    return multiGet(nativeHandle_, readOptions.nativeHandle_,
       keys, cfHandles);
  }

  /**
   * This function is similar to
   * {@link RocksDB#multiGetAsList(ReadOptions, List, List)} except it will
   * also read pending changes in this transaction.
   * Currently, this function will return Status::MergeInProgress if the most
   * recent write to the queried key in this batch is a Merge.
   * <p>
   * If {@link ReadOptions#snapshot()} is not set, the current version of the
   * key will be read. Calling {@link #setSnapshot()} does not affect the
   * version of the data returned.
   * <p>
   * Note that setting {@link ReadOptions#setSnapshot(Snapshot)} will affect
   * what is read from the DB but will NOT change which keys are read from this
   * transaction (the keys in this transaction do not yet belong to any snapshot
   * and will be fetched regardless).
   *
   * @param readOptions Read options.
   * @param columnFamilyHandles {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys of keys for which values need to be retrieved.
   *
   * @return Array of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   */

  public List<byte[]> multiGetAsList(final ReadOptions readOptions,
      final List<ColumnFamilyHandle> columnFamilyHandles, final List<byte[]> keys)
      throws RocksDBException {
    assert (isOwningHandle());
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size() != columnFamilyHandles.size()) {
      throw new IllegalArgumentException(FOR_EACH_KEY_THERE_MUST_BE_A_COLUMNFAMILYHANDLE);
    }
    if (keys.isEmpty()) {
      return new ArrayList<>(0);
    }
    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final long[] cfHandles = new long[columnFamilyHandles.size()];
    for (int i = 0; i < columnFamilyHandles.size(); i++) {
      cfHandles[i] = columnFamilyHandles.get(i).nativeHandle_;
    }

    return Arrays.asList(multiGet(nativeHandle_, readOptions.nativeHandle_, keysArray, cfHandles));
  }

  /**
   * This function is similar to
   * {@link RocksDB#multiGetAsList} except it will
   * also read pending changes in this transaction.
   * Currently, this function will return Status::MergeInProgress if the most
   * recent write to the queried key in this batch is a Merge.
   * <p>
   * If {@link ReadOptions#snapshot()} is not set, the current version of the
   * key will be read. Calling {@link #setSnapshot()} does not affect the
   * version of the data returned.
   * <p>
   * Note that setting {@link ReadOptions#setSnapshot(Snapshot)} will affect
   * what is read from the DB but will NOT change which keys are read from this
   * transaction (the keys in this transaction do not yet belong to any snapshot
   * and will be fetched regardless).
   *
   * @param readOptions Read options.=
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys of keys for which values need to be retrieved.
   *
   * @return Array of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  @Deprecated
  public byte[][] multiGet(final ReadOptions readOptions, final byte[][] keys)
      throws RocksDBException {
    assert(isOwningHandle());
    if(keys.length == 0) {
      return new byte[0][0];
    }

    return multiGet(nativeHandle_, readOptions.nativeHandle_,
        keys);
  }

  /**
   * This function is similar to
   * {@link RocksDB#multiGetAsList} except it will
   * also read pending changes in this transaction.
   * Currently, this function will return Status::MergeInProgress if the most
   * recent write to the queried key in this batch is a Merge.
   * <p>
   * If {@link ReadOptions#snapshot()} is not set, the current version of the
   * key will be read. Calling {@link #setSnapshot()} does not affect the
   * version of the data returned.
   * <p>
   * Note that setting {@link ReadOptions#setSnapshot(Snapshot)} will affect
   * what is read from the DB but will NOT change which keys are read from this
   * transaction (the keys in this transaction do not yet belong to any snapshot
   * and will be fetched regardless).
   *
   * @param readOptions Read options.=
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys of keys for which values need to be retrieved.
   *
   * @return Array of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<byte[]> multiGetAsList(final ReadOptions readOptions, final List<byte[]> keys)
      throws RocksDBException {
    if (keys.isEmpty()) {
      return new ArrayList<>(0);
    }
    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);

    return Arrays.asList(multiGet(nativeHandle_, readOptions.nativeHandle_, keysArray));
  }

  /**
   * This function is similar to
   * {@link RocksDB#multiGetAsList} except it will
   * also read pending changes in this transaction.
   * Currently, this function will return Status::MergeInProgress if the most
   * recent write to the queried key in this batch is a Merge.
   * <p>
   * If {@link ReadOptions#snapshot()} is not set, the current version of the
   * key will be read. Calling {@link #setSnapshot()} does not affect the
   * version of the data returned.
   * <p>
   * Note that setting {@link ReadOptions#setSnapshot(Snapshot)} will affect
   * what is read from the DB but will NOT change which keys are read from this
   * transaction (the keys in this transaction do not yet belong to any snapshot
   * and will be fetched regardless).
   * <p>
   * This method uses the optimized path with support for batched reads.
   *
   * @param readOptions Read options.=
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param keys of keys for which values need to be retrieved.
   *
   * @return Array of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<byte[]> multiGetAsList(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final List<byte[]> keys)
      throws RocksDBException {
    if (keys.isEmpty()) {
      return new ArrayList<>(0);
    }
    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    return Arrays.asList(multiGet(
        nativeHandle_, readOptions.nativeHandle_, columnFamilyHandle.nativeHandle_, keysArray));
  }

  /**
   * Read this key and ensure that this transaction will only
   * be able to be committed if this key is not written outside this
   * transaction after it has first been read (or after the snapshot if a
   * snapshot is set in this transaction). The transaction behavior is the
   * same regardless of whether the key exists or not.
   * <p>
   * Note: Currently, this function will return Status::MergeInProgress
   * if the most recent write to the queried key in this batch is a Merge.
   * <p>
   * The values returned by this function are similar to
   * {@link RocksDB#get(ColumnFamilyHandle, ReadOptions, byte[])}.
   * If value==nullptr, then this function will not read any data, but will
   * still ensure that this key cannot be written to by outside of this
   * transaction.
   * <p>
   * If this transaction was created by an {@link OptimisticTransactionDB},
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}
   * could cause {@link #commit()} to fail. Otherwise, it could return any error
   * that could be returned by
   * {@link RocksDB#get(ColumnFamilyHandle, ReadOptions, byte[])}.
   * <p>
   * If this transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *     {@link Status.Code#MergeInProgress} if merge operations cannot be
   *     resolved.
   *
   * @param readOptions Read options.
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value for.
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   * @param doValidate true if it should validate the snapshot before doing the read
   *
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] getForUpdate(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final boolean exclusive,
      final boolean doValidate) throws RocksDBException {
    assert (isOwningHandle());
    return getForUpdate(nativeHandle_, readOptions.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_, exclusive, doValidate);
  }

  /**
   * Same as
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean, boolean)}
   * with doValidate=true.
   *
   * @param readOptions Read options.
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value for.
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   *
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] getForUpdate(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final boolean exclusive) throws RocksDBException {
    assert(isOwningHandle());
    return getForUpdate(nativeHandle_, readOptions.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_, exclusive, true /*doValidate*/);
  }

  /**
   * Read this key and ensure that this transaction will only
   * be able to be committed if this key is not written outside this
   * transaction after it has first been read (or after the snapshot if a
   * snapshot is set in this transaction). The transaction behavior is the
   * same regardless of whether the key exists or not.
   * <p>
   * Note: Currently, this function will return Status::MergeInProgress
   * if the most recent write to the queried key in this batch is a Merge.
   * <p>
   * The values returned by this function are similar to
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * If value==nullptr, then this function will not read any data, but will
   * still ensure that this key cannot be written to by outside of this
   * transaction.
   * <p>
   * If this transaction was created on an {@link OptimisticTransactionDB},
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}
   * could cause {@link #commit()} to fail. Otherwise, it could return any error
   * that could be returned by
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * <p>
   * If this transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *     {@link Status.Code#MergeInProgress} if merge operations cannot be
   *     resolved.
   *
   * @param readOptions Read options.
   * @param key the key to retrieve the value for.
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   *
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] getForUpdate(final ReadOptions readOptions, final byte[] key,
      final boolean exclusive) throws RocksDBException {
    assert(isOwningHandle());
    return getForUpdate(nativeHandle_, readOptions.nativeHandle_, key, 0, key.length,
        defaultColumnFamilyHandle.nativeHandle_, exclusive, true /*doValidate*/);
  }

  /**
   * Read this key and ensure that this transaction will only
   * be able to be committed if this key is not written outside this
   * transaction after it has first been read (or after the snapshot if a
   * snapshot is set in this transaction). The transaction behavior is the
   * same regardless of whether the key exists or not.
   * <p>
   * Note: Currently, this function will return Status::MergeInProgress
   * if the most recent write to the queried key in this batch is a Merge.
   * <p>
   * The values returned by this function are similar to
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * If value==nullptr, then this function will not read any data, but will
   * still ensure that this key cannot be written to by outside of this
   * transaction.
   * <p>
   * If this transaction was created on an {@link OptimisticTransactionDB},
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}
   * could cause {@link #commit()} to fail. Otherwise, it could return any error
   * that could be returned by
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * <p>
   * If this transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *     {@link Status.Code#MergeInProgress} if merge operations cannot be
   *     resolved.
   *
   * @param readOptions Read options.
   * @param key the key to retrieve the value for.
   * @param value the value associated with the input key if
   *    *     any. The result is undefined in no value is associated with the key
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   *
   * @return a status object containing
   * Status.OK if the requested value was read
   * Status.NotFound if the requested value does not exist
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public GetStatus getForUpdate(final ReadOptions readOptions, final byte[] key, final byte[] value,
      final boolean exclusive) throws RocksDBException {
    final int result = getForUpdate(nativeHandle_, readOptions.nativeHandle_, key, 0, key.length,
        value, 0, value.length, defaultColumnFamilyHandle.nativeHandle_, exclusive,
        true /* doValidate */);
    if (result < 0) {
      return GetStatus.fromStatusCode(Status.Code.NotFound, 0);
    } else {
      return GetStatus.fromStatusCode(Status.Code.Ok, result);
    }
  }

  /**
   * Read this key and ensure that this transaction will only
   * be able to be committed if this key is not written outside this
   * transaction after it has first been read (or after the snapshot if a
   * snapshot is set in this transaction). The transaction behavior is the
   * same regardless of whether the key exists or not.
   * <p>
   * Note: Currently, this function will return Status::MergeInProgress
   * if the most recent write to the queried key in this batch is a Merge.
   * <p>
   * The values returned by this function are similar to
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * If value==nullptr, then this function will not read any data, but will
   * still ensure that this key cannot be written to by outside of this
   * transaction.
   * <p>
   * If this transaction was created on an {@link OptimisticTransactionDB},
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}
   * could cause {@link #commit()} to fail. Otherwise, it could return any error
   * that could be returned by
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * <p>
   * If this transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *     {@link Status.Code#MergeInProgress} if merge operations cannot be
   *     resolved.
   *
   * @param readOptions Read options.
   * @param key the key to retrieve the value for.
   * @param value the value associated with the input key if
   *    *     any. The result is undefined in no value is associated with the key
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   *
   * @return a status object containing
   * Status.OK if the requested value was read
   * Status.NotFound if the requested value does not exist
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public GetStatus getForUpdate(final ReadOptions readOptions, final ByteBuffer key,
      final ByteBuffer value, final boolean exclusive) throws RocksDBException {
    return getForUpdate(
        readOptions, defaultColumnFamilyHandle, key, value, exclusive, true /* doValidate */);
  }

  /**
   * Read this key and ensure that this transaction will only
   * be able to be committed if this key is not written outside this
   * transaction after it has first been read (or after the snapshot if a
   * snapshot is set in this transaction). The transaction behavior is the
   * same regardless of whether the key exists or not.
   * <p>
   * Note: Currently, this function will return Status::MergeInProgress
   * if the most recent write to the queried key in this batch is a Merge.
   * <p>
   * The values returned by this function are similar to
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * If value==nullptr, then this function will not read any data, but will
   * still ensure that this key cannot be written to by outside of this
   * transaction.
   * <p>
   * If this transaction was created on an {@link OptimisticTransactionDB},
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}
   * could cause {@link #commit()} to fail. Otherwise, it could return any error
   * that could be returned by
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * <p>
   * If this transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *     {@link Status.Code#MergeInProgress} if merge operations cannot be
   *     resolved.
   *
   * @param readOptions Read options.
   * @param columnFamilyHandle in which to find the key/value
   * @param key the key to retrieve the value for.
   * @param value the value associated with the input key if
   *    *     any. The result is undefined in no value is associated with the key
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   *
   * @return a status object containing
   * Status.OK if the requested value was read
   * Status.NotFound if the requested value does not exist
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public GetStatus getForUpdate(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final byte[] value,
      final boolean exclusive) throws RocksDBException {
    return getForUpdate(
        readOptions, columnFamilyHandle, key, value, exclusive, true /*doValidate*/);
  }

  /**
   * Read this key and ensure that this transaction will only
   * be able to be committed if this key is not written outside this
   * transaction after it has first been read (or after the snapshot if a
   * snapshot is set in this transaction). The transaction behavior is the
   * same regardless of whether the key exists or not.
   * <p>
   * Note: Currently, this function will return Status::MergeInProgress
   * if the most recent write to the queried key in this batch is a Merge.
   * <p>
   * The values returned by this function are similar to
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * If value==nullptr, then this function will not read any data, but will
   * still ensure that this key cannot be written to by outside of this
   * transaction.
   * <p>
   * If this transaction was created on an {@link OptimisticTransactionDB},
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}
   * could cause {@link #commit()} to fail. Otherwise, it could return any error
   * that could be returned by
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * <p>
   * If this transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *     {@link Status.Code#MergeInProgress} if merge operations cannot be
   *     resolved.
   *
   * @param readOptions Read options.
   * @param columnFamilyHandle in which to find the key/value
   * @param key the key to retrieve the value for.
   * @param value the value associated with the input key if
   *    *     any. The result is undefined in no value is associated with the key
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   * @param doValidate true if the transaction should validate the snapshot before doing the read
   *
   * @return a status object containing
   * Status.OK if the requested value was read
   * Status.NotFound if the requested value does not exist
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */

  public GetStatus getForUpdate(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final byte[] value,
      final boolean exclusive, final boolean doValidate) throws RocksDBException {
    final int result = getForUpdate(nativeHandle_, readOptions.nativeHandle_, key, 0, key.length,
        value, 0, value.length, columnFamilyHandle.nativeHandle_, exclusive, doValidate);
    if (result < 0) {
      return GetStatus.fromStatusCode(Status.Code.NotFound, 0);
    } else {
      return GetStatus.fromStatusCode(Status.Code.Ok, result);
    }
  }

  /**
   * Read this key and ensure that this transaction will only
   * be able to be committed if this key is not written outside this
   * transaction after it has first been read (or after the snapshot if a
   * snapshot is set in this transaction). The transaction behavior is the
   * same regardless of whether the key exists or not.
   * <p>
   * Note: Currently, this function will return Status::MergeInProgress
   * if the most recent write to the queried key in this batch is a Merge.
   * <p>
   * The values returned by this function are similar to
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * If value==nullptr, then this function will not read any data, but will
   * still ensure that this key cannot be written to by outside of this
   * transaction.
   * <p>
   * If this transaction was created on an {@link OptimisticTransactionDB},
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}
   * could cause {@link #commit()} to fail. Otherwise, it could return any error
   * that could be returned by
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * <p>
   * If this transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *     {@link Status.Code#MergeInProgress} if merge operations cannot be
   *     resolved.
   *
   * @param readOptions Read options.
   * @param columnFamilyHandle in which to find the key/value
   * @param key the key to retrieve the value for.
   * @param value the value associated with the input key if
   *    *     any. The result is undefined in no value is associated with the key
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   *
   * @return a status object containing
   * Status.OK if the requested value was read
   * Status.NotFound if the requested value does not exist
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */

  public GetStatus getForUpdate(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key, final ByteBuffer value,
      final boolean exclusive) throws RocksDBException {
    return getForUpdate(
        readOptions, columnFamilyHandle, key, value, exclusive, true /*doValidate*/);
  }

  /**
   * Read this key and ensure that this transaction will only
   * be able to be committed if this key is not written outside this
   * transaction after it has first been read (or after the snapshot if a
   * snapshot is set in this transaction). The transaction behavior is the
   * same regardless of whether the key exists or not.
   * <p>
   * Note: Currently, this function will return Status::MergeInProgress
   * if the most recent write to the queried key in this batch is a Merge.
   * <p>
   * The values returned by this function are similar to
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * If value==nullptr, then this function will not read any data, but will
   * still ensure that this key cannot be written to by outside of this
   * transaction.
   * <p>
   * If this transaction was created on an {@link OptimisticTransactionDB},
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}
   * could cause {@link #commit()} to fail. Otherwise, it could return any error
   * that could be returned by
   * {@link RocksDB#get(ReadOptions, byte[])}.
   * <p>
   * If this transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *     {@link Status.Code#MergeInProgress} if merge operations cannot be
   *     resolved.
   *
   * @param readOptions Read options.
   * @param columnFamilyHandle in which to find the key/value
   * @param key the key to retrieve the value for.
   * @param value the value associated with the input key if
   *    *     any. The result is undefined in no value is associated with the key
   * @param exclusive true if the transaction should have exclusive access to
   *     the key, otherwise false for shared access.
   * @param doValidate true if the transaction should validate the snapshot before doing the read
   *
   * @return a status object containing
   * Status.OK if the requested value was read
   * Status.NotFound if the requested value does not exist
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public GetStatus getForUpdate(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key, final ByteBuffer value,
      final boolean exclusive, final boolean doValidate) throws RocksDBException {
    final int result;
    if (key.isDirect() && value.isDirect()) {
      result = getDirectForUpdate(nativeHandle_, readOptions.nativeHandle_, key, key.position(),
          key.remaining(), value, value.position(), value.remaining(),
          columnFamilyHandle.nativeHandle_, exclusive, doValidate);
    } else if (!key.isDirect() && !value.isDirect()) {
      assert key.hasArray();
      assert value.hasArray();
      result = getForUpdate(nativeHandle_, readOptions.nativeHandle_, key.array(),
          key.arrayOffset() + key.position(), key.remaining(), value.array(),
          value.arrayOffset() + value.position(), value.remaining(),
          columnFamilyHandle.nativeHandle_, exclusive, doValidate);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    if (result < 0) {
      return GetStatus.fromStatusCode(Status.Code.NotFound, 0);
    } else {
      value.position(Math.min(value.limit(), value.position() + result));
      return GetStatus.fromStatusCode(Status.Code.Ok, result);
    }
  }

  /**
   * A multi-key version of
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}.
   * <p>
   *
   * @param readOptions Read options.
   * @param columnFamilyHandles {@link org.rocksdb.ColumnFamilyHandle}
   *     instances
   * @param keys the keys to retrieve the values for.
   *
   * @return Array of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  @Deprecated
  public byte[][] multiGetForUpdate(final ReadOptions readOptions,
      final List<ColumnFamilyHandle> columnFamilyHandles, final byte[][] keys)
      throws RocksDBException {
    assert(isOwningHandle());
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.length != columnFamilyHandles.size()){
      throw new IllegalArgumentException(FOR_EACH_KEY_THERE_MUST_BE_A_COLUMNFAMILYHANDLE);
    }
    if(keys.length == 0) {
      return new byte[0][0];
    }
    final long[] cfHandles = new long[columnFamilyHandles.size()];
    for (int i = 0; i < columnFamilyHandles.size(); i++) {
      cfHandles[i] = columnFamilyHandles.get(i).nativeHandle_;
    }
    return multiGetForUpdate(nativeHandle_, readOptions.nativeHandle_,
        keys, cfHandles);
  }

  /**
   * A multi-key version of
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}.
   * <p>
   *
   * @param readOptions Read options.
   * @param columnFamilyHandles {@link org.rocksdb.ColumnFamilyHandle}
   *     instances
   * @param keys the keys to retrieve the values for.
   *
   * @return Array of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<byte[]> multiGetForUpdateAsList(final ReadOptions readOptions,
      final List<ColumnFamilyHandle> columnFamilyHandles, final List<byte[]> keys)
      throws RocksDBException {
    assert (isOwningHandle());
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size() != columnFamilyHandles.size()) {
      throw new IllegalArgumentException(FOR_EACH_KEY_THERE_MUST_BE_A_COLUMNFAMILYHANDLE);
    }
    if (keys.isEmpty()) {
      return new ArrayList<>();
    }
    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);

    final long[] cfHandles = new long[columnFamilyHandles.size()];
    for (int i = 0; i < columnFamilyHandles.size(); i++) {
      cfHandles[i] = columnFamilyHandles.get(i).nativeHandle_;
    }
    return Arrays.asList(
        multiGetForUpdate(nativeHandle_, readOptions.nativeHandle_, keysArray, cfHandles));
  }

  /**
   * A multi-key version of {@link #getForUpdate(ReadOptions, byte[], boolean)}.
   * <p>
   *
   * @param readOptions Read options.
   * @param keys the keys to retrieve the values for.
   *
   * @return Array of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  @Deprecated
  public byte[][] multiGetForUpdate(final ReadOptions readOptions, final byte[][] keys)
      throws RocksDBException {
    assert(isOwningHandle());
    if(keys.length == 0) {
      return new byte[0][0];
    }

    return multiGetForUpdate(nativeHandle_,
        readOptions.nativeHandle_, keys);
  }

  /**
   * A multi-key version of {@link #getForUpdate(ReadOptions, byte[], boolean)}.
   * <p>
   *
   * @param readOptions Read options.
   * @param keys the keys to retrieve the values for.
   *
   * @return List of values, one for each key
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<byte[]> multiGetForUpdateAsList(
      final ReadOptions readOptions, final List<byte[]> keys) throws RocksDBException {
    assert (isOwningHandle());
    if (keys.isEmpty()) {
      return new ArrayList<>(0);
    }

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);

    return Arrays.asList(multiGetForUpdate(nativeHandle_, readOptions.nativeHandle_, keysArray));
  }

  /**
   * Returns an iterator that will iterate on all keys in the default
   * column family including both keys in the DB and uncommitted keys in this
   * transaction.
   * <p>
   * Caller is responsible for deleting the returned Iterator.
   * <p>
   * The returned iterator is only valid until {@link #commit()},
   * {@link #rollback()}, or {@link #rollbackToSavePoint()} is called.
   *
   * @return instance of iterator object.
   */
  public RocksIterator getIterator() {
    assert (isOwningHandle());
    try (ReadOptions readOptions = new ReadOptions()) {
      return new RocksIterator(parent,
          getIterator(
              nativeHandle_, readOptions.nativeHandle_, defaultColumnFamilyHandle.nativeHandle_));
    }
  }

  /**
   * Returns an iterator that will iterate on all keys in the default
   * column family including both keys in the DB and uncommitted keys in this
   * transaction.
   *
   * Setting {@link ReadOptions#setSnapshot(Snapshot)} will affect what is read
   * from the DB but will NOT change which keys are read from this transaction
   * (the keys in this transaction do not yet belong to any snapshot and will be
   * fetched regardless).
   * <p>
   * Caller is responsible for deleting the returned Iterator.
   * <p>
   * The returned iterator is only valid until {@link #commit()},
   * {@link #rollback()}, or {@link #rollbackToSavePoint()} is called.
   *
   * @param readOptions Read options.
   *
   * @return instance of iterator object.
   */
  public RocksIterator getIterator(final ReadOptions readOptions) {
    assert(isOwningHandle());
    return new RocksIterator(parent,
        getIterator(
            nativeHandle_, readOptions.nativeHandle_, defaultColumnFamilyHandle.nativeHandle_));
  }

  /**
   * Returns an iterator that will iterate on all keys in the column family
   * specified by {@code columnFamilyHandle} including both keys in the DB
   * and uncommitted keys in this transaction.
   * <p>
   * Setting {@link ReadOptions#setSnapshot(Snapshot)} will affect what is read
   * from the DB but will NOT change which keys are read from this transaction
   * (the keys in this transaction do not yet belong to any snapshot and will be
   * fetched regardless).
   * <p>
   * Caller is responsible for calling {@link RocksIterator#close()} on
   * the returned Iterator.
   * <p>
   * The returned iterator is only valid until {@link #commit()},
   * {@link #rollback()}, or {@link #rollbackToSavePoint()} is called.
   *
   * @param readOptions Read options.
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   *
   * @return instance of iterator object.
   */
  public RocksIterator getIterator(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle) {
    assert(isOwningHandle());
    return new RocksIterator(parent, getIterator(nativeHandle_,
        readOptions.nativeHandle_, columnFamilyHandle.nativeHandle_));
  }

  /**
   * Returns an iterator that will iterate on all keys in the column family
   * specified by {@code columnFamilyHandle} including both keys in the DB
   * and uncommitted keys in this transaction.
   * <p>
   * Setting {@link ReadOptions#setSnapshot(Snapshot)} will affect what is read
   * from the DB but will NOT change which keys are read from this transaction
   * (the keys in this transaction do not yet belong to any snapshot and will be
   * fetched regardless).
   * <p>
   * Caller is responsible for calling {@link RocksIterator#close()} on
   * the returned Iterator.
   * <p>
   * The returned iterator is only valid until {@link #commit()},
   * {@link #rollback()}, or {@link #rollbackToSavePoint()} is called.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   *
   * @return instance of iterator object.
   */
  public RocksIterator getIterator(final ColumnFamilyHandle columnFamilyHandle) {
    assert (isOwningHandle());
    try (ReadOptions readOptions = new ReadOptions()) {
      return new RocksIterator(parent,
          getIterator(nativeHandle_, readOptions.nativeHandle_, columnFamilyHandle.nativeHandle_));
    }
  }

  /**
   * Similar to {@link RocksDB#put(ColumnFamilyHandle, byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   * If this Transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *
   * @param columnFamilyHandle The column family to put the key/value into
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   * @param assumeTracked true when it is expected that the key is already
   *     tracked. More specifically, it means the the key was previous tracked
   *     in the same savepoint, with the same exclusive flag, and at a lower
   *     sequence number. If valid then it skips ValidateSnapshot,
   *     throws an error otherwise.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final byte[] value, final boolean assumeTracked) throws RocksDBException {
    assert (isOwningHandle());
    put(nativeHandle_, key, 0, key.length, value, 0, value.length, columnFamilyHandle.nativeHandle_,
        assumeTracked);
  }

  /**
   * Similar to {@link #put(ColumnFamilyHandle, byte[], byte[], boolean)}
   * but with {@code assumeTracked = false}.
   * <p>
   * Will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   * If this Transaction was created on a {@link TransactionDB}, an
   * {@link RocksDBException} may be thrown with an accompanying {@link Status}
   * when:
   *     {@link Status.Code#Busy} if there is a write conflict,
   *     {@link Status.Code#TimedOut} if a lock could not be acquired,
   *     {@link Status.Code#TryAgain} if the memtable history size is not large
   *         enough.
   *
   * @param columnFamilyHandle The column family to put the key/value into
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final byte[] value) throws RocksDBException {
    assert(isOwningHandle());
    put(nativeHandle_, key, 0, key.length, value, 0, value.length, columnFamilyHandle.nativeHandle_,
        false);
  }

  /**
   * Similar to {@link RocksDB#put(byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void put(final byte[] key, final byte[] value)
      throws RocksDBException {
    assert(isOwningHandle());
    put(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #put(ColumnFamilyHandle, byte[], byte[])} but allows
   * you to specify the key and value in several parts that will be
   * concatenated together.
   *
   * @param columnFamilyHandle The column family to put the key/value into
   * @param keyParts the specified key to be inserted.
   * @param valueParts the value associated with the specified key.
   * @param assumeTracked true when it is expected that the key is already
   *     tracked. More specifically, it means the the key was previous tracked
   *     in the same savepoint, with the same exclusive flag, and at a lower
   *     sequence number. If valid then it skips ValidateSnapshot,
   *     throws an error otherwise.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final byte[][] keyParts, final byte[][] valueParts,
      final boolean assumeTracked) throws RocksDBException {
    assert (isOwningHandle());
    put(nativeHandle_, keyParts, keyParts.length, valueParts, valueParts.length,
        columnFamilyHandle.nativeHandle_, assumeTracked);
  }

  /**
   * Similar to {@link #put(ColumnFamilyHandle, byte[][], byte[][], boolean)}
   * but with with {@code assumeTracked = false}.
   * <p>
   * Allows you to specify the key and value in several parts that will be
   * concatenated together.
   *
   * @param columnFamilyHandle The column family to put the key/value into
   * @param keyParts the specified key to be inserted.
   * @param valueParts the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final byte[][] keyParts, final byte[][] valueParts)
      throws RocksDBException {
    assert(isOwningHandle());
    put(nativeHandle_, keyParts, keyParts.length, valueParts, valueParts.length,
        columnFamilyHandle.nativeHandle_, false);
  }

  /**
   * Similar to {@link RocksDB#put(byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   *
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   *
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void put(final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    assert (isOwningHandle());
    if (key.isDirect() && value.isDirect()) {
      putDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
          value.remaining());
    } else if (!key.isDirect() && !value.isDirect()) {
      assert key.hasArray();
      assert value.hasArray();
      put(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining(),
          value.array(), value.arrayOffset() + value.position(), value.remaining());
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  /**
   * Similar to {@link RocksDB#put(byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   *
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   *
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle The column family to put the key/value into
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   * @param assumeTracked true when it is expected that the key is already
   *     tracked. More specifically, it means the the key was previous tracked
   *     in the same savepoint, with the same exclusive flag, and at a lower
   *     sequence number. If valid then it skips ValidateSnapshot,
   *     throws an error otherwise.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
      final ByteBuffer value, final boolean assumeTracked) throws RocksDBException {
    assert (isOwningHandle());
    if (key.isDirect() && value.isDirect()) {
      putDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
          value.remaining(), columnFamilyHandle.nativeHandle_, assumeTracked);
    } else if (!key.isDirect() && !value.isDirect()) {
      assert key.hasArray();
      assert value.hasArray();
      put(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining(),
          value.array(), value.arrayOffset() + value.position(), value.remaining(),
          columnFamilyHandle.nativeHandle_, assumeTracked);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }
  public void put(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
      final ByteBuffer value) throws RocksDBException {
    put(columnFamilyHandle, key, value, false);
  }

  // TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #put(byte[], byte[])} but allows
   * you to specify the key and value in several parts that will be
   * concatenated together
   *
   * @param keyParts the specified key to be inserted.
   * @param valueParts the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void put(final byte[][] keyParts, final byte[][] valueParts)
      throws RocksDBException {
    assert(isOwningHandle());
    put(nativeHandle_, keyParts, keyParts.length, valueParts,
        valueParts.length);
  }

  /**
   * Similar to {@link RocksDB#merge(ColumnFamilyHandle, byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle The column family to merge the key/value into
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   * @param assumeTracked true when it is expected that the key is already
   *     tracked. More specifically, it means the the key was previous tracked
   *     in the same savepoint, with the same exclusive flag, and at a lower
   *     sequence number. If valid then it skips ValidateSnapshot,
   *     throws an error otherwise.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value, final boolean assumeTracked)
      throws RocksDBException {
    assert (isOwningHandle());
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_, assumeTracked);
  }

  /**
   * Similar to {@link #merge(ColumnFamilyHandle, byte[], byte[], boolean)}
   * but with {@code assumeTracked = false}.
   * <p>
   * Will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle The column family to merge the key/value into
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    assert(isOwningHandle());
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_, false);
  }

  /**
   * Similar to {@link RocksDB#merge(byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void merge(final byte[] key, final byte[] value)
      throws RocksDBException {
    assert(isOwningHandle());
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * Similar to {@link RocksDB#merge(byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   *
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   *
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void merge(final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    assert (isOwningHandle());
    if (key.isDirect() && value.isDirect()) {
      mergeDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
          value.remaining());
    } else if (!key.isDirect() && !value.isDirect()) {
      assert key.hasArray();
      assert value.hasArray();
      merge(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining(),
          value.array(), value.arrayOffset() + value.position(), value.remaining());
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
  }

  /**
   * Similar to {@link RocksDB#merge(byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   *
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   *
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle in which to apply the merge
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   * @param assumeTracked expects the key be already tracked.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
      final ByteBuffer value, final boolean assumeTracked) throws RocksDBException {
    assert (isOwningHandle());
    if (key.isDirect() && value.isDirect()) {
      mergeDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
          value.remaining(), columnFamilyHandle.nativeHandle_, assumeTracked);
    } else if (!key.isDirect() && !value.isDirect()) {
      assert key.hasArray();
      assert value.hasArray();
      merge(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining(),
          value.array(), value.arrayOffset() + value.position(), value.remaining(),
          columnFamilyHandle.nativeHandle_, assumeTracked);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  /**
   * Similar to {@link RocksDB#merge(byte[], byte[])}, but
   * will also perform conflict checking on the keys be written.
   *
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   *
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle in which to apply the merge
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
      final ByteBuffer value) throws RocksDBException {
    merge(columnFamilyHandle, key, value, false);
  }

  /**
   * Similar to {@link RocksDB#delete(ColumnFamilyHandle, byte[])}, but
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param key the specified key to be deleted.
   * @param assumeTracked true when it is expected that the key is already
   *     tracked. More specifically, it means the the key was previous tracked
   *     in the same savepoint, with the same exclusive flag, and at a lower
   *     sequence number. If valid then it skips ValidateSnapshot,
   *     throws an error otherwise.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final boolean assumeTracked) throws RocksDBException {
    assert (isOwningHandle());
    delete(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_,
        assumeTracked);
  }

  /**
   * Similar to {@link #delete(ColumnFamilyHandle, byte[], boolean)}
   * but with {@code assumeTracked = false}.
   * <p>
   * Will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    assert(isOwningHandle());
    delete(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_,
        /*assumeTracked*/ false);
  }

  /**
   * Similar to {@link RocksDB#delete(byte[])}, but
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void delete(final byte[] key) throws RocksDBException {
    assert(isOwningHandle());
    delete(nativeHandle_, key, key.length);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #delete(ColumnFamilyHandle, byte[])} but allows
   * you to specify the key in several parts that will be
   * concatenated together.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param keyParts the specified key to be deleted.
   * @param assumeTracked true when it is expected that the key is already
   *     tracked. More specifically, it means the the key was previous tracked
   *     in the same savepoint, with the same exclusive flag, and at a lower
   *     sequence number. If valid then it skips ValidateSnapshot,
   *     throws an error otherwise.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[][] keyParts, final boolean assumeTracked)
      throws RocksDBException {
    assert (isOwningHandle());
    delete(nativeHandle_, keyParts, keyParts.length,
        columnFamilyHandle.nativeHandle_, assumeTracked);
  }

  /**
   * Similar to{@link #delete(ColumnFamilyHandle, byte[][], boolean)}
   * but with {@code assumeTracked = false}.
   * <p>
   * Allows you to specify the key in several parts that will be
   * concatenated together.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param keyParts the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[][] keyParts) throws RocksDBException {
    assert(isOwningHandle());
    delete(nativeHandle_, keyParts, keyParts.length,
        columnFamilyHandle.nativeHandle_, false);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #delete(byte[])} but allows
   * you to specify key the in several parts that will be
   * concatenated together.
   *
   * @param keyParts the specified key to be deleted
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void delete(final byte[][] keyParts) throws RocksDBException {
    assert(isOwningHandle());
    delete(nativeHandle_, keyParts, keyParts.length);
  }

  /**
   * Similar to {@link RocksDB#singleDelete(ColumnFamilyHandle, byte[])}, but
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param key the specified key to be deleted.
   * @param assumeTracked true when it is expected that the key is already
   *     tracked. More specifically, it means the key was previously tracked
   *     in the same savepoint, with the same exclusive flag, and at a lower
   *     sequence number. If valid then it skips ValidateSnapshot,
   *     throws an error otherwise.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final boolean assumeTracked) throws RocksDBException {
    assert (isOwningHandle());
    singleDelete(nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_, assumeTracked);
  }

  /**
   * Similar to {@link #singleDelete(ColumnFamilyHandle, byte[], boolean)}
   * but with {@code assumeTracked = false}.
   * <p>
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
      throws RocksDBException {
    assert(isOwningHandle());
    singleDelete(nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_, false);
  }

  /**
   * Similar to {@link RocksDB#singleDelete(byte[])}, but
   * will also perform conflict checking on the keys be written.
   * <p>
   * If this Transaction was created on an {@link OptimisticTransactionDB},
   * these functions should always succeed.
   * <p>
   *  If this Transaction was created on a {@link TransactionDB}, an
   *  {@link RocksDBException} may be thrown with an accompanying {@link Status}
   *  when:
   *    {@link Status.Code#Busy} if there is a write conflict,
   *    {@link Status.Code#TimedOut} if a lock could not be acquired,
   *    {@link Status.Code#TryAgain} if the memtable history size is not large
   *       enough.
   *
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final byte[] key) throws RocksDBException {
    assert(isOwningHandle());
    singleDelete(nativeHandle_, key, key.length);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #singleDelete(ColumnFamilyHandle, byte[])} but allows
   * you to specify the key in several parts that will be
   * concatenated together.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param keyParts the specified key to be deleted.
   * @param assumeTracked true when it is expected that the key is already
   *     tracked. More specifically, it means the key was previously tracked
   *     in the same savepoint, with the same exclusive flag, and at a lower
   *     sequence number. If valid then it skips ValidateSnapshot,
   *     throws an error otherwise.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle, final byte[][] keyParts,
      final boolean assumeTracked) throws RocksDBException {
    assert (isOwningHandle());
    singleDelete(nativeHandle_, keyParts, keyParts.length,
        columnFamilyHandle.nativeHandle_, assumeTracked);
  }

  /**
   * Similar to{@link #singleDelete(ColumnFamilyHandle, byte[][], boolean)}
   * but with {@code assumeTracked = false}.
   * <p>
   * Allows you to specify the key in several parts that will be
   * concatenated together.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param keyParts the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle, final byte[][] keyParts)
      throws RocksDBException {
    assert(isOwningHandle());
    singleDelete(nativeHandle_, keyParts, keyParts.length,
        columnFamilyHandle.nativeHandle_, false);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #singleDelete(byte[])} but allows
   * you to specify the key in several parts that will be
   * concatenated together.
   *
   * @param keyParts the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  @Experimental(PERFORMANCE_OPTIMIZATION_FOR_A_VERY_SPECIFIC_WORKLOAD)
  public void singleDelete(final byte[][] keyParts) throws RocksDBException {
    assert(isOwningHandle());
    singleDelete(nativeHandle_, keyParts, keyParts.length);
  }

  /**
   * Similar to {@link RocksDB#put(ColumnFamilyHandle, byte[], byte[])},
   * but operates on the transactions write batch. This write will only happen
   * if this transaction gets committed successfully.
   * <p>
   * Unlike {@link #put(ColumnFamilyHandle, byte[], byte[])} no conflict
   * checking will be performed for this key.
   * <p>
   * If this Transaction was created on a {@link TransactionDB}, this function
   * will still acquire locks necessary to make sure this write doesn't cause
   * conflicts in other transactions; This may cause a {@link RocksDBException}
   * with associated {@link Status.Code#Busy}.
   *
   * @param columnFamilyHandle The column family to put the key/value into
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void putUntracked(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    assert(isOwningHandle());
    putUntracked(nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Similar to {@link RocksDB#put(byte[], byte[])},
   * but operates on the transactions write batch. This write will only happen
   * if this transaction gets committed successfully.
   * <p>
   * Unlike {@link #put(byte[], byte[])} no conflict
   * checking will be performed for this key.
   * <p>
   * If this Transaction was created on a {@link TransactionDB}, this function
   * will still acquire locks necessary to make sure this write doesn't cause
   * conflicts in other transactions; This may cause a {@link RocksDBException}
   * with associated {@link Status.Code#Busy}.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void putUntracked(final byte[] key, final byte[] value)
      throws RocksDBException {
    assert(isOwningHandle());
    putUntracked(nativeHandle_, key, key.length, value, value.length);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #putUntracked(ColumnFamilyHandle, byte[], byte[])} but
   * allows you to specify the key and value in several parts that will be
   * concatenated together.
   *
   * @param columnFamilyHandle The column family to put the key/value into
   * @param keyParts the specified key to be inserted.
   * @param valueParts the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void putUntracked(final ColumnFamilyHandle columnFamilyHandle,
      final byte[][] keyParts, final byte[][] valueParts)
      throws RocksDBException {
    assert(isOwningHandle());
    putUntracked(nativeHandle_, keyParts, keyParts.length, valueParts,
        valueParts.length, columnFamilyHandle.nativeHandle_);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #putUntracked(byte[], byte[])} but
   * allows you to specify the key and value in several parts that will be
   * concatenated together.
   *
   * @param keyParts the specified key to be inserted.
   * @param valueParts the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void putUntracked(final byte[][] keyParts, final byte[][] valueParts)
      throws RocksDBException {
    assert(isOwningHandle());
    putUntracked(nativeHandle_, keyParts, keyParts.length, valueParts,
        valueParts.length);
  }

  /**
   * Similar to {@link RocksDB#merge(ColumnFamilyHandle, byte[], byte[])},
   * but operates on the transactions write batch. This write will only happen
   * if this transaction gets committed successfully.
   * <p>
   * Unlike {@link #merge(ColumnFamilyHandle, byte[], byte[])} no conflict
   * checking will be performed for this key.
   * <p>
   * If this Transaction was created on a {@link TransactionDB}, this function
   * will still acquire locks necessary to make sure this write doesn't cause
   * conflicts in other transactions; This may cause a {@link RocksDBException}
   * with associated {@link Status.Code#Busy}.
   *
   * @param columnFamilyHandle The column family to merge the key/value into
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void mergeUntracked(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    assert (isOwningHandle());
    mergeUntracked(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Similar to {@link RocksDB#merge(ColumnFamilyHandle, byte[], byte[])},
   * but operates on the transactions write batch. This write will only happen
   * if this transaction gets committed successfully.
   *
   * Unlike {@link #merge(ColumnFamilyHandle, byte[], byte[])} no conflict
   * checking will be performed for this key.
   *
   * If this Transaction was created on a {@link TransactionDB}, this function
   * will still acquire locks necessary to make sure this write doesn't cause
   * conflicts in other transactions; This may cause a {@link RocksDBException}
   * with associated {@link Status.Code#Busy}.
   *
   * @param columnFamilyHandle The column family to merge the key/value into
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void mergeUntracked(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
      final ByteBuffer value) throws RocksDBException {
    assert (isOwningHandle());
    if (key.isDirect() && value.isDirect()) {
      mergeUntrackedDirect(nativeHandle_, key, key.position(), key.remaining(), value,
          value.position(), value.remaining(), columnFamilyHandle.nativeHandle_);
    } else if (!key.isDirect() && !value.isDirect()) {
      assert key.hasArray();
      assert value.hasArray();
      mergeUntracked(nativeHandle_, key.array(), key.arrayOffset() + key.position(),
          key.remaining(), value.array(), value.arrayOffset() + value.position(), value.remaining(),
          columnFamilyHandle.nativeHandle_);
    } else {
      throw new RocksDBException(BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  /**
   * Similar to {@link RocksDB#merge(byte[], byte[])},
   * but operates on the transactions write batch. This write will only happen
   * if this transaction gets committed successfully.
   * <p>
   * Unlike {@link #merge(byte[], byte[])} no conflict
   * checking will be performed for this key.
   * <p>
   * If this Transaction was created on a {@link TransactionDB}, this function
   * will still acquire locks necessary to make sure this write doesn't cause
   * conflicts in other transactions; This may cause a {@link RocksDBException}
   * with associated {@link Status.Code#Busy}.
   *
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void mergeUntracked(final byte[] key, final byte[] value)
      throws RocksDBException {
    mergeUntracked(defaultColumnFamilyHandle, key, value);
  }

  /**
   * Similar to {@link RocksDB#merge(byte[], byte[])},
   * but operates on the transactions write batch. This write will only happen
   * if this transaction gets committed successfully.
   *
   * Unlike {@link #merge(byte[], byte[])} no conflict
   * checking will be performed for this key.
   *
   * If this Transaction was created on a {@link TransactionDB}, this function
   * will still acquire locks necessary to make sure this write doesn't cause
   * conflicts in other transactions; This may cause a {@link RocksDBException}
   * with associated {@link Status.Code#Busy}.
   *
   * @param key the specified key to be merged.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void mergeUntracked(final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    mergeUntracked(defaultColumnFamilyHandle, key, value);
  }

  /**
   * Similar to {@link RocksDB#delete(ColumnFamilyHandle, byte[])},
   * but operates on the transactions write batch. This write will only happen
   * if this transaction gets committed successfully.
   * <p>
   * Unlike {@link #delete(ColumnFamilyHandle, byte[])} no conflict
   * checking will be performed for this key.
   * <p>
   * If this Transaction was created on a {@link TransactionDB}, this function
   * will still acquire locks necessary to make sure this write doesn't cause
   * conflicts in other transactions; This may cause a {@link RocksDBException}
   * with associated {@link Status.Code#Busy}.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void deleteUntracked(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    assert(isOwningHandle());
    deleteUntracked(nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Similar to {@link RocksDB#delete(byte[])},
   * but operates on the transactions write batch. This write will only happen
   * if this transaction gets committed successfully.
   * <p>
   * Unlike {@link #delete(byte[])} no conflict
   * checking will be performed for this key.
   * <p>
   * If this Transaction was created on a {@link TransactionDB}, this function
   * will still acquire locks necessary to make sure this write doesn't cause
   * conflicts in other transactions; This may cause a {@link RocksDBException}
   * with associated {@link Status.Code#Busy}.
   *
   * @param key the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void deleteUntracked(final byte[] key) throws RocksDBException {
    assert(isOwningHandle());
    deleteUntracked(nativeHandle_, key, key.length);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #deleteUntracked(ColumnFamilyHandle, byte[])} but allows
   * you to specify the key in several parts that will be
   * concatenated together.
   *
   * @param columnFamilyHandle The column family to delete the key/value from
   * @param keyParts the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void deleteUntracked(final ColumnFamilyHandle columnFamilyHandle,
      final byte[][] keyParts) throws RocksDBException {
    assert(isOwningHandle());
    deleteUntracked(nativeHandle_, keyParts, keyParts.length,
        columnFamilyHandle.nativeHandle_);
  }

  //TODO(AR) refactor if we implement org.rocksdb.SliceParts in future
  /**
   * Similar to {@link #deleteUntracked(byte[])} but allows
   * you to specify the key in several parts that will be
   * concatenated together.
   *
   * @param keyParts the specified key to be deleted.
   *
   * @throws RocksDBException when one of the TransactionalDB conditions
   *     described above occurs, or in the case of an unexpected error
   */
  public void deleteUntracked(final byte[][] keyParts) throws RocksDBException {
    assert(isOwningHandle());
    deleteUntracked(nativeHandle_, keyParts, keyParts.length);
  }

  /**
   * Similar to {@link WriteBatch#putLogData(byte[])}
   *
   * @param blob binary object to be inserted
   */
  public void putLogData(final byte[] blob) {
    assert(isOwningHandle());
    putLogData(nativeHandle_, blob, blob.length);
  }

  /**
   * By default, all put/merge/delete operations will be indexed in the
   * transaction so that get/getForUpdate/getIterator can search for these
   * keys.
   * <p>
   * If the caller does not want to fetch the keys about to be written,
   * they may want to avoid indexing as a performance optimization.
   * Calling {@code #disableIndexing()} will turn off indexing for all future
   * put/merge/delete operations until {@link #enableIndexing()} is called.
   * <p>
   * If a key is put/merge/deleted after {@code #disableIndexing()} is called
   * and then is fetched via get/getForUpdate/getIterator, the result of the
   * fetch is undefined.
   */
  public void disableIndexing() {
    assert(isOwningHandle());
    disableIndexing(nativeHandle_);
  }

  /**
   * Re-enables indexing after a previous call to {@link #disableIndexing()}
   */
  public void enableIndexing() {
    assert(isOwningHandle());
    enableIndexing(nativeHandle_);
  }

  /**
   * Returns the number of distinct Keys being tracked by this transaction.
   * If this transaction was created by a {@link TransactionDB}, this is the
   * number of keys that are currently locked by this transaction.
   * If this transaction was created by an {@link OptimisticTransactionDB},
   * this is the number of keys that need to be checked for conflicts at commit
   * time.
   *
   * @return the number of distinct Keys being tracked by this transaction
   */
  public long getNumKeys() {
    assert(isOwningHandle());
    return getNumKeys(nativeHandle_);
  }

  /**
   * Returns the number of puts that have been applied to this
   * transaction so far.
   *
   * @return the number of puts that have been applied to this transaction
   */
  public long getNumPuts() {
    assert(isOwningHandle());
    return getNumPuts(nativeHandle_);
  }

  /**
   * Returns the number of deletes that have been applied to this
   * transaction so far.
   *
   * @return the number of deletes that have been applied to this transaction
   */
  public long getNumDeletes() {
    assert(isOwningHandle());
    return getNumDeletes(nativeHandle_);
  }

  /**
   * Returns the number of merges that have been applied to this
   * transaction so far.
   *
   * @return the number of merges that have been applied to this transaction
   */
  public long getNumMerges() {
    assert(isOwningHandle());
    return getNumMerges(nativeHandle_);
  }

  /**
   * Returns the elapsed time in milliseconds since this Transaction began.
   *
   * @return the elapsed time in milliseconds since this transaction began.
   */
  public long getElapsedTime() {
    assert(isOwningHandle());
    return getElapsedTime(nativeHandle_);
  }

  /**
   * Fetch the underlying write batch that contains all pending changes to be
   * committed.
   * <p>
   * Note: You should not write or delete anything from the batch directly and
   * should only use the functions in the {@link Transaction} class to
   * write to this transaction.
   *
   * @return The write batch
   */
  public WriteBatchWithIndex getWriteBatch() {
    assert(isOwningHandle());
    return new WriteBatchWithIndex(getWriteBatch(nativeHandle_));
  }

  /**
   * Change the value of {@link TransactionOptions#getLockTimeout()}
   * (in milliseconds) for this transaction.
   * <p>
   * Has no effect on OptimisticTransactions.
   *
   * @param lockTimeout the timeout (in milliseconds) for locks used by this
   *     transaction.
   */
  public void setLockTimeout(final long lockTimeout) {
    assert(isOwningHandle());
    setLockTimeout(nativeHandle_, lockTimeout);
  }

  /**
   * Return the WriteOptions that will be used during {@link #commit()}.
   *
   * @return the WriteOptions that will be used
   */
  public WriteOptions getWriteOptions() {
    assert(isOwningHandle());
    return new WriteOptions(getWriteOptions(nativeHandle_));
  }

  /**
   * Reset the WriteOptions that will be used during {@link #commit()}.
   *
   * @param writeOptions The new WriteOptions
   */
  public void setWriteOptions(final WriteOptions writeOptions) {
    assert(isOwningHandle());
    setWriteOptions(nativeHandle_, writeOptions.nativeHandle_);
  }

  /**
   * If this key was previously fetched in this transaction using
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}/
   * {@link #multiGetForUpdate(ReadOptions, List, byte[][])}, calling
   * {@code #undoGetForUpdate(ColumnFamilyHandle, byte[])} will tell
   * the transaction that it no longer needs to do any conflict checking
   * for this key.
   * <p>
   * If a key has been fetched N times via
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)}/
   * {@link #multiGetForUpdate(ReadOptions, List, byte[][])}, then
   * {@code #undoGetForUpdate(ColumnFamilyHandle, byte[])}  will only have an
   * effect if it is also called N times. If this key has been written to in
   * this transaction, {@code #undoGetForUpdate(ColumnFamilyHandle, byte[])}
   * will have no effect.
   * <p>
   * If {@link #setSavePoint()} has been called after the
   * {@link #getForUpdate(ReadOptions, ColumnFamilyHandle, byte[], boolean)},
   * {@code #undoGetForUpdate(ColumnFamilyHandle, byte[])} will not have any
   * effect.
   * <p>
   * If this Transaction was created by an {@link OptimisticTransactionDB},
   * calling {@code #undoGetForUpdate(ColumnFamilyHandle, byte[])} can affect
   * whether this key is conflict checked at commit time.
   * If this Transaction was created by a {@link TransactionDB},
   * calling {@code #undoGetForUpdate(ColumnFamilyHandle, byte[])} may release
   * any held locks for this key.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value for.
   */
  public void undoGetForUpdate(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) {
    assert(isOwningHandle());
    undoGetForUpdate(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * If this key was previously fetched in this transaction using
   * {@link #getForUpdate(ReadOptions, byte[], boolean)}/
   * {@link #multiGetForUpdate(ReadOptions, List, byte[][])}, calling
   * {@code #undoGetForUpdate(byte[])} will tell
   * the transaction that it no longer needs to do any conflict checking
   * for this key.
   * <p>
   * If a key has been fetched N times via
   * {@link #getForUpdate(ReadOptions, byte[], boolean)}/
   * {@link #multiGetForUpdate(ReadOptions, List, byte[][])}, then
   * {@code #undoGetForUpdate(byte[])}  will only have an
   * effect if it is also called N times. If this key has been written to in
   * this transaction, {@code #undoGetForUpdate(byte[])}
   * will have no effect.
   * <p>
   * If {@link #setSavePoint()} has been called after the
   * {@link #getForUpdate(ReadOptions, byte[], boolean)},
   * {@code #undoGetForUpdate(byte[])} will not have any
   * effect.
   * <p>
   * If this Transaction was created by an {@link OptimisticTransactionDB},
   * calling {@code #undoGetForUpdate(byte[])} can affect
   * whether this key is conflict checked at commit time.
   * If this Transaction was created by a {@link TransactionDB},
   * calling {@code #undoGetForUpdate(byte[])} may release
   * any held locks for this key.
   *
   * @param key the key to retrieve the value for.
   */
  public void undoGetForUpdate(final byte[] key) {
    assert(isOwningHandle());
    undoGetForUpdate(nativeHandle_, key, key.length);
  }

  /**
   * Adds the keys from the WriteBatch to the transaction
   *
   * @param writeBatch The write batch to read from
   *
   * @throws RocksDBException if an error occurs whilst rebuilding from the
   *     write batch.
   */
  public void rebuildFromWriteBatch(final WriteBatch writeBatch)
      throws RocksDBException {
    assert(isOwningHandle());
    rebuildFromWriteBatch(nativeHandle_, writeBatch.nativeHandle_);
  }

  /**
   * Get the Commit time Write Batch.
   *
   * @return the commit time write batch.
   */
  public WriteBatch getCommitTimeWriteBatch() {
    assert(isOwningHandle());
    return new WriteBatch(getCommitTimeWriteBatch(nativeHandle_));
  }

  /**
   * Set the log number.
   *
   * @param logNumber the log number
   */
  public void setLogNumber(final long logNumber) {
    assert(isOwningHandle());
    setLogNumber(nativeHandle_, logNumber);
  }

  /**
   * Get the log number.
   *
   * @return the log number
   */
  public long getLogNumber() {
    assert(isOwningHandle());
    return getLogNumber(nativeHandle_);
  }

  /**
   * Set the name of the transaction.
   *
   * @param transactionName the name of the transaction
   *
   * @throws RocksDBException if an error occurs when setting the transaction
   *     name.
   */
  public void setName(final String transactionName) throws RocksDBException {
    assert(isOwningHandle());
    setName(nativeHandle_, transactionName);
  }

  /**
   * Get the name of the transaction.
   *
   * @return the name of the transaction
   */
  public String getName() {
    assert(isOwningHandle());
    return getName(nativeHandle_);
  }

  /**
   * Get the ID of the transaction.
   *
   * @return the ID of the transaction.
   */
  public long getID() {
    assert(isOwningHandle());
    return getID(nativeHandle_);
  }

  /**
   * Determine if a deadlock has been detected.
   *
   * @return true if a deadlock has been detected.
   */
  public boolean isDeadlockDetect() {
    assert(isOwningHandle());
    return isDeadlockDetect(nativeHandle_);
  }

  /**
   * Get the list of waiting transactions.
   *
   * @return The list of waiting transactions.
   */
  public WaitingTransactions getWaitingTxns() {
    assert(isOwningHandle());
    return getWaitingTxns(nativeHandle_);
  }

  /**
   * Get the execution status of the transaction.
   * <p>
   * NOTE: The execution status of an Optimistic Transaction
   * never changes. This is only useful for non-optimistic transactions!
   *
   * @return The execution status of the transaction
   */
  public TransactionState getState() {
    assert(isOwningHandle());
    return TransactionState.getTransactionState(
        getState(nativeHandle_));
  }

  /**
   * The globally unique id with which the transaction is identified. This id
   * might or might not be set depending on the implementation. Similarly the
   * implementation decides the point in lifetime of a transaction at which it
   * assigns the id. Although currently it is the case, the id is not guaranteed
   * to remain the same across restarts.
   *
   * @return the transaction id.
   */
  @Experimental("NOTE: Experimental feature")
  public long getId() {
    assert(isOwningHandle());
    return getId(nativeHandle_);
  }

  public enum TransactionState {
    STARTED((byte)0),
    AWAITING_PREPARE((byte)1),
    PREPARED((byte)2),
    AWAITING_COMMIT((byte)3),
    COMMITTED((byte)4),
    AWAITING_ROLLBACK((byte)5),
    ROLLEDBACK((byte)6),
    LOCKS_STOLEN((byte)7);

    /*
     * Keep old misspelled variable as alias
     * Tip from https://stackoverflow.com/a/37092410/454544
     */
    public static final TransactionState COMMITED = COMMITTED;

    private final byte value;

    TransactionState(final byte value) {
      this.value = value;
    }

    /**
     * Get TransactionState by byte value.
     *
     * @param value byte representation of TransactionState.
     *
     * @return {@link org.rocksdb.Transaction.TransactionState} instance or null.
     * @throws java.lang.IllegalArgumentException if an invalid
     *     value is provided.
     */
    public static TransactionState getTransactionState(final byte value) {
      for (final TransactionState transactionState : TransactionState.values()) {
        if (transactionState.value == value){
          return transactionState;
        }
      }
      throw new IllegalArgumentException(
          "Illegal value provided for TransactionState.");
    }
  }

  /**
   * Called from C++ native method {@link #getWaitingTxns(long)}
   * to construct a WaitingTransactions object.
   *
   * @param columnFamilyId The id of the {@link ColumnFamilyHandle}
   * @param key The key
   * @param transactionIds The transaction ids
   *
   * @return The waiting transactions
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private WaitingTransactions newWaitingTransactions(
      final long columnFamilyId, final String key, final long[] transactionIds) {
    return new WaitingTransactions(columnFamilyId, key, transactionIds);
  }

  public static class WaitingTransactions {
    private final long columnFamilyId;
    private final String key;
    private final long[] transactionIds;

    private WaitingTransactions(final long columnFamilyId, final String key,
        final long[] transactionIds) {
      this.columnFamilyId = columnFamilyId;
      this.key = key;
      this.transactionIds = transactionIds;
    }

    /**
     * Get the Column Family ID.
     *
     * @return The column family ID
     */
    public long getColumnFamilyId() {
      return columnFamilyId;
    }

    /**
     * Get the key on which the transactions are waiting.
     *
     * @return The key
     */
    public String getKey() {
      return key;
    }

    /**
     * Get the IDs of the waiting transactions.
     *
     * @return The IDs of the waiting transactions
     */
    @SuppressWarnings("PMD.MethodReturnsInternalArray")
    public long[] getTransactionIds() {
      return transactionIds;
    }
  }

  private static native void setSnapshot(final long handle);
  private static native void setSnapshotOnNextOperation(final long handle);
  private static native void setSnapshotOnNextOperation(
      final long handle, final long transactionNotifierHandle);
  private static native long getSnapshot(final long handle);
  private static native void clearSnapshot(final long handle);
  private static native void prepare(final long handle) throws RocksDBException;
  private static native void commit(final long handle) throws RocksDBException;
  private static native void rollback(final long handle) throws RocksDBException;
  private static native void setSavePoint(final long handle) throws RocksDBException;
  private static native void rollbackToSavePoint(final long handle) throws RocksDBException;
  private static native byte[] get(final long handle, final long readOptionsHandle,
      final byte[] key, final int keyOffset, final int keyLength, final long columnFamilyHandle)
      throws RocksDBException;
  private static native int get(final long handle, final long readOptionsHandle, final byte[] key,
      final int keyOffset, final int keyLen, final byte[] value, final int valueOffset,
      final int valueLen, final long columnFamilyHandle) throws RocksDBException;
  private static native int getDirect(final long handle, final long readOptionsHandle,
      final ByteBuffer key, final int keyOffset, final int keyLength, final ByteBuffer value,
      final int valueOffset, final int valueLength, final long columnFamilyHandle)
      throws RocksDBException;

  private static native byte[][] multiGet(final long handle, final long readOptionsHandle,
      final byte[][] keys, final long[] columnFamilyHandles) throws RocksDBException;
  private static native byte[][] multiGet(
      final long handle, final long readOptionsHandle, final byte[][] keys) throws RocksDBException;
  private static native byte[][] multiGet(final long nativeHandle, final long readOptionsHandle,
      final long cfHandle, final byte[][] keys) throws RocksDBException;
  private static native byte[] getForUpdate(final long handle, final long readOptionsHandle,
      final byte[] key, final int keyOffset, final int keyLength, final long columnFamilyHandle,
      final boolean exclusive, final boolean doValidate) throws RocksDBException;
  private static native int getForUpdate(final long handle, final long readOptionsHandle,
      final byte[] key, final int keyOffset, final int keyLength, final byte[] value,
      final int valueOffset, final int valueLen, final long columnFamilyHandle,
      final boolean exclusive, final boolean doValidate) throws RocksDBException;
  private static native int getDirectForUpdate(final long handle, final long readOptionsHandle,
      final ByteBuffer key, final int keyOffset, final int keyLength, final ByteBuffer value,
      final int valueOffset, final int valueLen, final long columnFamilyHandle,
      final boolean exclusive, final boolean doValidate) throws RocksDBException;
  private static native byte[][] multiGetForUpdate(final long handle, final long readOptionsHandle,
      final byte[][] keys, final long[] columnFamilyHandles) throws RocksDBException;
  private static native byte[][] multiGetForUpdate(
      final long handle, final long readOptionsHandle, final byte[][] keys) throws RocksDBException;
  private static native long getIterator(
      final long handle, final long readOptionsHandle, final long columnFamilyHandle);
  private static native void put(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, final int valueLength)
      throws RocksDBException;
  private static native void put(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, final int valueLength,
      final long columnFamilyHandle, final boolean assumeTracked) throws RocksDBException;
  private static native void put(final long handle, final byte[][] keys, final int keysLength,
      final byte[][] values, final int valuesLength, final long columnFamilyHandle,
      final boolean assumeTracked) throws RocksDBException;
  private static native void put(final long handle, final byte[][] keys, final int keysLength,
      final byte[][] values, final int valuesLength) throws RocksDBException;
  private static native void putDirect(long handle, ByteBuffer key, int keyOffset, int keyLength,
      ByteBuffer value, int valueOffset, int valueLength, long cfHandle,
      final boolean assumeTracked) throws RocksDBException;
  private static native void putDirect(long handle, ByteBuffer key, int keyOffset, int keyLength,
      ByteBuffer value, int valueOffset, int valueLength) throws RocksDBException;

  private static native void merge(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, final int valueLength,
      final long columnFamilyHandle, final boolean assumeTracked) throws RocksDBException;
  private static native void mergeDirect(long handle, ByteBuffer key, int keyOffset, int keyLength,
      ByteBuffer value, int valueOffset, int valueLength, long cfHandle, boolean assumeTracked)
      throws RocksDBException;
  private static native void mergeDirect(long handle, ByteBuffer key, int keyOffset, int keyLength,
      ByteBuffer value, int valueOffset, int valueLength) throws RocksDBException;

  private static native void merge(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset, final int valueLength)
      throws RocksDBException;
  private static native void delete(final long handle, final byte[] key, final int keyLength,
      final long columnFamilyHandle, final boolean assumeTracked) throws RocksDBException;
  private static native void delete(final long handle, final byte[] key, final int keyLength)
      throws RocksDBException;
  private static native void delete(final long handle, final byte[][] keys, final int keysLength,
      final long columnFamilyHandle, final boolean assumeTracked) throws RocksDBException;
  private static native void delete(final long handle, final byte[][] keys, final int keysLength)
      throws RocksDBException;
  private static native void singleDelete(final long handle, final byte[] key, final int keyLength,
      final long columnFamilyHandle, final boolean assumeTracked) throws RocksDBException;
  private static native void singleDelete(final long handle, final byte[] key, final int keyLength)
      throws RocksDBException;
  private static native void singleDelete(final long handle, final byte[][] keys,
      final int keysLength, final long columnFamilyHandle, final boolean assumeTracked)
      throws RocksDBException;
  private static native void singleDelete(
      final long handle, final byte[][] keys, final int keysLength) throws RocksDBException;
  private static native void putUntracked(final long handle, final byte[] key, final int keyLength,
      final byte[] value, final int valueLength, final long columnFamilyHandle)
      throws RocksDBException;
  private static native void putUntracked(final long handle, final byte[] key, final int keyLength,
      final byte[] value, final int valueLength) throws RocksDBException;
  private static native void putUntracked(final long handle, final byte[][] keys,
      final int keysLength, final byte[][] values, final int valuesLength,
      final long columnFamilyHandle) throws RocksDBException;
  private static native void putUntracked(final long handle, final byte[][] keys,
      final int keysLength, final byte[][] values, final int valuesLength) throws RocksDBException;
  private static native void mergeUntracked(final long handle, final byte[] key, final int keyOff,
      final int keyLength, final byte[] value, final int valueOff, final int valueLength,
      final long columnFamilyHandle) throws RocksDBException;
  private static native void mergeUntrackedDirect(final long handle, final ByteBuffer key,
      final int keyOff, final int keyLength, final ByteBuffer value, final int valueOff,
      final int valueLength, final long columnFamilyHandle) throws RocksDBException;
  private static native void deleteUntracked(final long handle, final byte[] key,
      final int keyLength, final long columnFamilyHandle) throws RocksDBException;
  private static native void deleteUntracked(
      final long handle, final byte[] key, final int keyLength) throws RocksDBException;
  private static native void deleteUntracked(final long handle, final byte[][] keys,
      final int keysLength, final long columnFamilyHandle) throws RocksDBException;
  private static native void deleteUntracked(
      final long handle, final byte[][] keys, final int keysLength) throws RocksDBException;
  private static native void putLogData(final long handle, final byte[] blob, final int blobLength);
  private static native void disableIndexing(final long handle);
  private static native void enableIndexing(final long handle);
  private static native long getNumKeys(final long handle);
  private static native long getNumPuts(final long handle);
  private static native long getNumDeletes(final long handle);
  private static native long getNumMerges(final long handle);
  private static native long getElapsedTime(final long handle);
  private static native long getWriteBatch(final long handle);
  private static native void setLockTimeout(final long handle, final long lockTimeout);
  private static native long getWriteOptions(final long handle);
  private static native void setWriteOptions(final long handle, final long writeOptionsHandle);
  private static native void undoGetForUpdate(
      final long handle, final byte[] key, final int keyLength, final long columnFamilyHandle);
  private static native void undoGetForUpdate(
      final long handle, final byte[] key, final int keyLength);
  private static native void rebuildFromWriteBatch(final long handle, final long writeBatchHandle)
      throws RocksDBException;
  private static native long getCommitTimeWriteBatch(final long handle);
  private static native void setLogNumber(final long handle, final long logNumber);
  private static native long getLogNumber(final long handle);
  private static native void setName(final long handle, final String name) throws RocksDBException;
  private static native String getName(final long handle);
  private static native long getID(final long handle);
  private static native boolean isDeadlockDetect(final long handle);
  private native WaitingTransactions getWaitingTxns(final long handle);
  private static native byte getState(final long handle);
  private static native long getId(final long handle);

  @Override protected final native void disposeInternal(final long handle);
}
