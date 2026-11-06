// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Used to control the Optimistic Concurrency Control (OCC) aspect of a database.
 * It will be used during the creation of a
 * {@link org.rocksdb.OptimisticTransactionDB} (i.e., OptimisticTransactionDB.open()).
 * <p>
 * As a descendent of {@link AbstractNativeReference}, this class is {@link AutoCloseable}
 * and will be automatically released if opened in the preamble of a try with resources block.
 */
public class OptimisticTransactionDBOptions extends RocksObject {
  public OptimisticTransactionDBOptions() {
    super(newOptimisticTransactionOptions());
  }

  /**
   * Set OccValidationPolicy for this instance.
   * See {@link OccValidationPolicy}.
   *
   * @param policy The type of OCC Validation Policy
   *
   * @return this OptimisticTransactionOptions instance
   */
  public OptimisticTransactionDBOptions setOccValidationPolicy(final OccValidationPolicy policy) {
    assert (isOwningHandle());
    setOccValidationPolicy(nativeHandle_, policy.getValue());
    return this;
  }

  /**
   * Get OccValidationPolicy for this instance.
   * See {@link OccValidationPolicy}.
   *
   * @return The type of OCC Validation Policy used.
   */
  public OccValidationPolicy occValidationPolicy() {
    assert (isOwningHandle());
    return OccValidationPolicy.getOccValidationPolicy(occValidationPolicy(getNativeHandle()));
  }

  /**
   * Number of striped/bucketed mutex locks for validating transactions.
   * Used on only if validate_policy == {@link OccValidationPolicy}::VALIDATE_PARALLEL
   * and shared_lock_buckets (below) is empty. Larger number potentially
   * reduces contention but uses more memory.
   *
   * @param occLongBuckets Number of striped/bucketed mutex locks.
   *
   * @return this OptimisticTransactionOptions instance
   */
  public OptimisticTransactionDBOptions setOccLockBuckets(final long occLongBuckets) {
    assert (isOwningHandle());
    setOccLockBuckets(nativeHandle_, occLongBuckets);
    return this;
  }

  /**
   * Get the number of striped/bucketed mutex locks for validating transactions.
   * Used on only if validate_policy == {@link OccValidationPolicy}::VALIDATE_PARALLEL
   * and shared_lock_buckets (below) is empty. Larger number potentially
   * reduces contention but uses more memory.
   **
   * @return Number of striped/bucketed mutex locks.
   */
  public long getOccLockBuckets() {
    assert (isOwningHandle());
    return getOccLockBuckets(nativeHandle_);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native long newOptimisticTransactionOptions();
  private static native void setOccValidationPolicy(final long handle, final byte policy);
  private static native byte occValidationPolicy(final long handle);
  private static native void setOccLockBuckets(final long handle, final long occLongBuckets);
  private static native byte getOccLockBuckets(final long handle);
  private static native void disposeInternalJni(final long handle);
}
