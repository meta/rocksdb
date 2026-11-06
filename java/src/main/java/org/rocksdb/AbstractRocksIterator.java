// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Base class implementation for Rocks Iterators
 * in the Java API
 *
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @param <P> The type of the Parent Object from which the Rocks Iterator was
 *          created. This is used by disposeInternal to avoid double-free
 *          issues with the underlying C++ object.
 * @see RocksObject
 */
public abstract class AbstractRocksIterator<P extends RocksObject>
    extends RocksObject implements RocksIteratorInterface {
  final P parent_;
  final AtomicReference<Function<AbstractRocksIterator<P>, Boolean>> removeOnClosure_ =
      new AtomicReference<>();

  protected AbstractRocksIterator(final P parent, final long nativeHandle,
      final Function<AbstractRocksIterator<P>, Boolean> removeOnClosure) {
    super(nativeHandle);
    // parent must point to a valid RocksDB instance.
    assert (parent != null);
    // RocksIterator must hold a reference to the related parent instance
    // to guarantee that while a GC cycle starts RocksIterator instances
    // are freed prior to parent instances.
    parent_ = parent;
    removeOnClosure_.set(removeOnClosure);
  }

  protected AbstractRocksIterator(final P parent, final long nativeHandle) {
    this(parent, nativeHandle, null);
  }

  @Override
  public boolean isValid() {
    assert (isOwningHandle());
    return isValid0(nativeHandle_);
  }

  @Override
  public void seekToFirst() {
    assert (isOwningHandle());
    seekToFirst0(nativeHandle_);
  }

  @Override
  public void seekToLast() {
    assert (isOwningHandle());
    seekToLast0(nativeHandle_);
  }

  @Override
  public void seek(final byte[] target) {
    assert (isOwningHandle());
    seek0(nativeHandle_, target, target.length);
  }

  @Override
  public void seekForPrev(final byte[] target) {
    assert (isOwningHandle());
    seekForPrev0(nativeHandle_, target, target.length);
  }

  @Override
  public void seek(final ByteBuffer target) {
    assert (isOwningHandle());
    if (target.isDirect()) {
      seekDirect0(nativeHandle_, target, target.position(), target.remaining());
    } else {
      seekByteArray0(nativeHandle_, target.array(), target.arrayOffset() + target.position(),
          target.remaining());
    }
    target.position(target.limit());
  }

  @Override
  public void seekForPrev(final ByteBuffer target) {
    assert (isOwningHandle());
    if (target.isDirect()) {
      seekForPrevDirect0(nativeHandle_, target, target.position(), target.remaining());
    } else {
      seekForPrevByteArray0(nativeHandle_, target.array(), target.arrayOffset() + target.position(),
          target.remaining());
    }
    target.position(target.limit());
  }

  @Override
  public void next() {
    assert (isOwningHandle());
    next0(nativeHandle_);
  }

  @Override
  public void prev() {
    assert (isOwningHandle());
    prev0(nativeHandle_);
  }

  @Override
  public void refresh() throws RocksDBException {
    assert (isOwningHandle());
    refresh0(nativeHandle_);
  }

  @Override
  public void refresh(final Snapshot snapshot) throws RocksDBException {
    assert (isOwningHandle());
    refresh1(nativeHandle_, snapshot.getNativeHandle());
  }

  @Override
  public void status() throws RocksDBException {
    assert (isOwningHandle());
    status0(nativeHandle_);
  }

  @Override
  public void close() {
    super.close();
    Function<AbstractRocksIterator<P>, Boolean> removeOnClosure = removeOnClosure_.getAndSet(null);
    if (removeOnClosure != null) {
      removeOnClosure.apply(this);
    }
  }

  /**
   * <p>Deletes underlying C++ iterator pointer.</p>
   *
   * <p>Note: the underlying handle can only be safely deleted if the parent
   * instance related to a certain RocksIterator is still valid and initialized.
   * Therefore {@code disposeInternal()} checks if the parent is initialized
   * before freeing the native handle.</p>
   */
  @Override
  protected void disposeInternal() {
      if (parent_.isOwningHandle()) {
        disposeInternal(nativeHandle_);
      }
  }

  abstract boolean isValid0(long handle);
  abstract void seekToFirst0(long handle);
  abstract void seekToLast0(long handle);
  abstract void next0(long handle);
  abstract void prev0(long handle);
  abstract void refresh0(long handle) throws RocksDBException;
  abstract void refresh1(long handle, long snapshotHandle) throws RocksDBException;
  abstract void seek0(long handle, byte[] target, int targetLen);
  abstract void seekForPrev0(long handle, byte[] target, int targetLen);
  abstract void seekDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen);
  abstract void seekForPrevDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen);
  abstract void seekByteArray0(long handle, byte[] target, int targetOffset, int targetLen);
  abstract void seekForPrevByteArray0(long handle, byte[] target, int targetOffset, int targetLen);

  abstract void status0(long handle) throws RocksDBException;
}
