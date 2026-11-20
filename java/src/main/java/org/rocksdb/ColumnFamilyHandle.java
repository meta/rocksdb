// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.Objects;

/**
 * ColumnFamilyHandle class to hold handles to underlying rocksdb
 * ColumnFamily Pointers.
 */
public class ColumnFamilyHandle extends RocksObject {
  /**
   * Constructs column family Java object, which operates on underlying native object.
   *
   * @param rocksDB db instance associated with this column family
   * @param nativeHandle native handle to underlying native ColumnFamily object
   */
  ColumnFamilyHandle(final RocksDB rocksDB,
      final long nativeHandle) {
    super(nativeHandle);
    // rocksDB must point to a valid RocksDB instance;
    assert(rocksDB != null);
    // ColumnFamilyHandle must hold a reference to the related RocksDB instance
    // to guarantee that while a GC cycle starts ColumnFamilyHandle instances
    // are freed prior to RocksDB instances.
    this.rocksDB_ = rocksDB;
  }

  /**
   * Constructor called only from JNI.
   * <p>
   * NOTE: we are producing an additional Java Object here to represent the underlying native C++
   * ColumnFamilyHandle object. The underlying object is not owned by ourselves. The Java API user
   * likely already had a ColumnFamilyHandle Java object which owns the underlying C++ object, as
   * they will have been presented it when they opened the database or added a Column Family.
   * <p>
   *
   * TODO(AR) - Potentially a better design would be to cache the active Java Column Family Objects
   * in RocksDB, and return the same Java Object instead of instantiating a new one here. This could
   * also help us to improve the Java API semantics for Java users. See for example
   * <a href="https://github.com/meta/rocksdb/issues/2687">...</a>.
   *
   * @param nativeHandle native handle to the column family.
   */
  ColumnFamilyHandle(final long nativeHandle) {
    super(nativeHandle);
    rocksDB_ = null;
    disOwnNativeHandle();
  }

  /**
   * Gets the name of the Column Family.
   *
   * @return The name of the Column Family.
   *
   * @throws RocksDBException if an error occurs whilst retrieving the name.
   */
  public byte[] getName() throws RocksDBException {
    assert(isOwningHandle() || isDefaultColumnFamily());
    return getName(nativeHandle_);
  }

  /**
   * Gets the ID of the Column Family.
   *
   * @return the ID of the Column Family.
   */
  public int getID() {
    assert(isOwningHandle() || isDefaultColumnFamily());
    return getID(nativeHandle_);
  }

  /**
   * Gets the up-to-date descriptor of the column family
   * associated with this handle. Since it fills "*desc" with the up-to-date
   * information, this call might internally lock and release DB mutex to
   * access the up-to-date CF options. In addition, all the pointer-typed
   * options cannot be referenced any longer than the original options exist.
   * <p>
   * Note that this function is not supported in RocksDBLite.
   *
   * @return the up-to-date descriptor.
   *
   * @throws RocksDBException if an error occurs whilst retrieving the
   *     descriptor.
   */
  public ColumnFamilyDescriptor getDescriptor() throws RocksDBException {
    assert(isOwningHandle() || isDefaultColumnFamily());
    return getDescriptor(nativeHandle_);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    @SuppressWarnings("PMD.CloseResource") final ColumnFamilyHandle that = (ColumnFamilyHandle) o;
    try {
      return rocksDB_.nativeHandle_ == that.rocksDB_.nativeHandle_ &&
          getID() == that.getID() &&
          Arrays.equals(getName(), that.getName());
    } catch (final RocksDBException e) {
      throw new RuntimeException("Cannot compare column family handles", e);
    }
  }

  @Override
  public int hashCode() {
    try {
      int result = Objects.hash(getID(), rocksDB_.nativeHandle_);
      result = 31 * result + Arrays.hashCode(getName());
      return result;
    } catch (final RocksDBException e) {
      throw new RuntimeException("Cannot calculate hash code of column family handle", e);
    }
  }

  protected boolean isDefaultColumnFamily() {
    return nativeHandle_ == rocksDB_.getDefaultColumnFamily().nativeHandle_;
  }

  /**
   * <p>Deletes underlying C++ iterator pointer.</p>
   *
   * <p>Note: the underlying handle can only be safely deleted if the RocksDB
   * instance related to a certain ColumnFamilyHandle is still valid and
   * initialized. Therefore {@code disposeInternal()} checks if the RocksDB is
   * initialized before freeing the native handle.</p>
   */
  @Override
  protected void disposeInternal() {
    if(rocksDB_.isOwningHandle()) {
      disposeInternal(nativeHandle_);
    }
  }

  private static native byte[] getName(final long handle) throws RocksDBException;
  private static native int getID(final long handle);
  private static native ColumnFamilyDescriptor getDescriptor(final long handle)
      throws RocksDBException;
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  private final RocksDB rocksDB_;
}
