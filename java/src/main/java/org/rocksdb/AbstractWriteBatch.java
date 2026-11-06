// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

public abstract class AbstractWriteBatch extends RocksObject
    implements WriteBatchInterface {

  protected AbstractWriteBatch(final long nativeHandle) {
    super(nativeHandle);
  }

  @Override
  public int count() {
    return count0(nativeHandle_);
  }

  @Override
  public void put(final byte[] key, final byte[] value) throws RocksDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  @Override
  public void put(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final byte[] value)
      throws RocksDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void merge(final byte[] key, final byte[] value) throws RocksDBException {
    merge(nativeHandle_, key, key.length, value, value.length);
  }

  @Override
  public void merge(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final byte[] value) throws RocksDBException {
    merge(nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void put(final ByteBuffer key, final ByteBuffer value) throws RocksDBException {
    if (key.isDirect() && value.isDirect()) {
      putDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
          value.remaining(), 0);
    } else if (key.hasArray() && value.hasArray()) {
      put(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining(),
          value.array(), value.arrayOffset() + value.position(), value.remaining());
    } else {
      throw new RocksDBException(RocksDB.BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  @Override
  public void put(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key,
      final ByteBuffer value) throws RocksDBException {
    if (key.isDirect() && value.isDirect()) {
      putDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
          value.remaining(), columnFamilyHandle.nativeHandle_);
    } else if (key.hasArray() && value.hasArray()) {
      put(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining(),
          value.array(), value.arrayOffset() + value.position(), value.remaining(),
          columnFamilyHandle.nativeHandle_);
    } else {
      throw new RocksDBException(RocksDB.BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
    value.position(value.limit());
  }

  @Override
  public void delete(final byte[] key) throws RocksDBException {
    delete(nativeHandle_, key, key.length);
  }

  @Override
  public void delete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
      throws RocksDBException {
    delete(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void delete(final ByteBuffer key) throws RocksDBException {
    if (key.isDirect()) {
      deleteDirect(nativeHandle_, key, key.position(), key.remaining(), 0);
    } else if (key.hasArray()) {
      delete(nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining());
    } else {
      throw new RocksDBException(RocksDB.BB_ALL_DIRECT_OR_INDIRECT);
    }
    key.position(key.limit());
  }

  @Override
  public void delete(final ColumnFamilyHandle columnFamilyHandle, final ByteBuffer key)
      throws RocksDBException {
    if (key.isDirect()) {
      deleteDirect(
          nativeHandle_, key, key.position(), key.remaining(), columnFamilyHandle.nativeHandle_);
      key.position(key.limit());
    } else if (key.hasArray()) {
      // TODO - Refactor, add native method
      ByteBuffer buffer = ByteBuffer.allocateDirect(key.remaining());
      buffer.put(key).flip();
      deleteDirect(nativeHandle_, buffer, buffer.position(), buffer.remaining(),
          columnFamilyHandle.nativeHandle_);
    }
  }

  @Override
  public void singleDelete(final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, key, key.length);
  }

  @Override
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
      throws RocksDBException {
    singleDelete(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void deleteRange(final byte[] beginKey, final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, beginKey, beginKey.length, endKey, endKey.length);
  }

  @Override
  public void deleteRange(final ColumnFamilyHandle columnFamilyHandle, final byte[] beginKey,
      final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, beginKey, beginKey.length, endKey, endKey.length,
        columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void putLogData(final byte[] blob) throws RocksDBException {
    putLogData(nativeHandle_, blob, blob.length);
  }

  @Override
  public void clear() {
    clear0(nativeHandle_);
  }

  @Override
  public void setSavePoint() {
    setSavePoint0(nativeHandle_);
  }

  @Override
  public void rollbackToSavePoint() throws RocksDBException {
    rollbackToSavePoint0(nativeHandle_);
  }

  @Override
  public void popSavePoint() throws RocksDBException {
    popSavePoint(nativeHandle_);
  }

  @Override
  public void setMaxBytes(final long maxBytes) {
    setMaxBytes(nativeHandle_, maxBytes);
  }

  @Override
  public WriteBatch getWriteBatch() {
    return getWriteBatch(nativeHandle_);
  }

  abstract int count0(final long handle);

  abstract void put(final long handle, final byte[] key, final int keyOffset, final int keyLen,
      final byte[] value, final int valueOffset, final int valueLen) throws RocksDBException;

  abstract void put(final long handle, final byte[] key, final int keyOffset, final int keyLen,
      final byte[] value, final int valueOffset, final int valueLen, final long cfHandle)
      throws RocksDBException;

  abstract void putDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final ByteBuffer value, final int valueOffset, final int valueLength,
      final long cfHandle) throws RocksDBException;

  abstract void merge(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen) throws RocksDBException;

  abstract void merge(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle)
      throws RocksDBException;

  abstract void delete(final long handle, final byte[] key,
      final int keyLen) throws RocksDBException;

  abstract void delete(final long handle, final byte[] key,
      final int keyLen, final long cfHandle) throws RocksDBException;

  abstract void singleDelete(final long handle, final byte[] key, final int keyLen)
      throws RocksDBException;

  abstract void singleDelete(final long handle, final byte[] key, final int keyLen,
      final long cfHandle) throws RocksDBException;

  abstract void deleteDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final long cfHandle) throws RocksDBException;

  abstract void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen) throws RocksDBException;

  abstract void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen, final long cfHandle) throws RocksDBException;

  abstract void putLogData(final long handle, final byte[] blob,
      final int blobLen) throws RocksDBException;

  abstract void clear0(final long handle);

  abstract void setSavePoint0(final long handle);

  abstract void rollbackToSavePoint0(final long handle);

  abstract void popSavePoint(final long handle) throws RocksDBException;

  abstract void setMaxBytes(final long handle, long maxBytes);

  abstract WriteBatch getWriteBatch(final long handle);
}
