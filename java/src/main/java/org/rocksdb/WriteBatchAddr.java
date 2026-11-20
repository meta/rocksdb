//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for adding entries to a {@link WriteBatch} or {@link WriteBatchWithIndex} with keys
 * and values held in native memory, passed as a raw address and a length. See
 * {@link RocksDBAddr} for why this is faster than the {@code byte[]} and
 * {@link java.nio.ByteBuffer} methods, and for how to use it by subclassing.
 *
 * <p><strong>Addresses are not validated.</strong> Each address must point to native memory that
 * holds at least the given number of bytes, and that memory must remain valid, and must not be
 * modified, until the call returns. The batch keeps its own copy of the key and value, so the
 * memory can be reused as soon as the call returns. An invalid address can crash the JVM. An
 * address of {@code 0} or a negative length is rejected with a {@link RocksDBException}.
 *
 * <p>This class is not thread-safe, like the batch it wraps.
 */
public class WriteBatchAddr {
  private final AbstractWriteBatch writeBatch;

  /**
   * Constructs an instance that adds entries to the given batch.
   *
   * @param writeBatch the batch to add entries to. It is not owned by this object, and must not be
   *     closed while this object is in use.
   */
  protected WriteBatchAddr(final AbstractWriteBatch writeBatch) {
    this.writeBatch = writeBatch;
  }

  /**
   * Store the mapping from the key to the value in the default column family.
   *
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   * @param valueAddr the native address of the value
   * @param valueLen the length of the value in bytes
   *
   * @throws RocksDBException if the batch rejects the entry, or if an address is 0 or a length
   *     is negative
   */
  protected final void put(final long keyAddr, final int keyLen, final long valueAddr,
      final int valueLen) throws RocksDBException {
    writeBatch.putAddr(writeBatch.nativeHandle_, keyAddr, keyLen, valueAddr, valueLen, 0);
  }

  /**
   * Store the mapping from the key to the value in a column family.
   *
   * @param columnFamilyHandle the column family to write to
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   * @param valueAddr the native address of the value
   * @param valueLen the length of the value in bytes
   *
   * @throws RocksDBException if the batch rejects the entry, or if an address is 0 or a length
   *     is negative
   */
  protected final void put(final ColumnFamilyHandle columnFamilyHandle, final long keyAddr,
      final int keyLen, final long valueAddr, final int valueLen) throws RocksDBException {
    writeBatch.putAddr(writeBatch.nativeHandle_, keyAddr, keyLen, valueAddr, valueLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the mapping for the key in the default column family, if it exists.
   *
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   *
   * @throws RocksDBException if the batch rejects the entry, or if the address is 0 or the
   *     length is negative
   */
  protected final void delete(final long keyAddr, final int keyLen) throws RocksDBException {
    writeBatch.deleteAddr(writeBatch.nativeHandle_, keyAddr, keyLen, 0);
  }

  /**
   * Remove the mapping for the key in a column family, if it exists.
   *
   * @param columnFamilyHandle the column family to write to
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   *
   * @throws RocksDBException if the batch rejects the entry, or if the address is 0 or the
   *     length is negative
   */
  protected final void delete(final ColumnFamilyHandle columnFamilyHandle, final long keyAddr,
      final int keyLen) throws RocksDBException {
    writeBatch.deleteAddr(
        writeBatch.nativeHandle_, keyAddr, keyLen, columnFamilyHandle.nativeHandle_);
  }
}
