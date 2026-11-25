//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for reading and writing a {@link RocksDB} with keys and values held in native
 * memory, passed as a raw address and a length. The memory can be owned by anything that exposes
 * its address, such as a Netty {@code ByteBuf}.
 *
 * <p>Unlike the {@code byte[]} and {@link java.nio.ByteBuffer} methods of {@link RocksDB}, these
 * methods do not copy the key or value into temporary native memory, and make no JNI calls to
 * look up a buffer's address or capacity. That per-call overhead matters most for small keys and
 * values.
 *
 * <p>The methods are {@code protected}. An application opts in by subclassing this class and
 * exposing the operations it needs in terms of its own buffer type, for example:
 *
 * <pre>{@code
 * class NettyRocksDB extends RocksDBAddr {
 *   NettyRocksDB(RocksDB db) {
 *     super(db);
 *   }
 *
 *   void put(WriteOptions writeOpts, ByteBuf key, ByteBuf value) throws RocksDBException {
 *     put(writeOpts, key.memoryAddress() + key.readerIndex(), key.readableBytes(),
 *         value.memoryAddress() + value.readerIndex(), value.readableBytes());
 *   }
 * }
 * }</pre>
 *
 * <p><strong>Addresses are not validated.</strong> Each address must point to native memory that
 * holds at least the given number of bytes, and that memory must remain valid, and must not be
 * modified, until the call returns. An invalid address can crash the JVM. An address of {@code 0}
 * or a negative length is rejected with a {@link RocksDBException}.
 *
 * <p>This class is thread-safe, as is the {@link RocksDB} it wraps.
 */
public class RocksDBAddr {
  private final RocksDB db;

  /**
   * Constructs an instance that operates on the given database.
   *
   * @param db the database to operate on. It is not owned by this object, and must remain open
   *     while this object is in use.
   */
  protected RocksDBAddr(final RocksDB db) {
    this.db = db;
  }

  /**
   * Set the database entry for the key to the value in a column family.
   *
   * @param columnFamilyHandle the column family to write to
   * @param writeOpts the write options
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   * @param valueAddr the native address of the value
   * @param valueLen the length of the value in bytes
   *
   * @throws RocksDBException if the write fails, or if an address is 0 or a length is negative
   */
  protected final void put(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts, final long keyAddr, final int keyLen, final long valueAddr,
      final int valueLen) throws RocksDBException {
    db.putAddr(columnFamilyHandle, writeOpts, keyAddr, keyLen, valueAddr, valueLen);
  }

  /**
   * Set the database entry for the key to the value in a column family, using the default write
   * options.
   *
   * @param columnFamilyHandle the column family to write to
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   * @param valueAddr the native address of the value
   * @param valueLen the length of the value in bytes
   *
   * @throws RocksDBException if the write fails, or if an address is 0 or a length is negative
   */
  protected final void put(final ColumnFamilyHandle columnFamilyHandle, final long keyAddr,
      final int keyLen, final long valueAddr, final int valueLen) throws RocksDBException {
    db.putAddr(columnFamilyHandle, keyAddr, keyLen, valueAddr, valueLen);
  }

  /**
   * Set the database entry for the key to the value in the default column family.
   *
   * @param writeOpts the write options
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   * @param valueAddr the native address of the value
   * @param valueLen the length of the value in bytes
   *
   * @throws RocksDBException if the write fails, or if an address is 0 or a length is negative
   */
  protected final void put(final WriteOptions writeOpts, final long keyAddr, final int keyLen,
      final long valueAddr, final int valueLen) throws RocksDBException {
    db.putAddr(writeOpts, keyAddr, keyLen, valueAddr, valueLen);
  }

  /**
   * Get the value for the key in the default column family, copying it into native memory.
   *
   * @param readOpts the read options
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   * @param valueAddr the native address of the memory that receives the value
   * @param valueLen the capacity of that memory in bytes. At most this many bytes are copied.
   *
   * @return the length of the value in bytes, which is larger than {@code valueLen} if the copy
   *     was truncated, or {@link RocksDB#NOT_FOUND} if the key does not exist
   *
   * @throws RocksDBException if the read fails, or if an address is 0 or a length is negative
   */
  protected final int get(final ReadOptions readOpts, final long keyAddr, final int keyLen,
      final long valueAddr, final int valueLen) throws RocksDBException {
    return db.getAddr(readOpts, keyAddr, keyLen, valueAddr, valueLen);
  }

  /**
   * Get the value for the key in a column family, copying it into native memory.
   *
   * @param columnFamilyHandle the column family to read from
   * @param readOpts the read options
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   * @param valueAddr the native address of the memory that receives the value
   * @param valueLen the capacity of that memory in bytes. At most this many bytes are copied.
   *
   * @return the length of the value in bytes, which is larger than {@code valueLen} if the copy
   *     was truncated, or {@link RocksDB#NOT_FOUND} if the key does not exist
   *
   * @throws RocksDBException if the read fails, or if an address is 0 or a length is negative
   */
  protected final int get(final ColumnFamilyHandle columnFamilyHandle, final ReadOptions readOpts,
      final long keyAddr, final int keyLen, final long valueAddr, final int valueLen)
      throws RocksDBException {
    return db.getAddr(columnFamilyHandle, readOpts, keyAddr, keyLen, valueAddr, valueLen);
  }

  /**
   * Remove the database entry for the key in the default column family, if it exists.
   *
   * @param writeOpts the write options
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   *
   * @throws RocksDBException if the write fails, or if the address is 0 or the length is negative
   */
  protected final void delete(final WriteOptions writeOpts, final long keyAddr, final int keyLen)
      throws RocksDBException {
    db.deleteAddr(writeOpts, keyAddr, keyLen);
  }

  /**
   * Remove the database entry for the key in a column family, if it exists.
   *
   * @param columnFamilyHandle the column family to write to
   * @param writeOpts the write options
   * @param keyAddr the native address of the key
   * @param keyLen the length of the key in bytes
   *
   * @throws RocksDBException if the write fails, or if the address is 0 or the length is negative
   */
  protected final void delete(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts, final long keyAddr, final int keyLen) throws RocksDBException {
    db.deleteAddr(columnFamilyHandle, writeOpts, keyAddr, keyLen);
  }
}
