/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import io.netty.buffer.ByteBuf;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBAddr;
import org.rocksdb.RocksDBException;

/**
 * Exposes {@link RocksDBAddr} for Netty direct {@link ByteBuf}s, the way an application would.
 * Keys and values are the readable bytes of a buffer; {@link #get} writes into the writable bytes
 * of the value buffer.
 */
final class RocksDBBuf extends RocksDBAddr {
  RocksDBBuf(final RocksDB db) {
    super(db);
  }

  void put(final ColumnFamilyHandle columnFamilyHandle, final ByteBuf key, final ByteBuf value)
      throws RocksDBException {
    put(columnFamilyHandle, readAddress(key), key.readableBytes(), readAddress(value),
        value.readableBytes());
  }

  int get(final ColumnFamilyHandle columnFamilyHandle, final ReadOptions readOpts,
      final ByteBuf key, final ByteBuf value) throws RocksDBException {
    return get(columnFamilyHandle, readOpts, readAddress(key), key.readableBytes(),
        value.memoryAddress() + value.writerIndex(), value.writableBytes());
  }

  static long readAddress(final ByteBuf buffer) {
    return buffer.memoryAddress() + buffer.readerIndex();
  }
}
