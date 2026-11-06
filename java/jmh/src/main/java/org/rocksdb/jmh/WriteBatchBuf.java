/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.rocksdb.jmh;

import static org.rocksdb.jmh.RocksDBBuf.readAddress;

import io.netty.buffer.ByteBuf;
import org.rocksdb.AbstractWriteBatch;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchAddr;

/**
 * Exposes {@link WriteBatchAddr} for Netty direct {@link ByteBuf}s, the way an application would.
 * Keys and values are the readable bytes of a buffer.
 */
final class WriteBatchBuf extends WriteBatchAddr {
  WriteBatchBuf(final AbstractWriteBatch writeBatch) {
    super(writeBatch);
  }

  void put(final ByteBuf key, final ByteBuf value) throws RocksDBException {
    put(readAddress(key), key.readableBytes(), readAddress(value), value.readableBytes());
  }
}
