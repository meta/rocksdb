//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A ByteBuffer containing fetched data, together with a result for the fetch
 * and the total size of the object fetched.
 * <p>
 * Used for the individual results of
 * {@link RocksDB#multiGetByteBuffers(List, List)}
 * {@link RocksDB#multiGetByteBuffers(List, List, List)}
 * {@link RocksDB#multiGetByteBuffers(ReadOptions, List, List)}
 * {@link RocksDB#multiGetByteBuffers(ReadOptions, List, List, List)}
 */
public class ByteBufferGetStatus {
  public final Status status;
  public final int requiredSize;
  public final ByteBuffer value;

  /**
   * Constructor used for success status, when the value is contained in the buffer
   *
   * @param status the status of the request to fetch into the buffer
   * @param requiredSize the size of the data, which may be bigger than the buffer
   * @param value the buffer containing as much of the value as fits
   */
  ByteBufferGetStatus(final Status status, final int requiredSize, final ByteBuffer value) {
    this.status = status;
    this.requiredSize = requiredSize;
    this.value = value;
  }

  /**
   * Constructor used for a failure status, when no value is filled in
   *
   * @param status the status of the request to fetch into the buffer
   */
  ByteBufferGetStatus(final Status status) {
    this.status = status;
    this.requiredSize = 0;
    this.value = null;
  }
}
