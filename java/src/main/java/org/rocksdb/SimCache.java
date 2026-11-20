// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Simulation Cache
 */
public class SimCache extends Cache {

  /**
   * Create a new key only cache with a fixed size capacity. The cache is sharded
   * to 2^numShardBits shards, by hash of the key. The total capacity
   * is divided and evenly assigned to each shard.
   * numShardBits = -1 means it is automatically determined: every shard
   * will be at least 512KB and number of shard bits will not exceed 6.
   *
   * @param handle The native handle of the real cache
   * @param capacity The fixed size capacity of the key only cache
   * @param numShardBits The key only cache is sharded to 2^numShardBits shards,
   *     by hash of the key
   */
  public SimCache(final long handle, final long capacity, final int numShardBits) {
    super(newSimCache(handle, capacity, numShardBits));
  }

  private static native long newSimCache(final long handle, final long capacity, final int numShardBits);
  @Override protected final native void disposeInternal(final long handle);
}
