// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.*;

/**
 * JNI passthrough for MemoryUtil.
 */
public class MemoryUtil {

  /**
   * <p>Returns the approximate memory usage of different types in the input
   * list of DBs and Cache set.  For instance, in the output map the key
   * kMemTableTotal will be associated with the memory
   * usage of all the mem-tables from all the input rocksdb instances.</p>
   *
   * <p>Note that for memory usage inside Cache class, we will
   * only report the usage of the input "cache_set" without
   * including those Cache usage inside the input list "dbs"
   * of DBs.</p>
   *
   * @param dbs List of dbs to collect memory usage for.
   * @param caches Set of caches to collect memory usage for.
   * @return Map from {@link MemoryUsageType} to memory usage as a {@link Long}.
   */
  @SuppressWarnings({"PMD.UseEnumCollections", "PMD.CloseResource"})
  public static Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(
      final List<RocksDB> dbs, final Set<Cache> caches) {
    final int dbCount = (dbs == null) ? 0 : dbs.size();
    final int cacheCount = (caches == null) ? 0 : caches.size();
    final long[] dbHandles = new long[dbCount];
    final long[] cacheHandles = new long[cacheCount];
    if (dbCount > 0) {
      final ListIterator<RocksDB> dbIter = dbs.listIterator();
      while (dbIter.hasNext()) {
        dbHandles[dbIter.nextIndex()] = dbIter.next().nativeHandle_;
      }
    }
    if (cacheCount > 0) {
      // NOTE: This index handling is super ugly but I couldn't get a clean way to track both the
      // index and the iterator simultaneously within a Set.
      int i = 0;
      for (final Cache cache : caches) {
        cacheHandles[i] = cache.nativeHandle_;
        i++;
      }
    }
    final Map<Byte, Long> byteOutput = getApproximateMemoryUsageByType(dbHandles, cacheHandles);
    final Map<MemoryUsageType, Long> output = new HashMap<>();
    for (final Map.Entry<Byte, Long> longEntry : byteOutput.entrySet()) {
      output.put(MemoryUsageType.getMemoryUsageType(longEntry.getKey()), longEntry.getValue());
    }
    return output;
  }

  private static native Map<Byte, Long> getApproximateMemoryUsageByType(
      final long[] dbHandles, final long[] cacheHandles);
}
