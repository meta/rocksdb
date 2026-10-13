// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.ffm;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksFFM {
  static {
    RocksDB.loadLibrary();
  }

  final Arena dbArena;
  final MemorySegment db;

  private RocksFFM(final Arena dbArena, final MemorySegment db) {
    this.db = db;
    this.dbArena = dbArena;
  }

  private static final MemorySegment NULL = MemorySegment.ofAddress(0L);

  public static RocksFFM open(Arena dbArena, String dbPath) throws RocksDBException {
    // TODO (AP) -- Options lifecycle may not be safely confined to a confined arena ?
    // See RocksDB rules on options structs
    MemorySegment options = rocksdb_c_api_2.rocksdb_options_create();
    MemorySegment pathName = dbArena.allocateFrom(dbPath, UTF_8);
    MemorySegment errptr = dbArena.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment db = rocksdb_c_api_2.rocksdb_open(options, pathName, errptr);

    MemorySegment err = errptr.get(ValueLayout.ADDRESS, 0L);
    if (err.address() == NULL.address()) {
      return new RocksFFM(dbArena, db);
    } else {
      err = err.reinterpret(Long.MAX_VALUE);
      String message = err.getString(0, UTF_8);
      throw new RocksDBException("rocksdb_open() " + message);
    }
  }

  /**
   * Fetch the value for a supplied key
   * @param key to look up
   * @return a {@link java.lang.foreign.MemorySegment} containing the retrieved value, or {@code
   *     null} if no value exists for the key
   * @throws RocksDBException if an error occurs
   */
  public MemorySegment get(String key) throws RocksDBException {
    MemorySegment read_options = rocksdb_c_api.rocksdb_readoptions_create();
    MemorySegment keySegment = dbArena.allocateFrom(key, UTF_8);
    long keySize = keySegment.byteSize() - 1; // 0-terminated
    MemorySegment vallenSegment = dbArena.allocateFrom(ValueLayout.JAVA_LONG, 0L);
    MemorySegment errptr = dbArena.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment value =
        rocksdb_c_api_2.rocksdb_get(db, read_options, keySegment, keySize, vallenSegment, errptr);
    MemorySegment err = errptr.get(ValueLayout.ADDRESS, 0L);
    if (err.address() == NULL.address()) {
      // success
      if (value.address() == NULL.address()) {
        // Status is NOT_FOUND
        return null;
      }
      long vallen = vallenSegment.get(ValueLayout.JAVA_LONG, 0);
      return value.asSlice(0L, vallen);
    } else {
      err = err.reinterpret(Long.MAX_VALUE);
      String message = err.getString(0, UTF_8);
      throw new RocksDBException("rocksdb_get() " + message);
    }
  }

  /**
   * Extract a string from a segment
   * `MemorySegment.getString(offset, charset)` doesn't work for us, as it expects a C-style
   * null-terminated string
   *
   * @param segment to interpret as a String
   * @return the Java string contained in `segment`
   */
  public static String stringFromSegment(MemorySegment segment) {
    int byteSize = (int) segment.byteSize();
    byte[] bytes = new byte[byteSize];
    MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, bytes, 0, byteSize);
    return new String(bytes, UTF_8);
  }

  // TODO (AP) - multiGet is ***INTERESTING***
  // Investigate efficient calling via:
  //
  // rocksdb_c_api_2.rocksdb_batched_multi_get_pinned_cf()
  //
}
