// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.ffm;

import static org.rocksdb.ffm.rocksdb_c_api.*;
import static org.rocksdb.ffm.rocksdb_c_api_2.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.rocksdb.RocksDB;

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

  public static RocksFFM open(Arena dbArena, String dbPath) {
    // TODO (AP) -- Options lifecycle may not be safely confined to a confined arena ?
    // See RocksDB rules on options structs
    MemorySegment options = rocksdb_c_api_2.rocksdb_options_create();
    MemorySegment pathName = dbArena.allocateFrom(dbPath, StandardCharsets.UTF_8);
    MemorySegment errptr = dbArena.allocateFrom(ValueLayout.ADDRESS, NULL);
    MemorySegment db = rocksdb_c_api_2.rocksdb_open(options, pathName, errptr);

    return new RocksFFM(dbArena, db);
  }

  public MemorySegment get(String key) {
    MemorySegment read_options = rocksdb_c_api.rocksdb_readoptions_create();
    MemorySegment keySegment = dbArena.allocateFrom(key, StandardCharsets.UTF_8);
    long keySize = keySegment.byteSize() - 1; // 0-terminated
    MemorySegment vallenSegment = dbArena.allocateFrom(ValueLayout.JAVA_LONG, 0L);
    MemorySegment errptr = dbArena.allocateFrom(ValueLayout.ADDRESS, NULL);
    System.err.printf("Reading key %s of %d bytes from DB\n", key, keySegment.byteSize());
    MemorySegment value =
        rocksdb_c_api_2.rocksdb_get(db, read_options, keySegment, keySize, vallenSegment, errptr);
    MemorySegment err = errptr.get(ValueLayout.ADDRESS, 0L);
    if (err.address() == NULL.address()) {
      // success
      long vallen = vallenSegment.get(ValueLayout.JAVA_LONG, 0);
      return value.asSlice(0L, vallen);
    } else {
      byte statusCode = err.get(ValueLayout.JAVA_BYTE, 0L);
      throw new RuntimeException("rocksdb_get(() status %d" + statusCode);
    }
  }

  // TODO (AP) - multiGet is ***INTERESTING***
  // Investigate efficient calling via:
  //
  // rocksdb_c_api_2.rocksdb_batched_multi_get_pinned_cf()
  //
}
