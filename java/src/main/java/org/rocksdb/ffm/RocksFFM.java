// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.rocksdb.RocksDB;

import static org.rocksdb.ffm.c_h.*;
import static org.rocksdb.ffm.c_h_2.*;

public class RocksFFM {

    static {
        RocksDB.loadLibrary();
        System.err.println("Map library name is " + System.mapLibraryName("rocksdb"));
    }

    public static void justDoIt() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment db = arena.allocate(64, 1);
            MemorySegment options = arena.allocate(64, 1);
            MemorySegment column_family = arena.allocate(64, 1);
            long num_keys = 0;
            MemorySegment keys = arena.allocate(64, 1);
            byte sorted_input = 1;
            
            MemorySegment multiGet = c_h_2.rocksdb_batched_multi_get_pinned_cf(
                db, options, column_family, num_keys, keys, sorted_input);
        }

    }
}
