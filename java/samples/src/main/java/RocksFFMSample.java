// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.rocksdb.ffm.RocksdbOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.ffm.RocksFFM;
import org.rocksdb.ffm.RocksFFMException;

import static org.rocksdb.ffm.RocksFFM.segmentOf;
import static org.rocksdb.ffm.RocksFFM.stringOf;

public class RocksFFMSample {
  static {
    RocksDB.loadLibrary();
  }

  private final static String NAME = "RocksFFMSample";

  private static final Map<String, String> values =
      Map.of("key1", "value1", "key2", "value2", "key3", "somethingElseEntirely");

  public static void main(final String[] args) throws RocksFFMException {
    if (args.length < 1) {
      System.out.printf("usage: %s db_path\n", NAME);
      System.exit(-1);
    }

    try (Arena arena = Arena.ofConfined()) {
      try (RocksdbOptions rocksdbOptions = new RocksdbOptions()) {
        rocksdbOptions.setCreateIfMissing((byte) 1);
        System.err.println("Open DB (FFM) (create_if)");
        try (var db = RocksFFM.db(arena).open(rocksdbOptions, args[0])) {

          RocksFFM.Getter get2 = db.get(segmentOf(arena, "key2"));
          MemorySegment value2 = get2.invoke();
          if (value2 == null) {
            System.err.printf("Read unknown key \"key2\" correctly returned null\n");
          } else {
            System.err.printf("Read unknown key \"key2\" unexpectedly returned %s\n", value2);
          }

          RocksFFM.Putter put4 = db.put(segmentOf(arena, "key4"), segmentOf(arena, "wroteValue4"));
          put4.invoke();

          RocksFFM.Getter get4 = db.get(segmentOf(arena, "key4"));
          MemorySegment value4 = get4.invoke();
          if (value4 == null) {
            System.err.printf("ERROR - Read written key \"key4\" returned null\n");
          } else {
            String string4 = stringOf(value4);
            if (string4.equals("wroteValue4")) {
              System.err.printf("Read written key \"key4\" correctly returned %s\n", string4);
            } else {
              System.err.printf("Read written key \"key4\" returned wrong value %s\n", string4);
            }
          }
        }
      }
    }
  }
}
