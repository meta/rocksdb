// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.rocksdb.*;
import org.rocksdb.ffm.RocksFFM;

public class RocksFFMSample {
  private final static String NAME = "RocksFFMSample";

  private static final Map<String, String> values =
      Map.of("key1", "value1", "key2", "value2", "key3", "somethingElseEntirely");

  public static void main(final String[] args) {
    if (args.length < 1) {
      System.out.printf("usage: %s db_path\n", NAME);
      System.exit(-1);
    }

    final RocksFFMSample sample = new RocksFFMSample();
    System.err.println("Create DB (JNI)");
    
    createDB(args[0], values);

    try (Arena arena = Arena.ofConfined()) {
      System.err.println("Open DB (FFM)");
      RocksFFM rocksFFM = RocksFFM.open(arena, args[0]);
      System.err.println("Read from DB");
      MemorySegment value2 = rocksFFM.get("key2");
      System.err.printf(
          "Read %d bytes (%s) from DB\n", value2.byteSize(), RocksFFM.stringFromSegment(value2));

      MemorySegment value4 = rocksFFM.get("key4");
      if (value4 == null) {
        System.err.printf("Read unknown key correctly returned null\n");
      } else {
        System.err.printf("Read unknown key unexpectedly returned %s\n", value4);
      }
      System.err.println("");
    } catch (RocksDBException e) {
      System.err.printf("Exception during RocksFFMSample %s", e.getMessage());
    }
  }

  private static void createDB(String db_path, Map<String, String> entries) {
    try (final RocksDB db = RocksDB.open(db_path)) {
      for (var entry : entries.entrySet()) {
        db.put(entry.getKey().getBytes(StandardCharsets.UTF_8),
            entry.getValue().getBytes(StandardCharsets.UTF_8));
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }
}
