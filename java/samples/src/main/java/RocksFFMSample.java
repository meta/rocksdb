// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import org.rocksdb.*;
import org.rocksdb.ffm.RocksFFM;

public class RocksFFMSample {
  private final static String NAME = "RocksFFMSample";

  public static void main(final String[] args) {
    if (args.length < 1) {
      System.out.printf("usage: %s db_path\n", NAME);
      System.exit(-1);
    }

    final RocksFFMSample sample = new RocksFFMSample();
    System.err.println("Create DB (JNI)");
    sample.createDB(args[0]);
    // createCF(args[0]);
    try (Arena arena = Arena.ofConfined()) {
      System.err.println("Open DB (FFM)");
      RocksFFM rocksFFM = RocksFFM.open(arena, args[0]);
      System.err.println("Read from DB");
      MemorySegment value2 = rocksFFM.get("key2");
      System.err.printf(
          "Read %d bytes (%s) from DB\n", value2.byteSize(), stringFromSegment(value2));

      MemorySegment value4 = rocksFFM.get("key4");
      System.err.printf(
          "Read %d bytes (%s) from DB\n", value4.byteSize(), stringFromSegment(value4));

      System.err.println("Didn't die!");
    }
  }

  private static String stringFromSegment(MemorySegment segment) {
    int byteSize = (int) segment.byteSize();
    byte[] bytes = new byte[byteSize];
    MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, bytes, 0, byteSize);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private void createDB(String db_path) {
    try (final RocksDB db = RocksDB.open(db_path)) {
      db.put("key1".getBytes(), "value1".getBytes());
      db.put("key2".getBytes(), "value2".getBytes());
      db.put("key3".getBytes(), "somethingElseEntirely".getBytes());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private void createCF(String db_path) {
    System.out.printf("%s\n", NAME);
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, db_path)) {
      assert (db != null);

      // create column family
      try (final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
               new ColumnFamilyDescriptor("new_cf".getBytes(), new ColumnFamilyOptions()))) {
        assert (columnFamilyHandle != null);
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }
}
