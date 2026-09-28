// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import org.rocksdb.*;
import org.rocksdb.ffm.RocksFFM;

public class RocksFFMSample {
  private final static String NAME = "RocksFFMSample";

  public static void main(final String[] args) {
    if (args.length < 1) {
      System.out.printf("usage: %s db_path\n", NAME);
      System.exit(-1);
    }

    createDB(args[0]);
    // createCF(args[0]);

    //RocksFFM.wibble();
    System.err.println("Didn't die!");
  }

  private void createDB(String db_path) {
    try (final RocksDB db = RocksDB.open(db_path)) {
      db.put("key1", "value1");
      db.put("key2", "value2");
      db.put("key3", "somethingElseEntirely");
    }
  }

  private void createCF(String db_path) {
    System.out.println("%s\n", NAME);
    try (final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, db_path)) {
      assert (db != null);

      // create column family
      try (final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
               new ColumnFamilyDescriptor("new_cf".getBytes(), new ColumnFamilyOptions()))) {
        assert (columnFamilyHandle != null);
      }
    }
  }
}
