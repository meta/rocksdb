// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.MergeTest.longFromByteArray;
import static org.rocksdb.MergeTest.longToByteArray;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MergeCFVariantsTest {
  @FunctionalInterface
  interface FunctionMerge<PDatabase, PColumnFamilyHandle, PLeft, PRight> {
    public void apply(PDatabase db, PColumnFamilyHandle one, PLeft two, PRight three)
        throws RocksDBException;
  }

  @Parameterized.Parameters
  public static List<FunctionMerge<RocksDB, ColumnFamilyHandle, byte[], byte[]>> data() {
    return Arrays.asList(RocksDB::merge,
        (db, cfh, left, right)
            -> db.merge(cfh, new WriteOptions(), left, right),
        (db, cfh, left, right)
            -> {
          final byte[] left0 =
              ("1234567" + new String(left, StandardCharsets.UTF_8) + "890").getBytes();
          final byte[] right0 =
              ("1234" + new String(right, StandardCharsets.UTF_8) + "567890ab").getBytes();
          db.merge(cfh, left0, 7, left.length, right0, 4, right.length);
        },
        (db, cfh, left, right)
            -> {
          final byte[] left0 =
              ("1234567" + new String(left, StandardCharsets.UTF_8) + "890").getBytes();
          final byte[] right0 =
              ("1234" + new String(right, StandardCharsets.UTF_8) + "567890ab").getBytes();
          db.merge(cfh, new WriteOptions(), left0, 7, left.length, right0, 4, right.length);
        },
        (db, cfh, left, right)
            -> {
          final ByteBuffer bbLeft = ByteBuffer.allocateDirect(100);
          final ByteBuffer bbRight = ByteBuffer.allocateDirect(100);
          bbLeft.put(left).flip();
          bbRight.put(right).flip();
          db.merge(cfh, new WriteOptions(), bbLeft, bbRight);
        },
        (db, cfh, left, right) -> {
          final ByteBuffer bbLeft = ByteBuffer.allocate(100);
          final ByteBuffer bbRight = ByteBuffer.allocate(100);
          bbLeft.put(left).flip();
          bbRight.put(right).flip();
          db.merge(cfh, new WriteOptions(), bbLeft, bbRight);
        });
  }

  @Parameterized.Parameter
  public FunctionMerge<RocksDB, ColumnFamilyHandle, byte[], byte[]> mergeFunction;

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void cFUInt64AddOperatorOption() throws InterruptedException, RocksDBException {
    try (final UInt64AddOperator uint64AddOperator = new UInt64AddOperator();
        final ColumnFamilyOptions cfOpt1 =
            new ColumnFamilyOptions().setMergeOperator(uint64AddOperator);
        final ColumnFamilyOptions cfOpt2 =
            new ColumnFamilyOptions().setMergeOperator(uint64AddOperator);
        final ColumnFamilyDescriptor defaultCF =
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpt1);
        final ColumnFamilyDescriptor newCf =
            new ColumnFamilyDescriptor("new_cf".getBytes(), cfOpt2)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, newCf);
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions opt =
               new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(
               opt, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, columnFamilyHandleList)) {
        try {
          // writing (long)100 under key
          db.put(columnFamilyHandleList.get(1), "cfkey".getBytes(), longToByteArray(100));
          // merge (long)1 under key
          mergeFunction.apply(
              db, columnFamilyHandleList.get(1), "cfkey".getBytes(), longToByteArray(1));
          byte[] value = db.get(columnFamilyHandleList.get(1), "cfkey".getBytes());
          long longValue = longFromByteArray(value);

          // Test also with createColumnFamily

          try (final ColumnFamilyOptions cfHandleOpts =
                   new ColumnFamilyOptions().setMergeOperator(uint64AddOperator);
              ColumnFamilyDescriptor newCf2 =
                  new ColumnFamilyDescriptor("new_cf2".getBytes(), cfHandleOpts);
              final ColumnFamilyHandle cfHandle = db.createColumnFamily(newCf2)) {
            // writing (long)200 under cfkey2
            db.put(cfHandle, "cfkey2".getBytes(), longToByteArray(200));
            // merge (long)50 under cfkey2
            db.merge(cfHandle, new WriteOptions(), "cfkey2".getBytes(), longToByteArray(50));
            value = db.get(cfHandle, "cfkey2".getBytes());
            long longValueTmpCf = longFromByteArray(value);

            assertThat(longValue).isEqualTo(101);
            assertThat(longValueTmpCf).isEqualTo(250);
          }
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }
}
