// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.util.ByteUtil.bytes;
import static org.rocksdb.util.TestUtil.*;

public class WalFilterTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void walFilter() throws RocksDBException {
    // Create 3 batches with two keys each
    final byte[][][] batchKeys = {
        new byte[][] {
            bytes("key1"),
            bytes("key2")
        },
        new byte[][] {
            bytes("key3"),
            bytes("key4")
        },
        new byte[][] {
            bytes("key5"),
            bytes("key6")
        }

    };

    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    // Test with all WAL processing options
    for (final WalProcessingOption option : WalProcessingOption.values()) {
      try (final ColumnFamilyDescriptor defaultCF =
               new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
          final ColumnFamilyDescriptor pikachuCF = new ColumnFamilyDescriptor(bytes("pikachu"))) {
        List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(defaultCF, pikachuCF);

        try (final Options options = optionsForLogIterTest();
            final DBOptions dbOptions = new DBOptions(options).setCreateMissingColumnFamilies(true);

            final RocksDB db = RocksDB.open(
                dbOptions, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, cfHandles)) {
          try (final WriteOptions writeOptions = new WriteOptions()) {
            // Write given keys in given batches
            for (final byte[][] batchKey : batchKeys) {
              final WriteBatch batch = new WriteBatch();
              for (final byte[] bytes : batchKey) {
                batch.put(cfHandles.get(0), bytes, dummyString(1024));
              }
              db.write(writeOptions, batch);
            }
          } finally {
            for (final ColumnFamilyHandle cfHandle : cfHandles) {
              cfHandle.close();
            }
            cfHandles.clear();
          }
        }

        // Create a test filter that would apply wal_processing_option at the first
        // record
        final int applyOptionForRecordIndex = 1;
        try (final TestableWalFilter walFilter =
                 new TestableWalFilter(option, applyOptionForRecordIndex)) {
          try (final Options options = optionsForLogIterTest();
              final DBOptions dbOptions = new DBOptions(options).setWalFilter(walFilter)) {
            try (final RocksDB db = RocksDB.open(
                     dbOptions, dbFolder.getRoot().getAbsolutePath(), cfDescriptors, cfHandles)) {
              try {
                assertThat(walFilter.logNumbers).isNotEmpty();
                assertThat(walFilter.logFileNames).isNotEmpty();
              } finally {
                for (final ColumnFamilyHandle cfHandle : cfHandles) {
                  cfHandle.close();
                }
                cfHandles.clear();
              }
            } catch (final RocksDBException e) {
              if (option != WalProcessingOption.CORRUPTED_RECORD) {
                // exception is expected when CORRUPTED_RECORD!
                throw e;
              }
            }
          }
        }
      }
    }
  }


  private static class TestableWalFilter extends AbstractWalFilter {
    private final WalProcessingOption walProcessingOption;
    private final int applyOptionForRecordIndex;
    Map<Integer, Long> cfLognumber;
    Map<String, Integer> cfNameId;
    final List<Long> logNumbers = new ArrayList<>();
    final List<String> logFileNames = new ArrayList<>();
    private int currentRecordIndex = 0;

    public TestableWalFilter(final WalProcessingOption walProcessingOption,
        final int applyOptionForRecordIndex) {
      super();
      this.walProcessingOption = walProcessingOption;
      this.applyOptionForRecordIndex = applyOptionForRecordIndex;
    }

    @Override
    public void columnFamilyLogNumberMap(final Map<Integer, Long> cfLognumber,
        final Map<String, Integer> cfNameId) {
      this.cfLognumber = cfLognumber;
      this.cfNameId = cfNameId;
    }

    @Override
    public LogRecordFoundResult logRecordFound(
        final long logNumber, final String logFileName, final WriteBatch batch,
        final WriteBatch newBatch) {

      logNumbers.add(logNumber);
      logFileNames.add(logFileName);

      final WalProcessingOption optionToReturn;
      if (currentRecordIndex == applyOptionForRecordIndex) {
        optionToReturn = walProcessingOption;
      }
      else {
        optionToReturn = WalProcessingOption.CONTINUE_PROCESSING;
      }

      currentRecordIndex++;

      return new LogRecordFoundResult(optionToReturn, false);
    }

    @Override
    public String name() {
      return "testable-wal-filter";
    }
  }
}
