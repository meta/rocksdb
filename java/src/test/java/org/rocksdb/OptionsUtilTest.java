// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class OptionsUtilTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE = new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void loadLatestOptions() throws RocksDBException {
    verifyOptions(new LoaderUnderTest() {
      @Override
      List<ColumnFamilyDescriptor> loadOptions(final String dbPath, final DBOptions dbOptions)
          throws RocksDBException {
        try (final ConfigOptions configOptions = new ConfigOptions()
                                                     .setIgnoreUnknownOptions(false)
                                                     .setInputStringsEscaped(true)
                                                     .setEnv(Env.getDefault())) {
          final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
          OptionsUtil.loadLatestOptions(configOptions, dbPath, dbOptions, cfDescs);
          return cfDescs;
        }
      }
    });
  }

  @Test
  public void loadOptionsFromFile() throws RocksDBException {
    verifyOptions(new LoaderUnderTest() {
      @Override
      List<ColumnFamilyDescriptor> loadOptions(final String dbPath, final DBOptions dbOptions)
          throws RocksDBException {
        try (final ConfigOptions configOptions = new ConfigOptions()
                                                     .setIgnoreUnknownOptions(false)
                                                     .setInputStringsEscaped(true)
                                                     .setEnv(Env.getDefault())) {
          final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
          final String path =
              dbPath + "/" + OptionsUtil.getLatestOptionsFileName(dbPath, Env.getDefault());
          OptionsUtil.loadOptionsFromFile(configOptions, path, dbOptions, cfDescs);
          return cfDescs;
        }
      }
    });
  }

  @Test
  public void loadLatestTableFormatOptions() throws RocksDBException {
    verifyTableFormatOptions(new LoaderUnderTest() {
      @Override
      List<ColumnFamilyDescriptor> loadOptions(final String dbPath, final DBOptions dbOptions)
          throws RocksDBException {
        try (final ConfigOptions configOptions = new ConfigOptions()
                                                     .setIgnoreUnknownOptions(false)
                                                     .setInputStringsEscaped(true)
                                                     .setEnv(Env.getDefault())) {
          final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
          OptionsUtil.loadLatestOptions(configOptions, dbPath, dbOptions, cfDescs);
          return cfDescs;
        }
      }
    });
  }

  @Test
  public void loadLatestTableFormatOptions2() throws RocksDBException {
    verifyTableFormatOptions(new LoaderUnderTest() {
      @Override
      List<ColumnFamilyDescriptor> loadOptions(final String dbPath, final DBOptions dbOptions)
          throws RocksDBException {
        try (final ConfigOptions configOptions = new ConfigOptions()
                                                     .setIgnoreUnknownOptions(false)
                                                     .setInputStringsEscaped(true)
                                                     .setEnv(Env.getDefault())) {
          final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
          OptionsUtil.loadLatestOptions(configOptions, dbPath, dbOptions, cfDescs);
          return cfDescs;
        }
      }
    });
  }

  @Test
  public void loadLatestTableFormatOptions3() throws RocksDBException {
    verifyTableFormatOptions(new LoaderUnderTest() {
      @Override
      List<ColumnFamilyDescriptor> loadOptions(final String dbPath, final DBOptions dbOptions)
          throws RocksDBException {
        final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
        OptionsUtil.loadLatestOptions(new ConfigOptions(), dbPath, dbOptions, cfDescs);
        return cfDescs;
      }
    });
  }

  @Test
  public void loadTableFormatOptionsFromFile() throws RocksDBException {
    verifyTableFormatOptions(new LoaderUnderTest() {
      @Override
      List<ColumnFamilyDescriptor> loadOptions(final String dbPath, final DBOptions dbOptions)
          throws RocksDBException {
        try (final ConfigOptions configOptions = new ConfigOptions()
                                                     .setIgnoreUnknownOptions(false)
                                                     .setInputStringsEscaped(true)
                                                     .setEnv(Env.getDefault())) {
          final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
          final String path =
              dbPath + "/" + OptionsUtil.getLatestOptionsFileName(dbPath, Env.getDefault());
          OptionsUtil.loadOptionsFromFile(configOptions, path, dbOptions, cfDescs);
          return cfDescs;
        }
      }
    });
  }

  @Test
  public void loadTableFormatOptionsFromFile2() throws RocksDBException {
    verifyTableFormatOptions(new LoaderUnderTest() {
      @Override
      List<ColumnFamilyDescriptor> loadOptions(final String dbPath, final DBOptions dbOptions)
          throws RocksDBException {
        try (final ConfigOptions configOptions = new ConfigOptions()
                                                     .setIgnoreUnknownOptions(false)
                                                     .setInputStringsEscaped(true)
                                                     .setEnv(Env.getDefault())) {
          final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
          final String path =
              dbPath + "/" + OptionsUtil.getLatestOptionsFileName(dbPath, Env.getDefault());
          OptionsUtil.loadOptionsFromFile(configOptions, path, dbOptions, cfDescs);
          return cfDescs;
        }
      }
    });
  }

  @Test
  public void loadTableFormatOptionsFromFile3() throws RocksDBException {
    verifyTableFormatOptions(new LoaderUnderTest() {
      @Override
      List<ColumnFamilyDescriptor> loadOptions(final String dbPath, final DBOptions dbOptions)
          throws RocksDBException {
        final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
        final String path =
            dbPath + "/" + OptionsUtil.getLatestOptionsFileName(dbPath, Env.getDefault());
        OptionsUtil.loadOptionsFromFile(new ConfigOptions(), path, dbOptions, cfDescs);
        return cfDescs;
      }
    });
  }

  @Test
  public void getLatestOptionsFileName() throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbPath)) {
      assertThat(db).isNotNull();
    }

    final String fName = OptionsUtil.getLatestOptionsFileName(dbPath, Env.getDefault());
    assertThat(fName).isNotNull();
    assert (fName.startsWith("OPTIONS-"));
    // System.out.println("latest options fileName: " + fName);
  }

  static abstract class LoaderUnderTest {
    abstract List<ColumnFamilyDescriptor> loadOptions(final String path, final DBOptions dbOptions)
        throws RocksDBException;
  }

  private void verifyOptions(final LoaderUnderTest loaderUnderTest) throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();
    final Options options = new Options()
                                .setCreateIfMissing(true)
                                .setParanoidChecks(false)
                                .setMaxOpenFiles(478)
                                .setDelayedWriteRate(1234567L);
    try (final ColumnFamilyOptions baseDefaultCFOpts = new ColumnFamilyOptions()) {
      final byte[] secondCFName = "new_cf".getBytes();
      final ColumnFamilyOptions baseSecondCFOpts =
          new ColumnFamilyOptions()
              .setWriteBufferSize(70 * 1024)
              .setMaxWriteBufferNumber(7)
              .setMaxBytesForLevelBase(53 * 1024 * 1024)
              .setLevel0FileNumCompactionTrigger(3)
              .setLevel0SlowdownWritesTrigger(51)
              .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);

      // Create a database with a new column family
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        assertThat(db).isNotNull();

        // create column family
        try (final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
                 new ColumnFamilyDescriptor(secondCFName, baseSecondCFOpts))) {
          assert (columnFamilyHandle != null);
        }
      }

      // Read the options back and verify
      try (DBOptions dbOptions = new DBOptions()) {
        final List<ColumnFamilyDescriptor> cfDescs = loaderUnderTest.loadOptions(dbPath, dbOptions);

        assertThat(dbOptions.createIfMissing()).isEqualTo(options.createIfMissing());
        assertThat(dbOptions.paranoidChecks()).isEqualTo(options.paranoidChecks());
        assertThat(dbOptions.maxOpenFiles()).isEqualTo(options.maxOpenFiles());
        assertThat(dbOptions.delayedWriteRate()).isEqualTo(options.delayedWriteRate());

        assertThat(cfDescs.size()).isEqualTo(2);
        assertThat(cfDescs.get(0)).isNotNull();
        assertThat(cfDescs.get(1)).isNotNull();
        assertThat(cfDescs.get(0).getName()).isEqualTo(RocksDB.DEFAULT_COLUMN_FAMILY);
        assertThat(cfDescs.get(1).getName()).isEqualTo(secondCFName);

        final ColumnFamilyOptions defaultCFOpts = cfDescs.get(0).getOptions();
        assertThat(defaultCFOpts.writeBufferSize()).isEqualTo(baseDefaultCFOpts.writeBufferSize());
        assertThat(defaultCFOpts.maxWriteBufferNumber())
            .isEqualTo(baseDefaultCFOpts.maxWriteBufferNumber());
        assertThat(defaultCFOpts.maxBytesForLevelBase())
            .isEqualTo(baseDefaultCFOpts.maxBytesForLevelBase());
        assertThat(defaultCFOpts.level0FileNumCompactionTrigger())
            .isEqualTo(baseDefaultCFOpts.level0FileNumCompactionTrigger());
        assertThat(defaultCFOpts.level0SlowdownWritesTrigger())
            .isEqualTo(baseDefaultCFOpts.level0SlowdownWritesTrigger());
        assertThat(defaultCFOpts.bottommostCompressionType())
            .isEqualTo(baseDefaultCFOpts.bottommostCompressionType());

        final ColumnFamilyOptions secondCFOpts = cfDescs.get(1).getOptions();
        assertThat(secondCFOpts.writeBufferSize()).isEqualTo(baseSecondCFOpts.writeBufferSize());
        assertThat(secondCFOpts.maxWriteBufferNumber())
            .isEqualTo(baseSecondCFOpts.maxWriteBufferNumber());
        assertThat(secondCFOpts.maxBytesForLevelBase())
            .isEqualTo(baseSecondCFOpts.maxBytesForLevelBase());
        assertThat(secondCFOpts.level0FileNumCompactionTrigger())
            .isEqualTo(baseSecondCFOpts.level0FileNumCompactionTrigger());
        assertThat(secondCFOpts.level0SlowdownWritesTrigger())
            .isEqualTo(baseSecondCFOpts.level0SlowdownWritesTrigger());
        assertThat(secondCFOpts.bottommostCompressionType())
            .isEqualTo(baseSecondCFOpts.bottommostCompressionType());
      }
    }
  }

  @SuppressWarnings("deprecation")
  private void verifyTableFormatOptions(final LoaderUnderTest loaderUnderTest)
      throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();
    final Options options = new Options()
                                .setCreateIfMissing(true)
                                .setParanoidChecks(false)
                                .setMaxOpenFiles(478)
                                .setDelayedWriteRate(1234567L);
    try (final ColumnFamilyOptions defaultCFOptions = new ColumnFamilyOptions()) {
      defaultCFOptions.setTableFormatConfig(new BlockBasedTableConfig());
      final byte[] altCFName = "alt_cf".getBytes();
      try (final ColumnFamilyOptions altCFOptions = new ColumnFamilyOptions()
               .setWriteBufferSize(70 * 1024)
               .setMaxWriteBufferNumber(7)
               .setMaxBytesForLevelBase(53 * 1024 * 1024)
               .setLevel0FileNumCompactionTrigger(3)
               .setLevel0SlowdownWritesTrigger(51)
               .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)) {
        final BlockBasedTableConfig altCFTableConfig = new BlockBasedTableConfig();
        altCFTableConfig.setCacheIndexAndFilterBlocks(true);
        altCFTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(false);
        altCFTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
        altCFTableConfig.setPinTopLevelIndexAndFilter(false);
        altCFTableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
        altCFTableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash);
        altCFTableConfig.setDataBlockHashTableUtilRatio(0.65);
        altCFTableConfig.setChecksumType(ChecksumType.kxxHash64);
        altCFTableConfig.setNoBlockCache(true);
        altCFTableConfig.setBlockSize(35 * 1024);
        altCFTableConfig.setBlockSizeDeviation(20);
        altCFTableConfig.setBlockRestartInterval(12);
        altCFTableConfig.setIndexBlockRestartInterval(6);
        altCFTableConfig.setMetadataBlockSize(12 * 1024);
        altCFTableConfig.setPartitionFilters(true);
        altCFTableConfig.setOptimizeFiltersForMemory(false);
        altCFTableConfig.setUseDeltaEncoding(false);
        altCFTableConfig.setFilterPolicy(new BloomFilter(7.5));
        altCFTableConfig.setWholeKeyFiltering(false);
        altCFTableConfig.setVerifyCompression(true);
        altCFTableConfig.setReadAmpBytesPerBit(2);
        altCFTableConfig.setFormatVersion(8);
        altCFTableConfig.setSeparateKeyValueInDataBlock(true);
        altCFTableConfig.setEnableIndexCompression(false);
        altCFTableConfig.setBlockAlign(true);
        altCFTableConfig.setSuperBlockAlignmentSize(1024 * 1024);
        altCFTableConfig.setSuperBlockAlignmentSpaceOverheadRatio(4 * 1024);
        altCFTableConfig.setIndexShortening(IndexShorteningMode.kShortenSeparatorsAndSuccessor);
        altCFTableConfig.setBlockCacheSize(3 * 1024 * 1024);
        // Note cache objects are not set here, as they are not read back when reading config.

        altCFOptions.setTableFormatConfig(altCFTableConfig);

        // Create a database with a new column family
        try (final RocksDB db = RocksDB.open(options, dbPath)) {
          assertThat(db).isNotNull();

          // create column family
          try (final ColumnFamilyHandle columnFamilyHandle =
                   db.createColumnFamily(new ColumnFamilyDescriptor(altCFName, altCFOptions))) {
            assert (columnFamilyHandle != null);
          }
        }

        // Read the options back and verify
        final DBOptions dbOptions = new DBOptions();
        final List<ColumnFamilyDescriptor> cfDescs = loaderUnderTest.loadOptions(dbPath, dbOptions);

        assertThat(dbOptions.createIfMissing()).isEqualTo(options.createIfMissing());
        assertThat(dbOptions.paranoidChecks()).isEqualTo(options.paranoidChecks());
        assertThat(dbOptions.maxOpenFiles()).isEqualTo(options.maxOpenFiles());
        assertThat(dbOptions.delayedWriteRate()).isEqualTo(options.delayedWriteRate());

        assertThat(cfDescs.size()).isEqualTo(2);
        assertThat(cfDescs.get(0)).isNotNull();
        assertThat(cfDescs.get(1)).isNotNull();
        assertThat(cfDescs.get(0).getName()).isEqualTo(RocksDB.DEFAULT_COLUMN_FAMILY);
        assertThat(cfDescs.get(1).getName()).isEqualTo(altCFName);

        verifyBlockBasedTableConfig(
            cfDescs.get(0).getOptions().tableFormatConfig(), new BlockBasedTableConfig());
        verifyBlockBasedTableConfig(
            cfDescs.get(1).getOptions().tableFormatConfig(), altCFTableConfig);
      }
    }
  }

  private void verifyBlockBasedTableConfig(
      final TableFormatConfig actualTableConfig, final BlockBasedTableConfig expected) {
    assertThat(actualTableConfig).isNotNull();
    assertThat(actualTableConfig).isInstanceOf(BlockBasedTableConfig.class);
    final BlockBasedTableConfig actual = (BlockBasedTableConfig) actualTableConfig;
    assertThat(actual.cacheIndexAndFilterBlocks()).isEqualTo(expected.cacheIndexAndFilterBlocks());
    assertThat(actual.cacheIndexAndFilterBlocksWithHighPriority())
        .isEqualTo(expected.cacheIndexAndFilterBlocksWithHighPriority());
    assertThat(actual.pinL0FilterAndIndexBlocksInCache())
        .isEqualTo(expected.pinL0FilterAndIndexBlocksInCache());
    assertThat(actual.indexType()).isEqualTo(expected.indexType());
    assertThat(actual.dataBlockIndexType()).isEqualTo(expected.dataBlockIndexType());
    assertThat(actual.dataBlockHashTableUtilRatio())
        .isEqualTo(expected.dataBlockHashTableUtilRatio());
    assertThat(actual.checksumType()).isEqualTo(expected.checksumType());
    assertThat(actual.noBlockCache()).isEqualTo(expected.noBlockCache());
    assertThat(actual.blockSize()).isEqualTo(expected.blockSize());
    assertThat(actual.blockSizeDeviation()).isEqualTo(expected.blockSizeDeviation());
    assertThat(actual.blockRestartInterval()).isEqualTo(expected.blockRestartInterval());
    assertThat(actual.indexBlockRestartInterval()).isEqualTo(expected.indexBlockRestartInterval());
    assertThat(actual.metadataBlockSize()).isEqualTo(expected.metadataBlockSize());
    assertThat(actual.partitionFilters()).isEqualTo(expected.partitionFilters());
    assertThat(actual.optimizeFiltersForMemory()).isEqualTo(expected.optimizeFiltersForMemory());
    assertThat(actual.useDeltaEncoding()).isEqualTo(expected.useDeltaEncoding());
    assertThat(actual.wholeKeyFiltering()).isEqualTo(expected.wholeKeyFiltering());
    assertThat(actual.verifyCompression()).isEqualTo(expected.verifyCompression());
    assertThat(actual.readAmpBytesPerBit()).isEqualTo(expected.readAmpBytesPerBit());
    assertThat(actual.formatVersion()).isEqualTo(expected.formatVersion());
    assertThat(actual.separateKeyValueInDataBlock())
        .isEqualTo(expected.separateKeyValueInDataBlock());
    assertThat(actual.enableIndexCompression()).isEqualTo(expected.enableIndexCompression());
    assertThat(actual.blockAlign()).isEqualTo(expected.blockAlign());
    assertThat(actual.superBlockAlignmentSize()).isEqualTo(expected.superBlockAlignmentSize());
    assertThat(actual.superBlockAlignmentSpaceOverheadRatio())
        .isEqualTo(expected.superBlockAlignmentSpaceOverheadRatio());
    assertThat(actual.indexShortening()).isEqualTo(expected.indexShortening());
    if (expected.filterPolicy() == null) {
      assertThat(actual.filterPolicy()).isNull();
    } else {
      // TODO (AP) this test is a no-op and the true test fails
      assertThat(expected.filterPolicy().equals(actual.filterPolicy()));
    }
  }
}
