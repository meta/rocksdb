// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BlockBasedTableConfigTest {

  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void cacheIndexAndFilterBlocks() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
    assertThat(blockBasedTableConfig.cacheIndexAndFilterBlocks()).
        isTrue();
  }

  @Test
  public void cacheIndexAndFilterBlocksWithHighPriority() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(blockBasedTableConfig.cacheIndexAndFilterBlocksWithHighPriority()).
        isTrue();
    blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(false);
    assertThat(blockBasedTableConfig.cacheIndexAndFilterBlocksWithHighPriority()).isFalse();
  }

  @Test
  public void pinL0FilterAndIndexBlocksInCache() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
    assertThat(blockBasedTableConfig.pinL0FilterAndIndexBlocksInCache()).
        isTrue();
  }

  @Test
  public void pinTopLevelIndexAndFilter() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setPinTopLevelIndexAndFilter(false);
    assertThat(blockBasedTableConfig.pinTopLevelIndexAndFilter()).
        isFalse();
  }

  @Test
  public void indexType() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(IndexType.values().length).isEqualTo(4);
    blockBasedTableConfig.setIndexType(IndexType.kHashSearch);
    assertThat(blockBasedTableConfig.indexType()).isEqualTo(IndexType.kHashSearch);
    assertThat(IndexType.valueOf("kBinarySearch")).isNotNull();
    blockBasedTableConfig.setIndexType(IndexType.valueOf("kBinarySearch"));
    assertThat(blockBasedTableConfig.indexType()).isEqualTo(IndexType.kBinarySearch);
  }

  @Test
  public void dataBlockIndexType() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash);
    assertThat(blockBasedTableConfig.dataBlockIndexType())
        .isEqualTo(DataBlockIndexType.kDataBlockBinaryAndHash);
    blockBasedTableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch);
    assertThat(blockBasedTableConfig.dataBlockIndexType())
        .isEqualTo(DataBlockIndexType.kDataBlockBinarySearch);
  }

  @Test
  public void checksumType() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(ChecksumType.values().length).isEqualTo(5);
    assertThat(ChecksumType.valueOf("kxxHash")).
        isEqualTo(ChecksumType.kxxHash);
    blockBasedTableConfig.setChecksumType(ChecksumType.kNoChecksum);
    assertThat(blockBasedTableConfig.checksumType()).isEqualTo(ChecksumType.kNoChecksum);
    blockBasedTableConfig.setChecksumType(ChecksumType.kxxHash);
    assertThat(blockBasedTableConfig.checksumType()).isEqualTo(ChecksumType.kxxHash);
    blockBasedTableConfig.setChecksumType(ChecksumType.kxxHash64);
    assertThat(blockBasedTableConfig.checksumType()).isEqualTo(ChecksumType.kxxHash64);
    blockBasedTableConfig.setChecksumType(ChecksumType.kXXH3);
    assertThat(blockBasedTableConfig.checksumType()).isEqualTo(ChecksumType.kXXH3);
  }

  @Test
  public void jniPortal() throws Exception {
    // Verifies that the JNI layer is correctly translating options.
    // Since introspecting the options requires creating a database, the checks
    // cover multiple options at the same time.

    final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();

    tableConfig.setIndexType(IndexType.kBinarySearch);
    tableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch);
    tableConfig.setChecksumType(ChecksumType.kNoChecksum);
    tableConfig.setIndexSearchType(IndexSearchType.kBinary);
    tableConfig.setUniformCvThreshold(0.25);
    try (final Options options = new Options().setTableFormatConfig(tableConfig)) {
      final String opts = getOptionAsString(options);
      assertThat(opts).contains("index_type=kBinarySearch");
      assertThat(opts).contains("data_block_index_type=kDataBlockBinarySearch");
      assertThat(opts).contains("checksum=kNoChecksum");
      assertThat(opts).contains("index_block_search_type=kBinary");
      assertThat(opts).contains("uniform_cv_threshold=0.25");
    }

    tableConfig.setIndexType(IndexType.kHashSearch);
    tableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash);
    tableConfig.setChecksumType(ChecksumType.kCRC32c);
    try (final Options options = new Options().setTableFormatConfig(tableConfig)) {
      options.useCappedPrefixExtractor(1); // Needed to use kHashSearch
      final String opts = getOptionAsString(options);
      assertThat(opts).contains("index_type=kHashSearch");
      assertThat(opts).contains("data_block_index_type=kDataBlockBinaryAndHash");
      assertThat(opts).contains("checksum=kCRC32c");
    }

    tableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
    tableConfig.setChecksumType(ChecksumType.kxxHash);
    try (final Options options = new Options().setTableFormatConfig(tableConfig)) {
      final String opts = getOptionAsString(options);
      assertThat(opts).contains("index_type=kTwoLevelIndexSearch");
      assertThat(opts).contains("checksum=kxxHash");
    }

    tableConfig.setIndexType(IndexType.kBinarySearchWithFirstKey);
    tableConfig.setChecksumType(ChecksumType.kxxHash64);
    try (final Options options = new Options().setTableFormatConfig(tableConfig)) {
      final String opts = getOptionAsString(options);
      assertThat(opts).contains("index_type=kBinarySearchWithFirstKey");
      assertThat(opts).contains("checksum=kxxHash64");
    }

    tableConfig.setChecksumType(ChecksumType.kXXH3);
    try (final Options options = new Options().setTableFormatConfig(tableConfig)) {
      final String opts = getOptionAsString(options);
      assertThat(opts).contains("checksum=kXXH3");
    }
  }

  private String getOptionAsString(final Options options) throws Exception {
    options.setCreateIfMissing(true);
    final String dbPath = dbFolder.getRoot().getAbsolutePath();
    final String result;
    try (final RocksDB ignored = RocksDB.open(options, dbPath);
         final Stream<Path> pathStream = Files.walk(Paths.get(dbPath))) {
      final Path optionsPath =
          pathStream.filter(p -> p.getFileName().toString().startsWith("OPTIONS"))
              .findAny()
              .orElseThrow(() -> new AssertionError("Missing options file"));
      final byte[] optionsData = Files.readAllBytes(optionsPath);
      result = new String(optionsData, StandardCharsets.UTF_8);
    }
    RocksDB.destroyDB(dbPath, options);
    return result;
  }

  @Test
  public void noBlockCache() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setNoBlockCache(true);
    assertThat(blockBasedTableConfig.noBlockCache()).isTrue();
  }

  @Test
  public void blockCache() {
    try (
        final Cache cache = new LRUCache(17 * 1024 * 1024);
        final Options options = new Options().setTableFormatConfig(
            new BlockBasedTableConfig().setBlockCache(cache))) {
      assertThat(options.tableFactoryName()).isEqualTo("BlockBasedTable");
    }
  }

  @Test
  public void blockCacheIntegration() throws RocksDBException {
    try (final Cache cache = new LRUCache(8 * 1024 * 1024);
         final Statistics statistics = new Statistics()) {
      for (int shard = 0; shard < 8; shard++) {
        try (final Options options =
                 new Options()
                     .setCreateIfMissing(true)
                     .setStatistics(statistics)
                     .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache));
             final RocksDB db =
                 RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "/" + shard)) {
          final byte[] key = "some-key".getBytes(StandardCharsets.UTF_8);
          final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

          db.put(key, value);
          db.flush(new FlushOptions());
          db.get(key);

          assertThat(statistics.getTickerCount(TickerType.BLOCK_CACHE_ADD)).isEqualTo(shard + 1);
        }
      }
    }
  }

  @Test
  public void persistentCache() throws RocksDBException {
    try (final DBOptions dbOptions =
             new DBOptions().setInfoLogLevel(InfoLogLevel.INFO_LEVEL).setCreateIfMissing(true);
        final Logger logger = new Logger(dbOptions.infoLogLevel()) {
          @Override
          protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            System.out.println(infoLogLevel.name() + ": " + logMsg);
          }
        }) {
      try (final PersistentCache persistentCache =
               new PersistentCache(Env.getDefault(), dbFolder.getRoot().getPath(), 1024 * 1024 * 100, logger, false);
           final Options options = new Options().setTableFormatConfig(
               new BlockBasedTableConfig().setPersistentCache(persistentCache))) {
        assertThat(options.tableFactoryName()).isEqualTo("BlockBasedTable");
      }
    }
  }

  @Test
  public void blockSize() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockSize(10);
    assertThat(blockBasedTableConfig.blockSize()).isEqualTo(10);
  }

  @Test
  public void blockSizeDeviation() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockSizeDeviation(12);
    assertThat(blockBasedTableConfig.blockSizeDeviation()).
        isEqualTo(12);
  }

  @Test
  public void blockRestartInterval() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockRestartInterval(15);
    assertThat(blockBasedTableConfig.blockRestartInterval()).
        isEqualTo(15);
  }

  @Test
  public void indexBlockRestartInterval() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setIndexBlockRestartInterval(15);
    assertThat(blockBasedTableConfig.indexBlockRestartInterval()).
        isEqualTo(15);
  }

  @Test
  public void metadataBlockSize() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setMetadataBlockSize(1024);
    assertThat(blockBasedTableConfig.metadataBlockSize()).
        isEqualTo(1024);
  }

  @Test
  public void partitionFilters() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setPartitionFilters(true);
    assertThat(blockBasedTableConfig.partitionFilters()).
        isTrue();
  }

  @Test
  public void optimizeFiltersForMemory() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setOptimizeFiltersForMemory(true);
    assertThat(blockBasedTableConfig.optimizeFiltersForMemory()).isTrue();
  }

  @Test
  public void useDeltaEncoding() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setUseDeltaEncoding(false);
    assertThat(blockBasedTableConfig.useDeltaEncoding()).
        isFalse();
  }

  @Test
  public void blockBasedTableWithFilterPolicy() {
    try(final Options options = new Options()
        .setTableFormatConfig(new BlockBasedTableConfig()
            .setFilterPolicy(new BloomFilter(10)))) {
      assertThat(options.tableFactoryName()).
          isEqualTo("BlockBasedTable");
    }
  }

  @Test
  public void blockBasedTableWithoutFilterPolicy() {
    try(final Options options = new Options().setTableFormatConfig(
        new BlockBasedTableConfig().setFilterPolicy(null))) {
      assertThat(options.tableFactoryName()).
          isEqualTo("BlockBasedTable");
    }
  }

  @Test
  public void wholeKeyFiltering() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setWholeKeyFiltering(false);
    assertThat(blockBasedTableConfig.wholeKeyFiltering()).
        isFalse();
  }

  @Test
  public void verifyCompression() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(blockBasedTableConfig.verifyCompression()).isFalse();
    blockBasedTableConfig.setVerifyCompression(true);
    assertThat(blockBasedTableConfig.verifyCompression()).
        isTrue();
  }

  @Test
  public void readAmpBytesPerBit() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setReadAmpBytesPerBit(2);
    assertThat(blockBasedTableConfig.readAmpBytesPerBit()).
        isEqualTo(2);
  }

  @Test
  public void formatVersion() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    for (int version = 0; version <= 5; version++) {
      blockBasedTableConfig.setFormatVersion(version);
      assertThat(blockBasedTableConfig.formatVersion()).isEqualTo(version);
    }
  }

  @Test(expected = AssertionError.class)
  public void formatVersionFailNegative() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setFormatVersion(-1);
  }

  @Test(expected = RocksDBException.class)
  public void invalidFormatVersion() throws RocksDBException {
    final BlockBasedTableConfig blockBasedTableConfig =
        new BlockBasedTableConfig().setFormatVersion(99999);

    try (final Options options = new Options().setTableFormatConfig(blockBasedTableConfig);
         final RocksDB ignored = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      fail("Opening the database with an invalid format_version should have raised an exception");
    }
  }

  @Test
  public void separateKeyValueInDataBlock() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(blockBasedTableConfig.separateKeyValueInDataBlock()).isFalse();
    blockBasedTableConfig.setSeparateKeyValueInDataBlock(true);
    assertThat(blockBasedTableConfig.separateKeyValueInDataBlock()).isTrue();
  }

  @Test
  public void uniformCvThreshold() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(blockBasedTableConfig.uniformCvThreshold()).isEqualTo(0.2);
    blockBasedTableConfig.setUniformCvThreshold(0.5);
    assertThat(blockBasedTableConfig.uniformCvThreshold()).isEqualTo(0.5);
    blockBasedTableConfig.setUniformCvThreshold(-1.0);
    assertThat(blockBasedTableConfig.uniformCvThreshold()).isEqualTo(-1.0);
  }

  @Test
  public void enableIndexCompression() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setEnableIndexCompression(false);
    assertThat(blockBasedTableConfig.enableIndexCompression()).
        isFalse();
  }

  @Test
  public void blockAlign() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockAlign(true);
    assertThat(blockBasedTableConfig.blockAlign()).
        isTrue();
  }

  @Test
  public void superBlockAlignmentSize() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setSuperBlockAlignmentSize(1024 * 1024);
    assertThat(blockBasedTableConfig.superBlockAlignmentSize()).isEqualTo(1024 * 1024);
  }

  @Test
  public void superBlockAlignmentSpaceOverheadRatio() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setSuperBlockAlignmentSpaceOverheadRatio(4096);
    assertThat(blockBasedTableConfig.superBlockAlignmentSpaceOverheadRatio()).isEqualTo(4096);
  }

  @Test
  public void indexShortening() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setIndexShortening(IndexShorteningMode.kShortenSeparatorsAndSuccessor);
    assertThat(blockBasedTableConfig.indexShortening())
        .isEqualTo(IndexShorteningMode.kShortenSeparatorsAndSuccessor);
  }

  @Test
  public void indexSearchType() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(IndexSearchType.values().length).isEqualTo(3);
    blockBasedTableConfig.setIndexSearchType(IndexSearchType.kInterpolation);
    assertThat(blockBasedTableConfig.indexSearchType()).isEqualTo(IndexSearchType.kInterpolation);
    blockBasedTableConfig.setIndexSearchType(IndexSearchType.kBinary);
    assertThat(blockBasedTableConfig.indexSearchType()).isEqualTo(IndexSearchType.kBinary);
    blockBasedTableConfig.setIndexSearchType(IndexSearchType.kAuto);
    assertThat(blockBasedTableConfig.indexSearchType()).isEqualTo(IndexSearchType.kAuto);
  }

  @Test
  public void optimizeKeyCommonPrefix() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(OptimizeKeyCommonPrefix.values().length).isEqualTo(3);
    // Default is kIfFastSeek.
    assertThat(blockBasedTableConfig.optimizeKeyCommonPrefix())
        .isEqualTo(OptimizeKeyCommonPrefix.kIfFastSeek);
    blockBasedTableConfig.setOptimizeKeyCommonPrefix(OptimizeKeyCommonPrefix.kDisabled);
    assertThat(blockBasedTableConfig.optimizeKeyCommonPrefix())
        .isEqualTo(OptimizeKeyCommonPrefix.kDisabled);
    blockBasedTableConfig.setOptimizeKeyCommonPrefix(OptimizeKeyCommonPrefix.kEnabled);
    assertThat(blockBasedTableConfig.optimizeKeyCommonPrefix())
        .isEqualTo(OptimizeKeyCommonPrefix.kEnabled);
  }

  @Deprecated
  @Test
  public void hashIndexAllowCollision() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setHashIndexAllowCollision(false);
    assertThat(blockBasedTableConfig.hashIndexAllowCollision()).
        isTrue();  // NOTE: setHashIndexAllowCollision should do nothing!
  }

  @Deprecated
  @Test
  public void blockCacheSize() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockCacheSize(8 * 1024);
    assertThat(blockBasedTableConfig.blockCacheSize()).
        isEqualTo(8 * 1024);
  }

  @Deprecated
  @Test
  public void blockCacheNumShardBits() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setCacheNumShardBits(5);
    assertThat(blockBasedTableConfig.cacheNumShardBits()).
        isEqualTo(5);
  }

}
