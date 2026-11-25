// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BlobOptionsTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  final int minBlobSize = 65536;
  final int largeBlobSize = 65536 * 2;

  /**
   * Count the files in the temporary folder which end with a particular suffix
   * Used to query the state of a test database to check if it is as the test expects
   *
   * @param endsWith the suffix to match
   * @return the number of files with a matching suffix
   */
  @SuppressWarnings("CallToStringConcatCanBeReplacedByOperator")
  private int countDBFiles(final String endsWith) {
    return Objects.requireNonNull(dbFolder.getRoot().list((dir, name) -> name.endsWith(endsWith)))
        .length;
  }

  @SuppressWarnings("SameParameterValue")
  private byte[] small_key(final String suffix) {
    return ("small_key_" + suffix).getBytes(UTF_8);
  }

  @SuppressWarnings("SameParameterValue")
  private byte[] small_value(final String suffix) {
    return ("small_value_" + suffix).getBytes(UTF_8);
  }

  private byte[] large_key(final String suffix) {
    return ("large_key_" + suffix).getBytes(UTF_8);
  }

  private byte[] large_value(final String repeat) {
    final byte[] large_value = ("" + repeat + "_" + largeBlobSize + "b").getBytes(UTF_8);
    final byte[] large_buffer = new byte[largeBlobSize];
    for (int pos = 0; pos < largeBlobSize; pos += large_value.length) {
      final int numBytes = Math.min(large_value.length, large_buffer.length - pos);
      System.arraycopy(large_value, 0, large_buffer, pos, numBytes);
    }
    return large_buffer;
  }

  @Test
  public void blobOptions() {
    try (final Options options = new Options()) {
      assertThat(options.enableBlobFiles()).isEqualTo(false);
      assertThat(options.minBlobSize()).isEqualTo(0);
      assertThat(options.blobCompressionType()).isEqualTo(CompressionType.NO_COMPRESSION);
      assertThat(options.enableBlobGarbageCollection()).isEqualTo(false);
      assertThat(options.blobFileSize()).isEqualTo(268435456L);
      assertThat(options.blobGarbageCollectionAgeCutoff()).isEqualTo(0.25);
      assertThat(options.blobGarbageCollectionForceThreshold()).isEqualTo(1.0);
      assertThat(options.blobCompactionReadaheadSize()).isEqualTo(0);
      assertThat(options.prepopulateBlobCache())
          .isEqualTo(PrepopulateBlobCache.PREPOPULATE_BLOB_DISABLE);

      assertThat(options.setEnableBlobFiles(true)).isEqualTo(options);
      assertThat(options.setMinBlobSize(132768L)).isEqualTo(options);
      assertThat(options.setBlobCompressionType(CompressionType.BZLIB2_COMPRESSION))
          .isEqualTo(options);
      assertThat(options.setEnableBlobGarbageCollection(true)).isEqualTo(options);
      assertThat(options.setBlobFileSize(132768L)).isEqualTo(options);
      assertThat(options.setBlobGarbageCollectionAgeCutoff(0.89)).isEqualTo(options);
      assertThat(options.setBlobGarbageCollectionForceThreshold(0.80)).isEqualTo(options);
      assertThat(options.setBlobCompactionReadaheadSize(262144L)).isEqualTo(options);
      assertThat(options.setBlobFileStartingLevel(0)).isEqualTo(options);
      assertThat(options.setPrepopulateBlobCache(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY))
          .isEqualTo(options);

      assertThat(options.enableBlobFiles()).isEqualTo(true);
      assertThat(options.minBlobSize()).isEqualTo(132768L);
      assertThat(options.blobCompressionType()).isEqualTo(CompressionType.BZLIB2_COMPRESSION);
      assertThat(options.enableBlobGarbageCollection()).isEqualTo(true);
      assertThat(options.blobFileSize()).isEqualTo(132768L);
      assertThat(options.blobGarbageCollectionAgeCutoff()).isEqualTo(0.89);
      assertThat(options.blobGarbageCollectionForceThreshold()).isEqualTo(0.80);
      assertThat(options.blobCompactionReadaheadSize()).isEqualTo(262144L);
      assertThat(options.blobFileStartingLevel()).isEqualTo(0);
      assertThat(options.prepopulateBlobCache())
          .isEqualTo(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY);
    }
  }

  @Test
  public void blobColumnFamilyOptions() {
    try (final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()) {
      assertThat(columnFamilyOptions.enableBlobFiles()).isEqualTo(false);
      assertThat(columnFamilyOptions.minBlobSize()).isEqualTo(0);
      assertThat(columnFamilyOptions.blobCompressionType())
          .isEqualTo(CompressionType.NO_COMPRESSION);
      assertThat(columnFamilyOptions.enableBlobGarbageCollection()).isEqualTo(false);
      assertThat(columnFamilyOptions.blobFileSize()).isEqualTo(268435456L);
      assertThat(columnFamilyOptions.blobGarbageCollectionAgeCutoff()).isEqualTo(0.25);
      assertThat(columnFamilyOptions.blobGarbageCollectionForceThreshold()).isEqualTo(1.0);
      assertThat(columnFamilyOptions.blobCompactionReadaheadSize()).isEqualTo(0);

      assertThat(columnFamilyOptions.setEnableBlobFiles(true)).isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setMinBlobSize(132768L)).isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setBlobCompressionType(CompressionType.BZLIB2_COMPRESSION))
          .isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setEnableBlobGarbageCollection(true))
          .isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setBlobFileSize(132768L)).isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setBlobGarbageCollectionAgeCutoff(0.89))
          .isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setBlobGarbageCollectionForceThreshold(0.80))
          .isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setBlobCompactionReadaheadSize(262144L))
          .isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setBlobFileStartingLevel(0)).isEqualTo(columnFamilyOptions);
      assertThat(columnFamilyOptions.setPrepopulateBlobCache(
                     PrepopulateBlobCache.PREPOPULATE_BLOB_DISABLE))
          .isEqualTo(columnFamilyOptions);

      assertThat(columnFamilyOptions.enableBlobFiles()).isEqualTo(true);
      assertThat(columnFamilyOptions.minBlobSize()).isEqualTo(132768L);
      assertThat(columnFamilyOptions.blobCompressionType())
          .isEqualTo(CompressionType.BZLIB2_COMPRESSION);
      assertThat(columnFamilyOptions.enableBlobGarbageCollection()).isEqualTo(true);
      assertThat(columnFamilyOptions.blobFileSize()).isEqualTo(132768L);
      assertThat(columnFamilyOptions.blobGarbageCollectionAgeCutoff()).isEqualTo(0.89);
      assertThat(columnFamilyOptions.blobGarbageCollectionForceThreshold()).isEqualTo(0.80);
      assertThat(columnFamilyOptions.blobCompactionReadaheadSize()).isEqualTo(262144L);
      assertThat(columnFamilyOptions.blobFileStartingLevel()).isEqualTo(0);
      assertThat(columnFamilyOptions.prepopulateBlobCache())
          .isEqualTo(PrepopulateBlobCache.PREPOPULATE_BLOB_DISABLE);
    }
  }

  @Test
  public void blobMutableColumnFamilyOptionsBuilder() {
    final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder =
        MutableColumnFamilyOptions.builder();
    builder.setEnableBlobFiles(true)
        .setMinBlobSize(1024)
        .setBlobFileSize(132768)
        .setBlobCompressionType(CompressionType.BZLIB2_COMPRESSION)
        .setEnableBlobGarbageCollection(true)
        .setBlobGarbageCollectionAgeCutoff(0.89)
        .setBlobGarbageCollectionForceThreshold(0.80)
        .setBlobCompactionReadaheadSize(262144)
        .setBlobFileStartingLevel(1)
        .setPrepopulateBlobCache(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY);

    assertThat(builder.enableBlobFiles()).isEqualTo(true);
    assertThat(builder.minBlobSize()).isEqualTo(1024);
    assertThat(builder.blobFileSize()).isEqualTo(132768);
    assertThat(builder.blobCompressionType()).isEqualTo(CompressionType.BZLIB2_COMPRESSION);
    assertThat(builder.enableBlobGarbageCollection()).isEqualTo(true);
    assertThat(builder.blobGarbageCollectionAgeCutoff()).isEqualTo(0.89);
    assertThat(builder.blobGarbageCollectionForceThreshold()).isEqualTo(0.80);
    assertThat(builder.blobCompactionReadaheadSize()).isEqualTo(262144);
    assertThat(builder.blobFileStartingLevel()).isEqualTo(1);
    assertThat(builder.prepopulateBlobCache())
        .isEqualTo(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY);

    builder.setEnableBlobFiles(false)
        .setMinBlobSize(4096)
        .setBlobFileSize(2048)
        .setBlobCompressionType(CompressionType.LZ4_COMPRESSION)
        .setEnableBlobGarbageCollection(false)
        .setBlobGarbageCollectionAgeCutoff(0.91)
        .setBlobGarbageCollectionForceThreshold(0.96)
        .setBlobCompactionReadaheadSize(1024)
        .setBlobFileStartingLevel(0)
        .setPrepopulateBlobCache(PrepopulateBlobCache.PREPOPULATE_BLOB_DISABLE);

    assertThat(builder.enableBlobFiles()).isEqualTo(false);
    assertThat(builder.minBlobSize()).isEqualTo(4096);
    assertThat(builder.blobFileSize()).isEqualTo(2048);
    assertThat(builder.blobCompressionType()).isEqualTo(CompressionType.LZ4_COMPRESSION);
    assertThat(builder.enableBlobGarbageCollection()).isEqualTo(false);
    assertThat(builder.blobGarbageCollectionAgeCutoff()).isEqualTo(0.91);
    assertThat(builder.blobGarbageCollectionForceThreshold()).isEqualTo(0.96);
    assertThat(builder.blobCompactionReadaheadSize()).isEqualTo(1024);
    assertThat(builder.blobFileStartingLevel()).isEqualTo(0);
    assertThat(builder.prepopulateBlobCache())
        .isEqualTo(PrepopulateBlobCache.PREPOPULATE_BLOB_DISABLE);

    final MutableColumnFamilyOptions options = builder.build();
    assertThat(options.getKeys())
        .isEqualTo(new String[] {"enable_blob_files", "min_blob_size", "blob_file_size",
            "blob_compression_type", "enable_blob_garbage_collection",
            "blob_garbage_collection_age_cutoff", "blob_garbage_collection_force_threshold",
            "blob_compaction_readahead_size", "blob_file_starting_level",
            "prepopulate_blob_cache"});
    assertThat(options.getValues())
        .isEqualTo(new String[] {"false", "4096", "2048", "LZ4_COMPRESSION", "false", "0.91",
            "0.96", "1024", "0", "PREPOPULATE_BLOB_DISABLE"});
  }

  /**
   * Configure the default column family with BLOBs.
   * Confirm that BLOBs are generated when appropriately-sized writes are flushed.
   *
   * @throws RocksDBException if a db access throws an exception
   */
  @Test
  public void testBlobWriteAboveThreshold() throws RocksDBException {
    try (final Options options = new Options()
                                     .setCreateIfMissing(true)
                                     .setMinBlobSize(minBlobSize)
                                     .setEnableBlobFiles(true);

         final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      db.put(small_key("default"), small_value("default"));
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }

      // check there are no blobs in the database
      assertThat(countDBFiles(".sst")).isEqualTo(1);
      assertThat(countDBFiles(".blob")).isEqualTo(0);

      db.put(large_key("default"), large_value("default"));
      try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        db.flush(flushOptions);
      }

      // wrote and flushed a value larger than the blobbing threshold
      // check there is a single blob in the database
      assertThat(countDBFiles(".sst")).isEqualTo(2);
      assertThat(countDBFiles(".blob")).isEqualTo(1);

      assertThat(db.get(small_key("default"))).isEqualTo(small_value("default"));
      assertThat(db.get(large_key("default"))).isEqualTo(large_value("default"));

      final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder fetchOptions =
          db.getOptions(null);
      assertThat(fetchOptions.minBlobSize()).isEqualTo(minBlobSize);
      assertThat(fetchOptions.enableBlobFiles()).isEqualTo(true);
      assertThat(fetchOptions.writeBufferSize()).isEqualTo(64 << 20);
    }
  }

  /**
   * Configure 2 column families respectively with and without BLOBs.
   * Confirm that BLOB files are generated (once the DB is flushed) only for the appropriate column
   * family.
   *
   * @throws RocksDBException if a db access throws an exception
   */
  @Test
  public void testBlobWriteAboveThresholdCF() throws RocksDBException {
    try (final ColumnFamilyOptions columnFamilyOptions0 = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor columnFamilyDescriptor0 =
            new ColumnFamilyDescriptor("default".getBytes(UTF_8), columnFamilyOptions0)) {
      List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          Collections.singletonList(columnFamilyDescriptor0);
      List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
          final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
              columnFamilyDescriptors, columnFamilyHandles)) {
        db.put(columnFamilyHandles.get(0), small_key("default"), small_value("default"));
        try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
          db.flush(flushOptions);
        }

        assertThat(countDBFiles(".blob")).isEqualTo(0);

        try (final ColumnFamilyOptions columnFamilyOptions1 =
                 new ColumnFamilyOptions().setMinBlobSize(minBlobSize).setEnableBlobFiles(true);

            final ColumnFamilyOptions columnFamilyOptions2 =
                new ColumnFamilyOptions().setMinBlobSize(minBlobSize).setEnableBlobFiles(false);
            final ColumnFamilyDescriptor columnFamilyDescriptor1 = new ColumnFamilyDescriptor(
                "column_family_1".getBytes(UTF_8), columnFamilyOptions1);
            final ColumnFamilyDescriptor columnFamilyDescriptor2 = new ColumnFamilyDescriptor(
                "column_family_2".getBytes(UTF_8), columnFamilyOptions2);) {
          // Create the first column family with blob options
          db.createColumnFamily(columnFamilyDescriptor1);

          // Create the second column family with not-blob options
          db.createColumnFamily(columnFamilyDescriptor2);
        }
      }

      // Now re-open after auto-close - at this point the CF options we use are recognized.
      try (final ColumnFamilyOptions columnFamilyOptions1 =
               new ColumnFamilyOptions().setMinBlobSize(minBlobSize).setEnableBlobFiles(true);

          final ColumnFamilyOptions columnFamilyOptions2 =
              new ColumnFamilyOptions().setMinBlobSize(minBlobSize).setEnableBlobFiles(false);
          final ColumnFamilyDescriptor columnFamilyDescriptor1 =
              new ColumnFamilyDescriptor("column_family_1".getBytes(UTF_8), columnFamilyOptions1);
          final ColumnFamilyDescriptor columnFamilyDescriptor2 = new ColumnFamilyDescriptor(
              "column_family_2".getBytes(UTF_8), columnFamilyOptions2)) {
        assertThat(columnFamilyOptions1.enableBlobFiles()).isEqualTo(true);
        assertThat(columnFamilyOptions1.minBlobSize()).isEqualTo(minBlobSize);
        assertThat(columnFamilyOptions2.enableBlobFiles()).isEqualTo(false);
        assertThat(columnFamilyOptions1.minBlobSize()).isEqualTo(minBlobSize);

        columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(columnFamilyDescriptor0);
        columnFamilyDescriptors.add(columnFamilyDescriptor1);
        columnFamilyDescriptors.add(columnFamilyDescriptor2);
        columnFamilyHandles = new ArrayList<>();

        assertThat(columnFamilyDescriptor1.getOptions().enableBlobFiles()).isEqualTo(true);
        assertThat(columnFamilyDescriptor2.getOptions().enableBlobFiles()).isEqualTo(false);

        try (final DBOptions dbOptions = new DBOptions();
            final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
                columnFamilyDescriptors, columnFamilyHandles)) {
          final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder1 =
              db.getOptions(columnFamilyHandles.get(1));
          assertThat(builder1.enableBlobFiles()).isEqualTo(true);
          assertThat(builder1.minBlobSize()).isEqualTo(minBlobSize);

          final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder2 =
              db.getOptions(columnFamilyHandles.get(2));
          assertThat(builder2.enableBlobFiles()).isEqualTo(false);
          assertThat(builder2.minBlobSize()).isEqualTo(minBlobSize);

          db.put(columnFamilyHandles.get(1), large_key("column_family_1_k2"),
              large_value("column_family_1_k2"));
          try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions, columnFamilyHandles.get(1));
          }
          assertThat(countDBFiles(".blob")).isEqualTo(1);

          db.put(columnFamilyHandles.get(2), large_key("column_family_2_k2"),
              large_value("column_family_2_k2"));
          try (final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions, columnFamilyHandles.get(2));
          }
          assertThat(countDBFiles(".blob")).isEqualTo(1);
        }
      }
    }
  }
}
