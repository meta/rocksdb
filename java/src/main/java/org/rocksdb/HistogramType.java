// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum HistogramType {

  DB_GET((byte) 0x0),

  DB_WRITE((byte) 0x1),

  COMPACTION_TIME((byte) 0x2),

  COMPACTION_CPU_TIME((byte) 0x3),

  SUBCOMPACTION_SETUP_TIME((byte) 0x4),

  TABLE_SYNC_MICROS((byte) 0x5),

  COMPACTION_OUTFILE_SYNC_MICROS((byte) 0x6),

  WAL_FILE_SYNC_MICROS((byte) 0x7),

  MANIFEST_FILE_SYNC_MICROS((byte) 0x8),

  /**
   * TIME SPENT IN IO DURING TABLE OPEN.
   */
  TABLE_OPEN_IO_MICROS((byte) 0x9),

  DB_MULTIGET((byte) 0xA),

  READ_BLOCK_COMPACTION_MICROS((byte) 0xB),

  READ_BLOCK_GET_MICROS((byte) 0xC),

  WRITE_RAW_BLOCK_MICROS((byte) 0xD),

  NUM_FILES_IN_SINGLE_COMPACTION((byte) 0xE),

  DB_SEEK((byte) 0xF),

  WRITE_STALL((byte) 0x10),

  SST_READ_MICROS((byte) 0x11),

  FILE_READ_FLUSH_MICROS((byte) 0x12),

  FILE_READ_COMPACTION_MICROS((byte) 0x13),

  FILE_READ_DB_OPEN_MICROS((byte) 0x14),

  FILE_READ_GET_MICROS((byte) 0x15),

  FILE_READ_MULTIGET_MICROS((byte) 0x16),

  FILE_READ_DB_ITERATOR_MICROS((byte) 0x17),

  FILE_READ_VERIFY_DB_CHECKSUM_MICROS((byte) 0x18),

  FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS((byte) 0x19),

  SST_WRITE_MICROS((byte) 0x1A),

  FILE_WRITE_FLUSH_MICROS((byte) 0x1B),

  FILE_WRITE_COMPACTION_MICROS((byte) 0x1C),

  FILE_WRITE_DB_OPEN_MICROS((byte) 0x1D),

  /**
   * The number of subcompactions actually scheduled during a compaction.
   */
  NUM_SUBCOMPACTIONS_SCHEDULED((byte) 0x1E),

  /**
   * Value size distribution in each operation.
   */
  BYTES_PER_READ((byte) 0x1F),
  BYTES_PER_WRITE((byte) 0x20),
  BYTES_PER_MULTIGET((byte) 0x21),

  COMPRESSION_TIMES_NANOS((byte) 0x22),

  DECOMPRESSION_TIMES_NANOS((byte) 0x23),

  READ_NUM_MERGE_OPERANDS((byte) 0x24),

  /**
   * Size of keys written to BlobDB.
   */
  BLOB_DB_KEY_SIZE((byte) 0x25),

  /**
   * Size of values written to BlobDB.
   */
  BLOB_DB_VALUE_SIZE((byte) 0x26),

  /**
   * BlobDB Put/PutWithTTL/PutUntil/Write latency.
   */
  BLOB_DB_WRITE_MICROS((byte) 0x27),

  /**
   * BlobDB Get lagency.
   */
  BLOB_DB_GET_MICROS((byte) 0x28),

  /**
   * BlobDB MultiGet latency.
   */
  BLOB_DB_MULTIGET_MICROS((byte) 0x29),

  /**
   * BlobDB Seek/SeekToFirst/SeekToLast/SeekForPrev latency.
   */
  BLOB_DB_SEEK_MICROS((byte) 0x2A),

  /**
   * BlobDB Next latency.
   */
  BLOB_DB_NEXT_MICROS((byte) 0x2B),

  /**
   * BlobDB Prev latency.
   */
  BLOB_DB_PREV_MICROS((byte) 0x2C),

  /**
   * Blob file write latency.
   */
  BLOB_DB_BLOB_FILE_WRITE_MICROS((byte) 0x2D),

  /**
   * Blob file read latency.
   */
  BLOB_DB_BLOB_FILE_READ_MICROS((byte) 0x2E),

  /**
   * Blob file sync latency.
   */
  BLOB_DB_BLOB_FILE_SYNC_MICROS((byte) 0x2F),

  /**
   * BlobDB compression time.
   */
  BLOB_DB_COMPRESSION_MICROS((byte) 0x30),

  /**
   * BlobDB decompression time.
   */
  BLOB_DB_DECOMPRESSION_MICROS((byte) 0x31),

  /**
   * Time spent flushing memtable to disk.
   */
  FLUSH_TIME((byte) 0x32),

  /**
   * Number of MultiGet batch keys overlapping a file
   */
  SST_BATCH_SIZE((byte) 0x33),

  /**
   * Size of a single IO batch issued by MultiGet
   */
  MULTIGET_IO_BATCH_SIZE((byte) 0x34),

  /**
   * Num of Index and Filter blocks read from file system per level in MultiGet
   * request
   */
  NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL((byte) 0x35),

  /**
   * Num of SST files read from file system per level in MultiGet request.
   */
  NUM_SST_READ_PER_LEVEL((byte) 0x36),

  /**
   * Num of LSM levels read from file system per MultiGet request.
   */
  NUM_LEVEL_READ_PER_MULTIGET((byte) 0x37),

  /**
   * The number of retry in auto resume
   */
  ERROR_HANDLER_AUTORESUME_RETRY_COUNT((byte) 0x38),

  ASYNC_READ_BYTES((byte) 0x39),

  POLL_WAIT_MICROS((byte) 0x3A),

  /**
   * Number of prefetched bytes discarded by RocksDB.
   */
  PREFETCHED_BYTES_DISCARDED((byte) 0x3B),

  /**
   * Wait time for aborting async read in FilePrefetchBuffer destructor
   */
  ASYNC_PREFETCH_ABORT_MICROS((byte) 0x3C),

  /**
   * Number of bytes read for RocksDB's prefetching contents
   * (as opposed to file system's prefetch)
   * from the end of SST table during block based table open
   */
  TABLE_OPEN_PREFETCH_TAIL_READ_BYTES((byte) 0x3D),

  COMPACTION_PREFETCH_BYTES((byte) 0x3F),

  /**
   * MultiScan histogram statistics
   */

  /**
   * Time spent in Iterator::Prepare() for multi-scan (microseconds)
   */
  MULTISCAN_PREPARE_MICROS((byte) 0x40),

  /**
   * Number of blocks per multi-scan Prepare() call
   */
  MULTISCAN_BLOCKS_PER_PREPARE((byte) 0x41),

  /**
   * Coefficient of variation of key gaps in data blocks, scaled by 10000.
   */
  BLOCK_KEY_DISTRIBUTION_CV((byte) 0x42),

  /**
   * Time (us) in the prepare phase of an IngestExternalFile(s) call.
   */
  INGEST_EXTERNAL_FILE_PREPARE_TIME((byte) 0x43),

  /**
   * Time (us) in the run phase of an IngestExternalFile(s) call.
   */
  INGEST_EXTERNAL_FILE_RUN_TIME((byte) 0x44),

  /**
   * Time read requests spent waiting for rate limiter refills.
   */
  RATE_LIMITER_WAIT_MICROS_READ((byte) 0x45),

  /**
   * Time write requests spent waiting for rate limiter refills.
   */
  RATE_LIMITER_WAIT_MICROS_WRITE((byte) 0x46),

  /**
   * Total memtable memory usage at flush start.
   */
  FLUSH_MEMTABLE_MEMORY_BYTES((byte) 0x47),

  /**
   * Total memtable data size at flush start.
   */
  FLUSH_MEMTABLE_TOTAL_DATA_SIZE((byte) 0x48),

  /**
   * Total memtable memory usage for write-buffer-full flushes.
   */
  FLUSH_WRITE_BUFFER_FULL_MEMTABLE_MEMORY_BYTES((byte) 0x49),

  /**
   * Total memtable memory usage for WBM-triggered flushes.
   */
  FLUSH_WRITE_BUFFER_MANAGER_MEMTABLE_MEMORY_BYTES((byte) 0x4A),

  /**
   * Time spent opening the secondary DB inside DB::OpenAndCompact().
   */
  OPEN_AND_COMPACT_DB_OPEN_MICROS((byte) 0x4B),

  /**
   * Time from IODispatcher async read submission until completion callback.
   */
  IO_DISPATCHER_ASYNC_READ_OBSERVED_COMPLETION_MICROS((byte) 0x4C),

  /**
   * Time IODispatcher spends waiting in FileSystem::Poll().
   */
  IO_DISPATCHER_ASYNC_READ_POLL_WAIT_MICROS((byte) 0x4D),

  /**
   * Time from IODispatcher async read submission until the consumer polls.
   */
  IO_DISPATCHER_ASYNC_READ_PREFETCH_LEAD_MICROS((byte) 0x4E),

  // 0x3E is reserved for backwards compatibility on current minor version.
  HISTOGRAM_ENUM_MAX((byte) 0x3E);

  private final byte value;

  HistogramType(final byte value) {
    this.value = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * Get Histogram type by byte value.
   *
   * @param value byte representation of HistogramType.
   *
   * @return {@link org.rocksdb.HistogramType} instance.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static HistogramType getHistogramType(final byte value) {
    for (final HistogramType histogramType : values()) {
      if (histogramType.getValue() == value) {
        return histogramType;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for HistogramType.");
  }
}
