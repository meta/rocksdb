// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

/**
 * Keep adding tickers here. Note that the C++ enum values, unlike the values in
 * the Java bindings, are not guaranteed to be stable; also, the C++ and Java
 * values for any given ticker are not guaranteed to match.
 *  1. Add the new ticker before TICKER_ENUM_MAX.
 *  2. Add a readable string in TickersNameMap below for the newly added ticker.
 *  3. Add a corresponding enum value to TickerType.java in the Java API.
 *  4. Add the enum conversions from/to Java/C++ to portal.h's toJavaTickerType
 *     and toCppTickers.
 */
enum Tickers : uint32_t {
  // total block cache misses
  // REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
  //                               BLOCK_CACHE_FILTER_MISS +
  //                               BLOCK_CACHE_DATA_MISS;
  BLOCK_CACHE_MISS = 0,
  // total block cache hit
  // REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
  //                              BLOCK_CACHE_FILTER_HIT +
  //                              BLOCK_CACHE_DATA_HIT;
  BLOCK_CACHE_HIT,
  // # of blocks added to block cache.
  BLOCK_CACHE_ADD,
  // # of failures when adding blocks to block cache.
  BLOCK_CACHE_ADD_FAILURES,
  // # of times cache miss when accessing index block from block cache.
  BLOCK_CACHE_INDEX_MISS,
  // # of times cache hit when accessing index block from block cache.
  BLOCK_CACHE_INDEX_HIT,
  // # of index blocks added to block cache.
  BLOCK_CACHE_INDEX_ADD,
  // # of bytes of index blocks inserted into cache
  BLOCK_CACHE_INDEX_BYTES_INSERT,
  // # of times cache miss when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_MISS,
  // # of times cache hit when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_HIT,
  // # of filter blocks added to block cache.
  BLOCK_CACHE_FILTER_ADD,
  // # of bytes of bloom filter blocks inserted into cache
  BLOCK_CACHE_FILTER_BYTES_INSERT,
  // # of times cache miss when accessing data block from block cache.
  BLOCK_CACHE_DATA_MISS,
  // # of times cache hit when accessing data block from block cache.
  BLOCK_CACHE_DATA_HIT,
  // # of data blocks added to block cache.
  BLOCK_CACHE_DATA_ADD,
  // # of bytes of data blocks inserted into cache
  BLOCK_CACHE_DATA_BYTES_INSERT,
  // # of bytes read from cache.
  BLOCK_CACHE_BYTES_READ,
  // # of bytes written into cache.
  BLOCK_CACHE_BYTES_WRITE,

  BLOCK_CACHE_COMPRESSION_DICT_MISS,
  BLOCK_CACHE_COMPRESSION_DICT_HIT,
  BLOCK_CACHE_COMPRESSION_DICT_ADD,
  BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT,

  // # of blocks redundantly inserted into block cache.
  // REQUIRES: BLOCK_CACHE_ADD_REDUNDANT <= BLOCK_CACHE_ADD
  BLOCK_CACHE_ADD_REDUNDANT,
  // # of index blocks redundantly inserted into block cache.
  // REQUIRES: BLOCK_CACHE_INDEX_ADD_REDUNDANT <= BLOCK_CACHE_INDEX_ADD
  BLOCK_CACHE_INDEX_ADD_REDUNDANT,
  // # of filter blocks redundantly inserted into block cache.
  // REQUIRES: BLOCK_CACHE_FILTER_ADD_REDUNDANT <= BLOCK_CACHE_FILTER_ADD
  BLOCK_CACHE_FILTER_ADD_REDUNDANT,
  // # of data blocks redundantly inserted into block cache.
  // REQUIRES: BLOCK_CACHE_DATA_ADD_REDUNDANT <= BLOCK_CACHE_DATA_ADD
  BLOCK_CACHE_DATA_ADD_REDUNDANT,
  // # of dict blocks redundantly inserted into block cache.
  // REQUIRES: BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT
  //           <= BLOCK_CACHE_COMPRESSION_DICT_ADD
  BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT,

  // Secondary cache statistics
  SECONDARY_CACHE_HITS,

  // Fine grained secondary cache stats
  SECONDARY_CACHE_FILTER_HITS,
  SECONDARY_CACHE_INDEX_HITS,
  SECONDARY_CACHE_DATA_HITS,

  // Compressed secondary cache related stats
  COMPRESSED_SECONDARY_CACHE_DUMMY_HITS,
  COMPRESSED_SECONDARY_CACHE_HITS,
  COMPRESSED_SECONDARY_CACHE_PROMOTIONS,
  COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS,

  // # of times bloom filter has avoided file reads, i.e., negatives.
  BLOOM_FILTER_USEFUL,
  // # of times bloom FullFilter has not avoided the reads.
  BLOOM_FILTER_FULL_POSITIVE,
  // # of times bloom FullFilter has not avoided the reads and data actually
  // exist.
  BLOOM_FILTER_FULL_TRUE_POSITIVE,
  // Prefix filter stats when used for point lookups (Get / MultiGet).
  // (For prefix filter stats on iterators, see *_LEVEL_SEEK_*.)
  // Checked: filter was queried
  BLOOM_FILTER_PREFIX_CHECKED,
  // Useful: filter returned false so prevented accessing data+index blocks
  BLOOM_FILTER_PREFIX_USEFUL,
  // True positive: found a key matching the point query. When another key
  // with the same prefix matches, it is considered a false positive by
  // these statistics even though the filter returned a true positive.
  BLOOM_FILTER_PREFIX_TRUE_POSITIVE,

  // # persistent cache hit
  PERSISTENT_CACHE_HIT,
  // # persistent cache miss
  PERSISTENT_CACHE_MISS,

  // # total simulation block cache hits
  SIM_BLOCK_CACHE_HIT,
  // # total simulation block cache misses
  SIM_BLOCK_CACHE_MISS,

  // # of memtable hits.
  MEMTABLE_HIT,
  // # of memtable misses.
  MEMTABLE_MISS,

  // # of Get() queries served by L0
  GET_HIT_L0,
  // # of Get() queries served by L1
  GET_HIT_L1,
  // # of Get() queries served by L2 and up
  GET_HIT_L2_AND_UP,

  /**
   * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
   * There are 4 reasons currently.
   */
  COMPACTION_KEY_DROP_NEWER_ENTRY,  // key was written with a newer value.
                                    // Also includes keys dropped for range del.
  COMPACTION_KEY_DROP_OBSOLETE,     // The key is obsolete.
  COMPACTION_KEY_DROP_RANGE_DEL,    // key was covered by a range tombstone.
  COMPACTION_KEY_DROP_USER,  // user compaction function has dropped the key.
  COMPACTION_RANGE_DEL_DROP_OBSOLETE,  // all keys in range were deleted.
  // Deletions obsoleted before bottom level due to file gap optimization.
  COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
  // If a compaction was canceled in sfm to prevent ENOSPC
  COMPACTION_CANCELLED,
  // Number of compactions aborted via AbortAllCompactions() or
  // AbortCompactions().
  COMPACTION_ABORTED,

  // Number of keys written to the database via the Put and Write call's
  NUMBER_KEYS_WRITTEN,
  // Number of Keys read,
  NUMBER_KEYS_READ,
  // Number keys updated, if inplace update is enabled
  NUMBER_KEYS_UPDATED,
  // The number of uncompressed bytes issued by DB::Put(), DB::Delete(),
  // DB::Merge(), and DB::Write().
  BYTES_WRITTEN,
  // The number of uncompressed bytes read from DB::Get().  It could be
  // either from memtables, cache, or table files.
  // For the number of logical bytes read from DB::MultiGet(),
  // please use NUMBER_MULTIGET_BYTES_READ.
  BYTES_READ,
  // The number of calls to seek/next/prev
  NUMBER_DB_SEEK,
  NUMBER_DB_NEXT,
  NUMBER_DB_PREV,
  // The number of calls to seek/next/prev that returned data
  NUMBER_DB_SEEK_FOUND,
  NUMBER_DB_NEXT_FOUND,
  NUMBER_DB_PREV_FOUND,
  // The number of uncompressed bytes read from an iterator.
  // Includes size of key and value.
  ITER_BYTES_READ,
  // Number of internal keys skipped by Iterator
  NUMBER_ITER_SKIP,
  // Number of times we had to reseek inside an iteration to skip
  // over large number of keys with same userkey.
  NUMBER_OF_RESEEKS_IN_ITERATION,

  NO_ITERATOR_CREATED,  // number of iterators created
  NO_ITERATOR_DELETED,  // number of iterators deleted

  NO_FILE_OPENS,
  NO_FILE_ERRORS,
  // Writer has to wait for compaction or flush to finish.
  STALL_MICROS,
  // The wait time for db mutex.
  // Disabled by default. To enable it set stats level to kAll
  DB_MUTEX_WAIT_MICROS,

  // Number of MultiGet calls, keys read, and bytes read
  NUMBER_MULTIGET_CALLS,
  NUMBER_MULTIGET_KEYS_READ,
  NUMBER_MULTIGET_BYTES_READ,
  // Number of keys actually found in MultiGet calls (vs number requested by
  // caller)
  // NUMBER_MULTIGET_KEYS_READ gives the number requested by caller
  NUMBER_MULTIGET_KEYS_FOUND,

  NUMBER_MERGE_FAILURES,

  // Record the number of calls to GetUpdatesSince. Useful to keep track of
  // transaction log iterator refreshes
  GET_UPDATES_SINCE_CALLS,
  WAL_FILE_SYNCED,  // Number of times WAL sync is done
  WAL_FILE_BYTES,   // Number of bytes written to WAL
  // Number of WAL rotations that consumed an async precreated WAL.
  WAL_PRECREATE_HIT,
  // Number of WAL rotations that found no async precreated WAL to consume.
  WAL_PRECREATE_MISS,
  // Number of WAL rotations that waited for an in-flight WAL precreation.
  WAL_PRECREATE_WAITED,
  // Total foreground wait time for in-flight WAL precreation.
  WAL_PRECREATE_WAIT_MICROS,
  // Number of async WAL precreation attempts that failed.
  WAL_PRECREATE_FAILED,

  // Writes can be processed by requesting thread or by the thread at the
  // head of the writers queue.
  WRITE_DONE_BY_SELF,
  WRITE_DONE_BY_OTHER,  // Equivalent to writes done for others
  WRITE_WITH_WAL,       // Number of Write calls that request WAL
  COMPACT_READ_BYTES,   // Bytes read during compaction
  COMPACT_WRITE_BYTES,  // Bytes written during compaction
  FLUSH_WRITE_BYTES,    // Bytes written during flush

  // Compaction read and write statistics broken down by CompactionReason
  COMPACT_READ_BYTES_MARKED,
  COMPACT_READ_BYTES_PERIODIC,
  COMPACT_READ_BYTES_TTL,
  COMPACT_WRITE_BYTES_MARKED,
  COMPACT_WRITE_BYTES_PERIODIC,
  COMPACT_WRITE_BYTES_TTL,

  // Number of table's properties loaded directly from file, without creating
  // table reader object.
  NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
  NUMBER_SUPERVERSION_ACQUIRES,
  NUMBER_SUPERVERSION_RELEASES,
  NUMBER_SUPERVERSION_CLEANUPS,

  // # of compressions/decompressions executed
  NUMBER_BLOCK_COMPRESSED,
  NUMBER_BLOCK_DECOMPRESSED,

  // Number of input bytes (uncompressed) to compression for SST blocks that
  // are stored compressed.
  BYTES_COMPRESSED_FROM,
  // Number of output bytes (compressed) from compression for SST blocks that
  // are stored compressed.
  BYTES_COMPRESSED_TO,
  // Number of uncompressed bytes for SST blocks that are stored uncompressed
  // because compression type is kNoCompression, or some error case caused
  // compression not to run or produce an output. Index blocks are only counted
  // if enable_index_compression is true.
  BYTES_COMPRESSION_BYPASSED,
  // Number of input bytes (uncompressed) to compression for SST blocks that
  // are stored uncompressed because the compression result was rejected,
  // either because the ratio was not acceptable (see
  // CompressionOptions::max_compressed_bytes_per_kb) or found invalid by the
  // `verify_compression` option.
  BYTES_COMPRESSION_REJECTED,

  // Like BYTES_COMPRESSION_BYPASSED but counting number of blocks
  NUMBER_BLOCK_COMPRESSION_BYPASSED,
  // Like BYTES_COMPRESSION_REJECTED but counting number of blocks
  NUMBER_BLOCK_COMPRESSION_REJECTED,

  // Number of input bytes (compressed) to decompression in reading compressed
  // SST blocks from storage.
  BYTES_DECOMPRESSED_FROM,
  // Number of output bytes (uncompressed) from decompression in reading
  // compressed SST blocks from storage.
  BYTES_DECOMPRESSED_TO,

  // Tickers that record cumulative time.
  MERGE_OPERATION_TOTAL_TIME,
  FILTER_OPERATION_TOTAL_TIME,
  COMPACTION_CPU_TOTAL_TIME,

  // Row cache.
  ROW_CACHE_HIT,
  ROW_CACHE_MISS,

  // Read amplification statistics.
  // Read amplification can be calculated using this formula
  // (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
  //
  // REQUIRES: ReadOptions::read_amp_bytes_per_bit to be enabled
  READ_AMP_ESTIMATE_USEFUL_BYTES,  // Estimate of total bytes actually used.
  READ_AMP_TOTAL_READ_BYTES,       // Total size of loaded data blocks.

  // Number of refill intervals where rate limiter's bytes are fully consumed.
  NUMBER_RATE_LIMITER_DRAINS,
  // Bytes granted by the rate limiter for read requests.
  RATE_LIMITER_BYTES_READ,
  // Bytes granted by the rate limiter for write requests.
  RATE_LIMITER_BYTES_WRITE,
  // Number of read requests granted by the rate limiter.
  RATE_LIMITER_REQUESTS_READ,
  // Number of write requests granted by the rate limiter.
  RATE_LIMITER_REQUESTS_WRITE,
  // Number of read requests that waited for a future rate limiter refill.
  RATE_LIMITER_DELAYED_REQUESTS_READ,
  // Number of write requests that waited for a future rate limiter refill.
  RATE_LIMITER_DELAYED_REQUESTS_WRITE,
  // Total time read requests spent waiting for rate limiter refills.
  RATE_LIMITER_TOTAL_WAIT_MICROS_READ,
  // Total time write requests spent waiting for rate limiter refills.
  RATE_LIMITER_TOTAL_WAIT_MICROS_WRITE,

  // BlobDB specific stats
  // # of Put/PutWithTTL to BlobDB. Only applicable to legacy BlobDB.
  BLOB_DB_NUM_PUT,
  // # of Write to BlobDB. Only applicable to legacy BlobDB.
  BLOB_DB_NUM_WRITE,
  // # of Get to BlobDB. Only applicable to legacy BlobDB.
  BLOB_DB_NUM_GET,
  // # of MultiGet to BlobDB. Only applicable to legacy BlobDB.
  BLOB_DB_NUM_MULTIGET,
  // # of Seek/SeekToFirst/SeekToLast/SeekForPrev to BlobDB iterator. Only
  // applicable to legacy BlobDB.
  BLOB_DB_NUM_SEEK,
  // # of Next to BlobDB iterator. Only applicable to legacy BlobDB.
  BLOB_DB_NUM_NEXT,
  // # of Prev to BlobDB iterator. Only applicable to legacy BlobDB.
  BLOB_DB_NUM_PREV,
  // # of keys written to BlobDB. Only applicable to legacy BlobDB.
  BLOB_DB_NUM_KEYS_WRITTEN,
  // # of keys read from BlobDB. Only applicable to legacy BlobDB.
  BLOB_DB_NUM_KEYS_READ,
  // # of bytes (key + value) written to BlobDB. Only applicable to legacy
  // BlobDB.
  BLOB_DB_BYTES_WRITTEN,
  // # of bytes (keys + value) read from BlobDB. Only applicable to legacy
  // BlobDB.
  BLOB_DB_BYTES_READ,
  // Deprecated: min_blob_size is no longer configurable. Retained to avoid
  // shifting enum values.
  BLOB_DB_WRITE_INLINED_DEPRECATED,
  // Deprecated: min_blob_size is no longer configurable. Retained to avoid
  // shifting enum values.
  BLOB_DB_WRITE_INLINED_TTL_DEPRECATED,
  // # of keys written by BlobDB as non-TTL blob value. Only applicable to
  // legacy BlobDB.
  BLOB_DB_WRITE_BLOB,
  // # of keys written by BlobDB as TTL blob value. Only applicable to legacy
  // BlobDB.
  BLOB_DB_WRITE_BLOB_TTL,
  // # of bytes written to blob file.
  BLOB_DB_BLOB_FILE_BYTES_WRITTEN,
  // # of bytes read from blob file.
  BLOB_DB_BLOB_FILE_BYTES_READ,
  // # of times a blob files being synced.
  BLOB_DB_BLOB_FILE_SYNCED,
  // # of blob index evicted from base DB by BlobDB compaction filter because
  // of expiration. Only applicable to legacy BlobDB.
  BLOB_DB_BLOB_INDEX_EXPIRED_COUNT,
  // size of blob index evicted from base DB by BlobDB compaction filter
  // because of expiration. Only applicable to legacy BlobDB.
  BLOB_DB_BLOB_INDEX_EXPIRED_SIZE,
  // # of blob index evicted from base DB by BlobDB compaction filter because
  // of corresponding file deleted. Only applicable to legacy BlobDB.
  BLOB_DB_BLOB_INDEX_EVICTED_COUNT,
  // size of blob index evicted from base DB by BlobDB compaction filter
  // because of corresponding file deleted. Only applicable to legacy BlobDB.
  BLOB_DB_BLOB_INDEX_EVICTED_SIZE,
  // # of blob files that were obsoleted by garbage collection. Only applicable
  // to legacy BlobDB.
  BLOB_DB_GC_NUM_FILES,
  // # of blob files generated by garbage collection. Only applicable to legacy
  // BlobDB.
  BLOB_DB_GC_NUM_NEW_FILES,
  // # of BlobDB garbage collection failures. Only applicable to legacy BlobDB.
  BLOB_DB_GC_FAILURES,
  // # of keys relocated to new blob file by garbage collection.
  BLOB_DB_GC_NUM_KEYS_RELOCATED,
  // # of bytes relocated to new blob file by garbage collection.
  BLOB_DB_GC_BYTES_RELOCATED,
  // # of blob files evicted because of BlobDB is full. Only applicable to
  // legacy BlobDB.
  BLOB_DB_FIFO_NUM_FILES_EVICTED,
  // # of keys in the blob files evicted because of BlobDB is full. Only
  // applicable to legacy BlobDB.
  BLOB_DB_FIFO_NUM_KEYS_EVICTED,
  // # of bytes in the blob files evicted because of BlobDB is full. Only
  // applicable to legacy BlobDB.
  BLOB_DB_FIFO_BYTES_EVICTED,

  // Integrated BlobDB specific stats
  // # of times cache miss when accessing blob from blob cache.
  BLOB_DB_CACHE_MISS,
  // # of times cache hit when accessing blob from blob cache.
  BLOB_DB_CACHE_HIT,
  // # of data blocks added to blob cache.
  BLOB_DB_CACHE_ADD,
  // # of failures when adding blobs to blob cache.
  BLOB_DB_CACHE_ADD_FAILURES,
  // # of bytes read from blob cache.
  BLOB_DB_CACHE_BYTES_READ,
  // # of bytes written into blob cache.
  BLOB_DB_CACHE_BYTES_WRITE,

  // These counters indicate a performance issue in WritePrepared transactions.
  // We should not seem them ticking them much.
  // # of times prepare_mutex_ is acquired in the fast path.
  TXN_PREPARE_MUTEX_OVERHEAD,
  // # of times old_commit_map_mutex_ is acquired in the fast path.
  TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD,
  // # of times we checked a batch for duplicate keys.
  TXN_DUPLICATE_KEY_OVERHEAD,
  // # of times snapshot_mutex_ is acquired in the fast path.
  TXN_SNAPSHOT_MUTEX_OVERHEAD,
  // # of times ::Get returned TryAgain due to expired snapshot seq
  TXN_GET_TRY_AGAIN,

  // # of files marked as trash by sst file manager and will be deleted
  // later by background thread.
  FILES_MARKED_TRASH,
  // # of trash files deleted by the background thread from the trash queue.
  FILES_DELETED_FROM_TRASH_QUEUE,
  // # of files deleted immediately by sst file manager through delete
  // scheduler.
  FILES_DELETED_IMMEDIATELY,

  // The counters for error handler, note that, bg_io_error is the subset of
  // bg_error and bg_retryable_io_error is the subset of bg_io_error.
  ERROR_HANDLER_BG_ERROR_COUNT,
  ERROR_HANDLER_BG_IO_ERROR_COUNT,
  ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT,
  ERROR_HANDLER_AUTORESUME_COUNT,
  ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT,
  ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT,

  // Statistics for memtable garbage collection:
  // Raw bytes of data (payload) present on memtable at flush time.
  MEMTABLE_PAYLOAD_BYTES_AT_FLUSH,
  // Outdated bytes of data present on memtable at flush time.
  MEMTABLE_GARBAGE_BYTES_AT_FLUSH,

  // Bytes read by `VerifyChecksum()` and `VerifyFileChecksums()` APIs.
  VERIFY_CHECKSUM_READ_BYTES,

  // Bytes read/written while creating backups
  BACKUP_READ_BYTES,
  BACKUP_WRITE_BYTES,

  // Remote compaction read/write statistics
  REMOTE_COMPACT_READ_BYTES,
  REMOTE_COMPACT_WRITE_BYTES,

  // Bytes of output files successfully resumed during compaction
  REMOTE_COMPACT_RESUMED_BYTES,

  // Tiered storage related statistics
  HOT_FILE_READ_BYTES,
  WARM_FILE_READ_BYTES,
  COOL_FILE_READ_BYTES,
  COLD_FILE_READ_BYTES,
  ICE_FILE_READ_BYTES,
  HOT_FILE_READ_COUNT,
  WARM_FILE_READ_COUNT,
  COOL_FILE_READ_COUNT,
  COLD_FILE_READ_COUNT,
  ICE_FILE_READ_COUNT,

  // Last level and non-last level read statistics
  LAST_LEVEL_READ_BYTES,
  LAST_LEVEL_READ_COUNT,
  NON_LAST_LEVEL_READ_BYTES,
  NON_LAST_LEVEL_READ_COUNT,

  // Statistics on iterator Seek() (and variants) for each sorted run. I.e. a
  // single user Seek() can result in many sorted run Seek()s.
  // The stats are split between last level and non-last level.
  // Filtered: a filter such as prefix Bloom filter indicate the Seek() would
  // not find anything relevant, so avoided a likely access to data+index
  // blocks.
  LAST_LEVEL_SEEK_FILTERED,
  // Filter match: a filter such as prefix Bloom filter was queried but did
  // not filter out the seek.
  LAST_LEVEL_SEEK_FILTER_MATCH,
  // At least one data block was accessed for a Seek() (or variant) on a
  // sorted run.
  LAST_LEVEL_SEEK_DATA,
  // At least one value() was accessed for the seek (suggesting it was useful),
  // and no filter such as prefix Bloom was queried.
  LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER,
  // At least one value() was accessed for the seek (suggesting it was useful),
  // after querying a filter such as prefix Bloom.
  LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH,
  // The same set of stats, but for non-last level seeks.
  NON_LAST_LEVEL_SEEK_FILTERED,
  NON_LAST_LEVEL_SEEK_FILTER_MATCH,
  NON_LAST_LEVEL_SEEK_DATA,
  NON_LAST_LEVEL_SEEK_DATA_USEFUL_NO_FILTER,
  NON_LAST_LEVEL_SEEK_DATA_USEFUL_FILTER_MATCH,

  // Number of block checksum verifications
  BLOCK_CHECKSUM_COMPUTE_COUNT,
  // Number of times RocksDB detected a corruption while verifying a block
  // checksum. RocksDB does not remember corruptions that happened during user
  // reads so the same block corruption may be detected multiple times.
  BLOCK_CHECKSUM_MISMATCH_COUNT,

  MULTIGET_COROUTINE_COUNT,

  // Time spent in the ReadAsync file system call
  READ_ASYNC_MICROS,
  // Number of errors returned to the async read callback
  ASYNC_READ_ERROR_COUNT,

  // Number of lookup into the prefetched tail (see
  // `TABLE_OPEN_PREFETCH_TAIL_READ_BYTES`)
  // that can't find its data for table open
  TABLE_OPEN_PREFETCH_TAIL_MISS,
  // Number of lookup into the prefetched tail (see
  // `TABLE_OPEN_PREFETCH_TAIL_READ_BYTES`)
  // that finds its data for table open
  TABLE_OPEN_PREFETCH_TAIL_HIT,

  // Statistics on the filtering by user-defined timestamps
  // # of times timestamps are checked on accessing the table
  TIMESTAMP_FILTER_TABLE_CHECKED,
  // # of times timestamps can successfully help skip the table access
  TIMESTAMP_FILTER_TABLE_FILTERED,

  // Number of times readahead is trimmed during scans when
  // ReadOptions.auto_readahead_size is set.
  READAHEAD_TRIMMED,

  // Number of FIFO compactions that drop files based on different reasons
  FIFO_MAX_SIZE_COMPACTIONS,
  FIFO_TTL_COMPACTIONS,
  FIFO_CHANGE_TEMPERATURE_COMPACTIONS,

  // Number of bytes prefetched during user initiated scan
  PREFETCH_BYTES,

  // Number of prefetched bytes that were actually useful during user initiated
  // scan
  PREFETCH_BYTES_USEFUL,

  // Number of FS reads avoided due to prefetching during user initiated scan
  PREFETCH_HITS,

  // Footer corruption detected when opening an SST file for reading
  SST_FOOTER_CORRUPTION_COUNT,

  // Counters for file read retries with the verify_and_reconstruct_read
  // file system option after detecting a checksum mismatch
  FILE_READ_CORRUPTION_RETRY_COUNT,
  FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT,

  // Counter for the number of times a WBWI is ingested into the DB. This
  // happens when IngestWriteBatchWithIndex() is used and when large
  // transaction optimization is enabled through
  // TransactionOptions::large_txn_commit_optimize_threshold.
  NUMBER_WBWI_INGEST,

  // Failure to load the UDI during SST table open
  SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT,

  // MultiScan statistics
  // # of Prepare() calls
  MULTISCAN_PREPARE_CALLS,
  // # of Prepare() calls that failed
  MULTISCAN_PREPARE_ERRORS,
  // # of data blocks prefetched from storage during MultiScan
  MULTISCAN_BLOCKS_PREFETCHED,
  // # of blocks found already in cache during MultiScan Prepare
  MULTISCAN_BLOCKS_FROM_CACHE,
  // Total bytes prefetched during MultiScan
  MULTISCAN_PREFETCH_BYTES,
  // # of prefetched blocks that were never accessed
  MULTISCAN_PREFETCH_BLOCKS_WASTED,
  // # of actual I/O requests issued during MultiScan
  MULTISCAN_IO_REQUESTS,
  // # of non-adjacent blocks coalesced into single I/O (within
  // io_coalesce_threshold)
  MULTISCAN_IO_COALESCED_NONADJACENT,
  // # of seeks that failed validation (out of order, etc.)
  MULTISCAN_SEEK_ERRORS,

  // IODispatcher memory limiting statistics
  // # of bytes granted to prefetch requests
  PREFETCH_MEMORY_BYTES_GRANTED,
  // # of bytes released from prefetch memory
  PREFETCH_MEMORY_BYTES_RELEASED,
  // # of prefetch requests that were blocked waiting for memory
  PREFETCH_MEMORY_REQUESTS_BLOCKED,

  // # of range tombstones inserted by read-path conversion from contiguous
  // point tombstones
  READ_PATH_RANGE_TOMBSTONES_INSERTED,
  // # of range tombstones not inserted. Reasons include:
  // - iterator's snapshot predates the memtable
  // - tombstones may be uncommitted (transactions)
  // - memtable already has a covering range tombstone
  // - memtable switched to immutable state
  READ_PATH_RANGE_TOMBSTONES_DISCARDED,

  // Number of times file open metadata was retrieved from the file system
  // via GetFileOpenMetadata() (when fast_sst_open is enabled)
  FILE_OPEN_METADATA_RETRIEVED,
  // Number of times file open metadata was passed to NewRandomAccessFile()
  // via FileOptions::file_metadata (on DB open / table cache miss)
  FILE_OPEN_METADATA_PASSED,

  // # of times MANIFEST content validation detected corruption on DB close
  MANIFEST_VALIDATION_FAILURE_COUNT,

  // Number of flushes triggered because the memtable reached write_buffer_size.
  FLUSH_REASON_WRITE_BUFFER_FULL,
  // Number of flushes triggered by WriteBufferManager memory pressure.
  FLUSH_REASON_WRITE_BUFFER_MANAGER,
  // Number of flushes triggered because the memtable reached
  // memtable_max_range_deletions.
  FLUSH_REASON_MEMTABLE_MAX_RANGE_DELETIONS,
  // Number of atomic flush requests triggered because a memtable reached
  // write_buffer_size.
  ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_FULL,
  // Number of atomic flush requests triggered by WriteBufferManager memory
  // pressure.
  ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_MANAGER,
  // Number of atomic flush requests triggered because a memtable reached
  // memtable_max_range_deletions.
  ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS,
  // Number of atomic flush requests triggered for reasons that do not have a
  // dedicated atomic flush request reason ticker.
  ATOMIC_FLUSH_REQUEST_REASON_OTHER,

  // Bytes read/written while creating checkpoints
  CHECKPOINT_READ_BYTES,
  CHECKPOINT_WRITE_BYTES,

  // Number of SubmitReadAsync calls that fell back to a synchronous read
  FILE_SUBMIT_ASYNC_READ_FALLBACK,

  // Lazy wide-column resolution (DB::GetEntityLazy / DB::MultiGetEntityLazy):
  // metrics for the blob reads issued from storage while resolving a lazy
  // result (attributed via Env::IOActivity::kLazyResolve). Cache hits, which do
  // no storage read, are not counted.
  //
  // Number of storage reads issued by lazy resolution (both whole-column and
  // partial byte-range reads).
  BLOB_DB_LAZY_READ_COUNT,
  // Bytes actually read from storage by those reads (on-disk record bytes for
  // whole-column reads; the requested sub-range length for partial reads).
  BLOB_DB_LAZY_READ_BYTES,
  // Of the above, the number that were actual partial (byte-range) reads: only
  // a requested sub-range of an uncompressed blob's value was read (skipping
  // the whole-column read). A whole-column read or a force_verify read is not
  // counted here.
  BLOB_DB_LAZY_PARTIAL_READ_COUNT,
  // Bytes not read thanks to those partial reads: for each, the column's
  // logical value size minus the bytes actually read.
  BLOB_DB_LAZY_PARTIAL_BYTES_SAVED,

  // Number of distinct blob files advertised by remote compaction jobs that
  // dispatched at least one subcompaction.
  REMOTE_COMPACT_INPUT_BLOB_FILE_COUNT,
  // Number of remote compaction results declined by the primary because blob
  // metadata, files, or capabilities failed validation before installation.
  REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT,

  TICKER_ENUM_MAX
};

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
extern const std::vector<std::pair<Tickers, std::string>> TickersNameMap;

/**
 * Keep adding histograms here. Note that the C++ enum values, unlike the values
 * in the Java bindings, are not guaranteed to be stable; also, the C++ and Java
 * values for any given histogram are not guaranteed to match.
 *  1. Add the new histogram before HISTOGRAM_ENUM_MAX.
 *  2. Add a readable string in HistogramsNameMap below for the newly added
 *     histogram.
 *  3. Add a corresponding enum value to HistogramType.java in the Java API.
 *  4. Add the enum conversions from/to Java/C++ to portal.h's
 *     toJavaHistogramsType and toCppHistograms.
 */
enum Histograms : uint32_t {
  DB_GET = 0,
  DB_WRITE,
  COMPACTION_TIME,
  COMPACTION_CPU_TIME,
  SUBCOMPACTION_SETUP_TIME,
  TABLE_SYNC_MICROS,
  COMPACTION_OUTFILE_SYNC_MICROS,
  WAL_FILE_SYNC_MICROS,
  MANIFEST_FILE_SYNC_MICROS,
  // TIME SPENT IN IO DURING TABLE OPEN
  TABLE_OPEN_IO_MICROS,
  DB_MULTIGET,
  READ_BLOCK_COMPACTION_MICROS,
  READ_BLOCK_GET_MICROS,
  WRITE_RAW_BLOCK_MICROS,
  NUM_FILES_IN_SINGLE_COMPACTION,
  DB_SEEK,
  WRITE_STALL,
  // Time spent in reading block-based or plain SST table
  SST_READ_MICROS,
  // Time spent in reading SST table (currently only block-based table) or blob
  // file corresponding to `Env::IOActivity`
  FILE_READ_FLUSH_MICROS,
  FILE_READ_COMPACTION_MICROS,
  FILE_READ_DB_OPEN_MICROS,
  // The following `FILE_READ_*` require stats level greater than
  // `StatsLevel::kExceptDetailedTimers`
  FILE_READ_GET_MICROS,
  FILE_READ_MULTIGET_MICROS,
  FILE_READ_DB_ITERATOR_MICROS,
  FILE_READ_VERIFY_DB_CHECKSUM_MICROS,
  FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS,

  // Time spent in writing SST files
  SST_WRITE_MICROS,
  // Time spent in writing SST table (currently only block-based table) or blob
  // file for flush, compaction or db open
  FILE_WRITE_FLUSH_MICROS,
  FILE_WRITE_COMPACTION_MICROS,
  FILE_WRITE_DB_OPEN_MICROS,

  // The number of subcompactions actually scheduled during a compaction
  NUM_SUBCOMPACTIONS_SCHEDULED,
  // Value size distribution in each operation
  BYTES_PER_READ,
  BYTES_PER_WRITE,
  BYTES_PER_MULTIGET,

  COMPRESSION_TIMES_NANOS,
  DECOMPRESSION_TIMES_NANOS,
  // Number of merge operands passed to the merge operator in user read
  // requests.
  READ_NUM_MERGE_OPERANDS,

  // BlobDB specific stats
  // Size of keys written to BlobDB. Only applicable to legacy BlobDB.
  BLOB_DB_KEY_SIZE,
  // Size of values written to BlobDB. Only applicable to legacy BlobDB.
  BLOB_DB_VALUE_SIZE,
  // BlobDB Put/PutWithTTL/Write latency. Only applicable to legacy BlobDB.
  BLOB_DB_WRITE_MICROS,
  // BlobDB Get latency. Only applicable to legacy BlobDB.
  BLOB_DB_GET_MICROS,
  // BlobDB MultiGet latency. Only applicable to legacy BlobDB.
  BLOB_DB_MULTIGET_MICROS,
  // BlobDB Seek/SeekToFirst/SeekToLast/SeekForPrev latency. Only applicable to
  // legacy BlobDB.
  BLOB_DB_SEEK_MICROS,
  // BlobDB Next latency. Only applicable to legacy BlobDB.
  BLOB_DB_NEXT_MICROS,
  // BlobDB Prev latency. Only applicable to legacy BlobDB.
  BLOB_DB_PREV_MICROS,
  // Blob file write latency.
  BLOB_DB_BLOB_FILE_WRITE_MICROS,
  // Blob file read latency.
  BLOB_DB_BLOB_FILE_READ_MICROS,
  // Blob file sync latency.
  BLOB_DB_BLOB_FILE_SYNC_MICROS,
  // BlobDB compression time.
  BLOB_DB_COMPRESSION_MICROS,
  // BlobDB decompression time.
  BLOB_DB_DECOMPRESSION_MICROS,
  // Time spent flushing memtable to disk
  FLUSH_TIME,
  SST_BATCH_SIZE,

  // Number of IOs issued in parallel in a MultiGet batch
  MULTIGET_IO_BATCH_SIZE,
  // MultiGet stats logged per level
  // Num of index and filter blocks read from file system per level.
  NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
  // Num of sst files read from file system per level.
  NUM_SST_READ_PER_LEVEL,
  // Number of levels requiring IO for MultiGet
  NUM_LEVEL_READ_PER_MULTIGET,

  // Error handler statistics
  ERROR_HANDLER_AUTORESUME_RETRY_COUNT,

  // Stats related to asynchronous read requests.
  ASYNC_READ_BYTES,
  POLL_WAIT_MICROS,

  // Number of bytes for RocksDB's prefetching (as opposed to file
  // system's prefetch) on SST file during compaction read
  COMPACTION_PREFETCH_BYTES,

  // Number of prefetched bytes discarded by RocksDB.
  PREFETCHED_BYTES_DISCARDED,

  // Wait time for aborting async read in FilePrefetchBuffer destructor
  ASYNC_PREFETCH_ABORT_MICROS,

  // Number of bytes for RocksDB's prefetching contents (as opposed to file
  // system's prefetch) from the end of SST table during block based table open
  TABLE_OPEN_PREFETCH_TAIL_READ_BYTES,

  // Number of operations per transaction.
  NUM_OP_PER_TRANSACTION,

  // MultiScan Prefill iterator Prepare cost
  MULTISCAN_PREPARE_ITERATORS,

  // Total Prepare() latency for MultiScan
  MULTISCAN_PREPARE_MICROS,
  // Distribution of blocks prefetched per MultiScan Prepare()
  MULTISCAN_BLOCKS_PER_PREPARE,

  // Time (microseconds) from IODispatcher async read submission until
  // completion callback execution.
  IO_DISPATCHER_ASYNC_READ_OBSERVED_COMPLETION_MICROS,
  // Time (microseconds) IODispatcher spends waiting in FileSystem::Poll().
  IO_DISPATCHER_ASYNC_READ_POLL_WAIT_MICROS,
  // Time (microseconds) from async read submission until the block consumer
  // starts polling for completion.
  IO_DISPATCHER_ASYNC_READ_PREFETCH_LEAD_MICROS,

  // Coefficient of variation of key gaps in blocks, scaled by 10000
  // (e.g., CV of 0.4532 is recorded as 4532). Currently only used by index
  // blocks for uniform key distribution tracking.
  BLOCK_KEY_DISTRIBUTION_CV,

  // Time (microseconds) in the "prepare" phase of an IngestExternalFile(s) call
  // (validation and reading/linking the files; does not block writes). One
  // sample per successful call; requires stats level > kExceptTimers.
  INGEST_EXTERNAL_FILE_PREPARE_TIME,

  // Time (microseconds) in the "run" phase of an IngestExternalFile(s) call
  // (work under the DB mutex while writes are blocked). One sample per
  // successful call; requires stats level > kExceptTimers.
  INGEST_EXTERNAL_FILE_RUN_TIME,

  // Time read requests spent waiting for rate limiter refills.
  RATE_LIMITER_WAIT_MICROS_READ,
  // Time write requests spent waiting for rate limiter refills.
  RATE_LIMITER_WAIT_MICROS_WRITE,

  // Distribution of total memtable memory usage at flush start.
  FLUSH_MEMTABLE_MEMORY_BYTES,
  // Distribution of total memtable data size at flush start.
  FLUSH_MEMTABLE_TOTAL_DATA_SIZE,
  // Distribution of total memtable memory usage for write-buffer-full flushes.
  FLUSH_WRITE_BUFFER_FULL_MEMTABLE_MEMORY_BYTES,
  // Distribution of total memtable memory usage for WBM-triggered flushes.
  FLUSH_WRITE_BUFFER_MANAGER_MEMTABLE_MEMORY_BYTES,

  // Time spent opening the secondary DB inside DB::OpenAndCompact().
  OPEN_AND_COMPACT_DB_OPEN_MICROS,

  HISTOGRAM_ENUM_MAX
};

extern const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap;

struct HistogramData {
  double median;
  double percentile95;
  double percentile99;
  double average;
  double standard_deviation;
  // zero-initialize new members since old Statistics::histogramData()
  // implementations won't write them.
  double max = 0.0;
  uint64_t count = 0;
  uint64_t sum = 0;
  double min = 0.0;
};

// StatsLevel can be used to reduce statistics overhead by skipping certain
// types of stats in the stats collection process.
// Usage:
//   options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
enum StatsLevel : uint8_t {
  // Disable all metrics
  kDisableAll,
  // Disable tickers
  kExceptTickers = kDisableAll,
  // Disable timer stats, and skip histogram stats
  kExceptHistogramOrTimers,
  // Skip timer stats
  kExceptTimers,
  // Collect all stats except time inside mutex lock AND time spent on
  // compression.
  kExceptDetailedTimers,
  // Collect all stats except the counters requiring to get time inside the
  // mutex lock.
  kExceptTimeForMutex,
  // Collect all stats, including measuring duration of mutex operations.
  // Also enables sampled INFO logs for selected DB-wide mutex holds exceeding
  // a positive DBOptions::mutex_hold_log_threshold_us (disabled by default).
  // These describe completed ownership intervals, not waiter latency or
  // comprehensive tracing; an info logger is required.
  // If getting time is expensive on the platform to run, it can
  // reduce scalability to more threads, especially for writes.
  kAll,
};

// Analyze the performance of a db by providing cumulative stats over time.
// Usage:
//  Options options;
//  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
//  Status s = DB::Open(options, kDBPath, &db);
//  ...
//  options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
//  HistogramData hist;
//  options.statistics->histogramData(FLUSH_TIME, &hist);
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class Statistics : public Customizable {
 public:
  ~Statistics() override = default;
  static const char* Type() { return "Statistics"; }
  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& value,
                                 std::shared_ptr<Statistics>* result);
  // Default name of empty, for backwards compatibility.  Derived classes should
  // override this method.
  // This default implementation will likely be removed in a future release
  const char* Name() const override { return ""; }
  virtual uint64_t getTickerCount(uint32_t tickerType) const = 0;
  virtual void histogramData(uint32_t type,
                             HistogramData* const data) const = 0;
  virtual std::string getHistogramString(uint32_t /*type*/) const { return ""; }
  virtual void recordTick(uint32_t tickerType, uint64_t count = 1) = 0;
  virtual void setTickerCount(uint32_t tickerType, uint64_t count) = 0;
  virtual uint64_t getAndResetTickerCount(uint32_t tickerType) = 0;
  virtual void reportTimeToHistogram(uint32_t histogramType, uint64_t time) {
    if (get_stats_level() <= StatsLevel::kExceptTimers) {
      return;
    }
    recordInHistogram(histogramType, time);
  }
  // The function is here only for backward compatibility reason.
  // Users implementing their own Statistics class should override
  // recordInHistogram() instead and leave measureTime() as it is.
  virtual void measureTime(uint32_t /*histogramType*/, uint64_t /*time*/) {
    // This is not supposed to be called.
    assert(false);
  }
  virtual void recordInHistogram(uint32_t histogramType, uint64_t time) {
    // measureTime() is the old and inaccurate function name.
    // To keep backward compatible. If users implement their own
    // statistics, which overrides measureTime() but doesn't override
    // this function. We forward to measureTime().
    measureTime(histogramType, time);
  }

  // Resets all ticker and histogram stats
  virtual Status Reset() { return Status::NotSupported("Not implemented"); }

  using Customizable::ToString;
  // String representation of the statistic object. Must be thread-safe.
  virtual std::string ToString() const {
    // Do nothing by default
    return std::string("ToString(): not implemented");
  }

  virtual bool getTickerMap(std::map<std::string, uint64_t>*) const {
    // Do nothing by default
    return false;
  }

  // Override this function to disable particular histogram collection
  virtual bool HistEnabledForType(uint32_t type) const {
    return type < HISTOGRAM_ENUM_MAX;
  }
  void set_stats_level(StatsLevel sl) {
    stats_level_.store(sl, std::memory_order_relaxed);
  }
  StatsLevel get_stats_level() const {
    return stats_level_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<StatsLevel> stats_level_{kExceptDetailedTimers};
};

// Create a concrete DBStatistics object
std::shared_ptr<Statistics> CreateDBStatistics();

}  // namespace ROCKSDB_NAMESPACE
