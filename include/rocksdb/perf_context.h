// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <map>
#include <string>

#include "rocksdb/perf_level.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

/*
 * NOTE:
 * Please do not reorder the fields in this structure. If you plan to do that or
 * add/remove fields to this structure, builds would fail. The way to fix the
 * builds would be to add the appropriate fields to the
 * DEF_PERF_CONTEXT_LEVEL_METRICS() macro in the perf_context.cc file.
 *
 * If you plan to add new metrics, please read documentation in perf_level.h and
 * try to come up with a metric name that follows the naming conventions
 * mentioned there. It helps to indicate the metric's starting enabling P
 * erfLevel. Document this starting PerfLevel if the metric name cannot meet the
 * naming conventions.
 */

// Break down performance counters by level and store per-level perf context in
// PerfContextByLevel
struct PerfContextByLevelBase {
  // These Bloom stats apply to point reads (Get/MultiGet) for whole key and
  // prefix filters.
  // # of times bloom filter has avoided file reads, i.e., negatives.
  uint64_t bloom_filter_useful = 0;
  // # of times bloom FullFilter has not avoided the reads.
  uint64_t bloom_filter_full_positive = 0;
  // # of times bloom FullFilter has not avoided the reads and data actually
  // exist.
  uint64_t bloom_filter_full_true_positive = 0;

  // total number of user key returned (only include keys that are found, does
  // not include keys that are deleted or merged without a final put
  uint64_t user_key_return_count = 0;

  // total nanos spent on reading data from SST files
  uint64_t get_from_table_nanos = 0;

  uint64_t block_cache_hit_count = 0;   // total number of block cache hits
  uint64_t block_cache_miss_count = 0;  // total number of block cache misses
};

// A thread local context for gathering performance counter efficiently
// and transparently.
// Use SetPerfLevel(PerfLevel::kEnableTime) to enable time stats.

// Break down performance counters by level and store per-level perf context in
// PerfContextByLevel
struct PerfContextByLevel : public PerfContextByLevelBase {
  void Reset();  // reset all performance counters to zero
  void Merge(const PerfContextByLevel& other);
};

/*
 * NOTE:
 * Please do not reorder the fields in this structure. If you plan to do that or
 * add/remove fields to this structure, builds would fail. The way to fix the
 * builds would be to add the appropriate fields to the
 * DEF_PERF_CONTEXT_METRICS() macro in the perf_context.cc file.
 */

struct PerfContextBase {
  uint64_t user_key_comparison_count;  // total number of user key comparisons
  uint64_t block_cache_hit_count;      // total number of block cache hits
  uint64_t block_read_count;           // total number of block reads (with IO)
  uint64_t block_read_byte;            // total number of bytes from block reads
  uint64_t block_read_time;            // total nanos spent on block reads
  // total cpu time in nanos spent on block reads. Not supported for async read
  // requests.
  uint64_t block_read_cpu_time;
  uint64_t block_cache_index_hit_count;  // total number of index block hits
  // total number of standalone handles lookup from secondary cache
  uint64_t block_cache_standalone_handle_count;
  // total number of real handles lookup from secondary cache that are inserted
  // into primary cache
  uint64_t block_cache_real_handle_count;
  uint64_t index_block_read_count;        // total number of index block reads
  uint64_t block_cache_filter_hit_count;  // total number of filter block hits
  uint64_t filter_block_read_count;       // total number of filter block reads
  uint64_t compression_dict_block_read_count;  // total number of compression
                                               // dictionary block reads
  // Cumulative size of blocks found in block cache
  uint64_t block_cache_index_read_byte;
  uint64_t block_cache_filter_read_byte;
  uint64_t block_cache_compression_dict_read_byte;
  uint64_t block_cache_read_byte;

  uint64_t secondary_cache_hit_count;  // total number of secondary cache hits
  // total number of real handles inserted into secondary cache
  uint64_t compressed_sec_cache_insert_real_count;
  // total number of dummy handles inserted into secondary cache
  uint64_t compressed_sec_cache_insert_dummy_count;
  // bytes for vals before compression in secondary cache
  uint64_t compressed_sec_cache_uncompressed_bytes;
  // bytes for vals after compression in secondary cache
  uint64_t compressed_sec_cache_compressed_bytes;

  uint64_t block_checksum_time;     // total nanos spent on block checksum
  uint64_t block_decompress_time;   // total nanos spent on block decompression
  uint64_t block_decompress_count;  // total number of block decompressions

  uint64_t get_read_bytes;       // bytes for vals returned by Get
  uint64_t multiget_read_bytes;  // bytes for vals returned by MultiGet
  uint64_t iter_read_bytes;      // bytes for keys/vals decoded by iterator

  uint64_t blob_cache_hit_count;  // total number of blob cache hits
  uint64_t blob_cache_read_byte;  // total bytes read from blob cache
  uint64_t blob_read_count;       // total number of blob reads (with IO)
  uint64_t blob_read_byte;        // total number of bytes from blob reads
  uint64_t blob_read_time;        // total nanos spent on blob reads
  uint64_t blob_checksum_time;    // total nanos spent on blob checksum
  uint64_t blob_decompress_time;  // total nanos spent on blob decompression

  // total number of internal keys skipped over during iteration.
  // There are several reasons for it:
  // 1. when calling Next(), the iterator is in the position of the previous
  //    key, so that we'll need to skip it. It means this counter will always
  //    be incremented in Next().
  // 2. when calling Next(), we need to skip internal entries for the previous
  //    keys that are overwritten.
  // 3. when calling Next(), Seek() or SeekToFirst(), after previous key
  //    before calling Next(), the seek key in Seek() or the beginning for
  //    SeekToFirst(), there may be one or more deleted keys before the next
  //    valid key that the operation should place the iterator to. We need
  //    to skip both of the tombstone and updates hidden by the tombstones. The
  //    tombstones are not included in this counter, while previous updates
  //    hidden by the tombstones will be included here.
  // 4. symmetric cases for Prev() and SeekToLast()
  // internal_recent_skipped_count is not included in this counter.
  //
  uint64_t internal_key_skipped_count;
  // Total number of deletes and single deletes skipped over during iteration
  // When calling Next(), Seek() or SeekToFirst(), after previous position
  // before calling Next(), the seek key in Seek() or the beginning for
  // SeekToFirst(), there may be one or more deleted keys before the next valid
  // key. Every deleted key is counted once. We don't recount here if there are
  // still older updates invalidated by the tombstones.
  //
  uint64_t internal_delete_skipped_count;
  // How many times iterators skipped over internal keys that are more recent
  // than the snapshot that iterator is using.
  //
  uint64_t internal_recent_skipped_count;
  // How many merge operands were fed into the merge operator by iterators.
  // Note: base values are not included in the count.
  //
  uint64_t internal_merge_count;
  // How many merge operands were fed into the merge operator by point lookups.
  // Note: base values are not included in the count.
  //
  uint64_t internal_merge_point_lookup_count;
  // Number of times we reseeked inside a merging iterator, specifically to skip
  // after or before a range of keys covered by a range deletion in a newer LSM
  // component.
  uint64_t internal_range_del_reseek_count;

  uint64_t get_snapshot_time;        // total nanos spent on getting snapshot
  uint64_t get_from_memtable_time;   // total nanos spent on querying memtables
  uint64_t get_from_memtable_count;  // number of mem tables queried
  // total nanos spent after Get() finds a key
  uint64_t get_post_process_time;
  uint64_t get_from_output_files_time;  // total nanos reading from output files
  // total nanos spent on seeking memtable
  uint64_t seek_on_memtable_time;
  // number of seeks issued on memtable
  // (including SeekForPrev but not SeekToFirst and SeekToLast)
  uint64_t seek_on_memtable_count;
  // number of Next()s issued on memtable
  uint64_t next_on_memtable_count;
  // number of Prev()s issued on memtable
  uint64_t prev_on_memtable_count;
  // total nanos spent on seeking child iters
  uint64_t seek_child_seek_time;
  // number of seek issued in child iterators
  uint64_t seek_child_seek_count;
  uint64_t seek_min_heap_time;  // total nanos spent on the merge min heap
  uint64_t seek_max_heap_time;  // total nanos spent on the merge max heap
  // total nanos spent on seeking the internal entries
  uint64_t seek_internal_seek_time;
  // total nanos spent on iterating internal entries to find the next user entry
  uint64_t find_next_user_entry_time;

  // This group of stats provide a breakdown of time spent by Write().
  // May be inaccurate when 2PC, two_write_queues or enable_pipelined_write
  // are enabled.
  //
  // total nanos spent on writing to WAL
  uint64_t write_wal_time;
  // total nanos spent on writing to mem tables
  // This metric gets collected starting from PerfLevel::kEnableWait
  uint64_t write_memtable_time;
  // total nanos spent on delaying or throttling write
  uint64_t write_delay_time;
  // total nanos spent on switching memtable/wal and scheduling
  // flushes/compactions.
  uint64_t write_scheduling_flushes_compactions_time;
  // total nanos spent on writing a record, excluding the above four things
  uint64_t write_pre_and_post_process_time;

  // time spent waiting for other threads of the batch group
  uint64_t write_thread_wait_nanos;

  // time spent on acquiring DB mutex.
  uint64_t db_mutex_lock_nanos;
  // Time spent on waiting with a condition variable created with DB mutex.
  uint64_t db_condition_wait_nanos;
  // Time spent on merge operator.
  uint64_t merge_operator_time_nanos;

  // Time spent on reading index block from block cache or SST file
  uint64_t read_index_block_nanos;
  // Time spent on reading filter block from block cache or SST file
  uint64_t read_filter_block_nanos;
  // Time spent on creating data block iterator
  uint64_t new_table_block_iter_nanos;
  // Time spent on creating a iterator of an SST file.
  uint64_t new_table_iterator_nanos;
  // Time spent on seeking a key in data/index blocks
  uint64_t block_seek_nanos;
  // Time spent on finding or creating a table reader
  uint64_t find_table_nanos;
  // total number of mem table bloom hits
  uint64_t bloom_memtable_hit_count;
  // total number of mem table bloom misses
  uint64_t bloom_memtable_miss_count;
  // total number of SST bloom hits
  uint64_t bloom_sst_hit_count;
  // total number of SST bloom misses
  uint64_t bloom_sst_miss_count;

  // Time spent waiting on key locks in transaction lock manager.
  // This metric gets collected starting from
  // PerfLevel::kEnableTimeExceptForMutex
  uint64_t key_lock_wait_time;
  // number of times acquiring a lock was blocked by another transaction.
  uint64_t key_lock_wait_count;

  // Total time spent in Env filesystem operations. These are only populated
  // when TimedEnv is used.
  uint64_t env_new_sequential_file_nanos;
  uint64_t env_new_random_access_file_nanos;
  uint64_t env_new_writable_file_nanos;
  uint64_t env_reuse_writable_file_nanos;
  uint64_t env_new_random_rw_file_nanos;
  uint64_t env_new_directory_nanos;
  uint64_t env_file_exists_nanos;
  uint64_t env_get_children_nanos;
  uint64_t env_get_children_file_attributes_nanos;
  uint64_t env_delete_file_nanos;
  uint64_t env_create_dir_nanos;
  uint64_t env_create_dir_if_missing_nanos;
  uint64_t env_delete_dir_nanos;
  uint64_t env_get_file_size_nanos;
  uint64_t env_get_file_modification_time_nanos;
  uint64_t env_rename_file_nanos;
  uint64_t env_link_file_nanos;
  uint64_t env_lock_file_nanos;
  uint64_t env_unlock_file_nanos;
  uint64_t env_new_logger_nanos;

  uint64_t get_cpu_nanos;
  uint64_t iter_next_cpu_nanos;
  uint64_t iter_prev_cpu_nanos;
  uint64_t iter_seek_cpu_nanos;

  // EXPERIMENTAL
  // Total number of db iterator's Next(), Prev(), Seek-related APIs being
  // called
  uint64_t iter_next_count;
  uint64_t iter_prev_count;
  uint64_t iter_seek_count;

  // Time spent in encrypting data. Populated when EncryptedEnv is used.
  uint64_t encrypt_data_nanos;
  // Time spent in decrypting data. Populated when EncryptedEnv is used.
  uint64_t decrypt_data_nanos;

  uint64_t number_async_seek;

  // Metrics for file ingestion
  // Time spent end to end in an IngestExternalFile call.
  uint64_t file_ingestion_nanos;
  // Time IngestExternalFile blocked live writes.
  uint64_t file_ingestion_blocking_live_writes_nanos;

  // Bytes read from storage for each block category. These add up to
  // block_read_byte.
  uint64_t data_block_read_byte;
  uint64_t index_block_read_byte;
  uint64_t filter_block_read_byte;
  uint64_t compression_dict_block_read_byte;
  uint64_t metadata_block_read_byte;

  // MultiScan (scan Prepare) prefetch metrics. Populated by
  // BlockBasedTableIterator::Prepare and its IODispatcher prefetch path. These
  // mirror the rocksdb.multiscan.* tickers but are scoped to the current
  // thread/operation. Average blocks per prepare is derivable as
  // (multiscan_blocks_prefetched + multiscan_blocks_from_cache) /
  // multiscan_prepare_count.
  uint64_t multiscan_prepare_count;
  // Blocks for which a prefetch IO was dispatched (read from disk).
  uint64_t multiscan_blocks_prefetched;
  // Blocks that were already in the block cache at Prepare time.
  uint64_t multiscan_blocks_from_cache;
  // Total bytes spanned by the (coalesced) prefetch IO requests.
  uint64_t multiscan_prefetch_bytes;
  // Number of (coalesced) IO requests issued for the prefetch.
  uint64_t multiscan_io_requests;
  // Blocks coalesced into an IO request across a non-adjacent gap.
  uint64_t multiscan_io_coalesced_nonadjacent;
};

struct PerfContext : public PerfContextBase {
  ~PerfContext();

  PerfContext() : PerfContextBase() {}

  PerfContext(const PerfContext&);
  PerfContext& operator=(const PerfContext&);
  PerfContext(PerfContext&&) noexcept;
  PerfContext& operator=(PerfContext&&) noexcept;

  void Reset();  // reset all performance counters to zero
  void Merge(const PerfContext& other);

  std::string ToString(bool exclude_zero_counters = false) const;

  // enable per level perf context and allocate storage for PerfContextByLevel
  void EnablePerLevelPerfContext();

  // temporarily disable per level perf context by setting the flag to false
  void DisablePerLevelPerfContext();

  // free the space for PerfContextByLevel, also disable per level perf context
  void ClearPerLevelPerfContext();

  std::map<uint32_t, PerfContextByLevel>* level_to_perf_context = nullptr;
  bool per_level_perf_context_enabled = false;

  void copyMetrics(const PerfContext* other) noexcept;
};

// If RocksDB is compiled with -DNPERF_CONTEXT, then a pointer to a global,
// non-thread-local PerfContext object will be returned. Attempts to update
// this object will be ignored, and reading from it will also be no-op.
// Otherwise,
// a) if thread-local is supported on the platform, then a pointer to
//    a thread-local PerfContext object will be returned.
// b) if thread-local is NOT supported, then compilation will fail.
//
// This function never returns nullptr.
PerfContext* get_perf_context();

}  // namespace ROCKSDB_NAMESPACE
