// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
struct CompactionJobStats {
  CompactionJobStats() { Reset(); }
  void Reset();
  // Aggregate the CompactionJobStats from another instance with this one
  void Add(const CompactionJobStats& stats);

  // the elapsed time of this compaction in microseconds.
  uint64_t elapsed_micros = 0;

  // the elapsed CPU time of this compaction in microseconds.
  uint64_t cpu_micros = 0;

  // True if `num_input_records` is accurate across all subcompactions.
  // See CompactionIterator::must_count_input_entries for some implementation
  // details why `num_input_records` may not be accurate.
  bool has_accurate_num_input_records = true;
  // the number of compaction input records.
  uint64_t num_input_records = 0;
  // the number of blobs read from blob files
  uint64_t num_blobs_read = 0;
  // the number of compaction input files (table files)
  size_t num_input_files = 0;
  // The number of input files that get trivially moved.
  size_t num_input_files_trivially_moved = 0;
  // the number of compaction input files at the output level (table files)
  size_t num_input_files_at_output_level = 0;
  // the number of compaction input files that are filtered out by compaction
  // optimizations
  size_t num_filtered_input_files = 0;
  // the number of compaction input files at the output level that are filtered
  // out by compaction optimizations
  size_t num_filtered_input_files_at_output_level = 0;

  // the number of compaction output records.
  uint64_t num_output_records = 0;
  // the number of compaction output files (table files)
  size_t num_output_files = 0;
  // the number of compaction output files (blob files)
  size_t num_output_files_blob = 0;

  // true if the compaction is a full compaction (all live SST files input)
  bool is_full_compaction = false;
  // true if the compaction is a manual compaction
  bool is_manual_compaction = false;
  // true if the compaction ran in a remote worker
  bool is_remote_compaction = false;

  // the total size of table files in the compaction input
  uint64_t total_input_bytes = 0;
  // the total size of blobs read from blob files
  uint64_t total_blob_bytes_read = 0;
  // the total size of table files in the compaction output
  uint64_t total_output_bytes = 0;
  // the total size of blob files in the compaction output
  uint64_t total_output_bytes_blob = 0;
  // the total size of table files for compaction input files that are skipped
  // because input files are filtered out by compaction optimizations.
  uint64_t total_skipped_input_bytes = 0;

  // number of records being replaced by newer record associated with same key.
  // this could be a new value or a deletion entry for that key so this field
  // sums up all updated and deleted keys
  uint64_t num_records_replaced = 0;

  // the sum of the uncompressed input keys in bytes.
  uint64_t total_input_raw_key_bytes = 0;
  // the sum of the uncompressed input values in bytes.
  uint64_t total_input_raw_value_bytes = 0;

  // the number of deletion entries before compaction. Deletion entries
  // can disappear after compaction because they expired
  uint64_t num_input_deletion_records = 0;
  // number of deletion records that were found obsolete and discarded
  // because it is not possible to delete any more keys with this entry
  // (i.e. all possible deletions resulting from it have been completed)
  uint64_t num_expired_deletion_records = 0;

  // number of corrupt keys (ParseInternalKey returned false when applied to
  // the key) encountered and written out.
  uint64_t num_corrupt_keys = 0;

  // Following counters are only populated if
  // options.report_bg_io_stats = true;

  // Time spent on file's Append() call.
  uint64_t file_write_nanos = 0;

  // Time spent on sync file range.
  uint64_t file_range_sync_nanos = 0;

  // Time spent on file fsync.
  uint64_t file_fsync_nanos = 0;

  // Time spent on preparing file write (fallocate, etc)
  uint64_t file_prepare_write_nanos = 0;

  // 0-terminated strings storing the first 8 bytes of the smallest and
  // largest key in the output.
  static const size_t kMaxPrefixLength = 8;

  std::string smallest_output_key_prefix;
  std::string largest_output_key_prefix;

  // number of single-deletes which do not meet a put
  uint64_t num_single_del_fallthru = 0;

  // number of single-deletes which meet something other than a put
  uint64_t num_single_del_mismatch = 0;

  // TODO: Add output_to_proximal_level output information
};
}  // namespace ROCKSDB_NAMESPACE
