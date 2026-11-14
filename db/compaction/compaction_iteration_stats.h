//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct CompactionIterationStats {
  // Compaction statistics

  // Doesn't include records skipped because of
  // CompactionFilter::Decision::kRemoveAndSkipUntil.
  int64_t num_record_drop_user = 0;

  int64_t num_record_drop_hidden = 0;
  int64_t num_record_drop_obsolete = 0;
  int64_t num_record_drop_range_del = 0;
  int64_t num_range_del_drop_obsolete = 0;
  // Deletions obsoleted before bottom level due to file gap optimization.
  int64_t num_optimized_del_drop_obsolete = 0;
  uint64_t total_filter_time = 0;

  // Input statistics
  // TODO(noetzli): The stats are incomplete. They are lacking everything
  // consumed by MergeHelper.
  uint64_t num_input_records = 0;
  uint64_t num_input_deletion_records = 0;
  uint64_t num_input_corrupt_records = 0;
  uint64_t total_input_raw_key_bytes = 0;
  uint64_t total_input_raw_value_bytes = 0;

  // Single-Delete diagnostics for exceptional situations
  uint64_t num_single_del_fallthru = 0;
  uint64_t num_single_del_mismatch = 0;

  // Blob related statistics
  uint64_t num_blobs_read = 0;
  uint64_t total_blob_bytes_read = 0;
  uint64_t num_blobs_relocated = 0;
  uint64_t total_blob_bytes_relocated = 0;
  // The total size of the values (or wide-column entities) whose large parts
  // were extracted into blob files, measured before and after the extraction.
  // Only the post-extraction form, which holds blob references in place of the
  // extracted parts, is written to the table file. The difference between the
  // two is therefore the amount by which the raw value size of the table file
  // understates the amount of input it stands for.
  uint64_t total_value_bytes_before_blob_extraction = 0;
  uint64_t total_value_bytes_after_blob_extraction = 0;

  // TimedPut diagnostics
  // Total number of kTypeValuePreferredSeqno records encountered.
  uint64_t num_input_timed_put_records = 0;
  // Number of kTypeValuePreferredSeqno records we ended up swapping in
  // preferred seqno.
  uint64_t num_timed_put_swap_preferred_seqno = 0;
};

}  // namespace ROCKSDB_NAMESPACE
