//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class DB;

// The functions in this file copy, clip, and ingest SST files directly, so they
// support only a subset of DBs and column families. Every DB passed in must be
// the DB returned by DB::Open() itself, not a StackableDB such as
// TransactionDB, OptimisticTransactionDB, DBWithTTL, or BlobDB, nor the DB
// returned by such a wrapper's GetRootDB(). Every DB must not have allow_2pc,
// allow_ingest_behind, or row_cache set, and must keep all of its files in its
// own directory, with at most one db_paths entry. Every selected column family,
// and for SplitDB() the default column family, must use BlockBasedTable and
// leveled or universal compaction, and must not use user-defined timestamps,
// integrated blob files, cf_allow_ingest_behind, disallow_memtable_writes, more
// than one cf_paths entry or one outside the DB directory, or the compaction
// filter of a DBWithTTL or BlobDB. MergeDB() also requires
// preserve_internal_time_seconds and preclude_last_level_data_seconds to be
// zero on both sides, and min_tombstones_for_range_conversion to be zero on
// selected destination column families. These configurations return
// NotSupported before any file is created. Read-only and secondary instances
// and the root DB of an OptimisticTransactionDB cannot be detected and must not
// be passed in. The options above must not be changed with SetOptions() during
// a call.
//
// SplitDB() and MergeDB() create a checkpoint of source, which flushes all of
// its column families. Any compaction filter that source applies to flushes
// therefore runs as it would on any other flush.

// A range to extract from one column family during SplitDB(). The range is
// half-open: begin_key is included and end_key is excluded.
struct ColumnFamilySplit {
  std::string begin_key;
  std::string end_key;
  ColumnFamilyHandle* source_cf = nullptr;
};

struct SplitDBOptions {
  // Must name a path that does not exist. On success it contains the selected
  // column families plus the default column family, which RocksDB requires in
  // every database and which is empty unless selected. Unselected column
  // families are not copied.
  // The split DB is built in "<destination_db_path>.staging", using
  // "<destination_db_path>.staging.tmp" while creating its checkpoint, and is
  // renamed to destination_db_path only once it is complete. Neither staging
  // path may exist. The caller must not create any of these paths during the
  // call.
  std::string destination_db_path;

  // Each source column family may appear at most once.
  std::vector<ColumnFamilySplit> column_family_splits;

  // Verifies the checksums of the destination DB after clipping. Ignored by
  // DeleteSplitRanges().
  bool verify_checksums = true;
};

// A range to copy from one source column family to one existing destination
// column family during MergeDB(). The range is half-open.
struct ColumnFamilyMerge {
  std::string begin_key;
  std::string end_key;
  ColumnFamilyHandle* source_cf = nullptr;
  ColumnFamilyHandle* destination_cf = nullptr;
};

struct MergeDBOptions {
  // Temporary directory used for a checkpoint of the source DB. It must not
  // exist. It is removed before returning, best-effort after an error. If
  // removal fails after a successful merge, the call still succeeds and logs a
  // warning to the destination's info log, and later calls with this path fail
  // until it is removed. Placing it on the same device as the destination DB
  // enables hard links during ingestion when the file system supports them.
  // "<checkpoint_directory>.tmp" is used for staging, so it must not exist
  // either. The caller must not create either path during the call.
  std::string checkpoint_directory;

  // Source and destination column families must each be unique across this
  // vector. Every destination column family must already exist.
  std::vector<ColumnFamilyMerge> column_family_merges;

  // Verifies the checksums of the clipped files during ingestion.
  bool verify_checksums = true;
};

struct SplitMergeResult {
  uint64_t checkpoint_sequence = 0;
  uint64_t transferred_files = 0;
  uint64_t transferred_bytes = 0;
};

// Creates a checkpoint-derived DB containing only the requested ranges in the
// selected column families. The ranges stay in source. Once readers and
// writers of the selected ranges have moved to the destination DB, call
// DeleteSplitRanges() with the same options to remove the ranges from source.
// The caller must prevent writes to the selected ranges from the start of this
// call until DeleteSplitRanges() returns. Clipping fully compacts each selected
// column family in the destination, so the data kept in the selected ranges is
// read and rewritten once. The destination OPTIONS file records the source
// options, but the destination is not opened with them: the caller's first
// open of the destination validates it with the options the caller chooses.
Status SplitDB(DB* source, const SplitDBOptions& options,
               SplitMergeResult* result);

// Atomically deletes the ranges in options.column_family_splits from source,
// then flushes and compacts them to reclaim space. options.destination_db_path
// and options.verify_checksums are ignored. On a non-OK status the ranges may
// already be deleted; calling again is safe. The compaction runs source's
// compaction filters as any manual compaction does; with universal compaction
// it covers the whole column family, not only the ranges.
Status DeleteSplitRanges(DB* source, const SplitDBOptions& options);

// Copies the requested ranges from source into existing destination column
// families. Source and destination must use the same FileSystem object, because
// the checkpoint is created through source's and ingested through
// destination's. The destination changes are recorded in one atomic MANIFEST
// update, so after a crash either all or none of them are present. Concurrent
// readers can see some destination column families updated before others until
// this function returns. The caller must prevent writes to all source and
// destination ranges until this function returns. Each destination range must
// be empty, or the call returns InvalidArgument. Only the latest contents of
// the ranges are copied: older versions and the original sequence numbers are
// not preserved. Merged keys are not assigned new sequence numbers, so they
// would become visible in existing destination snapshots. The call fails if
// destination has live snapshots at the start, and the caller must not create
// destination snapshots until this function returns. Any destination iterator
// created while min_tombstones_for_range_conversion was nonzero must be closed
// before this call. The ranges are clipped by fully compacting each selected
// column family in a checkpoint of source, so the merged data is read and
// rewritten once before ingestion.
Status MergeDB(DB* source, DB* destination, const MergeDBOptions& options,
               SplitMergeResult* result);

}  // namespace ROCKSDB_NAMESPACE
