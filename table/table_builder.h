//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdint.h>

#include <cassert>
#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/seqno_to_time_mapping.h"
#include "db/table_properties_collector.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "table/embedded_blob_sst.h"
#include "table/unique_id_impl.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class BlobSource;

struct TableReaderOptions {
  // @param skip_filters Disables loading/accessing the filter block
  TableReaderOptions(
      const ImmutableOptions& _ioptions,
      const std::shared_ptr<const SliceTransform>& _prefix_extractor,
      UnownedPtr<CompressionManager> _compression_manager,
      const EnvOptions& _env_options,
      const InternalKeyComparator& _internal_comparator,
      uint8_t _block_protection_bytes_per_key, bool _skip_filters = false,
      bool _immortal = false, bool _force_direct_prefetch = false,
      int _level = -1, BlockCacheTracer* const _block_cache_tracer = nullptr,
      size_t _max_file_size_for_l0_meta_pin = 0,
      const std::string& _cur_db_session_id = "", uint64_t _cur_file_num = 0,
      UniqueId64x2 _unique_id = {}, SequenceNumber _largest_seqno = 0,
      uint64_t _tail_size = 0, bool _user_defined_timestamps_persisted = true,
      bool _avoid_shared_metadata_cache = false,
      const std::string& _column_family_name = "")
      : ioptions(_ioptions),
        prefix_extractor(_prefix_extractor),
        compression_manager(_compression_manager),
        env_options(_env_options),
        internal_comparator(_internal_comparator),
        skip_filters(_skip_filters),
        immortal(_immortal),
        force_direct_prefetch(_force_direct_prefetch),
        level(_level),
        largest_seqno(_largest_seqno),
        block_cache_tracer(_block_cache_tracer),
        max_file_size_for_l0_meta_pin(_max_file_size_for_l0_meta_pin),
        cur_db_session_id(_cur_db_session_id),
        cur_file_num(_cur_file_num),
        unique_id(_unique_id),
        block_protection_bytes_per_key(_block_protection_bytes_per_key),
        tail_size(_tail_size),
        user_defined_timestamps_persisted(_user_defined_timestamps_persisted),
        avoid_shared_metadata_cache(_avoid_shared_metadata_cache),
        column_family_name(_column_family_name) {}

  const ImmutableOptions& ioptions;
  const std::shared_ptr<const SliceTransform>& prefix_extractor;
  // NOTE: the compression manager is not saved, just potentially a decompressor
  // from it, so we don't need a shared_ptr copy
  UnownedPtr<CompressionManager> compression_manager;
  const EnvOptions& env_options;
  const InternalKeyComparator& internal_comparator;
  // This is only used for BlockBasedTable (reader)
  bool skip_filters;
  // Whether the table will be valid as long as the DB is open
  bool immortal;
  // When data prefetching is needed, even if direct I/O is off, read data to
  // fetch into RocksDB's buffer, rather than relying
  // RandomAccessFile::Prefetch().
  bool force_direct_prefetch;
  // What level this table/file is on, -1 for "not set, don't know." Used
  // for level-specific statistics.
  int level;
  // Largest seqno in the table. kMaxSequenceNumber means unknown;
  // kDisableGlobalSequenceNumber prevents file-wide seqno replacement.
  SequenceNumber largest_seqno;
  BlockCacheTracer* const block_cache_tracer;
  // Largest L0 file size whose meta-blocks may be pinned (can be zero when
  // unknown).
  const size_t max_file_size_for_l0_meta_pin;

  std::string cur_db_session_id;

  uint64_t cur_file_num;

  // Known unique_id or {}, kNullUniqueId64x2 means unknown
  UniqueId64x2 unique_id;

  uint8_t block_protection_bytes_per_key;

  uint64_t tail_size;

  // Whether the key in the table contains user-defined timestamps.
  bool user_defined_timestamps_persisted;

  // Open-time metadata reads should not insert index/filter/dictionary blocks
  // into the shared block cache.
  bool avoid_shared_metadata_cache;

  // Column family that owns this table. Empty outside DB-owned table opens.
  std::string column_family_name;

  // Blob source for routing same-file ("embedded") blob reads through the blob
  // value cache + BLOB_DB_* statistics. Owned by the ColumnFamilyData; the
  // table reader's lifetime is a subset of the CFD's (it is owned via the CFD's
  // TableCache), so a raw pointer is lifetime-safe. nullptr for non-DB openers
  // (SstFileReader, sst_dump, repair, external-file ingestion prevalidation,
  // etc.), in which case embedded reads fall back to a direct (uncached) read.
  BlobSource* blob_source = nullptr;
};

struct TableBuilderOptions : public TablePropertiesCollectorFactory::Context {
  TableBuilderOptions(
      const ImmutableOptions& _ioptions, const MutableCFOptions& _moptions,
      const ReadOptions& _read_options, const WriteOptions& _write_options,
      const InternalKeyComparator& _internal_comparator,
      const InternalTblPropCollFactories* _internal_tbl_prop_coll_factories,
      CompressionType _compression_type,
      const CompressionOptions& _compression_opts, uint32_t _column_family_id,
      const std::string& _column_family_name, int _level,
      const int64_t _newest_key_time, bool _is_bottommost = false,
      TableFileCreationReason _reason = TableFileCreationReason::kMisc,
      const int64_t _oldest_key_time = 0,
      const uint64_t _file_creation_time = 0, const std::string& _db_id = "",
      const std::string& _db_session_id = "",
      const uint64_t _target_file_size = 0, const uint64_t _cur_file_num = 0,
      const SequenceNumber _last_level_inclusive_max_seqno_threshold =
          kMaxSequenceNumber,
      const Slice _db_name = Slice(), const bool _is_remote_compaction = false,
      const EmbeddedBlobSstBuilderOptions* _embedded_blob_options = nullptr)
      : TablePropertiesCollectorFactory::Context(
            _column_family_id, _level, _ioptions.num_levels,
            _last_level_inclusive_max_seqno_threshold),
        ioptions(_ioptions),
        moptions(_moptions),
        read_options(_read_options),
        write_options(_write_options),
        internal_comparator(_internal_comparator),
        internal_tbl_prop_coll_factories(_internal_tbl_prop_coll_factories),
        compression_type(_compression_type),
        compression_opts(_compression_opts),
        column_family_name(_column_family_name),
        oldest_key_time(_oldest_key_time),
        newest_key_time(_newest_key_time),
        target_file_size(_target_file_size),
        file_creation_time(_file_creation_time),
        db_id(_db_id),
        db_session_id(_db_session_id),
        is_bottommost(_is_bottommost),
        reason(_reason),
        db_name(_db_name),
        is_remote_compaction(_is_remote_compaction),
        cur_file_num(_cur_file_num),
        embedded_blob_options(_embedded_blob_options) {}

  const ImmutableOptions& ioptions;
  const MutableCFOptions& moptions;
  const ReadOptions& read_options;
  const WriteOptions& write_options;
  const InternalKeyComparator& internal_comparator;
  const InternalTblPropCollFactories* internal_tbl_prop_coll_factories;
  const CompressionType compression_type;
  const CompressionOptions& compression_opts;
  const std::string& column_family_name;
  const int64_t oldest_key_time;
  const int64_t newest_key_time;
  const uint64_t target_file_size;
  const uint64_t file_creation_time;
  const std::string db_id;
  const std::string db_session_id;
  // BEGIN for FilterBuildingContext
  const bool is_bottommost;
  const TableFileCreationReason reason;
  const Slice db_name;
  const bool is_remote_compaction;
  // END for FilterBuildingContext

  const uint64_t cur_file_num;
  // Non-null only for table builders that should write eligible large values as
  // same-file ("embedded") blob records. Currently only honored by
  // BlockBasedTableBuilder.
  const EmbeddedBlobSstBuilderOptions* embedded_blob_options;

  // Reject unresolved duplicate internal keys before they reach the builder.
  bool require_strict_key_order = false;
};

enum class TableBuilderAddResult : uint8_t {
  // The row was consumed by this builder. The caller may advance input.
  kAdded,
  // The row was not consumed. The caller should finish the current table,
  // create another table through the same TableFactory, and retry the same row.
  kRequiresNewTable,
  // A real error occurred. The caller should abandon the current output.
  kError,
};

enum class TableBuilderNewTableReason : uint8_t {
  kNone = 0,
  kRowClassificationChanged,
  kSchemaIdChanged,
  kSchemaVersionIncompatible,
  kUnsupportedEntryType,
  kBuilderPolicy,
};

inline const char* TableBuilderNewTableReasonToString(
    TableBuilderNewTableReason reason) {
  switch (reason) {
    case TableBuilderNewTableReason::kNone:
      return "None";
    case TableBuilderNewTableReason::kRowClassificationChanged:
      return "RowClassificationChanged";
    case TableBuilderNewTableReason::kSchemaIdChanged:
      return "SchemaIdChanged";
    case TableBuilderNewTableReason::kSchemaVersionIncompatible:
      return "SchemaVersionIncompatible";
    case TableBuilderNewTableReason::kUnsupportedEntryType:
      return "UnsupportedEntryType";
    case TableBuilderNewTableReason::kBuilderPolicy:
      return "BuilderPolicy";
  }
  assert(false);
  return "Invalid";
}

struct TableBuilderAddContext {
  TableBuilderNewTableReason new_table_reason =
      TableBuilderNewTableReason::kNone;
};

// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.
class TableBuilder {
 public:
  // REQUIRES: Either Finish() or Abandon() has been called.
  virtual ~TableBuilder() = default;

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Add key,value to the table being constructed and report whether the row was
  // accepted or whether the caller should cut the current table and retry this
  // same row with a new table builder. A kRequiresNewTable result is not an
  // error: the row must not have been consumed and status() must remain OK.
  // A kError result must be reported through status().
  //
  // Existing table builders do not request table cuts; they inherit this
  // default adapter around Add().
  //
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  // REQUIRES: context != nullptr
  virtual TableBuilderAddResult TryAdd(const Slice& key, const Slice& value,
                                       TableBuilderAddContext* context) {
    assert(context != nullptr);
    context->new_table_reason = TableBuilderNewTableReason::kNone;
    Add(key, value);
    return TableBuilderAddResult::kAdded;
  }

  // Return non-ok iff some error has been detected.
  virtual Status status() const = 0;

  // Return non-ok iff some error happens during IO.
  virtual IOStatus io_status() const = 0;

  // Finish building the table.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual Status Finish() = 0;

  // Indicate that the contents of this builder should be abandoned.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Abandon() = 0;

  // Number of calls to Add() so far.
  virtual uint64_t NumEntries() const = 0;

  // Whether the output file is completely empty. It has neither entries
  // or tombstones.
  virtual bool IsEmpty() const {
    return NumEntries() == 0 && GetTableProperties().num_range_deletions == 0;
  }

  // Size of the file before its content is compressed.
  virtual uint64_t PreCompressionSize() const { return 0; }

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  virtual uint64_t FileSize() const = 0;

  // Estimated size of the file generated so far. This is used when
  // FileSize() cannot estimate final SST size, e.g. parallel compression
  // is enabled.
  virtual uint64_t EstimatedFileSize() const { return FileSize(); }

  // Estimated tail size of the SST file generated so far. The "tail" refers to
  // all blocks written after data blocks (index + filter). This value helps
  // estimate the total file size when deciding when to cut files.
  virtual uint64_t EstimatedTailSize() const { return 0; }

  virtual uint64_t GetTailSize() const { return 0; }

  // If the user defined table properties collector suggest the file to
  // be further compacted.
  virtual bool NeedCompact() const { return false; }

  // Returns table properties
  virtual TableProperties GetTableProperties() const = 0;

  // Return file checksum
  virtual std::string GetFileChecksum() const = 0;

  // Return file checksum function name
  virtual const char* GetFileChecksumFuncName() const = 0;

  // Set the sequence number to time mapping. `relevant_mapping` must be in
  // enforced state (ready to encode to string).
  virtual void SetSeqnoTimeTableProperties(
      const SeqnoToTimeMapping& /*relevant_mapping*/,
      uint64_t /*oldest_ancestor_time*/) {}

  // If this builder used CPU work from threads other than the caller, return
  // the CPU microseconds used. 0 = no work outside calling thread, or not
  // supported.
  virtual uint64_t GetWorkerCPUMicros() const { return 0; }
};

}  // namespace ROCKSDB_NAMESPACE
