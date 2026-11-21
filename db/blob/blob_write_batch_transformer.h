//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class BlobFilePartitionManager;
class BlobFileReader;
class BlobIndex;
class Cache;
class ColumnFamilyData;
class ColumnFamilySet;

// Callback to look up per-CF blob settings.
struct BlobDirectWriteSettings {
  // Whether this column family should attempt direct-write blob separation.
  bool enable_blob_direct_write = false;
  // Minimum value size eligible for direct-write blob separation.
  uint64_t min_blob_size = 0;
  // Compression to use for newly written blob records.
  CompressionType compression_type = kNoCompression;
  // Compression options for newly written blob records.
  CompressionOptions compression_opts;
  // Raw pointer -- the Cache is owned by ColumnFamilyOptions and outlives all
  // settings snapshots. Using raw avoids 2 atomic ref-count ops per Put().
  Cache* blob_cache = nullptr;
  // Blob-cache prepopulation policy for direct-write records.
  PrepopulateBlobCache prepopulate_blob_cache = PrepopulateBlobCache::kDisable;
};

using BlobDirectWriteSettingsProvider =
    std::function<BlobDirectWriteSettings(uint32_t column_family_id)>;

// Callback to look up per-CF partition manager.
using BlobPartitionManagerProvider =
    std::function<BlobFilePartitionManager*(uint32_t column_family_id)>;

// Transforms a WriteBatch by writing large values directly to blob files
// and replacing them with BlobIndex entries. Non-qualifying entries
// (small values, deletes, merges, etc.) are passed through unchanged.
class BlobWriteBatchTransformer : public WriteBatch::Handler {
 public:
  struct RollbackInfo {
    // Manager that owns the file updated during transform.
    BlobFilePartitionManager* partition_mgr = nullptr;
    // Blob file number written during transform.
    uint64_t file_number = 0;
    // Number of blob records appended to that file.
    uint64_t count = 0;
    // Number of bytes appended to that file.
    uint64_t bytes = 0;
  };

  // Builds the final serialized entity for one PutEntity write.
  //
  // `columns` must already be sorted by column name. When direct-write blob
  // separation is enabled and one or more columns meet `min_blob_size`, this
  // writes those columns to blob files and returns a V2 entity containing
  // BlobIndex references. Otherwise, if `serialize_inline_entity` is true, it
  // returns a normal V1 entity with all columns inline.
  //
  // `rollback_infos`, when provided, is appended with the exact blob writes
  // performed before the method returns, even if it later fails, so callers
  // can account those bytes as initial garbage.
  static Status MaybePreprocessWideColumns(
      const WriteOptions& write_options, uint32_t column_family_id,
      const Slice& key, const WideColumns& columns,
      BlobFilePartitionManager* partition_mgr,
      const BlobDirectWriteSettings& settings, bool serialize_inline_entity,
      std::string* entity, bool* transformed,
      std::vector<RollbackInfo>* rollback_infos = nullptr);

  // Constructs a per-batch transformer. Most callers should use
  // TransformBatch() instead of driving the handler directly.
  BlobWriteBatchTransformer(
      const BlobPartitionManagerProvider& partition_mgr_provider,
      WriteBatch* output_batch,
      const BlobDirectWriteSettingsProvider& settings_provider,
      const WriteOptions& write_options);

  // Transform a WriteBatch. If no values qualify for blob separation,
  // output_batch will be empty and the caller should use the original batch.
  // If any values are separated, output_batch contains the full transformed
  // batch. used_managers (if non-null) receives the set of partition managers
  // that had data written to them, so the caller can flush/sync them.
  // rollback_infos (if non-null) receives the exact file/count/byte writes so
  // a failed transformed attempt can be accounted as initial garbage. Partial
  // results are returned even if this method fails after some blob writes.
  static Status TransformBatch(
      const WriteOptions& write_options, WriteBatch* input_batch,
      WriteBatch* output_batch,
      const BlobPartitionManagerProvider& partition_mgr_provider,
      const BlobDirectWriteSettingsProvider& settings_provider,
      bool* transformed,
      std::vector<BlobFilePartitionManager*>* used_managers = nullptr,
      std::vector<RollbackInfo>* rollback_infos = nullptr);

  // WriteBatch::Handler overrides
  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override;

  Status TimedPutCF(uint32_t column_family_id, const Slice& key,
                    const Slice& value, uint64_t write_time) override;

  Status PutEntityCF(uint32_t column_family_id, const Slice& key,
                     const Slice& entity) override;

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override;

  Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) override;

  Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                       const Slice& end_key) override;

  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override;

  Status PutBlobIndexCF(uint32_t column_family_id, const Slice& key,
                        const Slice& value) override;

  void LogData(const Slice& blob) override;

  Status MarkBeginPrepare(bool unprepared = false) override;
  Status MarkEndPrepare(const Slice& xid) override;
  Status MarkCommit(const Slice& xid) override;
  Status MarkCommitWithTimestamp(const Slice& xid, const Slice& ts) override;
  Status MarkRollback(const Slice& xid) override;
  Status MarkNoop(bool empty_batch) override;

  // Returns true once at least one value has been rewritten to a BlobIndex.
  bool HasTransformed() const { return has_transformed_; }

 private:
  // Callback to look up the partition manager for a given column family ID.
  BlobPartitionManagerProvider partition_mgr_provider_;
  // Output batch that receives transformed entries (BlobIndex for qualifying
  // values, passthrough for everything else).
  WriteBatch* output_batch_;
  // Callback to look up blob direct write settings for a given CF ID.
  BlobDirectWriteSettingsProvider settings_provider_;
  // Write options from the caller, forwarded to WriteBlob calls.
  const WriteOptions& write_options_;
  // True once at least one value has been separated into a blob file.
  bool has_transformed_ = false;
  // Reusable buffer for encoding BlobIndex entries (avoids per-Put alloc).
  std::string blob_index_buf_;
  // Per-batch cache of the last CF's settings and manager, avoiding
  // redundant provider lookups when consecutive entries share the same CF.
  uint32_t cached_cf_id_ = UINT32_MAX;
  // Cached settings for `cached_cf_id_`.
  BlobDirectWriteSettings cached_settings_;
  // Cached partition manager for `cached_cf_id_`.
  BlobFilePartitionManager* cached_partition_mgr_ = nullptr;
  // Set of partition managers that received data during this batch,
  // returned to the caller so it can flush/sync them.
  UnorderedSet<BlobFilePartitionManager*> used_managers_;
  // Exact blob writes performed during this batch. We only aggregate these
  // entries if rollback is needed so the normal path keeps minimal overhead.
  std::vector<RollbackInfo> rollback_infos_;
};

// Rewrites WAL records during recovery so that they no longer point into blob
// files that are not in the MANIFEST.
//
// Background: blob direct write appends each large value to an open blob file
// first. The WAL record and the memtable then hold only a BlobIndex that
// points to the value. An open blob file is added to the MANIFEST only when
// the memtable that refers to it is flushed. After a crash, WAL replay can
// therefore meet a BlobIndex whose file is on disk but in no column family's
// MANIFEST state. This class calls such a file an "unregistered blob file".
// The file may have no footer, and its tail can be torn. DBImpl deletes the
// file later (see DBImpl::wal_replay_blob_files_).
//
// Replay: before DBImpl passes a WAL record to the WAL filter and the
// memtable, Resolve() reads every value that the record keeps in an
// unregistered blob file. The blob reader checks the record key and checksum.
// Resolve() then builds a copy of the record where each such value is raw
// again: a PutBlobIndex becomes a Put, and a wide-column entity with blob
// columns becomes an entity with all columns inline. The copy keeps the
// sequence number and the entry order. A later flush writes the raw values to
// new blob files in the usual way. References to blob files that are in the
// MANIFEST stay as they are.
//
// Errors: if a value cannot be read because its file is missing, too short,
// or fails the checksum, Resolve() returns Corruption and builds no copy. The
// caller handles this record like a corrupted WAL record, so
// `wal_recovery_mode` decides whether recovery stops, fails, or skips it.
// One exception: if the column family has enable_blob_files off, a BlobIndex
// to a missing file is kept unchanged. A record with a 2PC prepare section
// that needs a rewrite returns NotSupported, which fails the open. Blob direct
// write never puts a BlobIndex into a 2PC prepare section, so this error is
// only a safety check. Other errors, such as I/O errors, are returned as they
// are.
//
// Threading: recovery is single threaded and holds the DB mutex, so this
// class has no locking.
class BlobDirectWriteRecoveryResolver {
 public:
  // `column_families` gives, for each column family, the blob files in its
  // current Version, its log number (WAL files older than it are already
  // flushed for this column family), and the directory of its blob files. It
  // must outlive this object.
  // `unregistered_blob_files` lists the blob file numbers that are on disk but
  // in no column family's MANIFEST state.
  BlobDirectWriteRecoveryResolver(
      const ColumnFamilySet* column_families, const FileOptions& file_options,
      UnorderedSet<uint64_t> unregistered_blob_files);
  ~BlobDirectWriteRecoveryResolver();

  BlobDirectWriteRecoveryResolver(const BlobDirectWriteRecoveryResolver&) =
      delete;
  BlobDirectWriteRecoveryResolver& operator=(
      const BlobDirectWriteRecoveryResolver&) = delete;

  // If `batch`, which was read from WAL `wal_number`, has a value in an
  // unregistered blob file, sets `*resolved_batch` to the rewritten copy.
  // Otherwise leaves `*resolved_batch` empty, and the caller uses `batch`.
  // Entries that the memtable insert skips (missing or dropped column family,
  // or a column family whose log number is newer than `wal_number`) are copied
  // unchanged, because their blob files can be gone already.
  Status Resolve(uint64_t wal_number, const WriteBatch& batch,
                 std::unique_ptr<WriteBatch>* resolved_batch);

  // Maps each unregistered blob file that Resolve() read from to the largest
  // WAL number whose records refer to it. The caller keeps these files until
  // those WALs are no longer replayed.
  const UnorderedMap<uint64_t, uint64_t>& referenced_files() const {
    return referenced_files_;
  }

  // Number of values that Resolve() made raw again.
  uint64_t resolved_value_count() const { return resolved_value_count_; }

 private:
  // Resolve() makes two passes over a batch. Scanner only checks if any value
  // needs a rewrite, so most batches are not copied. Rewriter then builds the
  // copy.
  class Scanner;
  class Rewriter;

  // Returns the column family whose memtable receives entries for
  // `column_family_id` from WAL `wal_number`, or null if the insert skips
  // them.
  ColumnFamilyData* GetReplayedColumnFamily(uint32_t column_family_id,
                                            uint64_t wal_number) const;

  // Sets `*needs_resolve` if `blob_index` points into an unregistered blob
  // file. Returns Corruption if the file is neither in the MANIFEST nor on
  // disk and the column family has enable_blob_files on. If enable_blob_files
  // is off, the BlobIndex is kept unchanged (see the comment in
  // CheckBlobIndex()).
  Status CheckBlobIndex(ColumnFamilyData* cfd, const BlobIndex& blob_index,
                        bool* needs_resolve);

  // Sets `*reader` to the open reader of unregistered blob file
  // `file_number`, and opens one if needed. Returns Corruption if the file is
  // missing.
  Status GetReader(ColumnFamilyData* cfd, uint64_t wal_number,
                   uint64_t file_number, BlobFileReader** reader);

  // Reads one value from an unregistered blob file into `*value`.
  Status ReadValue(ColumnFamilyData* cfd, uint64_t wal_number,
                   const Slice& user_key, const BlobIndex& blob_index,
                   std::string* value);

  const ColumnFamilySet* const column_families_;
  const FileOptions file_options_;
  // Blob files on disk that are in no column family's MANIFEST state.
  UnorderedSet<uint64_t> unregistered_blob_files_;
  // File numbers that CheckBlobIndex() found neither in the MANIFEST nor on
  // disk, so that it checks the disk only once for each.
  UnorderedSet<uint64_t> missing_blob_files_;
  // The most readers that stay open at the same time. 64 is a choice, not a
  // measured value: replay reads from few files at a time (about one per
  // partition and column family), and 64 open files stay far below common
  // open-file limits.
  static constexpr size_t kMaxOpenReaders = 64;
  // Open readers of unregistered blob files, at most kMaxOpenReaders.
  UnorderedMap<uint64_t, std::unique_ptr<BlobFileReader>> readers_;
  // See referenced_files().
  UnorderedMap<uint64_t, uint64_t> referenced_files_;
  // See resolved_value_count().
  uint64_t resolved_value_count_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
