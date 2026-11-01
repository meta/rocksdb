//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "db/column_family.h"
#include "db/internal_stats.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "env/file_system_tracer.h"
#include "logging/event_logger.h"
#include "options/db_options.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/sst_file_writer.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Directories;
class SystemClock;

struct KeyRangeInfo {
  // Smallest internal key in an external file or for a batch of external files.
  // unset() could be either invalid or "before all keys"
  InternalKey smallest_internal_key;
  // Largest internal key in an external file or for a batch of external files.
  // unset() could be either invalid or "after all keys"
  InternalKey largest_internal_key;

  bool unset() const {
    // Legal internal keys are at least 8 bytes.
    return smallest_internal_key.unset() || largest_internal_key.unset();
  }
};

// Helper class to apply SST file key range checks to the external files.
// XXX: using sstableKeyCompare with user comparator on internal keys is
// very broken
class ExternalFileRangeChecker {
 public:
  explicit ExternalFileRangeChecker(const Comparator* ucmp) : ucmp_(ucmp) {}

  // Operator used for sorting ranges.
  bool operator()(const KeyRangeInfo* range1,
                  const KeyRangeInfo* range2) const {
    assert(range1);
    assert(range2);
    assert(!range1->unset());
    assert(!range2->unset());
    return sstableKeyCompare(ucmp_, range1->smallest_internal_key,
                             range2->smallest_internal_key) < 0;
  }

  bool Overlaps(const KeyRangeInfo& range1, const KeyRangeInfo& range2,
                bool known_sorted = false) const {
    return Overlaps(range1, range2.smallest_internal_key,
                    range2.largest_internal_key, known_sorted);
  }
  bool Overlaps(const KeyRangeInfo& range1, const InternalKey& range2_smallest,
                const InternalKey& range2_largest,
                bool known_sorted = false) const {
    bool any_unset =
        range1.unset() || range2_smallest.unset() || range2_largest.unset();
    if (any_unset) {
      assert(!any_unset);
      return false;
    }
    if (known_sorted) {
      return sstableKeyCompare(ucmp_, range1.largest_internal_key,
                               range2_smallest) >= 0;
    }

    return sstableKeyCompare(ucmp_, range1.largest_internal_key,
                             range2_smallest) >= 0 &&
           sstableKeyCompare(ucmp_, range1.smallest_internal_key,
                             range2_largest) <= 0;
  }

  bool Contains(const KeyRangeInfo& range1, const KeyRangeInfo& range2) const {
    return Contains(range1, range2.smallest_internal_key,
                    range2.largest_internal_key);
  }
  bool Contains(const KeyRangeInfo& range1, const InternalKey& range2_smallest,
                const InternalKey& range2_largest) const {
    bool any_unset =
        range1.unset() || range2_smallest.unset() || range2_largest.unset();
    if (any_unset) {
      assert(!any_unset);
      return false;
    }
    return sstableKeyCompare(ucmp_, range1.smallest_internal_key,
                             range2_smallest) <= 0 &&
           sstableKeyCompare(ucmp_, range1.largest_internal_key,
                             range2_largest) >= 0;
  }

  void MaybeUpdateRange(const InternalKey& start_key,
                        const InternalKey& end_key, KeyRangeInfo* range) const {
    assert(range);
    if (range->smallest_internal_key.size() == 0 ||
        sstableKeyCompare(ucmp_, start_key, range->smallest_internal_key) < 0) {
      range->smallest_internal_key = start_key;
    }
    if (range->largest_internal_key.size() == 0 ||
        sstableKeyCompare(ucmp_, end_key, range->largest_internal_key) > 0) {
      range->largest_internal_key = end_key;
    }
  }

 private:
  const Comparator* ucmp_;
};

struct IngestedFileInfo : public KeyRangeInfo {
  // External file path
  std::string external_file_path;
  // NOTE: use below two fields for all `*Overlap*` types of checks instead of
  // smallest_internal_key.user_key() and largest_internal_key.user_key().
  // The smallest / largest user key contained in the file for key range checks.
  // These could be different from smallest_internal_key.user_key(), and
  // largest_internal_key.user_key() when user-defined timestamps are enabled,
  // because the check is about making sure the user key without timestamps part
  // does not overlap. To achieve that, the smallest user key will be updated
  // with the maximum timestamp while the largest user key will be updated with
  // the min timestamp. It's otherwise the same.
  std::string start_ukey;
  std::string limit_ukey;
  // Sequence number for keys in external file
  SequenceNumber original_seqno;
  // External file size
  uint64_t file_size;
  // total number of keys in external file
  uint64_t num_entries;
  // total number of range deletions in external file
  uint64_t num_range_deletions;
  // Id of column family this file should be ingested into
  uint32_t cf_id;
  // TableProperties read from external file
  TableProperties table_properties;
  // Version of external file
  int version;

  // FileDescriptor for the file inside the DB
  FileDescriptor fd;
  // file path that we picked for file inside the DB
  std::string internal_file_path;
  // Global sequence number that we picked for the file inside the DB
  SequenceNumber assigned_seqno = 0;
  // Level inside the DB we picked for the external file.
  int picked_level = 0;
  // Whether to copy or link the external sst file. copy_file will be set to
  // false if ingestion_options.move_files is true and underlying FS
  // supports link operation. Need to provide a default value to make the
  // undefined-behavior sanity check of llvm happy. Since
  // ingestion_options.move_files is false by default, thus copy_file is true
  // by default.
  bool copy_file = true;
  // The checksum of ingested file
  std::string file_checksum;
  // The name of checksum function that generate the checksum
  std::string file_checksum_func_name;
  // The temperature of the file to be ingested
  Temperature file_temperature = Temperature::kUnknown;
  // Unique id of the file to be ingested
  UniqueId64x2 unique_id{};
  // Whether the external file should be treated as if it has user-defined
  // timestamps or not. If this flag is false, and the column family enables
  // UDT feature, the file will have min-timestamp artificially padded to its
  // user keys when it's read. Since it will affect how `TableReader` reads a
  // table file, it's defaulted to optimize for the majority of the case where
  // the user key's format in the external file matches the column family's
  // setting.
  bool user_defined_timestamps_persisted = true;

  // Whether Lmax commit-time table opening should prefetch index/filter blocks.
  bool prefetch_lmax_index_and_filter_blocks = true;

  // Whether RocksDB generated this file as part of the ingestion job. Such a
  // file is already in the DB directory and must not be unlinked as a caller
  // input when move_files is enabled.
  bool generated_for_ingestion = false;

  SequenceNumber largest_seqno = kMaxSequenceNumber;
  SequenceNumber smallest_seqno = kMaxSequenceNumber;
};

// A batch of files.
struct FileBatchInfo : public KeyRangeInfo {
  autovector<IngestedFileInfo*> files;
  // When true, `smallest_internal_key` and `largest_internal_key` will be
  // tracked and updated as new file get added via `AddFile`. When false, we
  // bypass this tracking. This is used when the all input external files
  // are already checked and not overlapping, and they just need to be added
  // into one default batch.
  bool track_batch_range;

  void AddFile(IngestedFileInfo* file,
               const ExternalFileRangeChecker& key_range_checker) {
    assert(file);
    files.push_back(file);
    if (track_batch_range) {
      key_range_checker.MaybeUpdateRange(file->smallest_internal_key,
                                         file->largest_internal_key, this);
    }
  }

  explicit FileBatchInfo(bool _track_batch_range)
      : track_batch_range(_track_batch_range) {}
};

class ExternalSstFileIngestionJob {
 public:
  ExternalSstFileIngestionJob(
      VersionSet* versions, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options,
      const MutableDBOptions& mutable_db_options, const EnvOptions& env_options,
      const ShardedSnapshotList* db_snapshots,
      const IngestExternalFileOptions& ingestion_options,
      Directories* directories, EventLogger* event_logger,
      const std::shared_ptr<IOTracer>& io_tracer)
      : clock_(db_options.clock),
        fs_(db_options.fs, io_tracer),
        versions_(versions),
        cfd_(cfd),
        ucmp_(cfd ? cfd->user_comparator() : nullptr),
        file_range_checker_(ucmp_),
        db_options_(db_options),
        mutable_db_options_(mutable_db_options),
        env_options_(env_options),
        db_snapshots_(db_snapshots),
        ingestion_options_(ingestion_options),
        directories_(directories),
        event_logger_(event_logger),
        job_start_time_(clock_->NowMicros()),
        max_assigned_seqno_(0),
        io_tracer_(io_tracer) {
    assert(directories != nullptr);
    assert(cfd_);
    assert(ucmp_);
  }

  // No copying allowed
  ExternalSstFileIngestionJob(const ExternalSstFileIngestionJob&) = delete;
  ExternalSstFileIngestionJob& operator=(const ExternalSstFileIngestionJob&) =
      delete;
  ExternalSstFileIngestionJob(ExternalSstFileIngestionJob&&) = delete;
  ExternalSstFileIngestionJob& operator=(ExternalSstFileIngestionJob&&) =
      delete;

  ~ExternalSstFileIngestionJob() {
    UnregisterRange(/*restore_compaction_candidates=*/false);
  }

  ColumnFamilyData* GetColumnFamilyData() const { return cfd_; }

  // Prepare the job by copying external files into the DB.
  Status Prepare(const std::vector<std::string>& external_files_paths,
                 const std::vector<std::string>& files_checksums,
                 const std::vector<std::string>& files_checksum_func_names,
                 const std::vector<const PreparedFileInfo*>& file_infos,
                 const std::optional<RangeOpt>& atomic_replace_range,
                 const Temperature& file_temperature, uint64_t next_file_number,
                 SuperVersion* sv);

  // Check if we need to flush the memtable before running the ingestion job
  // This will be true if the files we are ingesting are overlapping with any
  // key range in the memtable.
  //
  // @param super_version A referenced SuperVersion that will be held for the
  //    duration of this function.
  //
  // Thread-safe
  Status NeedsFlush(bool* flush_needed, SuperVersion* super_version);

  void SetFlushedBeforeRun() { flushed_before_run_ = true; }

  // Will execute the ingestion job and prepare edit() to be applied.
  // REQUIRES: Mutex held
  Status Run();

  // Register key range involved in this ingestion job
  // to prevent key range conflict with other ongoing compaction/file ingestion.
  // Also marks the existing files this job deletes (atomic replace) as being
  // compacted, so that no compaction picks them while LogAndApply() releases
  // the DB mutex to write the MANIFEST.
  // REQUIRES: Mutex held
  void RegisterRange();

  // Unregister key range registered for this ingestion job, and clear the
  // being-compacted mark set by RegisterRange() on replaced files.
  // Restore compaction eligibility when the commit failed.
  // REQUIRES: Mutex held
  void UnregisterRange(bool restore_compaction_candidates);

  // Update column family stats.
  // REQUIRES: Mutex held
  void UpdateStats();

  // Cleanup after successful/failed job
  void Cleanup(const Status& status);

  VersionEdit* edit() { return &edit_; }

  const autovector<IngestedFileInfo>& files_to_ingest() const {
    return files_to_ingest_;
  }

  // Return the maximum assigned sequence number for all files in this job.
  // When allow_db_generated_files = false, we may assign global sequence
  // numbers to ingested files. The global sequence numbers are sequence numbers
  // following versions_->LastSequence().
  // When allow_db_generated_files = true, we ingest files that already have
  // sequence numbers assigned. max_assigned_seqno_ will be the max sequence
  // number among ingested files.
  SequenceNumber MaxAssignedSequenceNumber() const {
    return max_assigned_seqno_;
  }

  // Max threads requested for opening this job's files during commit, from the
  // per-CF IngestExternalFileOptions.
  int file_opening_threads() const {
    return ingestion_options_.file_opening_threads;
  }

  size_t NumFilesToPrepare(
      size_t num_external_files,
      const std::optional<RangeOpt>& atomic_replace_range) const;

  // Merge another already-Prepare()d job for the SAME column family into this
  // one so both sets of files are committed by a single Run(). The other job's
  // files are appended after this job's, so for any overlapping keys the other
  // job's data wins via a higher assigned sequence number -- the same semantics
  // as passing all the files to a single ingestion call in this order. The
  // other job is left empty.
  Status MergeForSameColumnFamily(ExternalSstFileIngestionJob* other);

 private:
  bool SupportsAtomicReplaceRangeTombstone() const;

  bool CanUseAtomicReplaceRangeTombstone(
      const std::optional<RangeOpt>& atomic_replace_range) const;

  bool HasPartialOverlap(const VersionStorageInfo* vstorage) const;

  Status PrepareAtomicReplaceRangeTombstone(const Slice& start,
                                            const Slice& limit,
                                            uint64_t file_number,
                                            SuperVersion* super_version);

  void ActivateAtomicReplaceRangeTombstone();

  Status ResetTableReader(const std::string& external_file,
                          uint64_t new_file_number,
                          bool user_defined_timestamps_persisted,
                          SuperVersion* sv, IngestedFileInfo* file_to_ingest,
                          std::unique_ptr<TableReader>* table_reader);

  // Read the external file's table properties to do various sanity checks and
  // populates certain fields in `IngestedFileInfo` according to some table
  // properties.
  // In some cases when sanity check passes, `table_reader` could be reset with
  // different options. For example: when external file does not contain
  // timestamps while column family enables UDT in Memtables only feature.
  Status SanityCheckTableProperties(const std::string& external_file,
                                    const TableProperties& props,
                                    IngestedFileInfo* file_to_ingest);

  // Open the external file and populate `file_to_ingest` with all the
  // external information we need to ingest this file. When
  // `prepared_file_info` is non-null, its caller-supplied metadata is reused
  // instead of opening and scanning the file.
  Status GetIngestedFileInfo(const std::string& external_file,
                             uint64_t new_file_number,
                             const PreparedFileInfo* prepared_file_info,
                             IngestedFileInfo* file_to_ingest,
                             SuperVersion* sv);

  // Acquire the per-file metadata from the caller-supplied opaque
  // `PreparedFileInfo` (produced by SstFileWriter::Finish) instead of opening
  // the file.
  Status GetIngestedFileInfoFromFileInfo(
      const std::string& external_file,
      const PreparedFileInfo& prepared_file_info,
      IngestedFileInfo* file_to_ingest);

  // Acquire the per-file metadata by opening the external file and scanning it
  // (table properties, sequence number bounds, and boundary keys including any
  // range-tombstone extensions). Used when no file_info is available. The
  // opened `TableReader` is returned via `*table_reader` so the caller can
  // reuse it (e.g. to verify the file checksum) without re-opening the file.
  Status GetIngestedFileInfoFromFile(
      const std::string& external_file, uint64_t new_file_number,
      IngestedFileInfo* file_to_ingest, SuperVersion* sv,
      std::unique_ptr<TableReader>* out_table_reader);

  // If the input files' key range overlaps themselves, this function divides
  // them in the user specified order into multiple batches. Where the files
  // within a batch do not overlap with each other, but key range could overlap
  // between batches.
  // If the input files' key range don't overlap themselves, they always just
  // make one batch.
  void DivideInputFilesIntoBatches();

  // Returns whether any two files in `files` have overlapping key ranges, by
  // sorting the file ranges and checking adjacent pairs.
  bool ComputeFilesOverlap(const autovector<IngestedFileInfo>& files) const;

  // Assign level for the files in one batch. The files within one batch are not
  // overlapping, and we assign level to each file one after another.
  // If `prev_batch_uppermost_level` is specified, all files in this batch will
  // be assigned to levels that are higher than `prev_batch_uppermost_level`.
  // The uppermost level used by this batch of files is tracked too, so that it
  // can be used by the next batch.
  // REQUIRES: Mutex held
  Status AssignLevelsForOneBatch(FileBatchInfo& batch, bool force_global_seqno,
                                 SequenceNumber* last_seqno,
                                 int* batch_uppermost_level,
                                 std::optional<int> prev_batch_uppermost_level);

  // Assign `file_to_ingest` the appropriate sequence number and the lowest
  // possible level that it can be ingested to according to compaction_style.
  // If `prev_batch_uppermost_level` is specified, the file will only be
  // assigned to levels tha are higher than `prev_batch_uppermost_level`.
  // REQUIRES: Mutex held
  Status AssignLevelAndSeqnoForIngestedFile(
      bool force_global_seqno, CompactionStyle compaction_style,
      SequenceNumber last_seqno, IngestedFileInfo* file_to_ingest,
      SequenceNumber* assigned_seqno,
      std::optional<int> prev_batch_uppermost_level);

  // File that we want to ingest behind always goes to the lowest level;
  // we just check that it fits in the level, that the CF allows ingest_behind,
  // and that we don't have 0 seqnums at the upper levels.
  // REQUIRES: Mutex held
  Status CheckLevelForIngestedBehindFile(IngestedFileInfo* file_to_ingest);

  // Set the file's assigned global sequence number to `seqno`.
  Status AssignGlobalSeqnoForIngestedFile(IngestedFileInfo* file_to_ingest,
                                          SequenceNumber seqno);

  // Check if `file_to_ingest` can fit in level `level`
  // REQUIRES: Mutex held
  bool IngestedFileFitInLevel(const IngestedFileInfo* file_to_ingest,
                              int level);

  // Helper function to obtain the smallest and largest sequence number from a
  // file. When OK is returned, file_to_ingest->smallest_seqno and
  // file_to_ingest->largest_seqno will be updated.
  Status GetSeqnoBoundaryForFile(TableReader* table_reader, SuperVersion* sv,
                                 IngestedFileInfo* file_to_ingest,
                                 bool allow_data_in_errors);

  // Create equivalent `Compaction` objects to this file ingestion job
  // , which will be used to check range conflict with other ongoing
  // compactions.
  void CreateEquivalentFileIngestingCompactions();

  // Add `file` at `level` to the files deleted by this job's atomic replace.
  // Fails if `file` is already being compacted.
  // REQUIRES: Mutex held
  Status AddFileToReplace(int level, FileMetaData* file);

  // Remove all the internal files created, called when ingestion job fails.
  void DeleteInternalFiles();

  SystemClock* clock_;
  FileSystemPtr fs_;
  VersionSet* versions_;
  ColumnFamilyData* cfd_;
  const Comparator* ucmp_;
  ExternalFileRangeChecker file_range_checker_;
  const ImmutableDBOptions& db_options_;
  const MutableDBOptions& mutable_db_options_;
  const EnvOptions& env_options_;
  const ShardedSnapshotList* db_snapshots_;
  autovector<IngestedFileInfo> files_to_ingest_;
  std::vector<FileBatchInfo> file_batches_to_ingest_;
  const IngestExternalFileOptions ingestion_options_;
  std::optional<KeyRangeInfo> atomic_replace_range_;
  std::optional<IngestedFileInfo> atomic_replace_range_tombstone_;
  bool atomic_replace_range_tombstone_active_{false};
  Directories* directories_;
  EventLogger* event_logger_;
  VersionEdit edit_;
  uint64_t job_start_time_;
  SequenceNumber max_assigned_seqno_;
  // Set in ExternalSstFileIngestionJob::Prepare(), if true all files are
  // ingested in L0
  bool files_overlap_{false};
  std::shared_ptr<IOTracer> io_tracer_;

  // Flag indicating whether the column family is flushed after `Prepare` and
  // before `Run`.
  bool flushed_before_run_{false};

  // Below are variables used in (un)registering range for this ingestion job
  //
  // FileMetaData used in inputs of compactions equivalent to this ingestion
  // job
  std::vector<FileMetaData*> compaction_input_metdatas_;
  // Compactions equivalent to this ingestion job
  std::vector<Compaction*> file_ingesting_compactions_;

  // Existing files deleted by atomic replace. Their version is retained until
  // UnregisterRange() clears their being_compacted flags.
  std::vector<FileMetaData*> files_to_replace_;
  Version* files_to_replace_version_{nullptr};
  bool files_to_replace_marked_{false};
};

}  // namespace ROCKSDB_NAMESPACE
