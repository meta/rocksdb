//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_garbage.h"
#include "db/dbformat.h"
#include "db/wal_edit.h"
#include "memory/arena.h"
#include "port/malloc.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/advanced_options.h"
#include "table/table_reader.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed. The number should be forward compatible so
// users can down-grade RocksDB safely. A future Tag is ignored by doing '&'
// between Tag and kTagSafeIgnoreMask field.
enum Tag : uint32_t {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactCursor = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9,
  kMinLogNumberToKeep = 10,

  // these are new formats divergent from open source leveldb
  kNewFile2 = 100,
  kNewFile3 = 102,
  kNewFile4 = 103,      // 4th (the latest) format version of adding files
  kColumnFamily = 200,  // specify column family for version edit
  kColumnFamilyAdd = 201,
  kColumnFamilyDrop = 202,
  kMaxColumnFamily = 203,

  kInAtomicGroup = 300,

  kBlobFileAddition = 400,
  kBlobFileGarbage,

  // Mask for an unidentified tag from the future which can be safely ignored.
  kTagSafeIgnoreMask = 1 << 13,

  // Forward compatible (aka ignorable) records
  kDbId,
  kBlobFileAddition_DEPRECATED,
  kBlobFileGarbage_DEPRECATED,
  kWalAddition,
  kWalDeletion,
  kFullHistoryTsLow,
  kWalAddition2,
  kWalDeletion2,
  kPersistUserDefinedTimestamps,
  kSubcompactionProgress,
  kLastCompactedManifestFileSize,
  kCommittedOptionsFileNumber,
};

enum SubcompactionProgressPerLevelCustomTag : uint32_t {
  kSubcompactionProgressPerLevelTerminate = 1,  // End of fields marker
  kOutputFilesDelta = 2,
  kNumProcessedOutputRecords = 3,
  kSubcompactionProgressPerLevelCustomTagSafeIgnoreMask = 1 << 16,
};

enum SubcompactionProgressCustomTag : uint32_t {
  kSubcompactionProgressTerminate = 1,  // End of fields marker
  kNextInternalKeyToCompact = 2,
  kNumProcessedInputRecords = 3,
  kOutputLevelProgress = 4,
  kProximalOutputLevelProgress = 5,
  kBlobCompactionProgress = 6,
  kSubcompactionProgressCustomTagSafeIgnoreMask = 1 << 16,
};

enum NewFileCustomTag : uint32_t {
  kTerminate = 1,  // The end of customized fields
  kNeedCompaction = 2,
  // Since Manifest is not entirely forward-compatible, we currently encode
  // kMinLogNumberToKeep as part of NewFile as a hack. This should be removed
  // when manifest becomes forward-compatible.
  kMinLogNumberToKeepHack = 3,
  kOldestBlobFileNumber = 4,
  kOldestAncestorTime = 5,
  kFileCreationTime = 6,
  kFileChecksum = 7,
  kFileChecksumFuncName = 8,
  kTemperature = 9,
  kMinTimestamp = 10,
  kMaxTimestamp = 11,
  kUniqueId = 12,
  kEpochNumber = 13,
  kCompensatedRangeDeletionSize = 14,
  kTailSize = 15,
  kUserDefinedTimestampsPersisted = 16,
  kFileOpenMetadata = 17,

  // If this bit for the custom tag is set, opening DB should fail if
  // we don't know this field.
  kCustomTagNonSafeIgnoreMask = 1 << 6,

  // Forward incompatible (aka unignorable) fields
  kPathId,
};

class VersionSet;

constexpr uint64_t kFileNumberMask = 0x3FFFFFFFFFFFFFFF;
constexpr uint64_t kUnknownOldestAncestorTime = 0;
constexpr uint64_t kUnknownNewestKeyTime = 0;
constexpr uint64_t kUnknownFileCreationTime = 0;
constexpr uint64_t kUnknownEpochNumber = 0;
// If `Options::cf_allow_ingest_behind` is true, this epoch number
// will be dedicated to files ingested behind.
constexpr uint64_t kReservedEpochNumberForFileIngestedBehind = 1;

uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id);

// PinnedTableReader is used to safely access a table reader in a multi-threaded
// context. It holds both a pointer to the table reader and a cache handle.
class PinnedTableReader {
 public:
  PinnedTableReader() : reader_(nullptr), handle_(nullptr) {}
  ~PinnedTableReader() = default;
  PinnedTableReader(const PinnedTableReader& other)
      : reader_(nullptr), handle_(nullptr) {
    *this = other;
  }
  PinnedTableReader& operator=(const PinnedTableReader& other);
  PinnedTableReader(PinnedTableReader&&) = delete;
  PinnedTableReader& operator=(PinnedTableReader&&) = delete;

  // Returns the pinned TableReader, or nullptr if not pinned.
  TableReader* Get() const { return reader_.load(std::memory_order_acquire); }

  // Returns the cache handle that keeps TableReader alive, or nullptr if not
  // pinned.
  Cache::Handle* GetCacheHandle() const;

  // Pin a table reader with its cache handle.
  void Pin(Cache::Handle* handle, TableReader* reader);

  // Release the pinned handle via the given cache and reset state.
  void Release(Cache* cache);

  // Test-only: set a reader without a cache handle.
  void TEST_SetReader(TableReader* reader) {
    reader_.store(reader, std::memory_order_release);
  }

 private:
  // Internally, we need to ensure reads and writes to reader_ and handle_ are
  // properly ordered. handle_ must be written to before reader_ is written to
  // with release semantics. handle_ must be read after reader_ is read with
  // acquire semantics.
  std::atomic<TableReader*> reader_;
  Cache::Handle* handle_;
};

// A copyable structure contains information needed to read data from an SST
// file. It can contain a pointer to a table reader opened for the file, or
// file number and size, which can be used to create a new table reader for it.
// The behavior is undefined when a copied of the structure is used when the
// file is not in any live version any more.
struct FileDescriptor {
  // Fast access to table reader without cache lookup. Marked mutable because
  // reads can pin the table reader, but can also be done safely in a
  // multi-threaded context.
  mutable PinnedTableReader pinned_reader;
  uint64_t packed_number_and_path_id;
  uint64_t file_size;             // File size in bytes
  SequenceNumber smallest_seqno;  // The smallest seqno in this file
  SequenceNumber largest_seqno;   // The largest seqno in this file

  FileDescriptor() : FileDescriptor(0, 0, 0) {}

  FileDescriptor(uint64_t number, uint32_t path_id, uint64_t _file_size)
      : FileDescriptor(number, path_id, _file_size, kMaxSequenceNumber, 0) {}

  FileDescriptor(uint64_t number, uint32_t path_id, uint64_t _file_size,
                 SequenceNumber _smallest_seqno, SequenceNumber _largest_seqno)
      : pinned_reader(),
        packed_number_and_path_id(PackFileNumberAndPathId(number, path_id)),
        file_size(_file_size),
        smallest_seqno(_smallest_seqno),
        largest_seqno(_largest_seqno) {}

  uint64_t GetNumber() const {
    return packed_number_and_path_id & kFileNumberMask;
  }
  uint32_t GetPathId() const {
    return static_cast<uint32_t>(packed_number_and_path_id /
                                 (kFileNumberMask + 1));
  }
  uint64_t GetFileSize() const { return file_size; }
};

struct FileSampledStats {
  FileSampledStats()
      : num_reads_sampled(0), num_collapsible_entry_reads_sampled(0) {}
  ~FileSampledStats() = default;
  FileSampledStats(const FileSampledStats& other) { *this = other; }
  FileSampledStats& operator=(const FileSampledStats& other) {
    num_reads_sampled = other.num_reads_sampled.load();
    num_collapsible_entry_reads_sampled =
        other.num_collapsible_entry_reads_sampled.load();
    return *this;
  }
  // std::atomic is not movable, so moving copies. Don't `= delete` these: that
  // would delete FileMetaData's implicit move, making it copy every member.
  FileSampledStats(FileSampledStats&& other) noexcept { *this = other; }
  FileSampledStats& operator=(FileSampledStats&& other) noexcept {
    *this = other;
    return *this;
  }

  // number of user reads to this file.
  mutable std::atomic<uint64_t> num_reads_sampled;
  // number of reads of type kNotFound, kMerge, kTypeSingleDeletion
  mutable std::atomic<uint64_t> num_collapsible_entry_reads_sampled;
};

struct FileMetaData {
  FileDescriptor fd;
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table

  FileSampledStats stats;

  // Stats for compensating deletion entries during compaction

  // File size compensated by deletion entry.
  // This is used to compute a file's compaction priority, and is updated in
  // Version::ComputeCompensatedSizes() first time when the file is created or
  // loaded.  After it is updated (!= 0), it is immutable.
  uint64_t compensated_file_size = 0;
  // These values can mutate, but they can only be read or written from
  // single-threaded LogAndApply thread
  uint64_t num_entries =
      0;  // The number of entries, including deletions and range deletions.
  // The number of deletion entries, including range deletions.
  uint64_t num_deletions = 0;
  uint64_t raw_key_size = 0;    // total uncompressed key size.
  uint64_t raw_value_size = 0;  // total uncompressed value size.
  uint64_t num_range_deletions = 0;
  // This is computed during Flush/Compaction, and is added to
  // `compensated_file_size`. Currently, this estimates the size of keys in the
  // next level covered by range tombstones in this file.
  uint64_t compensated_range_deletion_size = 0;

  int refs = 0;  // Reference count

  bool being_compacted = false;       // Is this file undergoing compaction?
  bool init_stats_from_file = false;  // true if the data-entry stats of this
                                      // file has initialized from file.

  bool marked_for_compaction = false;  // True if client asked us nicely to
                                       // compact this file.
  Temperature temperature = Temperature::kUnknown;

  // Used only in BlobDB. The file number of the oldest blob file this SST file
  // refers to. 0 is an invalid value; BlobDB numbers the files starting from 1.
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;

  // For flush output file, oldest ancestor time is the oldest key time in the
  // file.  If the oldest key time is not available, flush time is used.
  //
  // For compaction output file, oldest ancestor time is the oldest
  // among all the oldest key time of its input files, since the file could be
  // the compaction output from other SST files, which could in turn be outputs
  // for compact older SST files. If that's not available, creation time of this
  // compaction output file is used.
  //
  // 0 means the information is not available.
  uint64_t oldest_ancestor_time = kUnknownOldestAncestorTime;

  // Unix time when the SST file is created.
  uint64_t file_creation_time = kUnknownFileCreationTime;

  // The order of a file being flushed or ingested/imported.
  // Compaction output file will be assigned with the minimum `epoch_number`
  // among input files'.
  // For L0, larger `epoch_number` indicates newer L0 file.
  uint64_t epoch_number = kUnknownEpochNumber;

  // File checksum
  std::string file_checksum = kUnknownFileChecksum;

  // File checksum function name
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;

  // SST unique id
  UniqueId64x2 unique_id{};

  // Size of the "tail" part of a SST file
  // "Tail" refers to all blocks after data blocks till the end of the SST file
  uint64_t tail_size = 0;

  // Value of the `AdvancedColumnFamilyOptions.persist_user_defined_timestamps`
  // flag when the file is created. Default to true, only when this flag is
  // false, it's explicitly written to Manifest.
  bool user_defined_timestamps_persisted = true;

  // Minimum user-defined timestamp in the file. Empty if no UDT or unknown.
  // This is populated from the table properties "rocksdb.timestamp_min".
  std::string min_timestamp;

  // Maximum user-defined timestamp in the file. Empty if no UDT or unknown.
  // This is populated from the table properties "rocksdb.timestamp_max".
  std::string max_timestamp;

  // Opaque file system metadata that can be used to accelerate file opening.
  // Retrieved via FSRandomAccessFile::GetFileOpenMetadata() and passed back
  // via FileOptions::file_metadata on subsequent opens. Empty if not available.
  std::string file_open_metadata;

  // Skips prefetching index and filter blocks into block cache on file open,
  // not persisted in MANIFEST. NOTE: false does not guarantee prefetching
  // either (e.g. when index and filter blocks are not cached in block cache).
  bool skip_index_and_filter_blocks_prefetch = false;

  FileMetaData() = default;

  FileMetaData(uint64_t file, uint32_t file_path_id, uint64_t file_size,
               const InternalKey& smallest_key, const InternalKey& largest_key,
               const SequenceNumber& smallest_seq,
               const SequenceNumber& largest_seq, bool marked_for_compact,
               Temperature _temperature, uint64_t oldest_blob_file,
               uint64_t _oldest_ancestor_time, uint64_t _file_creation_time,
               uint64_t _epoch_number, const std::string& _file_checksum,
               const std::string& _file_checksum_func_name,
               UniqueId64x2 _unique_id,
               const uint64_t _compensated_range_deletion_size,
               uint64_t _tail_size, bool _user_defined_timestamps_persisted,
               const std::string& _min_timestamp,
               const std::string& _max_timestamp)
      : fd(file, file_path_id, file_size, smallest_seq, largest_seq),
        smallest(smallest_key),
        largest(largest_key),
        compensated_range_deletion_size(_compensated_range_deletion_size),
        marked_for_compaction(marked_for_compact),
        temperature(_temperature),
        oldest_blob_file_number(oldest_blob_file),
        oldest_ancestor_time(_oldest_ancestor_time),
        file_creation_time(_file_creation_time),
        epoch_number(_epoch_number),
        file_checksum(_file_checksum),
        file_checksum_func_name(_file_checksum_func_name),
        unique_id(std::move(_unique_id)),
        tail_size(_tail_size),
        user_defined_timestamps_persisted(_user_defined_timestamps_persisted),
        min_timestamp(_min_timestamp),
        max_timestamp(_max_timestamp) {
    TEST_SYNC_POINT_CALLBACK("FileMetaData::FileMetaData", this);
  }

  // REQUIRED: Keys must be given to the function in sorted order (it expects
  // the last key to be the largest).
  Status UpdateBoundaries(const Slice& key, const Slice& value,
                          SequenceNumber seqno, ValueType value_type);

  // Unlike UpdateBoundaries, ranges do not need to be presented in any
  // particular order.
  void UpdateBoundariesForRange(const InternalKey& start,
                                const InternalKey& end, SequenceNumber seqno,
                                const InternalKeyComparator& icmp) {
    if (smallest.size() == 0 || icmp.Compare(start, smallest) < 0) {
      smallest = start;
    }
    if (largest.size() == 0 || icmp.Compare(largest, end) < 0) {
      largest = end;
    }
    assert(icmp.Compare(smallest, largest) <= 0);
    fd.smallest_seqno = std::min(fd.smallest_seqno, seqno);
    fd.largest_seqno = std::max(fd.largest_seqno, seqno);
  }

  // Try to get oldest ancestor time from the class itself or table properties
  // if table reader is already pinned.
  // 0 means the information is not available.
  uint64_t TryGetOldestAncestorTime() {
    if (oldest_ancestor_time != kUnknownOldestAncestorTime) {
      return oldest_ancestor_time;
    }
    TableReader* reader = fd.pinned_reader.Get();
    if (reader != nullptr && reader->GetTableProperties() != nullptr) {
      return reader->GetTableProperties()->oldest_ancestor_time;
    }
    return kUnknownOldestAncestorTime;
  }

  uint64_t TryGetFileCreationTime() {
    if (file_creation_time != kUnknownFileCreationTime) {
      return file_creation_time;
    }
    TableReader* reader = fd.pinned_reader.Get();
    if (reader != nullptr && reader->GetTableProperties() != nullptr) {
      return reader->GetTableProperties()->file_creation_time;
    }
    return kUnknownFileCreationTime;
  }

  // Tries to get the newest key time from the current file
  // Falls back on oldest ancestor time of previous (newer) file
  uint64_t TryGetNewestKeyTime(FileMetaData* prev_file = nullptr) {
    TableReader* reader = fd.pinned_reader.Get();
    if (reader != nullptr && reader->GetTableProperties() != nullptr) {
      uint64_t newest_key_time = reader->GetTableProperties()->newest_key_time;
      if (newest_key_time != kUnknownNewestKeyTime) {
        return newest_key_time;
      }
    }
    if (prev_file != nullptr) {
      uint64_t prev_oldest_ancestor_time =
          prev_file->TryGetOldestAncestorTime();
      if (prev_oldest_ancestor_time != kUnknownOldestAncestorTime) {
        return prev_oldest_ancestor_time;
      }
    }
    return kUnknownNewestKeyTime;
  }

  // WARNING: manual update to this function is needed
  // whenever a new string property is added to FileMetaData
  // to reduce approximation error.
  //
  // TODO: eliminate the need of manually updating this function
  // for new string properties
  size_t ApproximateMemoryUsage() const {
    size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size(const_cast<FileMetaData*>(this));
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    usage += smallest.size() + largest.size() + file_checksum.size() +
             file_checksum_func_name.size() + min_timestamp.size() +
             max_timestamp.size() + file_open_metadata.size();
    return usage;
  }

  // Returns whether this file is one with just one range tombstone. These type
  // of file should always be marked for compaction.
  bool FileIsStandAloneRangeTombstone() const {
    bool res = num_range_deletions == 1 && num_entries == num_range_deletions;
    assert(!res || fd.smallest_seqno == fd.largest_seqno);
    return res;
  }

  static uint64_t CalculateTailSize(uint64_t file_size,
                                    const TableProperties& props) {
#ifndef NDEBUG
    bool skip = false;
    TEST_SYNC_POINT_CALLBACK("FileMetaData::CalculateTailSize", &skip);
    if (skip) {
      return 0;
    }
#endif  // NDEBUG
    uint64_t tail_size = 0;

    // Differentiate between a file with no data blocks (tail_start_offset = 0)
    // and a file with unknown tail_start_offset (also set to 0 due to
    // non-negative integer storage limitation)
    bool contain_no_data_blocks =
        props.num_entries == 0 ||
        (props.num_entries > 0 &&
         (props.num_entries == props.num_range_deletions));

    if (props.tail_start_offset > 0 || contain_no_data_blocks) {
      assert(props.tail_start_offset <= file_size);
      tail_size = file_size - props.tail_start_offset;
    }

    return tail_size;
  }
};

// A compressed copy of file meta data that just contain minimum data needed
// to serve read operations, while still keeping the pointer to full metadata
// of the file in case it is needed.
struct FdWithKeyRange {
  FileDescriptor fd;
  FileMetaData* file_metadata;  // Point to all metadata
  Slice smallest_key;           // slice that contain smallest key
  Slice largest_key;            // slice that contain largest key

  FdWithKeyRange()
      : fd(), file_metadata(nullptr), smallest_key(), largest_key() {}

  FdWithKeyRange(FileDescriptor _fd, Slice _smallest_key, Slice _largest_key,
                 FileMetaData* _file_metadata)
      : fd(_fd),
        file_metadata(_file_metadata),
        smallest_key(_smallest_key),
        largest_key(_largest_key) {}
};

// Data structure to store an array of FdWithKeyRange in one level
// Actual data is guaranteed to be stored closely
struct LevelFilesBrief {
  size_t num_files;
  FdWithKeyRange* files;
  LevelFilesBrief() {
    num_files = 0;
    files = nullptr;
  }
};

struct SubcompactionProgressPerLevel {
  uint64_t GetNumProcessedOutputRecords() const {
    return num_processed_output_records_;
  }

  void SetNumProcessedOutputRecords(uint64_t num) {
    num_processed_output_records_ = num;
  }

  const autovector<FileMetaData>& GetOutputFiles() const {
    return output_files_;
  }

  void AddToOutputFiles(const FileMetaData& file) {
    output_files_.push_back(file);
  }

  size_t GetLastPersistedOutputFilesCount() const {
    return last_persisted_output_files_count_;
  }

  void UpdateLastPersistedOutputFilesCount() {
    last_persisted_output_files_count_ = output_files_.size();
  }

  void EncodeTo(std::string* dst) const;

  Status DecodeFrom(Slice* input);

  void Clear() {
    num_processed_output_records_ = 0;
    output_files_.clear();
    last_persisted_output_files_count_ = 0;
  }

  std::string ToString() const {
    std::ostringstream oss;
    oss << "SubcompactionProgressPerLevel{";
    oss << " num_processed_output_records=" << num_processed_output_records_;
    oss << ", output_files_count=" << output_files_.size();
    oss << ", last_persisted_output_files_count="
        << last_persisted_output_files_count_;
    oss << " }";
    return oss.str();
  }

  void TEST_ClearOutputFiles() { output_files_.clear(); }

 private:
  uint64_t num_processed_output_records_ = 0;

  autovector<FileMetaData> output_files_ = {};

  // Number of files already persisted to help calculate the new output files to
  // persist in the future. This is to prevent having to persist all the output
  // files metadata so far every time of a "snapshot" of a progress is persisted
  // which can lead to O(1+2+...+n) = O(n^2) file metadata being persisted. The
  // current approach of persisting only the delta should always persist
  // exactly the number (n) of output files in total.
  size_t last_persisted_output_files_count_ = 0;

  void EncodeOutputFiles(std::string* dst) const;

  Status DecodeOutputFiles(Slice* input,
                           autovector<FileMetaData>& temp_storage);
};

struct SubcompactionProgress {
  std::string next_internal_key_to_compact;

  uint64_t num_processed_input_records = 0;

  SubcompactionProgressPerLevel output_level_progress;

  SubcompactionProgressPerLevel proximal_output_level_progress;

  // Blob state is checkpointed together with the SSTs that can reference it.
  // The additions are delta-encoded across progress records, while garbage is
  // a cumulative snapshot. The next number is the first unused number in the
  // worker's pre-allocated range.
  bool has_blob_compaction_progress = false;
  std::vector<BlobFileAddition> blob_file_additions;
  std::vector<BlobFileGarbage> blob_file_garbages;
  uint64_t next_blob_file_number = kInvalidBlobFileNumber;

  SubcompactionProgress() = default;

  void Clear() {
    next_internal_key_to_compact.clear();
    num_processed_input_records = 0;
    output_level_progress.Clear();
    proximal_output_level_progress.Clear();
    has_blob_compaction_progress = false;
    blob_file_additions.clear();
    blob_file_garbages.clear();
    next_blob_file_number = kInvalidBlobFileNumber;
    last_persisted_blob_file_additions_count_ = 0;
  }

  void UpdateLastPersistedBlobFileAdditionsCount() {
    last_persisted_blob_file_additions_count_ = blob_file_additions.size();
  }

  size_t GetLastPersistedBlobFileAdditionsCount() const {
    return last_persisted_blob_file_additions_count_;
  }

  void EncodeTo(std::string* dst) const;

  Status DecodeFrom(Slice* input);

  std::string ToString() const {
    std::ostringstream oss;
    oss << "SubcompactionProgress{";
    oss << " next_internal_key_to_compact=";
    if (next_internal_key_to_compact.empty()) {
      oss << "";
    } else {
      ParsedInternalKey parsed_key;
      Slice key_slice(next_internal_key_to_compact);
      if (ParseInternalKey(key_slice, &parsed_key, false /* log_err_key */)
              .ok()) {
        oss << "user_key(hex)=" << parsed_key.user_key.ToString(true /* hex */);
        oss << ", seq=";
        if (parsed_key.sequence == kMaxSequenceNumber) {
          oss << "kMaxSequenceNumber";
        } else {
          oss << parsed_key.sequence;
        }
        oss << ", type=";
        if (parsed_key.type == kValueTypeForSeek) {
          oss << "kValueTypeForSeek";
        } else {
          oss << static_cast<int>(parsed_key.type);
        }
      } else {
        oss << "raw=" << key_slice.ToString(true /* hex */);
      }
    }
    oss << ", num_processed_input_records=" << num_processed_input_records;
    oss << ", output_level_progress=" << output_level_progress.ToString();
    oss << ", proximal_output_level_progress="
        << proximal_output_level_progress.ToString();
    oss << ", blob_file_additions_count=" << blob_file_additions.size();
    oss << ", blob_file_garbages_count=" << blob_file_garbages.size();
    oss << ", next_blob_file_number=" << next_blob_file_number;
    oss << " }";
    return oss.str();
  }

 private:
  size_t last_persisted_blob_file_additions_count_ = 0;
};

class VersionEdit;

// Builder class to reconstruct complete subcompaction progress object
// from multiple decoded VersionEdits containing delta output files information
// of the same subcompaction. See
// `SubcompactionProgressPerLevel::last_persisted_output_files_count_`'s comment
//
// WARNING: This class currently assumes all input VersionEdits contain progress
// information for the SAME subcompaction. It does not validate
// progress data from different subcompactions so mixing progress from
// multiple subcompactions can result in corrupted state silently. The caller is
// responsible for ensuring all VersionEdits processed by a single instance
// of this builder correspond to the same subcompaction.
class SubcompactionProgressBuilder {
 public:
  SubcompactionProgressBuilder() = default;

  bool ProcessVersionEdit(const VersionEdit& edit);

  const SubcompactionProgress& GetAccumulatedSubcompactionProgress() const {
    return accumulated_subcompaction_progress_;
  }

  bool HasAccumulatedSubcompactionProgress() const {
    return has_subcompaction_progress_;
  }

  void Clear();

 private:
  void MergeDeltaProgress(const SubcompactionProgress& delta_progress);

  void MaybeMergeDeltaProgressPerLevel(
      SubcompactionProgressPerLevel& accumulated_level_progress,
      const SubcompactionProgressPerLevel& delta_level_progress);

  SubcompactionProgress accumulated_subcompaction_progress_;
  bool has_subcompaction_progress_ = false;
};

// Type alias for backward compatibility - vector of subcompaction progress
using CompactionProgress = std::vector<SubcompactionProgress>;

// The state of a DB at any given time is referred to as a Version.
// Any modification to the Version is considered a Version Edit. A Version is
// constructed by joining a sequence of Version Edits. Version Edits are written
// to the MANIFEST file.
class VersionEdit {
 public:
  // Retrieve the table files added as well as their associated levels.
  using NewFiles = std::vector<std::pair<int, FileMetaData>>;

  static void EncodeToNewFile4(const FileMetaData& f, int level, size_t ts_sz,
                               bool has_min_log_number_to_keep,
                               uint64_t min_log_number_to_keep,
                               bool& min_log_num_written, std::string* dst);

  static const char* DecodeNewFile4From(Slice* input, int& max_level,
                                        uint64_t& min_log_number_to_keep,
                                        bool& has_min_log_number_to_keep,
                                        NewFiles& new_files, FileMetaData& f);

  void Clear();

  void SetDBId(const std::string& db_id) {
    has_db_id_ = true;
    db_id_ = db_id;
  }
  bool HasDbId() const { return has_db_id_; }
  const std::string& GetDbId() const { return db_id_; }

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  bool HasComparatorName() const { return has_comparator_; }
  const std::string& GetComparatorName() const { return comparator_; }

  void SetPersistUserDefinedTimestamps(bool persist_user_defined_timestamps) {
    has_persist_user_defined_timestamps_ = true;
    persist_user_defined_timestamps_ = persist_user_defined_timestamps;
  }
  bool HasPersistUserDefinedTimestamps() const {
    return has_persist_user_defined_timestamps_;
  }
  bool GetPersistUserDefinedTimestamps() const {
    return persist_user_defined_timestamps_;
  }

  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  bool HasLogNumber() const { return has_log_number_; }
  uint64_t GetLogNumber() const { return log_number_; }

  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  bool HasPrevLogNumber() const { return has_prev_log_number_; }
  uint64_t GetPrevLogNumber() const { return prev_log_number_; }

  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  bool HasNextFile() const { return has_next_file_number_; }
  uint64_t GetNextFile() const { return next_file_number_; }

  void SetMaxColumnFamily(uint32_t max_column_family) {
    has_max_column_family_ = true;
    max_column_family_ = max_column_family;
  }
  bool HasMaxColumnFamily() const { return has_max_column_family_; }
  uint32_t GetMaxColumnFamily() const { return max_column_family_; }

  void SetMinLogNumberToKeep(uint64_t num) {
    has_min_log_number_to_keep_ = true;
    min_log_number_to_keep_ = num;
  }
  bool HasMinLogNumberToKeep() const { return has_min_log_number_to_keep_; }
  uint64_t GetMinLogNumberToKeep() const { return min_log_number_to_keep_; }

  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  bool HasLastSequence() const { return has_last_sequence_; }
  SequenceNumber GetLastSequence() const { return last_sequence_; }

  // Delete the specified table file from the specified level.
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.emplace(level, file);
  }

  // Retrieve the table files deleted as well as their associated levels.
  using DeletedFiles = std::set<std::pair<int, uint64_t>>;
  const DeletedFiles& GetDeletedFiles() const { return deleted_files_; }

  // Add the specified table file at the specified level.
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  // REQUIRES: "oldest_blob_file_number" is the number of the oldest blob file
  // referred to by this file if any, kInvalidBlobFileNumber otherwise.
  void AddFile(int level, uint64_t file, uint32_t file_path_id,
               uint64_t file_size, const InternalKey& smallest,
               const InternalKey& largest, const SequenceNumber& smallest_seqno,
               const SequenceNumber& largest_seqno, bool marked_for_compaction,
               Temperature temperature, uint64_t oldest_blob_file_number,
               uint64_t oldest_ancestor_time, uint64_t file_creation_time,
               uint64_t epoch_number, const std::string& file_checksum,
               const std::string& file_checksum_func_name,
               const UniqueId64x2& unique_id,
               const uint64_t compensated_range_deletion_size,
               uint64_t tail_size, bool user_defined_timestamps_persisted,
               const std::string& min_timestamp = "",
               const std::string& max_timestamp = "",
               const std::string& file_open_metadata = "") {
    assert(smallest_seqno <= largest_seqno);
    new_files_.emplace_back(
        level,
        FileMetaData(
            file, file_path_id, file_size, smallest, largest, smallest_seqno,
            largest_seqno, marked_for_compaction, temperature,
            oldest_blob_file_number, oldest_ancestor_time, file_creation_time,
            epoch_number, file_checksum, file_checksum_func_name, unique_id,
            compensated_range_deletion_size, tail_size,
            user_defined_timestamps_persisted, min_timestamp, max_timestamp));
    if (!file_open_metadata.empty()) {
      new_files_.back().second.file_open_metadata = file_open_metadata;
    }
    files_to_quarantine_.push_back(file);
    if (!HasLastSequence() || largest_seqno > GetLastSequence()) {
      SetLastSequence(largest_seqno);
    }
  }

  void AddFile(int level, const FileMetaData& f) {
    assert(f.fd.smallest_seqno <= f.fd.largest_seqno);
    new_files_.emplace_back(level, f);
    files_to_quarantine_.push_back(f.fd.GetNumber());
    if (!HasLastSequence() || f.fd.largest_seqno > GetLastSequence()) {
      SetLastSequence(f.fd.largest_seqno);
    }
  }

  const NewFiles& GetNewFiles() const { return new_files_; }

  NewFiles& GetMutableNewFiles() { return new_files_; }

  // Retrieve all the compact cursors
  using CompactCursors = std::vector<std::pair<int, InternalKey>>;
  const CompactCursors& GetCompactCursors() const { return compact_cursors_; }
  void AddCompactCursor(int level, const InternalKey& cursor) {
    compact_cursors_.push_back(std::make_pair(level, cursor));
  }
  void SetCompactCursors(
      const std::vector<InternalKey>& compact_cursors_by_level) {
    compact_cursors_.clear();
    compact_cursors_.reserve(compact_cursors_by_level.size());
    for (int i = 0; i < (int)compact_cursors_by_level.size(); i++) {
      if (compact_cursors_by_level[i].Valid()) {
        compact_cursors_.push_back(
            std::make_pair(i, compact_cursors_by_level[i]));
      }
    }
  }

  // Add a new blob file.
  void AddBlobFile(uint64_t blob_file_number, uint64_t total_blob_count,
                   uint64_t total_blob_bytes, std::string checksum_method,
                   std::string checksum_value) {
    blob_file_additions_.emplace_back(
        blob_file_number, total_blob_count, total_blob_bytes,
        std::move(checksum_method), std::move(checksum_value));
    files_to_quarantine_.push_back(blob_file_number);
  }

  void AddBlobFile(BlobFileAddition blob_file_addition) {
    blob_file_additions_.emplace_back(std::move(blob_file_addition));
    files_to_quarantine_.push_back(
        blob_file_additions_.back().GetBlobFileNumber());
  }

  // Retrieve all the blob files added.
  using BlobFileAdditions = std::vector<BlobFileAddition>;
  const BlobFileAdditions& GetBlobFileAdditions() const {
    return blob_file_additions_;
  }

  void SetBlobFileAdditions(BlobFileAdditions blob_file_additions) {
    assert(blob_file_additions_.empty());
    blob_file_additions_ = std::move(blob_file_additions);
    std::for_each(
        blob_file_additions_.begin(), blob_file_additions_.end(),
        [&](const BlobFileAddition& blob_file) {
          files_to_quarantine_.push_back(blob_file.GetBlobFileNumber());
        });
  }

  // Add garbage for an existing blob file.  Note: intentionally broken English
  // follows.
  void AddBlobFileGarbage(uint64_t blob_file_number,
                          uint64_t garbage_blob_count,
                          uint64_t garbage_blob_bytes) {
    blob_file_garbages_.emplace_back(blob_file_number, garbage_blob_count,
                                     garbage_blob_bytes);
  }

  void AddBlobFileGarbage(BlobFileGarbage blob_file_garbage) {
    blob_file_garbages_.emplace_back(std::move(blob_file_garbage));
  }

  // Retrieve all the blob file garbage added.
  using BlobFileGarbages = std::vector<BlobFileGarbage>;
  const BlobFileGarbages& GetBlobFileGarbages() const {
    return blob_file_garbages_;
  }

  void SetBlobFileGarbages(BlobFileGarbages blob_file_garbages) {
    assert(blob_file_garbages_.empty());
    blob_file_garbages_ = std::move(blob_file_garbages);
  }

  // Add a WAL (either just created or closed).
  // AddWal and DeleteWalsBefore cannot be called on the same VersionEdit.
  void AddWal(WalNumber number, WalMetadata metadata = WalMetadata()) {
    assert(NumEntries() == wal_additions_.size());
    wal_additions_.emplace_back(number, std::move(metadata));
  }

  // Retrieve all the added WALs.
  const WalAdditions& GetWalAdditions() const { return wal_additions_; }

  bool IsWalAddition() const { return !wal_additions_.empty(); }

  // Delete a WAL (either directly deleted or archived).
  // AddWal and DeleteWalsBefore cannot be called on the same VersionEdit.
  void DeleteWalsBefore(WalNumber number) {
    assert((NumEntries() == 1) == !wal_deletion_.IsEmpty());
    wal_deletion_ = WalDeletion(number);
  }

  const WalDeletion& GetWalDeletion() const { return wal_deletion_; }

  bool IsWalDeletion() const { return !wal_deletion_.IsEmpty(); }

  bool IsWalManipulation() const {
    size_t entries = NumEntries();
    return (entries > 0) && ((entries == wal_additions_.size()) ||
                             (entries == !wal_deletion_.IsEmpty()));
  }

  // Number of edits
  size_t NumEntries() const {
    return new_files_.size() + deleted_files_.size() +
           blob_file_additions_.size() + blob_file_garbages_.size() +
           wal_additions_.size() + !wal_deletion_.IsEmpty();
  }

  void SetColumnFamily(uint32_t column_family_id) {
    column_family_ = column_family_id;
  }
  uint32_t GetColumnFamily() const { return column_family_; }

  const std::string& GetColumnFamilyName() const { return column_family_name_; }

  // set column family ID by calling SetColumnFamily()
  void AddColumnFamily(const std::string& name) {
    assert(!is_column_family_drop_);
    assert(!is_column_family_add_);
    assert(NumEntries() == 0);
    is_column_family_add_ = true;
    column_family_name_ = name;
  }

  // set column family ID by calling SetColumnFamily()
  void DropColumnFamily() {
    assert(!is_column_family_drop_);
    assert(!is_column_family_add_);
    assert(NumEntries() == 0);
    is_column_family_drop_ = true;
  }

  bool IsColumnFamilyManipulation() const {
    return is_column_family_add_ || is_column_family_drop_;
  }

  bool IsColumnFamilyAdd() const { return is_column_family_add_; }

  bool IsColumnFamilyDrop() const { return is_column_family_drop_; }

  void MarkForegroundOperation() { is_foreground_operation_ = true; }
  bool IsForegroundOperation() const {
    return is_foreground_operation_ || IsColumnFamilyManipulation();
  }

  void MarkNoManifestWriteDummy() { is_no_manifest_write_dummy_ = true; }
  bool IsNoManifestWriteDummy() const { return is_no_manifest_write_dummy_; }

  void MarkAtomicGroup(uint32_t remaining_entries) {
    is_in_atomic_group_ = true;
    remaining_entries_ = remaining_entries;
  }
  bool IsInAtomicGroup() const { return is_in_atomic_group_; }
  void SetRemainingEntries(uint32_t remaining_entries) {
    remaining_entries_ = remaining_entries;
  }
  uint32_t GetRemainingEntries() const { return remaining_entries_; }

  bool HasFullHistoryTsLow() const { return !full_history_ts_low_.empty(); }
  const std::string& GetFullHistoryTsLow() const {
    assert(HasFullHistoryTsLow());
    return full_history_ts_low_;
  }
  void SetFullHistoryTsLow(std::string full_history_ts_low) {
    assert(!full_history_ts_low.empty());
    full_history_ts_low_ = std::move(full_history_ts_low);
  }

  void SetSubcompactionProgress(const SubcompactionProgress& progress) {
    has_subcompaction_progress_ = true;
    subcompaction_progress_ = progress;
  }

  bool HasSubcompactionProgress() const { return has_subcompaction_progress_; }

  const SubcompactionProgress& GetSubcompactionProgress() const {
    return subcompaction_progress_;
  }

  void ClearSubcompactionProgress() {
    has_subcompaction_progress_ = false;
    subcompaction_progress_.Clear();
  }

  void SetLastCompactedManifestFileSize(uint64_t size) {
    has_last_compacted_manifest_file_size_ = true;
    last_compacted_manifest_file_size_ = size;
  }
  bool HasLastCompactedManifestFileSize() const {
    return has_last_compacted_manifest_file_size_;
  }
  uint64_t GetLastCompactedManifestFileSize() const {
    return last_compacted_manifest_file_size_;
  }

  // Records the exact OPTIONS file that describes the DB state after this
  // edit. This tag is safely ignorable so older RocksDB binaries retain their
  // legacy behavior of selecting the newest OPTIONS file by file number.
  void SetCommittedOptionsFileNumber(uint64_t number) {
    has_committed_options_file_number_ = true;
    committed_options_file_number_ = number;
  }
  bool HasCommittedOptionsFileNumber() const {
    return has_committed_options_file_number_;
  }
  uint64_t GetCommittedOptionsFileNumber() const {
    return committed_options_file_number_;
  }

  // Marks a live OPTIONS-only MANIFEST write as DB-wide metadata that does not
  // change an LSM Version. This is intentionally not encoded; recovery applies
  // the persisted fields through VersionEditHandler instead.
  void MarkOptionsFileManipulation() {
    assert(NumEntries() == 0);
    assert(!IsColumnFamilyManipulation());
    assert(HasCommittedOptionsFileNumber());
    is_options_file_manipulation_ = true;
  }
  bool IsOptionsFileManipulation() const {
    return is_options_file_manipulation_;
  }

  // Recovery-time per-column-family edits only need to be written when they
  // advance the CF's log number or carry some other manifest state.
  bool ShouldEmitPerColumnFamilyRecoveryEdit(uint64_t current_log_number) const;

  // return true on success.
  // `ts_sz` is the size in bytes for the user-defined timestamp contained in
  // a user key. This argument is optional because it's only required for
  // encoding a `VersionEdit` with new SST files to add. It's used to handle the
  // file boundaries: `smallest`, `largest` when
  // `FileMetaData.user_defined_timestamps_persisted` is false. When reading
  // the Manifest file, a mirroring change needed to handle
  // file boundaries are not added to the `VersionEdit.DecodeFrom` function
  // because timestamp size is not available at `VersionEdit` decoding time,
  // it's instead added to `VersionEditHandler::OnNonCfOperation`.
  bool EncodeTo(std::string* dst,
                std::optional<size_t> ts_sz = std::nullopt) const;
  Status DecodeFrom(const Slice& src);

  const autovector<uint64_t>* GetFilesToQuarantineIfCommitFail() const {
    return &files_to_quarantine_;
  }

  std::string DebugString(bool hex_key = false) const;
  std::string DebugJSON(int edit_num, bool hex_key = false) const;

 private:
  // Decode level information from serialized VersionEdit data and and track the
  // maximum level seen.
  //
  // Parameters:
  //   input: Pointer to serialized data slice
  //   level: Output parameter for the decoded level value
  //   max_level: get updated if the decoded level is higher than passed in
  //   value
  //
  // Returns: true on successful decode, false on parse error
  static bool GetLevel(Slice* input, int* level, int& max_level);
  // Encode file boundaries `FileMetaData.smallest` and `FileMetaData.largest`.
  // User-defined timestamps in the user key will be stripped if they shouldn't
  // be persisted.
  static void EncodeFileBoundaries(std::string* dst, const FileMetaData& meta,
                                   size_t ts_sz);

  uint64_t log_number_ = 0;
  uint64_t prev_log_number_ = 0;
  uint64_t next_file_number_ = 0;
  // The most recent WAL log number that is deleted
  uint64_t min_log_number_to_keep_ = 0;
  SequenceNumber last_sequence_ = 0;
  WalDeletion wal_deletion_;
  uint64_t last_compacted_manifest_file_size_ = 0;
  uint64_t committed_options_file_number_ = 0;

  // Compaction cursors for round-robin compaction policy
  CompactCursors compact_cursors_;
  NewFiles new_files_;
  BlobFileAdditions blob_file_additions_;
  BlobFileGarbages blob_file_garbages_;
  WalAdditions wal_additions_;

  std::string db_id_;
  std::string comparator_;
  std::string column_family_name_;
  std::string full_history_ts_low_;

  DeletedFiles deleted_files_;

  // Newly created table files and blob files are eligible for deletion if they
  // are not registered as live files after the background jobs creating them
  // have finished. In case committing the VersionEdit containing such changes
  // to manifest encountered an error, we want to quarantine these files from
  // deletion to avoid prematurely deleting files that ended up getting recorded
  // in Manifest as live files.
  // Since table files and blob files share the same file number space, we just
  // record the file number here.
  autovector<uint64_t> files_to_quarantine_;

  SubcompactionProgress subcompaction_progress_;

  int max_level_ = 0;
  uint32_t max_column_family_ = 0;

  // Each version edit record should have column_family_ set
  // If it's not set, it is default (0)
  uint32_t column_family_ = 0;
  uint32_t remaining_entries_ = 0;

  bool has_db_id_ = false;
  bool has_comparator_ = false;
  bool has_log_number_ = false;
  bool has_prev_log_number_ = false;
  bool has_next_file_number_ = false;
  bool has_max_column_family_ = false;
  bool has_min_log_number_to_keep_ = false;
  bool has_last_sequence_ = false;
  bool has_persist_user_defined_timestamps_ = false;
  bool is_column_family_drop_ = false;
  bool is_column_family_add_ = false;
  bool is_foreground_operation_ = false;
  bool is_in_atomic_group_ = false;
  bool is_no_manifest_write_dummy_ = false;
  bool persist_user_defined_timestamps_ = true;
  bool has_subcompaction_progress_ = false;
  bool has_last_compacted_manifest_file_size_ = false;
  bool has_committed_options_file_number_ = false;

  // Non-persisted marker for the live MANIFEST writer fast path.
  bool is_options_file_manipulation_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
