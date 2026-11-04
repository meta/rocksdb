// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Basic identifiers and metadata for a file in a DB. This only includes
// information considered relevant for taking backups, checkpoints, or other
// services relating to DB file storage.
// This is only appropriate for immutable files, such as SST files or all
// files in a backup. See also LiveFileStorageInfo.
struct FileStorageInfo {
  // The name of the file within its directory (e.g. "123456.sst")
  std::string relative_filename;
  // The directory containing the file, without a trailing '/'. This could be
  // a DB path, wal_dir, etc.
  std::string directory;

  // The id of the file within a single DB. Set to 0 if the file does not have
  // a number (e.g. CURRENT)
  uint64_t file_number = 0;
  // The type of the file as part of a DB.
  FileType file_type = kTempFile;

  // File size in bytes. See also `trim_to_size`.
  uint64_t size = 0;

  // This feature is experimental and subject to change.
  Temperature temperature = Temperature::kUnknown;

  // The checksum of a SST file, the value is decided by the file content and
  // the checksum algorithm used for this SST file. The checksum function is
  // identified by the file_checksum_func_name. If the checksum function is
  // not specified, file_checksum is "0" by default.
  std::string file_checksum;

  // The name of the checksum function used to generate the file checksum
  // value. If file checksum is not enabled (e.g., sst_file_checksum_func is
  // null), file_checksum_func_name is UnknownFileChecksumFuncName, which is
  // "Unknown".
  std::string file_checksum_func_name;
};

// Adds to FileStorageInfo the ability to capture the state of files that
// might change in a running DB.
struct LiveFileStorageInfo : public FileStorageInfo {
  // If non-empty, this string represents the "saved" contents of the file
  // for the current context. (This field is used for checkpointing CURRENT
  // file.) In that case, size == replacement_contents.size() and file on disk
  // should be ignored. If empty string, the file on disk should still have
  // "saved" contents. (See trim_to_size.)
  std::string replacement_contents;

  // If true, the file on disk is allowed to be larger than `size` but only
  // the first `size` bytes should be used for the current context. If false,
  // the file is corrupt if size on disk does not equal `size`.
  bool trim_to_size = false;
};

// Decoded form of SstFileMetaData::db_ownership_info: how this DB got an SST
// file's contents, and what has happened to the file since without its
// contents being rewritten. Recorded in the MANIFEST (not in the file), since
// the same file contents can be added to several DBs. Round-trips through
// Encode() / DecodeFrom().
struct DbOwnershipInfo {
  // How this DB got the file's contents. Values may be added in the future;
  // DecodeFrom() reports values it doesn't recognize as kUnknown.
  enum class Origin : uint8_t {
    // Not recorded, e.g. for a file recorded by an older RocksDB version or
    // found by RepairDB.
    kUnknown = 0,
    // Written by a flush of this DB, including flushes of WAL data during DB
    // open or RepairDB.
    kFlush = 1,
    // Written by a compaction of this DB, local or remote.
    kCompaction = 2,
    // Added by external file ingestion (one- or two-phase), whether the file
    // was written by SstFileWriter or by another DB.
    kIngestion = 3,
    // Added by CreateColumnFamilyWithImport().
    kImport = 4,
  };
  Origin origin = Origin::kUnknown;

  // The following flags each record that something has happened to the file
  // at least once without its contents being rewritten. Once set, a flag stays
  // set when the file is moved or copied again.

  // Moved to another level without being rewritten (e.g. trivial move, or
  // CompactRange() with change_level).
  bool moved_between_levels = false;
  // Copied as-is to a new file with a different temperature, as FIFO
  // compaction does with allow_trivial_copy_when_change_temperature. The new
  // file's db_ownership_start_time is the time of the latest such copy.
  bool temperature_changed_by_copy = false;
  // The temperature recorded for the file was updated to match where the
  // FileSystem reports the file to be (UpdateManifestForFilesState()), so
  // the file was moved at a time unknown to RocksDB.
  bool temperature_updated_from_file_system = false;
  // Recorded by RepairDB, which rebuilds the MANIFEST. For files that existed
  // before the repair, the origin and db_ownership_start_time are lost.
  bool recorded_by_repair = false;

  // Serialize to / deserialize from the packed
  // SstFileMetaData::db_ownership_info representation. DecodeFrom() ignores
  // unrecognized bits so the encoding can be extended in the future, which
  // means re-encoding a decoded value drops any bits added by newer versions.
  uint32_t Encode() const;
  static DbOwnershipInfo DecodeFrom(uint32_t encoded);

  bool operator==(const DbOwnershipInfo& other) const {
    return origin == other.origin &&
           moved_between_levels == other.moved_between_levels &&
           temperature_changed_by_copy == other.temperature_changed_by_copy &&
           temperature_updated_from_file_system ==
               other.temperature_updated_from_file_system &&
           recorded_by_repair == other.recorded_by_repair;
  }
};

// The metadata that describes an SST file. (Does not need to extend
// LiveFileStorageInfo because SST files are always immutable.)
struct SstFileMetaData : public FileStorageInfo {
  SstFileMetaData() { file_type = kTableFile; }

  SstFileMetaData(const std::string& _file_name, uint64_t _file_number,
                  const std::string& _directory, uint64_t _size,
                  SequenceNumber _smallest_seqno, SequenceNumber _largest_seqno,
                  const std::string& _smallestkey,
                  const std::string& _largestkey, uint64_t _num_reads_sampled,
                  bool _being_compacted, Temperature _temperature,
                  uint64_t _oldest_blob_file_number,
                  uint64_t _oldest_ancestor_time, uint64_t _file_creation_time,
                  uint64_t _epoch_number, std::string& _file_checksum,
                  std::string& _file_checksum_func_name)
      : smallest_seqno(_smallest_seqno),
        largest_seqno(_largest_seqno),
        smallestkey(_smallestkey),
        largestkey(_largestkey),
        num_reads_sampled(_num_reads_sampled),
        being_compacted(_being_compacted),
        num_entries(0),
        num_deletions(0),
        oldest_blob_file_number(_oldest_blob_file_number),
        oldest_ancestor_time(_oldest_ancestor_time),
        file_creation_time(_file_creation_time),
        epoch_number(_epoch_number) {
    if (!_file_name.empty()) {
      if (_file_name[0] == '/') {
        relative_filename = _file_name.substr(1);
        name = _file_name;  // Deprecated field
      } else {
        relative_filename = _file_name;
        name = std::string("/") + _file_name;  // Deprecated field
      }
      assert(relative_filename.size() + 1 == name.size());
      assert(relative_filename[0] != '/');
      assert(name[0] == '/');
    }
    directory = _directory;
    db_path = _directory;  // Deprecated field
    file_number = _file_number;
    file_type = kTableFile;
    size = _size;
    temperature = _temperature;
    file_checksum = _file_checksum;
    file_checksum_func_name = _file_checksum_func_name;
  }

  SequenceNumber smallest_seqno = 0;  // Smallest sequence number in file.
  SequenceNumber largest_seqno = 0;   // Largest sequence number in file.
  std::string smallestkey;            // Smallest user defined key in the file.
  std::string largestkey;             // Largest user defined key in the file.
  uint64_t num_reads_sampled = 0;     // How many times the file is read.
  bool being_compacted =
      false;  // true if the file is currently being compacted.

  uint64_t num_entries = 0;
  uint64_t num_deletions = 0;

  uint64_t oldest_blob_file_number = 0;  // The id of the oldest blob file
                                         // referenced by the file.
  // An SST file may be generated by compactions whose input files may
  // in turn be generated by earlier compactions. The creation time of the
  // oldest SST file that is the compaction ancestor of this file.
  // The timestamp is provided SystemClock::GetCurrentTime().
  // 0 if the information is not available.
  //
  // Note: for TTL blob files, it contains the start of the expiration range.
  union {
    uint64_t oldest_ancestor_time = 0;
    // DEPRECATED: misspelled old name for oldest_ancestor_time. Will be
    // removed in a future release.
    uint64_t oldest_ancester_time;
  };
  // Timestamp when the SST file's contents were built, provided by
  // SystemClock::GetCurrentTime(). For an ingested or imported file, this is
  // when it was built by SstFileWriter or by the DB it came from. Preserved
  // when the file is moved between levels or copied to another temperature.
  // 0 if the information is not available.
  uint64_t file_creation_time = 0;
  // The order of a file being flushed or ingested/imported.
  // Compaction output file will be assigned with the minimum `epoch_number`
  // among input files'.
  // For L0, larger `epoch_number` indicates newer L0 file.
  // 0 if the information is not available.
  uint64_t epoch_number = 0;
  // Timestamp, provided by SystemClock::GetCurrentTime(), when this DB began
  // holding this physical file (this file number): when the DB started
  // writing it (flush or compaction output), when ingestion (in the prepare
  // phase, for two-phase ingestion) or CreateColumnFamilyWithImport() linked
  // or copied it into the DB, or when it was copied as a new file with a
  // different temperature (see DbOwnershipInfo). Preserved when the file is
  // moved between levels. A DB opened from a checkpoint or backup keeps the
  // values recorded by the DB it was copied from. 0 if the information is not
  // available, e.g. for a file recorded by an older RocksDB version (whose
  // file_creation_time was the ingestion, import or copy time for files that
  // arrived that way) or found by RepairDB.
  uint64_t db_ownership_start_time = 0;
  // How this DB got the file's contents and what has happened to the file
  // since, packed as described by DbOwnershipInfo (decode with
  // DbOwnershipInfo::DecodeFrom()). 0 if the information is not available.
  // (CreateColumnFamilyWithImport() ignores this field and
  // db_ownership_start_time in its input metadata, recording its own.)
  uint32_t db_ownership_info = 0;

  // These bounds define the effective key range for range tombstones
  // in this file.
  // Currently only used by CreateColumnFamilyWithImport().
  std::string smallest{};  // Smallest internal key served by table
  std::string largest{};   // Largest internal key served by table

  // DEPRECATED: The name of the file within its directory with a
  // leading slash (e.g. "/123456.sst"). Use relative_filename from base struct
  // instead.
  std::string name;

  // DEPRECATED: replaced by `directory` in base struct
  std::string db_path;
};

// The full set of metadata associated with each SST file.
struct LiveFileMetaData : SstFileMetaData {
  std::string column_family_name;  // Name of the column family
  int level;                       // Level at which this file resides.
  LiveFileMetaData() : column_family_name(), level(0) {}
};

// The MetaData that describes a Blob file
struct BlobMetaData {
  BlobMetaData()
      : blob_file_number(0),
        blob_file_size(0),
        total_blob_count(0),
        total_blob_bytes(0),
        garbage_blob_count(0),
        garbage_blob_bytes(0) {}

  BlobMetaData(uint64_t _file_number, const std::string& _file_name,
               const std::string& _file_path, uint64_t _file_size,
               uint64_t _total_blob_count, uint64_t _total_blob_bytes,
               uint64_t _garbage_blob_count, uint64_t _garbage_blob_bytes,
               const std::string& _file_checksum,
               const std::string& _file_checksum_func_name)
      : blob_file_number(_file_number),
        blob_file_name(_file_name),
        blob_file_path(_file_path),
        blob_file_size(_file_size),
        total_blob_count(_total_blob_count),
        total_blob_bytes(_total_blob_bytes),
        garbage_blob_count(_garbage_blob_count),
        garbage_blob_bytes(_garbage_blob_bytes),
        checksum_method(_file_checksum),
        checksum_value(_file_checksum_func_name) {}
  uint64_t blob_file_number;
  std::string blob_file_name;
  std::string blob_file_path;
  uint64_t blob_file_size;
  uint64_t total_blob_count;
  uint64_t total_blob_bytes;
  uint64_t garbage_blob_count;
  uint64_t garbage_blob_bytes;
  std::string checksum_method;
  std::string checksum_value;
};

// The metadata that describes a level.
struct LevelMetaData {
  LevelMetaData(int _level, uint64_t _size,
                const std::vector<SstFileMetaData>&& _files)
      : level(_level), size(_size), files(_files) {}

  // The level which this meta data describes.
  const int level;
  // The size of this level in bytes, which is equal to the sum of
  // the file size of its "files".
  const uint64_t size;
  // The metadata of all sst files in this level.
  const std::vector<SstFileMetaData> files;
};

// Options for filtering column family metadata by key range.
struct GetColumnFamilyMetaDataOptions {
  RangeOpt range;

  // The level to filter on. If -1, all levels are included.
  int level = -1;

  GetColumnFamilyMetaDataOptions() = default;

  GetColumnFamilyMetaDataOptions(const OptSlice& _start_key,
                                 const OptSlice& _end_key, int _level = -1)
      : range(_start_key, _end_key), level(_level) {}
};

// The metadata that describes a column family.
struct ColumnFamilyMetaData {
  ColumnFamilyMetaData() : size(0), file_count(0) {}
  ColumnFamilyMetaData(const std::string& _name, uint64_t _size,
                       const std::vector<LevelMetaData>&& _levels)
      : size(_size), name(_name), levels(_levels) {}

  // The size of this column family in bytes, which is equal to the sum of
  // the file size of its "levels".
  uint64_t size;
  // The number of files in this column family.
  size_t file_count;
  // The name of the column family.
  std::string name;
  // The metadata of all levels in this column family.
  // levels[i] contains files in level i.
  // For level 0, files with recent updates are ordered first.
  // For level 1+, files are ordered by increasing key range.
  std::vector<LevelMetaData> levels;

  // The total size of all blob files
  uint64_t blob_file_size = 0;
  // The number of blob files in this column family.
  size_t blob_file_count = 0;
  // The metadata of the blobs in this column family.
  std::vector<BlobMetaData> blob_files;
};

// Metadata returned as output from ExportColumnFamily() and used as input to
// CreateColumnFamiliesWithImport().
struct ExportImportFilesMetaData {
  std::string db_comparator_name;       // Used to safety check at import.
  std::vector<LiveFileMetaData> files;  // Vector of file metadata.
};
}  // namespace ROCKSDB_NAMESPACE
