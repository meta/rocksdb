// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/compaction_job_stats.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/customizable.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

using TablePropertiesCollection =
    std::unordered_map<std::string, std::shared_ptr<const TableProperties>>;

class DB;
class ColumnFamilyHandle;
class Status;
struct CompactionJobStats;

struct FileCreationBriefInfo {
  FileCreationBriefInfo() = default;
  FileCreationBriefInfo(const std::string& _db_name,
                        const std::string& _cf_name,
                        const std::string& _file_path, int _job_id)
      : db_name(_db_name),
        cf_name(_cf_name),
        file_path(_file_path),
        job_id(_job_id) {}
  // the name of the database where the file was created.
  std::string db_name;
  // the name of the column family where the file was created.
  std::string cf_name;
  // the path to the created file.
  std::string file_path;
  // the id of the job (which could be flush or compaction) that
  // created the file.
  int job_id = 0;
};

struct TableFileCreationBriefInfo : public FileCreationBriefInfo {
  // reason of creating the table.
  TableFileCreationReason reason;
};

struct TableFileCreationInfo : public TableFileCreationBriefInfo {
  TableFileCreationInfo() = default;
  explicit TableFileCreationInfo(TableProperties&& prop)
      : table_properties(prop) {}
  // the size of the file.
  uint64_t file_size;
  // Detailed properties of the created file.
  TableProperties table_properties;
  // The status indicating whether the creation was successful or not.
  Status status;
  // The checksum of the table file being created
  std::string file_checksum;
  // The checksum function name of checksum generator used for this table file
  std::string file_checksum_func_name;
};

struct BlobFileCreationBriefInfo : public FileCreationBriefInfo {
  BlobFileCreationBriefInfo(const std::string& _db_name,
                            const std::string& _cf_name,
                            const std::string& _file_path, int _job_id,
                            BlobFileCreationReason _reason)
      : FileCreationBriefInfo(_db_name, _cf_name, _file_path, _job_id),
        reason(_reason) {}
  // reason of creating the blob file.
  BlobFileCreationReason reason;
};

struct BlobFileCreationInfo : public BlobFileCreationBriefInfo {
  BlobFileCreationInfo(const std::string& _db_name, const std::string& _cf_name,
                       const std::string& _file_path, int _job_id,
                       BlobFileCreationReason _reason,
                       uint64_t _total_blob_count, uint64_t _total_blob_bytes,
                       Status _status, const std::string& _file_checksum,
                       const std::string& _file_checksum_func_name)
      : BlobFileCreationBriefInfo(_db_name, _cf_name, _file_path, _job_id,
                                  _reason),
        total_blob_count(_total_blob_count),
        total_blob_bytes(_total_blob_bytes),
        status(_status),
        file_checksum(_file_checksum),
        file_checksum_func_name(_file_checksum_func_name) {}

  // the number of blob in a file.
  uint64_t total_blob_count;
  // the total bytes in a file.
  uint64_t total_blob_bytes;
  // The status indicating whether the creation was successful or not.
  Status status;
  // The checksum of the blob file being created.
  std::string file_checksum;
  // The checksum function name of checksum generator used for this blob file.
  std::string file_checksum_func_name;
};

enum class CompactionReason : int {
  kUnknown = 0,
  // [Level] number of L0 files > level0_file_num_compaction_trigger
  kLevelL0FilesNum,
  // [Level] total size of level > MaxBytesForLevel()
  kLevelMaxLevelSize,
  // [Universal] Compacting for size amplification
  kUniversalSizeAmplification,
  // [Universal] Compacting for size ratio
  kUniversalSizeRatio,
  // [Universal] number of sorted runs > level0_file_num_compaction_trigger
  kUniversalSortedRunNum,
  // [FIFO] total size > max_table_files_size
  kFIFOMaxSize,
  // [FIFO] reduce number of files.
  kFIFOReduceNumFiles,
  // [FIFO] files with creation time < (current_time - interval)
  kFIFOTtl,
  // Manual compaction
  kManualCompaction,
  // DB::SuggestCompactRange() marked files for compaction
  kFilesMarkedForCompaction,
  // [Level] Automatic compaction within bottommost level to cleanup duplicate
  // versions of same user key, usually due to a released snapshot.
  kBottommostFiles,
  // Compaction based on TTL
  kTtl,
  // According to the comments in flush_job.cc, RocksDB treats flush as
  // a level 0 compaction in internal stats.
  kFlush,
  // [InternalOnly] External sst file ingestion treated as a compaction
  // with placeholder input level L0 as file ingestion
  // technically does not have an input level like other compactions.
  // Used only for internal stats and conflict checking with other compactions
  kExternalSstIngestion,
  // Compaction due to SST file being too old
  kPeriodicCompaction,
  // Compaction in order to move files to temperature
  kChangeTemperature,
  // Compaction scheduled to force garbage collection of blob files
  kForcedBlobGC,
  // A special TTL compaction for RoundRobin policy, which basically the same as
  // kLevelMaxLevelSize, but the goal is to compact TTLed files.
  kRoundRobinTtl,
  // [InternalOnly] DBImpl::ReFitLevel treated as a compaction,
  // Used only for internal conflict checking with other compactions
  kRefitLevel,
  // Compaction triggered by high read frequency on SST files
  kReadTriggered,
  // total number of compaction reasons, new reasons must be added above this.
  kNumOfReasons,
};

const char* GetCompactionReasonString(CompactionReason compaction_reason);

// When adding flush reason, make sure to also update `GetFlushReasonString()`.
enum class FlushReason : int {
  kOthers = 0x00,
  kGetLiveFiles = 0x01,
  kShutDown = 0x02,
  kExternalFileIngestion = 0x03,
  kManualCompaction = 0x04,
  kWriteBufferManager = 0x05,
  kWriteBufferFull = 0x06,
  kTest = 0x07,
  kDeleteFiles = 0x08,
  kAutoCompaction = 0x09,
  kManualFlush = 0x0a,
  kErrorRecovery = 0xb,
  // When set the flush reason to kErrorRecoveryRetryFlush, SwitchMemtable
  // will not be called to avoid many small immutable memtables.
  kErrorRecoveryRetryFlush = 0xc,
  kWalFull = 0xd,
  // SwitchMemtable will not be called for this flush reason.
  kCatchUpAfterErrorRecovery = 0xe,
  kMemtableMaxRangeDeletions = 0xf,

  // When adding flush reason, make sure to also add it to FlushReason in Java.
};

const char* GetFlushReasonString(FlushReason flush_reason);

// TODO: In the future, BackgroundErrorReason will only be used to indicate
// why the BG Error is happening (e.g., flush, compaction). We may introduce
// other data structure to indicate other essential information such as
// the file type (e.g., Manifest, SST) and special context.
enum class BackgroundErrorReason {
  kFlush,
  kCompaction,
  kWriteCallback,
  kMemTable,
  kManifestWrite,
  kFlushNoWAL,
  kManifestWriteNoWAL,
  kAsyncFileOpen,
};

struct WriteStallInfo {
  // the name of the column family
  std::string cf_name;
  // state of the write controller
  struct {
    WriteStallCondition cur;
    WriteStallCondition prev;
  } condition;
};

struct FileDeletionInfo {
  FileDeletionInfo() = default;

  FileDeletionInfo(const std::string& _db_name, const std::string& _file_path,
                   int _job_id, Status _status)
      : db_name(_db_name),
        file_path(_file_path),
        job_id(_job_id),
        status(_status) {}
  // The name of the database where the file was deleted.
  std::string db_name;
  // The path to the deleted file.
  std::string file_path;
  // The id of the job which deleted the file.
  int job_id = 0;
  // The status indicating whether the deletion was successful or not.
  Status status;
};

struct TableFileDeletionInfo : public FileDeletionInfo {};

struct BlobFileDeletionInfo : public FileDeletionInfo {
  BlobFileDeletionInfo(const std::string& _db_name,
                       const std::string& _file_path, int _job_id,
                       Status _status)
      : FileDeletionInfo(_db_name, _file_path, _job_id, _status) {}
};

enum class FileOperationType {
  kRead,
  kWrite,
  kTruncate,
  kClose,
  kFlush,
  kSync,
  kFsync,
  kRangeSync,
  kAppend,
  kPositionedAppend,
  kOpen,
  kVerify
};

struct FileOperationInfo {
  using Duration = std::chrono::nanoseconds;
  using SteadyTimePoint =
      std::chrono::time_point<std::chrono::steady_clock, Duration>;
  using SystemTimePoint =
      std::chrono::time_point<std::chrono::system_clock, Duration>;
  using StartTimePoint = std::pair<SystemTimePoint, SteadyTimePoint>;
  using FinishTimePoint = SteadyTimePoint;

  FileOperationType type;
  const std::string& path;
  // Rocksdb try to provide file temperature information, but it's not
  // guaranteed.
  Temperature temperature;
  uint64_t offset;
  size_t length;
  const Duration duration;
  const SystemTimePoint& start_ts;
  Status status;

  FileOperationInfo(const FileOperationType _type, const std::string& _path,
                    const StartTimePoint& _start_ts,
                    const FinishTimePoint& _finish_ts, const Status& _status,
                    const Temperature _temperature = Temperature::kUnknown)
      : type(_type),
        path(_path),
        temperature(_temperature),
        duration(std::chrono::duration_cast<std::chrono::nanoseconds>(
            _finish_ts - _start_ts.second)),
        start_ts(_start_ts.first),
        status(_status) {}
  static StartTimePoint StartNow() {
    return std::make_pair<SystemTimePoint, SteadyTimePoint>(
        std::chrono::system_clock::now(), std::chrono::steady_clock::now());
  }
  static FinishTimePoint FinishNow() {
    return std::chrono::steady_clock::now();
  }
};

struct BlobFileInfo {
  BlobFileInfo(const std::string& _blob_file_path,
               const uint64_t _blob_file_number)
      : blob_file_path(_blob_file_path), blob_file_number(_blob_file_number) {}

  std::string blob_file_path;
  uint64_t blob_file_number;
};

struct BlobFileAdditionInfo : public BlobFileInfo {
  BlobFileAdditionInfo(const std::string& _blob_file_path,
                       const uint64_t _blob_file_number,
                       const uint64_t _total_blob_count,
                       const uint64_t _total_blob_bytes)
      : BlobFileInfo(_blob_file_path, _blob_file_number),
        total_blob_count(_total_blob_count),
        total_blob_bytes(_total_blob_bytes) {}
  uint64_t total_blob_count;
  uint64_t total_blob_bytes;
};

struct BlobFileGarbageInfo : public BlobFileInfo {
  BlobFileGarbageInfo(const std::string& _blob_file_path,
                      const uint64_t _blob_file_number,
                      const uint64_t _garbage_blob_count,
                      const uint64_t _garbage_blob_bytes)
      : BlobFileInfo(_blob_file_path, _blob_file_number),
        garbage_blob_count(_garbage_blob_count),
        garbage_blob_bytes(_garbage_blob_bytes) {}
  uint64_t garbage_blob_count;
  uint64_t garbage_blob_bytes;
};

struct ManualFlushInfo {
  // the id of the column family
  uint32_t cf_id;
  // the name of the column family
  std::string cf_name;
  // Reason that triggered this manual flush
  FlushReason flush_reason;
};

struct FlushJobInfo {
  // the id of the column family
  uint32_t cf_id;
  // the name of the column family
  std::string cf_name;
  // the path to the newly created file
  std::string file_path;
  // the file number of the newly created file
  uint64_t file_number;
  // the oldest blob file referenced by the newly created file
  uint64_t oldest_blob_file_number;
  // the id of the thread that completed this flush job.
  uint64_t thread_id;
  // the job id, which is unique in the same thread.
  int job_id;
  // If true, then rocksdb is currently slowing-down all writes to prevent
  // creating too many Level 0 files as compaction seems not able to
  // catch up the write request speed.  This indicates that there are
  // too many files in Level 0.
  bool triggered_writes_slowdown;
  // If true, then rocksdb is currently blocking any writes to prevent
  // creating more L0 files.  This indicates that there are too many
  // files in level 0.  Compactions should try to compact L0 files down
  // to lower levels as soon as possible.
  bool triggered_writes_stop;
  // The smallest sequence number in the newly created file
  SequenceNumber smallest_seqno;
  // The largest sequence number in the newly created file
  SequenceNumber largest_seqno;
  // Table properties of the table being flushed
  TableProperties table_properties;

  FlushReason flush_reason;

  // Compression algorithm used for blob output files
  CompressionType blob_compression_type;

  // Information about blob files created during flush in Integrated BlobDB.
  std::vector<BlobFileAdditionInfo> blob_file_addition_infos;
};

struct CompactionFileInfo {
  // The level of the file.
  int level;

  // The file number of the file.
  uint64_t file_number;

  // The file number of the oldest blob file this SST file references.
  uint64_t oldest_blob_file_number;
};

struct SubcompactionJobInfo {
  ~SubcompactionJobInfo() { status.PermitUncheckedError(); }
  // the id of the column family where the compaction happened.
  uint32_t cf_id;
  // the name of the column family where the compaction happened.
  std::string cf_name;
  // the status indicating whether the compaction was successful or not.
  Status status;
  // the id of the thread that completed this compaction job.
  uint64_t thread_id;
  // the job id, which is unique in the same thread.
  int job_id;

  // The execution pool selected for the parent job.
  bool use_separate_remote_compaction_pool = false;

  // sub-compaction job id, which is only unique within the same compaction, so
  // use both 'job_id' and 'subcompaction_job_id' to identify a subcompaction
  // within an instance.
  // For non subcompaction job, it's set to -1.
  int subcompaction_job_id;
  // the smallest input level of the compaction.
  int base_input_level;
  // the output level of the compaction.
  int output_level;

  // Reason to run the compaction
  CompactionReason compaction_reason;

  // Compression algorithm used for output files
  CompressionType compression;

  // Statistics and other additional details on the compaction
  CompactionJobStats stats;

  // Compression algorithm used for blob output files.
  CompressionType blob_compression_type;
};

struct CompactionJobInfo {
  ~CompactionJobInfo() { status.PermitUncheckedError(); }
  // the id of the column family where the compaction happened.
  uint32_t cf_id;
  // the name of the column family where the compaction happened.
  std::string cf_name;
  // the status indicating whether the compaction was successful or not.
  Status status;
  // the id of the thread that completed this compaction job.
  uint64_t thread_id;
  // the job id, which is unique in the same thread.
  int job_id;

  // The execution pool selected for this job, independent of later DB option
  // updates.
  bool use_separate_remote_compaction_pool = false;

  // the number of L0 files in the CF right before and after the compaction
  int num_l0_files;

  // the smallest input level of the compaction.
  int base_input_level;
  // the output level of the compaction.
  int output_level;

  // The following variables contain information about compaction inputs
  // and outputs. A file may appear in both the input and output lists
  // if it was simply moved to a different level. The order of elements
  // is the same across input_files and input_file_infos; similarly, it is
  // the same across output_files and output_file_infos.

  // The names of the compaction input files.
  std::vector<std::string> input_files;

  // Additional information about the compaction input files.
  std::vector<CompactionFileInfo> input_file_infos;

  // The names of the compaction output files.
  std::vector<std::string> output_files;

  // Additional information about the compaction output files.
  std::vector<CompactionFileInfo> output_file_infos;

  // Table properties for input and output tables.
  // The map is keyed by values from input_files and output_files.
  TablePropertiesCollection table_properties;

  // Reason to run the compaction
  CompactionReason compaction_reason;

  // Compression algorithm used for output files
  CompressionType compression;

  // Statistics and other additional details on the compaction
  CompactionJobStats stats;

  // Compression algorithm used for blob output files.
  CompressionType blob_compression_type;

  // Information about blob files created during compaction in Integrated
  // BlobDB.
  std::vector<BlobFileAdditionInfo> blob_file_addition_infos;

  // Information about blob files deleted during compaction in Integrated
  // BlobDB.
  std::vector<BlobFileGarbageInfo> blob_file_garbage_infos;

  // Whether this compaction was aborted via AbortAllCompactions() or
  // AbortCompactions().
  bool aborted = false;
};

struct MemTableInfo {
  // the name of the column family to which memtable belongs
  std::string cf_name;
  // Sequence number of the first element that was inserted
  // into the memtable.
  SequenceNumber first_seqno;
  // Sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  SequenceNumber earliest_seqno;
  // Total number of entries in memtable
  uint64_t num_entries;
  // Total number of deletes in memtable
  uint64_t num_deletes;

  // The newest user-defined timestamps in the memtable. Note this field is
  // only populated when `persist_user_defined_timestamps` is false.
  std::string newest_udt;
};

struct ExternalFileIngestionInfo {
  // the name of the column family
  std::string cf_name;
  // Path of the file outside the DB
  std::string external_file_path;
  // Path of the file inside the DB
  std::string internal_file_path;
  // The global sequence number assigned to keys in this file
  SequenceNumber global_seqno;
  // Table properties of the table being flushed
  TableProperties table_properties;
};

// Result of auto background error recovery
struct BackgroundErrorRecoveryInfo {
  // The original error that triggered the recovery
  Status old_bg_error;

  // The final bg_error after all recovery attempts. Status::OK() means
  // the recovery was successful and the database is fully operational.
  Status new_bg_error;
};

struct IOErrorInfo {
  IOErrorInfo(const IOStatus& _io_status, FileOperationType _operation,
              const std::string& _file_path, size_t _length, uint64_t _offset)
      : io_status(_io_status),
        operation(_operation),
        file_path(_file_path),
        length(_length),
        offset(_offset) {}

  IOStatus io_status;
  FileOperationType operation;
  std::string file_path;
  size_t length;
  uint64_t offset;
};

// EXPERIMENTAL -- under active development, fields may change.
// Point-in-time snapshot of background job pressure for one DB: how busy
// compaction and flush are, and how close the DB is to write-stalling.
struct BackgroundJobPressure {
  // Scheduled compaction work, including remote coordinators and admitted
  // local subcompactions.
  int compaction_scheduled = 0;
  // Running compaction coordinators, including remote coordinators.
  int compaction_running = 0;

  // Per-priority local pool usage. With the separate remote pool enabled,
  // these count queued/running local subcompactions and exclude remote
  // coordinators. Their running sum need not equal compaction_running, which
  // counts coordinators rather than individual local subcompactions.
  int compaction_low_scheduled = 0;
  int compaction_low_running = 0;
  int compaction_bottom_scheduled = 0;
  int compaction_bottom_running = 0;

  // Flush scheduling
  int flush_scheduled = 0;
  int flush_running = 0;

  // How close the DB is to a write stall, as a percentage (0 = healthy,
  // 100 = at stall threshold). Can exceed 100 when already stalling.
  // Max across all column families based on write-stall triggers.
  int write_stall_proximity_pct = 0;
  // Whether RocksDB has activated compaction speedup due to write pressure
  bool compaction_speedup_active = false;

  bool operator==(const BackgroundJobPressure&) const = default;
};

// EventListener class contains a set of callback functions that will
// be called when specific RocksDB event happens such as flush.  It can
// be used as a building block for developing custom features such as
// stats-collector or external compaction algorithm.
//
// IMPORTANT
// Because compaction is needed to resolve a "writes stopped" condition,
// calling or waiting for any blocking DB write function (no_slowdown=false)
// from a compaction-related listener callback can hang RocksDB. For DB
// writes from a callback we recommend a WriteBatch and no_slowdown=true,
// because the WriteBatch can accumulate writes for later in case DB::Write
// returns Status::Incomplete. Similarly, calling CompactRange or similar
// could hang by waiting for a background worker that is occupied until the
// callback returns.
//
// Otherwise, callback functions should not run for an extended period of
// time before the function returns, because this will slow RocksDB.
//
// [Threading] All EventListener callback will be called using the
// actual thread that involves in that specific event.   For example, it
// is the RocksDB background flush thread that does the actual flush to
// call EventListener::OnFlushCompleted().
//
// [Locking] All EventListener callbacks are designed to be called without
// the current thread holding any DB mutex. This is to prevent potential
// deadlock and performance issue when using EventListener callback
// in a complex way.
//
// [Exceptions] Exceptions MUST NOT propagate out of overridden functions into
// RocksDB, because RocksDB is not exception-safe. This could cause undefined
// behavior including data loss, unreported corruption, deadlocks, and more.
class EventListener : public Customizable {
 public:
  static const char* Type() { return "EventListener"; }
  static Status CreateFromString(const ConfigOptions& options,
                                 const std::string& id,
                                 std::shared_ptr<EventListener>* result);
  const char* Name() const override {
    // Since EventListeners did not have a name previously, we will assume
    // an empty name.  Instances should override this method.
    return "";
  }
  // A callback function to RocksDB which will be called whenever a
  // registered RocksDB flushes a file.  The default implementation is
  // no-op.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  virtual void OnFlushCompleted(DB* /*db*/,
                                const FlushJobInfo& /*flush_job_info*/) {}

  // A callback function to RocksDB which will be called before a
  // RocksDB starts to flush memtables.  The default implementation is
  // no-op.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  virtual void OnFlushBegin(DB* /*db*/,
                            const FlushJobInfo& /*flush_job_info*/) {}

  // A callback function to RocksDB which will be called after a manual flush
  // is scheduled. The default implementation is no-op.
  // The size of the `manual_flush_info` vector should only be bigger than 1 if
  // the DB enables atomic flush and has more than 1 column families. Its size
  // should be 1 in all other cases.
  virtual void OnManualFlushScheduled(
      DB* /*db*/, const std::vector<ManualFlushInfo>& /*manual_flush_info*/) {}

  // A callback function for RocksDB which will be called whenever
  // a SST file is deleted.  Different from OnCompactionCompleted and
  // OnFlushCompleted, this callback is designed for external logging
  // service and thus only provide string parameters instead
  // of a pointer to DB.  Applications that build logic basic based
  // on file creations and deletions is suggested to implement
  // OnFlushCompleted and OnCompactionCompleted.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from the
  // returned value.
  virtual void OnTableFileDeleted(const TableFileDeletionInfo& /*info*/) {}

  // A callback function to RocksDB which will be called before a
  // RocksDB starts to compact.  The default implementation is
  // no-op.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  //
  // WART: this callback is skipped during DB shutdown, so a compaction
  // that commits during shutdown may never be observed by listeners.
  virtual void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& /*ci*/) {}

  // A callback function for RocksDB which will be called when a registered
  // RocksDB has finished writing a compaction job's output files, but
  // *before* the manifest write commits the new Version (i.e. before
  // input files are released and output files become visible to readers
  // and the compaction picker). At this point the input files in
  // `ci.input_files` still have `FileMetaData::being_compacted == true`,
  // so no other thread can pick them up for a new compaction.
  //
  // This callback fires strictly between `OnCompactionBegin` and
  // `OnCompactionCompleted` for the same compaction. It is the right
  // place for listeners to clean up their own bookkeeping of which input
  // files they think are being compacted: doing it here, rather than in
  // `OnCompactionCompleted`, avoids a race where `OnCompactionBegin` for
  // the same file fires before `OnCompactionCompleted` for the previous
  // compaction. The default implementation is a no-op.
  //
  // Unlike `OnCompactionCompleted`, `ci.status` does not reflect the
  // final outcome: the manifest commit has not yet occurred, so it
  // does not capture commit failures. Other `CompactionJobInfo` quirks
  // that also apply to `OnCompactionCompleted`: for trivial moves and
  // FIFO deletion-only compactions, `ci.stats` is minimal (no
  // `CompactionJob` runs), and FIFO deletions have no output files.
  //
  // As with `OnCompactionCompleted`, the DB mutex is released for the
  // duration of this callback, but other manifest writers may be waiting,
  // so cheap implementations are strongly preferred.
  //
  // WART: this callback is skipped during DB shutdown, so a compaction
  // that commits during shutdown may never be observed by listeners.
  //
  // @param db a pointer to the rocksdb instance which just compacted a file.
  // @param ci a reference to a CompactionJobInfo struct. 'ci' is released
  //  after this function is returned, and must be copied if it is needed
  //  outside of this function.
  virtual void OnCompactionPreCommit(DB* /*db*/,
                                     const CompactionJobInfo& /*ci*/) {}

  // A callback function for RocksDB which will be called whenever
  // a registered RocksDB compacts a file. The default implementation
  // is a no-op.
  //
  // At the time this callback fires, the manifest commit has applied:
  // output files in `ci.output_files` are live in the DB (visible to
  // readers and the compaction picker, and possibly already picked up
  // by another compaction), and input files in `ci.input_files` have
  // been removed from the live Version with `being_compacted == false`
  // (their on-disk files may still exist if held by snapshots / iterators
  // / pending physical deletion; see `OnTableFileDeleted`). To observe
  // state *before* input files are released, use
  // `OnCompactionPreCommit` instead.
  //
  // Note that this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns. Otherwise, RocksDB may be blocked.
  //
  // WART: this callback is skipped during DB shutdown, so a compaction
  // that commits during shutdown may never be observed by listeners.
  //
  // @param db a pointer to the rocksdb instance which just compacted
  //   a file.
  // @param ci a reference to a CompactionJobInfo struct. 'ci' is released
  //  after this function is returned, and must be copied if it is needed
  //  outside of this function.
  virtual void OnCompactionCompleted(DB* /*db*/,
                                     const CompactionJobInfo& /*ci*/) {}

  // A callback function to RocksDB which will be called before a sub-compaction
  // begins. If a compaction is split to 2 sub-compactions, it will trigger one
  // `OnCompactionBegin()` first, then two `OnSubcompactionBegin()`.
  // If compaction is not split, it will still trigger one
  // `OnSubcompactionBegin()`, as internally, compaction is always handled by
  // sub-compaction. The default implementation is a no-op.
  //
  // Note that this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  //
  // @param ci a reference to a CompactionJobInfo struct, it contains a
  //  `sub_job_id` which is only unique within the specified compaction (which
  //  can be identified by `job_id`). 'ci' is released after this function is
  //  returned, and must be copied if it's needed outside this function.
  //  Note: `table_properties` is not set for sub-compaction, the information
  //  could be got from `OnCompactionBegin()`.
  virtual void OnSubcompactionBegin(const SubcompactionJobInfo& /*si*/) {}

  // A callback function to RocksDB which will be called whenever a
  // sub-compaction completed. The same as `OnSubcompactionBegin()`, if a
  // compaction is split to 2 sub-compactions, it will be triggered twice. If
  // a compaction is not split, it will still be triggered once.
  // The default implementation is a no-op.
  //
  // Note that this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  //
  // @param ci a reference to a CompactionJobInfo struct, it contains a
  //  `sub_job_id` which is only unique within the specified compaction (which
  //  can be identified by `job_id`). 'ci' is released after this function is
  //  returned, and must be copied if it's needed outside this function.
  //  Note: `table_properties` is not set for sub-compaction, the information
  //  could be got from `OnCompactionCompleted()`.
  virtual void OnSubcompactionCompleted(const SubcompactionJobInfo& /*si*/) {}

  // A callback function for RocksDB which will be called whenever
  // a SST file is created.  Different from OnCompactionCompleted and
  // OnFlushCompleted, this callback is designed for external logging
  // service and thus only provide string parameters instead
  // of a pointer to DB.  Applications that build logic basic based
  // on file creations and deletions is suggested to implement
  // OnFlushCompleted and OnCompactionCompleted.
  //
  // Historically it will only be called if the file is successfully created.
  // Now it will also be called on failure case. User can check info.status
  // to see if it succeeded or not.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnTableFileCreated(const TableFileCreationInfo& /*info*/) {}

  // A callback function for RocksDB which will be called before
  // a SST file is being created. It will follow by OnTableFileCreated after
  // the creation finishes.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*info*/) {}

  // A callback function for RocksDB which will be called before
  // a memtable is made immutable.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnMemTableSealed(const MemTableInfo& /*info*/) {}

  // A callback function for RocksDB which will be called before
  // a column family handle is deleted.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  // @param handle is a pointer to the column family handle to be deleted
  // which will become a dangling pointer after the deletion.
  virtual void OnColumnFamilyHandleDeletionStarted(
      ColumnFamilyHandle* /*handle*/) {}

  // A callback function for RocksDB which will be called after an external
  // file is ingested using IngestExternalFile.
  //
  // Note that the this function will run on the same thread as
  // IngestExternalFile(), if this function is blocked, IngestExternalFile()
  // will be blocked from finishing.
  virtual void OnExternalFileIngested(
      DB* /*db*/, const ExternalFileIngestionInfo& /*info*/) {}

  // A callback function for RocksDB which will be called before setting the
  // background error status to a non-OK value. The new background error status
  // is provided in `bg_error` and can be modified by the callback. E.g., a
  // callback can suppress errors by resetting it to Status::OK(), thus
  // preventing the database from entering read-only mode. We do not provide any
  // guarantee when failed flushes/compactions will be rescheduled if the user
  // suppresses an error.
  //
  // Note that this function can run on the same threads as flush, compaction,
  // and user writes. So, it is extremely important not to perform heavy
  // computations or blocking calls in this function.
  virtual void OnBackgroundError(BackgroundErrorReason /* reason */,
                                 Status* /* bg_error */) {}

  // A callback function for RocksDB which will be called whenever a change
  // of superversion triggers a change of the stall conditions.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  virtual void OnStallConditionsChanged(const WriteStallInfo& /*info*/) {}

  // A callback function for RocksDB which will be called whenever a file read
  // operation finishes.
  virtual void OnFileReadFinish(const FileOperationInfo& /* info */) {}

  // A callback function for RocksDB which will be called whenever a file write
  // operation finishes.
  virtual void OnFileWriteFinish(const FileOperationInfo& /* info */) {}

  // A callback function for RocksDB which will be called whenever a file flush
  // operation finishes.
  virtual void OnFileFlushFinish(const FileOperationInfo& /* info */) {}

  // A callback function for RocksDB which will be called whenever a file sync
  // operation finishes.
  virtual void OnFileSyncFinish(const FileOperationInfo& /* info */) {}

  // A callback function for RocksDB which will be called whenever a file
  // rangeSync operation finishes.
  virtual void OnFileRangeSyncFinish(const FileOperationInfo& /* info */) {}

  // A callback function for RocksDB which will be called whenever a file
  // truncate operation finishes.
  virtual void OnFileTruncateFinish(const FileOperationInfo& /* info */) {}

  // A callback function for RocksDB which will be called whenever a file close
  // operation finishes.
  virtual void OnFileCloseFinish(const FileOperationInfo& /* info */) {}

  // If true, the OnFile*Finish functions will be called. If
  // false, then they won't be called.
  virtual bool ShouldBeNotifiedOnFileIO() { return false; }

  // A callback function for RocksDB which will be called just before
  // starting the automatic recovery process for recoverable background
  // errors, such as NoSpace(). The callback can suppress the automatic
  // recovery by setting *auto_recovery to false. The database will then
  // have to be transitioned out of read-only mode by calling DB::Resume()
  virtual void OnErrorRecoveryBegin(BackgroundErrorReason /* reason */,
                                    Status /* bg_error */,
                                    bool* /* auto_recovery */) {}

  // DEPRECATED
  // A callback function for RocksDB which will be called once the database
  // is recovered from read-only mode after an error. When this is called, it
  // means normal writes to the database can be issued and the user can
  // initiate any further recovery actions needed
  virtual void OnErrorRecoveryCompleted(Status old_bg_error) {
    old_bg_error.PermitUncheckedError();
  }

  // A callback function for RocksDB which will be called once the recovery
  // attempt from a background retryable error is completed. The recovery
  // may have been successful or not. In either case, the callback is called
  // with the old and new error. If info.new_bg_error is Status::OK(), that
  // means the recovery succeeded.
  virtual void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& /*info*/) {
  }

  // A callback function for RocksDB which will be called before
  // a blob file is being created. It will follow by OnBlobFileCreated after
  // the creation finishes.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnBlobFileCreationStarted(
      const BlobFileCreationBriefInfo& /*info*/) {}

  // A callback function for RocksDB which will be called whenever
  // a blob file is created.
  // It will be called whether the file is successfully created or not. User can
  // check info.status to see if it succeeded or not.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnBlobFileCreated(const BlobFileCreationInfo& /*info*/) {}

  // A callback function for RocksDB which will be called whenever
  // a blob file is deleted.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnBlobFileDeleted(const BlobFileDeletionInfo& /*info*/) {}

  // A callback function for RocksDB which will be called whenever an IO error
  // happens. ShouldBeNotifiedOnFileIO should be set to true to get a callback.
  virtual void OnIOError(const IOErrorInfo& /*info*/) {}

  // EXPERIMENTAL
  // Called after a flush or compaction background job completes, providing a
  // snapshot of current background job scheduling pressure and write-stall
  // proximity. Fires on the background thread that completed the job, without
  // holding db_mutex_. This callback fires on every completion, even if
  // pressure values have not changed from the previous call.
  // Implementations should not run for an extended period of time before
  // returning, as this blocks RocksDB background work.
  virtual void OnBackgroundJobPressureChanged(
      DB* /*db*/, const BackgroundJobPressure& /*pressure*/) {}

  // A callback function for RocksDB which will be called once when a DB begins
  // shutting down, before background work cancellation publishes the DB's
  // shutdown state. This callback can also fire during cleanup of a failed
  // DB::Open() attempt, in which case the DB pointer refers to the DB instance
  // that failed to open and was never returned to the application.
  //
  // Background work may still be running when this callback fires, and other
  // listener callbacks may still run concurrently or afterward. Implementations
  // should not call blocking DB APIs or run for an extended period of time.
  virtual void OnDBShutdownBegin(DB* /*db*/) {}

  ~EventListener() override = default;
};

}  // namespace ROCKSDB_NAMESPACE
