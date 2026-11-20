//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <condition_variable>
#include <limits>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/db_iter.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "util/mutexlock.h"
#include "util/timer_queue.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/blob_db/blob_file.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class ColumnFamilyHandle;
class ColumnFamilyData;
class SystemClock;

struct FlushJobInfo;

namespace blob_db {

struct BlobCompactionContext;
struct BlobCompactionContextGC;
class BlobDBImpl;
class BlobFile;

// Comparator to sort "TTL" aware Blob files based on the lower value of
// TTL range.
struct BlobFileComparatorTTL {
  bool operator()(const std::shared_ptr<BlobFile>& lhs,
                  const std::shared_ptr<BlobFile>& rhs) const;
};

struct BlobFileComparator {
  bool operator()(const std::shared_ptr<BlobFile>& lhs,
                  const std::shared_ptr<BlobFile>& rhs) const;
};

/**
 * The implementation class for BlobDB. It manages the blob logs, which
 * are sequentially written files. Blob logs can be of the TTL or non-TTL
 * varieties; the former are cleaned up when they expire, while the latter
 * are (optionally) garbage collected.
 */
class BlobDBImpl : public BlobDB {
  friend class BlobFile;
  friend class BlobDBIterator;
  friend class BlobDBListener;
  friend class BlobDBListenerGC;
  friend class BlobIndexCompactionFilterBase;
  friend class BlobIndexCompactionFilterGC;

 public:
  // sanity check task
  static constexpr uint32_t kSanityCheckPeriodMillisecs = 20 * 60 * 1000;

  // how many random access open files can we tolerate
  static constexpr uint32_t kOpenFilesTrigger = 100;

  // how often to schedule reclaim open files.
  static constexpr uint32_t kReclaimOpenFilesPeriodMillisecs = 1 * 1000;

  // how often to schedule delete obs files periods
  static constexpr uint32_t kDeleteObsoleteFilesPeriodMillisecs = 10 * 1000;

  // how often to schedule expired files eviction.
  static constexpr uint32_t kEvictExpiredFilesPeriodMillisecs = 10 * 1000;

  using BlobDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  using BlobDB::Get;
  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             PinnableSlice* value, std::string* timestamp) override;

  using BlobDB::GetWithMetadata;
  Status GetWithMetadata(const ReadOptions& _read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         PinnableSlice* value,
                         OutputMetadata* output_metadata) override;

  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             PinnableSlice* value, uint64_t* expiration) override;

  using BlobDB::NewIterator;
  Iterator* NewIterator(const ReadOptions& read_options) override;

  using BlobDB::NewIterators;
  Status NewIterators(
      const ReadOptions& /*read_options*/,
      const std::vector<ColumnFamilyHandle*>& /*column_families*/,
      std::vector<Iterator*>* /*iterators*/) override {
    return Status::NotSupported("Not implemented");
  }
  Status NewIterators(
      const std::vector<ReadOptions>& /*read_options*/,
      const std::vector<ColumnFamilyHandle*>& /*column_families*/,
      std::vector<Iterator*>* /*iterators*/) override {
    return Status::NotSupported("Not implemented");
  }

  using BlobDB::MultiGet;
  void MultiGet(const ReadOptions& _read_options, size_t num_keys,
                ColumnFamilyHandle** column_families, const Slice* keys,
                PinnableSlice* values, std::string* timestamps,
                Status* statuses, const bool sorted_input) override;

  using BlobDB::MultiGetWithMetadata;
  void MultiGetWithMetadata(const ReadOptions& _read_options, size_t num_keys,
                            ColumnFamilyHandle* const* column_families,
                            const Slice* keys, PinnableSlice* values,
                            Status* statuses,
                            MultiGetOutputMetadata* output_metadata,
                            const bool sorted_input) override;

  using BlobDB::Write;
  Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  Status Close() override;

  using BlobDB::PutWithTTL;
  Status PutWithTTL(const WriteOptions& options, const Slice& key,
                    const Slice& value, uint64_t ttl) override;

  using BlobDB::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override;

  BlobDBImpl(const std::string& dbname, const BlobDBOptions& bdb_options,
             const DBOptions& db_options,
             const ColumnFamilyOptions& cf_options);

  Status DisableFileDeletions() override;

  Status EnableFileDeletions() override;

  Status GetLiveFiles(std::vector<std::string>&, uint64_t* manifest_file_size,
                      bool flush_memtable = true) override;
  void GetLiveFilesMetaData(std::vector<LiveFileMetaData>*) override;
  Status GetLiveFilesStorageInfo(
      const LiveFilesStorageInfoOptions& opts,
      std::vector<LiveFileStorageInfo>* files) override;

  ~BlobDBImpl() override;

  Status Open(std::vector<ColumnFamilyHandle*>* handles);

  // Common part of the two GetCompactionContext methods below.
  // REQUIRES: read lock on mutex_
  void GetCompactionContextCommon(BlobCompactionContext* context);

  void GetCompactionContext(BlobCompactionContext* context);
  void GetCompactionContext(BlobCompactionContext* context,
                            BlobCompactionContextGC* context_gc);

#ifndef NDEBUG
  Status TEST_GetBlobValue(const Slice& key, const Slice& index_entry,
                           PinnableSlice* value);

  void TEST_AddDummyBlobFile(uint64_t blob_file_number,
                             SequenceNumber immutable_sequence);

  std::vector<std::shared_ptr<BlobFile>> TEST_GetBlobFiles() const;

  std::vector<std::shared_ptr<BlobFile>> TEST_GetLiveImmNonTTLFiles() const;

  std::vector<std::shared_ptr<BlobFile>> TEST_GetObsoleteFiles() const;

  Status TEST_CloseBlobFile(std::shared_ptr<BlobFile>& bfile);

  void TEST_ObsoleteBlobFile(std::shared_ptr<BlobFile>& blob_file,
                             SequenceNumber obsolete_seq = 0,
                             bool update_size = true);

  void TEST_DeleteObsoleteFiles();

  void TEST_EvictExpiredFiles();

  uint64_t TEST_live_sst_size();

  const std::string& TEST_blob_dir() const { return blob_dir_; }

  void TEST_InitializeBlobFileToSstMapping(
      const std::vector<LiveFileMetaData>& live_files);

  void TEST_ProcessFlushJobInfo(const FlushJobInfo& info);

  void TEST_ProcessCompactionJobInfo(const CompactionJobInfo& info);

#endif  //  !NDEBUG

 private:
  class BlobInserter;

  // Create a snapshot if there isn't one in read options.
  // Return true if a snapshot is created.
  bool SetSnapshotIfNeeded(ReadOptions* read_options);

  Status SyncBlobFiles(const WriteOptions& write_options);

  Status GetImpl(const ReadOptions& read_options,
                 ColumnFamilyHandle* column_family, const Slice& key,
                 PinnableSlice* value, uint64_t* expiration = nullptr,
                 bool* newer_version_present = nullptr);

  void MultiGetImpl(const ReadOptions& read_options, size_t num_keys,
                    ColumnFamilyHandle* const* column_families,
                    const Slice* keys, PinnableSlice* values,
                    std::string* timestamps, Status* statuses,
                    std::vector<uint8_t>* newer_version_present = nullptr);

  Status GetBlobValue(const Slice& key, const Slice& index_entry,
                      PinnableSlice* value, uint64_t* expiration = nullptr);

  Status GetRawBlobFromFile(const Slice& key, uint64_t file_number,
                            uint64_t offset, uint64_t size,
                            PinnableSlice* value);

  // Close a file by appending a footer, and removes file from open files list.
  // REQUIRES: lock held on write_mutex_, write lock held on both the db mutex_
  // and the blob file's mutex_. If called on a blob file which is visible only
  // to a single thread (like in the case of new files written during
  // compaction/GC), the locks on write_mutex_ and the blob file's mutex_ can be
  // avoided.
  Status CloseBlobFile(const WriteOptions& write_options,
                       std::shared_ptr<BlobFile> bfile);

  // Close a file if its size exceeds blob_file_size
  // REQUIRES: lock held on write_mutex_.
  Status CloseBlobFileIfNeeded(const WriteOptions& write_options,
                               std::shared_ptr<BlobFile>& bfile);

  // Mark file as obsolete and move the file to obsolete file list.
  //
  // REQUIRED: hold write lock of mutex_ or during DB open.
  void ObsoleteBlobFile(std::shared_ptr<BlobFile> blob_file,
                        SequenceNumber obsolete_seq, bool update_size);

  Status PutBlobValue(const WriteOptions& options, const Slice& key,
                      const Slice& value, uint64_t expiration,
                      WriteBatch* batch);

  Status AppendBlob(const WriteOptions& write_options,
                    const std::shared_ptr<BlobFile>& bfile,
                    const std::string& headerbuf, const Slice& key,
                    const Slice& value, uint64_t expiration,
                    std::string* index_entry);

  // Create a new blob file and associated writer.
  Status CreateBlobFileAndWriter(const WriteOptions& write_options,
                                 bool has_ttl,
                                 const ExpirationRange& expiration_range,
                                 const std::string& reason,
                                 std::shared_ptr<BlobFile>* blob_file,
                                 std::shared_ptr<BlobLogWriter>* writer);

  // Get the open non-TTL blob log file, or create a new one if no such file
  // exists.
  Status SelectBlobFile(const WriteOptions& write_options,
                        std::shared_ptr<BlobFile>* blob_file);

  // Get the open TTL blob log file for a certain expiration, or create a new
  // one if no such file exists.
  Status SelectBlobFileTTL(const WriteOptions& write_options,
                           uint64_t expiration,
                           std::shared_ptr<BlobFile>* blob_file);

  std::shared_ptr<BlobFile> FindBlobFileLocked(uint64_t expiration) const;

  // periodic sanity check. Bunch of checks
  std::pair<bool, int64_t> SanityCheck(bool aborted);

  // Delete files that have been marked obsolete (either because of TTL
  // or GC). Check whether any snapshots exist which refer to the same.
  std::pair<bool, int64_t> DeleteObsoleteFiles(bool aborted);

  // if the number of open files, approaches ULIMIT's this
  // task will close random readers, which are kept around for
  // efficiency
  std::pair<bool, int64_t> ReclaimOpenFiles(bool aborted);

  // Evict expired blob files from the TTL queue.
  std::pair<bool, int64_t> EvictExpiredFiles(bool aborted);

  // Adds the background tasks to the timer queue
  void StartBackgroundTasks();

  // add a new Blob File
  std::shared_ptr<BlobFile> NewBlobFile(bool has_ttl,
                                        const ExpirationRange& expiration_range,
                                        const std::string& reason);

  // Register a new blob file.
  // REQUIRES: write lock on mutex_.
  void RegisterBlobFile(std::shared_ptr<BlobFile> blob_file);

  // collect all the blob log files from the blob directory
  Status GetAllBlobFiles(std::set<uint64_t>* file_numbers);

  // Open all blob files found in blob_dir.
  Status OpenAllBlobFiles();

  // Link an SST to a blob file. Comes in locking and non-locking varieties
  // (the latter is used during Open).
  template <typename Linker>
  void LinkSstToBlobFileImpl(uint64_t sst_file_number,
                             uint64_t blob_file_number, Linker linker);

  void LinkSstToBlobFile(uint64_t sst_file_number, uint64_t blob_file_number);

  void LinkSstToBlobFileNoLock(uint64_t sst_file_number,
                               uint64_t blob_file_number);

  // Unlink an SST from a blob file.
  void UnlinkSstFromBlobFile(uint64_t sst_file_number,
                             uint64_t blob_file_number);

  // Initialize the mapping between blob files and SSTs during Open.
  void InitializeBlobFileToSstMapping(
      const std::vector<LiveFileMetaData>& live_files);

  // Update the mapping between blob files and SSTs after a flush and mark
  // any unneeded blob files obsolete.
  void ProcessFlushJobInfo(const FlushJobInfo& info);

  // Update the mapping between blob files and SSTs after a compaction and
  // mark any unneeded blob files obsolete.
  void ProcessCompactionJobInfo(const CompactionJobInfo& info);

  // Mark an immutable non-TTL blob file obsolete assuming it has no more SSTs
  // linked to it, and all memtables from before the blob file became immutable
  // have been flushed. Note: should only be called if the condition holds for
  // all lower-numbered non-TTL blob files as well.
  bool MarkBlobFileObsoleteIfNeeded(const std::shared_ptr<BlobFile>& blob_file,
                                    SequenceNumber obsolete_seq);

  // Mark all immutable non-TTL blob files that aren't needed by any SSTs as
  // obsolete. Comes in two varieties; the version used during Open need not
  // worry about locking or snapshots.
  template <class Functor>
  void MarkUnreferencedBlobFilesObsoleteImpl(Functor mark_if_needed);

  void MarkUnreferencedBlobFilesObsolete();
  void MarkUnreferencedBlobFilesObsoleteDuringOpen();

  void UpdateLiveSSTSize();

  // Check if writing blob_size bytes would exceed max_db_size limit.
  // Returns Status::NoSpace() if limit would be exceeded.
  Status CheckDbSizeLimit(uint64_t blob_size);

  Status GetBlobFileReader(const std::shared_ptr<BlobFile>& blob_file,
                           std::shared_ptr<RandomAccessFileReader>* reader);

  // hold write mutex on file and call.
  // Close the above Random Access reader
  void CloseRandomAccessLocked(const std::shared_ptr<BlobFile>& bfile);

  // hold write mutex on file and call
  // creates a sequential (append) writer for this blobfile
  Status CreateWriterLocked(const std::shared_ptr<BlobFile>& bfile);

  // returns a BlobLogWriter object for the file. If writer is not
  // already present, creates one. Needs Write Mutex to be held
  Status CheckOrCreateWriterLocked(const std::shared_ptr<BlobFile>& blob_file,
                                   std::shared_ptr<BlobLogWriter>* writer);

  // checks if there is no snapshot which is referencing the
  // blobs
  bool VisibleToActiveSnapshot(const std::shared_ptr<BlobFile>& file);

  uint64_t EpochNow() { return clock_->NowMicros() / 1000000; }

  Status CloseImpl();

  // name of the database directory
  std::string dbname_;

  // the base DB
  DBImpl* db_impl_;
  Env* env_;
  SystemClock* clock_;
  // the options that govern the behavior of Blob Storage
  BlobDBOptions bdb_options_;
  DBOptions db_options_;
  ColumnFamilyOptions cf_options_;
  FileOptions file_options_;

  // Raw pointer of statistic. db_options_ has a std::shared_ptr to hold
  // ownership.
  Statistics* statistics_;

  // by default this is "blob_dir" under dbname_
  // but can be configured
  std::string blob_dir_;

  // pointer to directory
  std::unique_ptr<FSDirectory> dir_ent_;

  // Read Write Mutex, which protects all the data structures
  // HEAVILY TRAFFICKED
  mutable port::RWMutex mutex_;

  // Writers has to hold write_mutex_ before writing.
  mutable port::Mutex write_mutex_;

  // counter for blob file number
  std::atomic<uint64_t> next_file_number_;

  // entire metadata of all the BLOB files memory
  std::map<uint64_t, std::shared_ptr<BlobFile>> blob_files_;

  // All live immutable non-TTL blob files.
  std::map<uint64_t, std::shared_ptr<BlobFile>> live_imm_non_ttl_blob_files_;

  // The largest sequence number that has been flushed.
  SequenceNumber flush_sequence_;

  // opened non-TTL blob file.
  std::shared_ptr<BlobFile> open_non_ttl_file_;

  // all the blob files which are currently being appended to based
  // on variety of incoming TTL's
  std::set<std::shared_ptr<BlobFile>, BlobFileComparatorTTL> open_ttl_files_;

  // Flag to check whether Close() has been called on this DB
  bool closed_;

  // timer based queue to execute tasks
  TimerQueue tqueue_;

  // number of files opened for random access/GET
  // counter is used to monitor and close excess RA files.
  std::atomic<uint32_t> open_file_count_;

  // Total size of all live blob files (i.e. exclude obsolete files).
  std::atomic<uint64_t> total_blob_size_;

  // total size of SST files.
  std::atomic<uint64_t> live_sst_size_;

  std::list<std::shared_ptr<BlobFile>> obsolete_files_;

  // DeleteObsoleteFiles, DiableFileDeletions and EnableFileDeletions block
  // on the mutex to avoid contention.
  //
  // While DeleteObsoleteFiles hold both mutex_ and delete_file_mutex_, note
  // the difference. mutex_ only needs to be held when access the
  // data-structure, and delete_file_mutex_ needs to be held the whole time
  // during DeleteObsoleteFiles to avoid being run simultaneously with
  // DisableFileDeletions.
  //
  // If both of mutex_ and delete_file_mutex_ needs to be held, it is adviced
  // to hold delete_file_mutex_ first to avoid deadlock.
  mutable port::Mutex delete_file_mutex_;

  // Each call of DisableFileDeletions will increase disable_file_deletion_
  // by 1. EnableFileDeletions will either decrease the count by 1 or reset
  // it to zero, depending on the force flag.
  //
  // REQUIRES: access with delete_file_mutex_ held.
  int disable_file_deletions_ = 0;
};

}  // namespace blob_db
}  // namespace ROCKSDB_NAMESPACE
