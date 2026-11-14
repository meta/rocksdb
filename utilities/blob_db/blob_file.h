//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <unordered_set>

#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "file/random_access_file_reader.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
namespace blob_db {

class BlobDBImpl;

class BlobFile {
  friend class BlobDBImpl;
  friend struct BlobFileComparator;
  friend struct BlobFileComparatorTTL;
  friend class BlobIndexCompactionFilterBase;
  friend class BlobIndexCompactionFilterGC;

 private:
  // access to parent
  const BlobDBImpl* parent_{nullptr};

  // path to blob directory
  std::string path_to_dir_;

  // the id of the file.
  // the above 2 are created during file creation and never changed
  // after that
  uint64_t file_number_{0};

  // The file numbers of the SST files whose oldest blob file reference
  // points to this blob file.
  std::unordered_set<uint64_t> linked_sst_files_;

  // Info log.
  Logger* info_log_{nullptr};

  // Column family id.
  uint32_t column_family_id_{std::numeric_limits<uint32_t>::max()};

  // If true, the keys in this file all has TTL. Otherwise all keys don't
  // have TTL.
  bool has_ttl_{false};

  // TTL range of blobs in the file.
  ExpirationRange expiration_range_;

  // number of blobs in the file
  std::atomic<uint64_t> blob_count_{0};

  // size of the file
  std::atomic<uint64_t> file_size_{0};

  BlobLogHeader header_;

  // closed_ = true implies the file is no more mutable
  // no more blobs will be appended and the footer has been written out
  std::atomic<bool> closed_{false};

  // The latest sequence number when the file was closed/made immutable.
  SequenceNumber immutable_sequence_{0};

  // Whether the file was marked obsolete (due to either TTL or GC).
  // obsolete_ still needs to do iterator/snapshot checks
  std::atomic<bool> obsolete_{false};

  // The last sequence number by the time the file marked as obsolete.
  // Data in this file is visible to a snapshot taken before the sequence.
  SequenceNumber obsolete_sequence_{0};

  // Sequential/Append writer for blobs
  std::shared_ptr<BlobLogWriter> log_writer_;

  // random access file reader for GET calls
  std::shared_ptr<RandomAccessFileReader> ra_file_reader_;

  // This Read-Write mutex is per file specific and protects
  // all the datastructures
  mutable port::RWMutex mutex_;

  // time when the random access reader was last created.
  std::atomic<std::int64_t> last_access_{-1};

  bool header_valid_{false};

  bool footer_valid_{false};

 public:
  BlobFile() = default;

  BlobFile(const BlobDBImpl* parent, const std::string& bdir, uint64_t fnum,
           Logger* info_log);

  BlobFile(const BlobDBImpl* parent, const std::string& bdir, uint64_t fnum,
           Logger* info_log, uint32_t column_family_id, bool has_ttl,
           const ExpirationRange& expiration_range);

  ~BlobFile();

  // Returns log file's absolute pathname.
  std::string PathName() const;

  // Primary identifier for blob file.
  // once the file is created, this never changes
  uint64_t BlobFileNumber() const { return file_number_; }

  // Get the set of SST files whose oldest blob file reference points to
  // this file.
  const std::unordered_set<uint64_t>& GetLinkedSstFiles() const {
    return linked_sst_files_;
  }

  // Link an SST file whose oldest blob file reference points to this file.
  void LinkSstFile(uint64_t sst_file_number) {
    assert(linked_sst_files_.find(sst_file_number) == linked_sst_files_.end());
    linked_sst_files_.insert(sst_file_number);
  }

  // Unlink an SST file whose oldest blob file reference points to this file.
  // Returns true if the SST was found and unlinked, false otherwise.
  bool UnlinkSstFile(uint64_t sst_file_number) {
    auto it = linked_sst_files_.find(sst_file_number);
    if (it == linked_sst_files_.end()) {
      return false;
    }
    linked_sst_files_.erase(it);
    return true;
  }

  // the following functions are atomic, and don't need
  // read lock
  uint64_t BlobCount() const {
    return blob_count_.load(std::memory_order_acquire);
  }

  std::string DumpState() const;

  // if the file is not taking any more appends.
  bool Immutable() const { return closed_.load(); }

  // Mark the file as immutable.
  // REQUIRES: write lock held, or access from single thread (on DB open).
  void MarkImmutable(SequenceNumber sequence) {
    closed_ = true;
    immutable_sequence_ = sequence;
  }

  SequenceNumber GetImmutableSequence() const {
    assert(Immutable());
    return immutable_sequence_;
  }

  // Whether the file was marked obsolete (due to either TTL or GC).
  bool Obsolete() const {
    assert(Immutable() || !obsolete_.load());
    return obsolete_.load();
  }

  // Mark file as obsolete (due to either TTL or GC). The file is not visible to
  // snapshots with sequence greater or equal to the given sequence.
  void MarkObsolete(SequenceNumber sequence);

  SequenceNumber GetObsoleteSequence() const {
    assert(Obsolete());
    return obsolete_sequence_;
  }

  Status Fsync(const WriteOptions& write_options);

  uint64_t GetFileSize() const {
    return file_size_.load(std::memory_order_acquire);
  }

  // All Get functions which are not atomic, will need ReadLock on the mutex

  const ExpirationRange& GetExpirationRange() const {
    return expiration_range_;
  }

  void ExtendExpirationRange(uint64_t expiration) {
    expiration_range_.first = std::min(expiration_range_.first, expiration);
    expiration_range_.second = std::max(expiration_range_.second, expiration);
  }

  bool HasTTL() const { return has_ttl_; }

  void SetHasTTL(bool has_ttl) { has_ttl_ = has_ttl; }

  std::shared_ptr<BlobLogWriter> GetWriter() const { return log_writer_; }

  // Read blob file header and footer. Return corruption if file header is
  // malform or incomplete. If footer is malform or incomplete, set
  // footer_valid_ to false and return Status::OK.
  Status ReadMetadata(const std::shared_ptr<FileSystem>& fs,
                      const FileOptions& file_options);

  Status GetReader(Env* env, const FileOptions& file_options,
                   std::shared_ptr<RandomAccessFileReader>* reader,
                   bool* fresh_open);

 private:
  Status ReadFooter(BlobLogFooter* footer);

  Status WriteFooterAndCloseLocked(const WriteOptions& write_options,
                                   SequenceNumber sequence);

  void CloseRandomAccessLocked();

  // this is used, when you are reading only the footer of a
  // previously closed file
  Status SetFromFooterLocked(const BlobLogFooter& footer);

  void set_expiration_range(const ExpirationRange& expiration_range) {
    expiration_range_ = expiration_range;
  }

  // The following functions are atomic, and don't need locks
  void SetFileSize(uint64_t fs) { file_size_ = fs; }

  void SetBlobCount(uint64_t bc) { blob_count_ = bc; }

  void BlobRecordAdded(uint64_t record_size) {
    ++blob_count_;
    file_size_ += record_size;
  }
};
}  // namespace blob_db
}  // namespace ROCKSDB_NAMESPACE
