//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#pragma once

#include <stdint.h>

#include <atomic>
#include <memory>

#include "db/dbformat.h"
#include "db_stress_tool/expected_value.h"
#include "file/file_util.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
// `ExpectedState` provides read/write access to expected values stored in
// `ExpectedState` for every key.
class ExpectedState {
 public:
  explicit ExpectedState(size_t max_key, size_t num_column_families);

  virtual ~ExpectedState() = default;

  // Requires external locking preventing concurrent execution with any other
  // member function.
  virtual Status Open(bool create) = 0;

  // Requires external locking covering all keys in `cf`.
  void ClearColumnFamily(int cf);

  // Requires external locking
  void SetPersistedSeqno(SequenceNumber seqno) {
    persisted_seqno_->store(
        std::max(persisted_seqno_->load(std::memory_order_relaxed), seqno),
        std::memory_order_relaxed);
  }

  // Requires external locking
  SequenceNumber GetPersistedSeqno() {
    return persisted_seqno_->load(std::memory_order_relaxed);
  }

  // Prepare a Put that will be started but not finished yet
  // This is useful for crash-recovery testing when the process may crash
  // before updating the corresponding expected value
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  PendingExpectedValue PreparePut(int cf, int64_t key);

  // Does not requires external locking.
  ExpectedValue Get(int cf, int64_t key);

  // Prepare a Delete that will be started but not finished yet.
  // This is useful for crash-recovery testing when the process may crash
  // before updating the corresponding expected value
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  PendingExpectedValue PrepareDelete(int cf, int64_t key);

  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  PendingExpectedValue PrepareSingleDelete(int cf, int64_t key);

  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`
  // to prevent concurrent write or delete to the same `key`.
  std::vector<PendingExpectedValue> PrepareDeleteRange(int cf,
                                                       int64_t begin_key,
                                                       int64_t end_key);

  // Update the expected value for start of an incomplete write or delete
  // operation on the key assoicated with this expected value
  void Precommit(int cf, int64_t key, const ExpectedValue& value);

  // Requires external locking covering `key` in `cf` to prevent concurrent
  // delete to the same `key`.
  bool Exists(int cf, int64_t key);

  // Sync the `value_base` to the corresponding expected value
  //
  // Requires external locking covering `key` in `cf` or be in single thread
  // to prevent concurrent write or delete to the same `key`
  void SyncPut(int cf, int64_t key, uint32_t value_base);

  // Sync the corresponding expected value to be pending Put
  //
  // Requires external locking covering `key` in `cf` or be in single thread
  // to prevent concurrent write or delete to the same `key`
  void SyncPendingPut(int cf, int64_t key);

  // Sync the corresponding expected value to be deleted
  //
  // Requires external locking covering `key` in `cf` or be in single thread
  // to prevent concurrent write or delete to the same `key`
  void SyncDelete(int cf, int64_t key);

  // Sync the corresponding expected values to be deleted
  //
  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`
  // to prevent concurrent write or delete to the same `key`
  void SyncDeleteRange(int cf, int64_t begin_key, int64_t end_key);

 private:
  // Does not requires external locking.
  std::atomic<uint32_t>& Value(int cf, int64_t key) const {
    return values_[cf * max_key_ + key];
  }

  // Does not requires external locking
  ExpectedValue Load(int cf, int64_t key) const {
    return ExpectedValue(Value(cf, key).load());
  }

  const size_t max_key_;
  const size_t num_column_families_;

 protected:
  size_t GetValuesLen() const {
    return sizeof(std::atomic<uint32_t>) * num_column_families_ * max_key_;
  }

  // Requires external locking preventing concurrent execution with any other
  // member function.
  void Reset();

  std::atomic<uint32_t>* values_;
  std::atomic<SequenceNumber>* persisted_seqno_;
};

// A `FileExpectedState` implements `ExpectedState` backed by a file.
class FileExpectedState : public ExpectedState {
 public:
  explicit FileExpectedState(
      const std::string& expected_state_file_path,
      const std::string& expected_persisted_seqno_file_path, size_t max_key,
      size_t num_column_families);

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open(bool create) override;

 private:
  static Status CreateFile(Env* env, const EnvOptions& options,
                           const std::string& file_path,
                           const std::string& content) {
    std::unique_ptr<WritableFile> wfile;
    Status status = env->NewWritableFile(file_path, &wfile, options);
    if (status.ok()) {
      status = wfile->Append(content);
    }
    return status;
  }

  static Status MemoryMappedFile(
      Env* env, const std::string& file_path,
      std::unique_ptr<MemoryMappedFileBuffer>& memory_mapped_file_buffer,
      std::size_t size) {
    Status status =
        env->NewMemoryMappedFileBuffer(file_path, &memory_mapped_file_buffer);
    if (status.ok()) {
      assert(memory_mapped_file_buffer->GetLen() == size);
    }
    (void)size;
    return status;
  }

  const std::string expected_state_file_path_;
  const std::string expected_persisted_seqno_file_path_;
  std::unique_ptr<MemoryMappedFileBuffer> expected_state_mmap_buffer_;
  std::unique_ptr<MemoryMappedFileBuffer> expected_persisted_seqno_mmap_buffer_;
};

// An `AnonExpectedState` implements `ExpectedState` backed by a memory
// allocation.
class AnonExpectedState : public ExpectedState {
 public:
  explicit AnonExpectedState(size_t max_key, size_t num_column_families);

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open(bool create) override;

 private:
  std::unique_ptr<std::atomic<uint32_t>[]> values_allocation_;
  std::unique_ptr<std::atomic<SequenceNumber>> persisted_seqno_allocation_;
};

// An `ExpectedStateManager` manages data about the expected state of the
// database. It exposes operations for reading and modifying the latest
// expected state.
class ExpectedStateManager {
 public:
  explicit ExpectedStateManager(size_t max_key, size_t num_column_families);

  virtual ~ExpectedStateManager();

  // Requires external locking preventing concurrent execution with any other
  // member function.
  virtual Status Open() = 0;

  // Saves expected values for the current state of `db` and begins tracking
  // changes. Following a successful `SaveAtAndAfter()`, `Restore()` can be
  // called on the same DB, as long as its state does not roll back to before
  // its current state.
  //
  // Requires external locking preventing concurrent execution with any other
  // member function. Furthermore, `db` must not be mutated while this function
  // is executing.
  virtual Status SaveAtAndAfter(DB* db) = 0;

  // Returns true if at least one state of historical expected values can be
  // restored.
  //
  // Requires external locking preventing concurrent execution with any other
  // member function.
  virtual bool HasHistory() = 0;

  // Restores expected values according to the current state of `db`. See
  // `SaveAtAndAfter()` for conditions where this can be called.
  //
  // Requires external locking preventing concurrent execution with any other
  // member function. Furthermore, `db` must not be mutated while this function
  // is executing.
  virtual Status Restore(DB* db) = 0;

  // Requires external locking covering all keys in `cf`.
  void ClearColumnFamily(int cf) { return latest_->ClearColumnFamily(cf); }

  void SetPersistedSeqno(SequenceNumber seqno) {
    return latest_->SetPersistedSeqno(seqno);
  }

  SequenceNumber GetPersistedSeqno() { return latest_->GetPersistedSeqno(); }

  // See ExpectedState::PreparePut()
  PendingExpectedValue PreparePut(int cf, int64_t key) {
    return latest_->PreparePut(cf, key);
  }

  // See ExpectedState::Get()
  ExpectedValue Get(int cf, int64_t key) { return latest_->Get(cf, key); }

  // See ExpectedState::PrepareDelete()
  PendingExpectedValue PrepareDelete(int cf, int64_t key) {
    return latest_->PrepareDelete(cf, key);
  }

  // See ExpectedState::PrepareSingleDelete()
  PendingExpectedValue PrepareSingleDelete(int cf, int64_t key) {
    return latest_->PrepareSingleDelete(cf, key);
  }

  // See ExpectedState::PrepareDeleteRange()
  std::vector<PendingExpectedValue> PrepareDeleteRange(int cf,
                                                       int64_t begin_key,
                                                       int64_t end_key) {
    return latest_->PrepareDeleteRange(cf, begin_key, end_key);
  }

  // See ExpectedState::Exists()
  bool Exists(int cf, int64_t key) { return latest_->Exists(cf, key); }

  // See ExpectedState::SyncPut()
  void SyncPut(int cf, int64_t key, uint32_t value_base) {
    return latest_->SyncPut(cf, key, value_base);
  }

  // See ExpectedState::SyncPendingPut()
  void SyncPendingPut(int cf, int64_t key) {
    return latest_->SyncPendingPut(cf, key);
  }

  // See ExpectedState::SyncDelete()
  void SyncDelete(int cf, int64_t key) { return latest_->SyncDelete(cf, key); }

  // See ExpectedState::SyncDeleteRange()
  void SyncDeleteRange(int cf, int64_t begin_key, int64_t end_key) {
    return latest_->SyncDeleteRange(cf, begin_key, end_key);
  }

 protected:
  const size_t max_key_;
  const size_t num_column_families_;
  std::unique_ptr<ExpectedState> latest_;
};

// A `FileExpectedStateManager` implements an `ExpectedStateManager` backed by
// a directory of files containing data about the expected state of the
// database.
class FileExpectedStateManager : public ExpectedStateManager {
 public:
  explicit FileExpectedStateManager(size_t max_key, size_t num_column_families,
                                    std::string expected_state_dir_path);

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open() override;

  // See `ExpectedStateManager::SaveAtAndAfter()` API doc.
  //
  // This implementation makes a copy of "LATEST.state" into
  // "<current seqno>.state", and starts a trace in "<current seqno>.trace".
  // Due to using external files, a following `Restore()` can happen even
  // from a different process.
  Status SaveAtAndAfter(DB* db) override;

  // See `ExpectedStateManager::HasHistory()` API doc.
  bool HasHistory() override;

  // See `ExpectedStateManager::Restore()` API doc.
  //
  // Say `db->GetLatestSequenceNumber()` was `a` last time `SaveAtAndAfter()`
  // was called and now it is `b`. Then this function replays `b - a` write
  // operations from "`a`.trace" onto "`a`.state", and then copies the resulting
  // file into "LATEST.state". This relies on "`a`.trace" being an ordered
  // superset of writes that could survive recovery: missing trace entries are
  // fatal because replay length comes from DB sequence space, while extra tail
  // entries are tolerated.
  Status Restore(DB* db) override;

 private:
  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Clean();

  std::string GetTempPathForFilename(const std::string& filename);
  std::string GetPathForFilename(const std::string& filename);

  static const std::string kLatestBasename;
  static const std::string kStateFilenameSuffix;
  static const std::string kTraceFilenameSuffix;
  static const std::string kPersistedSeqnoBasename;
  static const std::string kPersistedSeqnoFilenameSuffix;
  static const std::string kTempFilenamePrefix;
  static const std::string kTempFilenameSuffix;

  const std::string expected_state_dir_path_;
  SequenceNumber saved_seqno_ = kMaxSequenceNumber;
};

// An `AnonExpectedStateManager` implements an `ExpectedStateManager` backed by
// a memory allocation containing data about the expected state of the database.
class AnonExpectedStateManager : public ExpectedStateManager {
 public:
  explicit AnonExpectedStateManager(size_t max_key, size_t num_column_families);

  // See `ExpectedStateManager::SaveAtAndAfter()` API doc.
  //
  // This implementation returns `Status::NotSupported` since we do not
  // currently have a need to keep history of expected state within a process.
  Status SaveAtAndAfter(DB* /* db */) override {
    return Status::NotSupported();
  }

  // See `ExpectedStateManager::HasHistory()` API doc.
  bool HasHistory() override { return false; }

  // See `ExpectedStateManager::Restore()` API doc.
  //
  // This implementation returns `Status::NotSupported` since we do not
  // currently have a need to keep history of expected state within a process.
  Status Restore(DB* /* db */) override { return Status::NotSupported(); }

  // Requires external locking preventing concurrent execution with any other
  // member function.
  Status Open() override;
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
