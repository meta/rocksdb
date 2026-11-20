//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom Env to keep track of the state of a filesystem as of
// the last "sync". It then checks for data loss errors by purposely dropping
// file data (or entire files) not protected by a "sync".

#pragma once

#include <map>
#include <set>
#include <string>
#include <utility>

#include "file/filename.h"
#include "rocksdb/env.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
class Random;
class TestWritableFile;
class FaultInjectionTestEnv;

struct FileState {
  std::string filename_;
  ssize_t pos_;
  ssize_t pos_at_last_sync_;
  ssize_t pos_at_last_flush_;

  explicit FileState(const std::string& filename)
      : filename_(filename),
        pos_(-1),
        pos_at_last_sync_(-1),
        pos_at_last_flush_(-1) {}

  FileState() : pos_(-1), pos_at_last_sync_(-1), pos_at_last_flush_(-1) {}

  bool IsFullySynced() const { return pos_ <= 0 || pos_ == pos_at_last_sync_; }

  Status DropUnsyncedData(Env* env) const;

  Status DropRandomUnsyncedData(Env* env, Random* rand) const;
};

class TestRandomAccessFile : public RandomAccessFile {
 public:
  TestRandomAccessFile(std::unique_ptr<RandomAccessFile>&& target,
                       FaultInjectionTestEnv* env);

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override;

  Status Prefetch(uint64_t offset, size_t n) override;

  Status MultiRead(ReadRequest* reqs, size_t num_reqs) override;

  Status GetFileSize(uint64_t* file_size) override;

 private:
  std::unique_ptr<RandomAccessFile> target_;
  FaultInjectionTestEnv* env_;
};

// A wrapper around WritableFileWriter* file
// is written to or sync'ed.
class TestWritableFile : public WritableFile {
 public:
  explicit TestWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>&& f,
                            FaultInjectionTestEnv* env);
  ~TestWritableFile() override;
  Status Append(const Slice& data) override;
  Status Append(const Slice& data,
                const DataVerificationInfo& /*verification_info*/) override {
    return Append(data);
  }
  Status Truncate(uint64_t size) override { return target_->Truncate(size); }
  Status Close() override;
  Status Flush() override;
  Status Sync() override;
  bool IsSyncThreadSafe() const override { return true; }
  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    return target_->PositionedAppend(data, offset);
  }
  Status PositionedAppend(
      const Slice& data, uint64_t offset,
      const DataVerificationInfo& /*verification_info*/) override {
    return PositionedAppend(data, offset);
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  uint64_t GetFileSize() final { return target_->GetFileSize(); }

 private:
  FileState state_;
  std::unique_ptr<WritableFile> target_;
  bool writable_file_opened_;
  FaultInjectionTestEnv* env_;
};

// A wrapper around WritableFileWriter* file
// is written to or sync'ed.
class TestRandomRWFile : public RandomRWFile {
 public:
  explicit TestRandomRWFile(const std::string& fname,
                            std::unique_ptr<RandomRWFile>&& f,
                            FaultInjectionTestEnv* env);
  ~TestRandomRWFile() override;
  Status Write(uint64_t offset, const Slice& data) override;
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override;
  Status Close() override;
  Status Flush() override;
  Status Sync() override;
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }

 private:
  std::unique_ptr<RandomRWFile> target_;
  bool file_opened_;
  FaultInjectionTestEnv* env_;
};

class TestDirectory : public Directory {
 public:
  explicit TestDirectory(FaultInjectionTestEnv* env, std::string dirname,
                         Directory* dir)
      : env_(env), dirname_(std::move(dirname)), dir_(dir) {}
  ~TestDirectory() override = default;

  Status Fsync() override;
  Status Close() override;

 private:
  FaultInjectionTestEnv* env_;
  std::string dirname_;
  std::unique_ptr<Directory> dir_;
};

class FaultInjectionTestEnv : public EnvWrapper {
 public:
  explicit FaultInjectionTestEnv(Env* base)
      : EnvWrapper(base), filesystem_active_(true) {}
  // No copying allowed
  FaultInjectionTestEnv(const FaultInjectionTestEnv&) = delete;
  FaultInjectionTestEnv& operator=(const FaultInjectionTestEnv&) = delete;
  FaultInjectionTestEnv(FaultInjectionTestEnv&&) = delete;
  FaultInjectionTestEnv& operator=(FaultInjectionTestEnv&&) = delete;
  ~FaultInjectionTestEnv() override { error_.PermitUncheckedError(); }

  static const char* kClassName() { return "FaultInjectionTestEnv"; }
  const char* Name() const override { return kClassName(); }

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override;

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& soptions) override;

  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& soptions) override;

  Status SyncFile(const std::string& fname, const EnvOptions& options,
                  bool use_fsync) override;

  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& soptions) override;

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& soptions) override;

  Status DeleteFile(const std::string& f) override;

  Status RenameFile(const std::string& s, const std::string& t) override;

  Status LinkFile(const std::string& s, const std::string& t) override;

// Undef to eliminate clash on Windows
#undef GetFreeSpace
  Status GetFreeSpace(const std::string& path, uint64_t* disk_free) override {
    bool inactive_no_space = false;
    {
      MutexLock l(&mutex_);
      inactive_no_space = !filesystem_active_ &&
                          error_.subcode() == IOStatus::SubCode::kNoSpace;
    }
    if (inactive_no_space) {
      *disk_free = 0;
      return Status::OK();
    } else {
      return target()->GetFreeSpace(path, disk_free);
    }
  }

  void WritableFileClosed(const FileState& state);

  void WritableFileSynced(const FileState& state);

  void WritableFileAppended(const FileState& state);

  // For every file that is not fully synced, make a call to `func` with
  // FileState of the file as the parameter.
  Status DropFileData(const std::function<Status(Env*, FileState)>& func);

  Status DropUnsyncedFileData();

  Status DropRandomUnsyncedFileData(Random* rnd);

  Status DeleteFilesCreatedAfterLastDirSync();

  void ResetState();

  void UntrackFile(const std::string& f);

  void SyncDir(const std::string& dirname) {
    MutexLock l(&mutex_);
    dir_to_new_files_since_last_sync_.erase(dirname);
  }

  // Setting the filesystem to inactive is the test equivalent to simulating a
  // system reset. Setting to inactive will freeze our saved filesystem state so
  // that it will stop being recorded. It can then be reset back to the state at
  // the time of the reset.
  bool IsFilesystemActive() {
    MutexLock l(&mutex_);
    return filesystem_active_;
  }
  void SetFilesystemActiveNoLock(
      bool active, const Status& error = Status::Corruption("Not active")) {
    error.PermitUncheckedError();
    filesystem_active_ = active;
    if (!active) {
      error_ = error;
    }
    error.PermitUncheckedError();
  }
  void SetFilesystemActive(
      bool active, const Status& error = Status::Corruption("Not active")) {
    error.PermitUncheckedError();
    MutexLock l(&mutex_);
    SetFilesystemActiveNoLock(active, error);
    error.PermitUncheckedError();
  }
  void AssertNoOpenFile() { assert(open_managed_files_.empty()); }
  Status GetError() {
    MutexLock l(&mutex_);
    return error_;
  }

 private:
  port::Mutex mutex_;
  std::map<std::string, FileState> db_file_state_;
  std::set<std::string> open_managed_files_;
  std::unordered_map<std::string, std::set<std::string>>
      dir_to_new_files_since_last_sync_;
  bool filesystem_active_;  // Record flushes, syncs, writes
  Status error_;
};

}  // namespace ROCKSDB_NAMESPACE
