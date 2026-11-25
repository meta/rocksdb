//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
class MemFile;
class MockFileSystem : public FileSystem {
 public:
  explicit MockFileSystem(const std::shared_ptr<SystemClock>& clock,
                          bool supports_direct_io = true);
  ~MockFileSystem() override;

  static const char* kClassName() { return "MemoryFileSystem"; }
  const char* Name() const override { return kClassName(); }
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override;
  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* dbg) override;

  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override;
  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;
  IOStatus NewDirectory(const std::string& /*name*/, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;
  IOStatus FileExists(const std::string& fname, const IOOptions& /*io_opts*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;
  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus Truncate(const std::string& fname, size_t size,
                    const IOOptions& options, IODebugContext* dbg) override;
  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;
  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;
  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& options, IODebugContext* dbg) override;
  IOStatus LinkFile(const std::string& /*src*/, const std::string& /*target*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override;
  IOStatus SyncFile(const std::string& /*fname*/,
                    const FileOptions& /*file_opts*/,
                    const IOOptions& /*io_opts*/, bool /*use_fsync*/,
                    IODebugContext* /*dbg*/) override;
  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override;
  IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override;
  IOStatus NewLogger(const std::string& fname, const IOOptions& io_opts,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;
  // Get full directory name for this db.
  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& /*options*/,
                           std::string* output_path,
                           IODebugContext* /*dbg*/) override;
  IOStatus IsDirectory(const std::string& /*path*/,
                       const IOOptions& /*options*/, bool* /*is_dir*/,
                       IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("IsDirectory");
  }

  Status CorruptBuffer(const std::string& fname);
  Status PrepareOptions(const ConfigOptions& options) override;

 private:
  bool RenameFileInternal(const std::string& src, const std::string& dest);
  void DeleteFileInternal(const std::string& fname);
  bool GetChildrenInternal(const std::string& fname,
                           std::vector<std::string>* results);

  std::string NormalizeMockPath(const std::string& path);

 private:
  // Map from filenames to MemFile objects, representing a simple file system.
  port::Mutex mutex_;
  std::map<std::string, MemFile*> file_map_;  // Protected by mutex_.
  std::shared_ptr<SystemClock> system_clock_;
  SystemClock* clock_;
  bool supports_direct_io_;
};

class MockEnv : public CompositeEnvWrapper {
 public:
  static MockEnv* Create(Env* base);
  static MockEnv* Create(Env* base, const std::shared_ptr<SystemClock>& clock);

  static const char* kClassName() { return "MockEnv"; }
  const char* Name() const override { return kClassName(); }

  Status CorruptBuffer(const std::string& fname);

 private:
  MockEnv(Env* env, const std::shared_ptr<FileSystem>& fs,
          const std::shared_ptr<SystemClock>& clock);
};

}  // namespace ROCKSDB_NAMESPACE
