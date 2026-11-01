//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/env.h"

#include <sstream>
#include <thread>

#include "env/composite_env_wrapper.h"
#include "env/emulated_clock.h"
#include "env/mock_env.h"
#include "env/unique_id_gen.h"
#include "logging/env_logger.h"
#include "memory/arena.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static int RegisterBuiltinEnvs(ObjectLibrary& library,
                               const std::string& /*arg*/) {
  library.AddFactory<Env>(MockEnv::kClassName(), [](const std::string& /*uri*/,
                                                    std::unique_ptr<Env>* guard,
                                                    std::string* /* errmsg */) {
    guard->reset(MockEnv::Create(Env::Default()));
    return guard->get();
  });
  library.AddFactory<Env>(
      CompositeEnvWrapper::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
         std::string* /* errmsg */) {
        guard->reset(new CompositeEnvWrapper(Env::Default()));
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}

static void RegisterSystemEnvs() {
  static std::once_flag loaded;
  std::call_once(loaded, [&]() {
    RegisterBuiltinEnvs(*(ObjectLibrary::Default().get()), "");
  });
}

class LegacySystemClock : public SystemClock {
 private:
  Env* env_;

 public:
  explicit LegacySystemClock(Env* env) : env_(env) {}
  const char* Name() const override { return "LegacySystemClock"; }

  // Returns the number of micro-seconds since some fixed point in time.
  // It is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  uint64_t NowMicros() override { return env_->NowMicros(); }

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros.
  // In platform-specific implementations, NowNanos() should return time points
  // that are MONOTONIC.
  uint64_t NowNanos() override { return env_->NowNanos(); }

  uint64_t CPUMicros() override { return CPUNanos() / 1000; }
  uint64_t CPUNanos() override { return env_->NowCPUNanos(); }

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  void SleepForMicroseconds(int micros) override {
    env_->SleepForMicroseconds(micros);
  }

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  // Only overwrites *unix_time on success.
  Status GetCurrentTime(int64_t* unix_time) override {
    return env_->GetCurrentTime(unix_time);
  }
  // Converts seconds-since-Jan-01-1970 to a printable string
  std::string TimeToString(uint64_t time) override {
    return env_->TimeToString(time);
  }

  std::string SerializeOptions(const ConfigOptions& /*config_options*/,
                               const std::string& /*prefix*/) const override {
    // We do not want the LegacySystemClock to appear in the serialized output.
    // This clock is an internal class for those who do not implement one and
    // would be part of the Env.  As such, do not serialize it here.
    return "";
  }
};

class LegacySequentialFileWrapper : public FSSequentialFile {
 public:
  explicit LegacySequentialFileWrapper(
      std::unique_ptr<SequentialFile>&& _target)
      : target_(std::move(_target)) {}

  IOStatus Read(size_t n, const IOOptions& /*options*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Read(n, result, scratch));
  }
  IOStatus Skip(uint64_t n) override {
    return status_to_io_status(target_->Skip(n));
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return status_to_io_status(target_->InvalidateCache(offset, length));
  }
  IOStatus PositionedRead(uint64_t offset, size_t n,
                          const IOOptions& /*options*/, Slice* result,
                          char* scratch, IODebugContext* /*dbg*/) override {
    return status_to_io_status(
        target_->PositionedRead(offset, n, result, scratch));
  }

 private:
  std::unique_ptr<SequentialFile> target_;
};

class LegacyRandomAccessFileWrapper : public FSRandomAccessFile {
 public:
  explicit LegacyRandomAccessFileWrapper(
      std::unique_ptr<RandomAccessFile>&& target)
      : target_(std::move(target)) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    return status_to_io_status(target_->Read(offset, n, result, scratch));
  }

  IOStatus MultiRead(FSReadRequest* fs_reqs, size_t num_reqs,
                     const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    std::vector<ReadRequest> reqs;
    Status status;

    reqs.reserve(num_reqs);
    for (size_t i = 0; i < num_reqs; ++i) {
      ReadRequest req;

      req.offset = fs_reqs[i].offset;
      req.len = fs_reqs[i].len;
      req.scratch = fs_reqs[i].scratch;
      req.status = Status::OK();
      reqs.emplace_back(std::move(req));
    }
    status = target_->MultiRead(reqs.data(), num_reqs);
    for (size_t i = 0; i < num_reqs; ++i) {
      fs_reqs[i].result = reqs[i].result;
      fs_reqs[i].status = status_to_io_status(std::move(reqs[i].status));
    }
    return status_to_io_status(std::move(status));
  }

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Prefetch(offset, n));
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }
  void Hint(AccessPattern pattern) override {
    target_->Hint((RandomAccessFile::AccessPattern)pattern);
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return status_to_io_status(target_->InvalidateCache(offset, length));
  }
  IOStatus GetFileSize(uint64_t* result) override {
    auto status = target_->GetFileSize(result);
    return status_to_io_status(std::move(status));
  }

 private:
  std::unique_ptr<RandomAccessFile> target_;
};

class LegacyRandomRWFileWrapper : public FSRandomRWFile {
 public:
  explicit LegacyRandomRWFileWrapper(std::unique_ptr<RandomRWFile>&& target)
      : target_(std::move(target)) {}

  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus Write(uint64_t offset, const Slice& data,
                 const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Write(offset, data));
  }
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    return status_to_io_status(target_->Read(offset, n, result, scratch));
  }
  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Flush());
  }
  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Sync());
  }
  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Fsync());
  }
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Close());
  }

 private:
  std::unique_ptr<RandomRWFile> target_;
};

class LegacyWritableFileWrapper : public FSWritableFile {
 public:
  explicit LegacyWritableFileWrapper(std::unique_ptr<WritableFile>&& _target)
      : target_(std::move(_target)) {}

  IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Append(data));
  }
  IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                  const DataVerificationInfo& /*verification_info*/,
                  IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Append(data));
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->PositionedAppend(data, offset));
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& /*options*/,
                            const DataVerificationInfo& /*verification_info*/,
                            IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->PositionedAppend(data, offset));
  }
  IOStatus Truncate(uint64_t size, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Truncate(size));
  }
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Close());
  }
  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Flush());
  }
  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Sync());
  }
  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Fsync());
  }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize(const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/) override {
    return target_->GetFileSize();
  }

  void SetPreallocationBlockSize(size_t size) override {
    target_->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {
    target_->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return status_to_io_status(target_->InvalidateCache(offset, length));
  }

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                     const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->RangeSync(offset, nbytes));
  }

  void PrepareWrite(size_t offset, size_t len, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    target_->PrepareWrite(offset, len);
  }

  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Allocate(offset, len));
  }

 private:
  std::unique_ptr<WritableFile> target_;
};

class LegacyDirectoryWrapper : public FSDirectory {
 public:
  explicit LegacyDirectoryWrapper(std::unique_ptr<Directory>&& target)
      : target_(std::move(target)) {}

  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Fsync());
  }
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Close());
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  std::unique_ptr<Directory> target_;
};

// A helper class to make legacy `Env` implementations be backward compatible
// now that all `Env` implementations are expected to have a `FileSystem` type
// member `file_system_` and a `SystemClock` type member `clock_`.
// This class wraps a legacy `Env` object and expose its file system related
// APIs as a `FileSystem` interface. Also check `LegacySystemClock` that does
// the same thing for the clock related APIs.
class LegacyFileSystemWrapper : public FileSystem {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit LegacyFileSystemWrapper(Env* t) : target_(t) {}
  ~LegacyFileSystemWrapper() override = default;

  static const char* kClassName() { return "LegacyFileSystem"; }
  const char* Name() const override { return kClassName(); }

  // The following text is boilerplate that forwards all methods to target_
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* /*dbg*/) override {
    std::unique_ptr<SequentialFile> file;
    Status s = target_->NewSequentialFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacySequentialFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* /*dbg*/) override {
    std::unique_ptr<RandomAccessFile> file;
    Status s = target_->NewRandomAccessFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyRandomAccessFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->NewWritableFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->ReopenWritableFile(fname, &file, file_opts);
    if (s.ok()) {
      result->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* r,
                             IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->ReuseWritableFile(fname, old_fname, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* /*dbg*/) override {
    std::unique_ptr<RandomRWFile> file;
    Status s = target_->NewRandomRWFile(fname, &file, file_opts);
    if (s.ok()) {
      result->reset(new LegacyRandomRWFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return status_to_io_status(
        target_->NewMemoryMappedFileBuffer(fname, result));
  }
  IOStatus NewDirectory(const std::string& name, const IOOptions& /*io_opts*/,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* /*dbg*/) override {
    std::unique_ptr<Directory> dir;
    Status s = target_->NewDirectory(name, &dir);
    if (s.ok()) {
      result->reset(new LegacyDirectoryWrapper(std::move(dir)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus FileExists(const std::string& f, const IOOptions& /*io_opts*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->FileExists(f));
  }
  IOStatus GetChildren(const std::string& dir, const IOOptions& /*io_opts*/,
                       std::vector<std::string>* r,
                       IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetChildren(dir, r));
  }
  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& /*options*/,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetChildrenFileAttributes(dir, result));
  }
  IOStatus DeleteFile(const std::string& f, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->DeleteFile(f));
  }
  IOStatus Truncate(const std::string& fname, size_t size,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Truncate(fname, size));
  }
  IOStatus CreateDir(const std::string& d, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->CreateDir(d));
  }
  IOStatus CreateDirIfMissing(const std::string& d,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->CreateDirIfMissing(d));
  }
  IOStatus DeleteDir(const std::string& d, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->DeleteDir(d));
  }
  IOStatus GetFileSize(const std::string& f, const IOOptions& /*options*/,
                       uint64_t* s, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetFileSize(f, s));
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& /*options*/,
                                   uint64_t* file_mtime,
                                   IODebugContext* /*dbg*/) override {
    return status_to_io_status(
        target_->GetFileModificationTime(fname, file_mtime));
  }

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& /*options*/,
                           std::string* output_path,
                           IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetAbsolutePath(db_path, output_path));
  }

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->RenameFile(s, t));
  }

  IOStatus LinkFile(const std::string& s, const std::string& t,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->LinkFile(s, t));
  }

  IOStatus SyncFile(const std::string& fname, const FileOptions& file_options,
                    const IOOptions& /*io_options*/, bool use_fsync,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(
        target_->SyncFile(fname, file_options, use_fsync));
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& /*options*/,
                        uint64_t* count, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->NumFileLinks(fname, count));
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& /*options*/, bool* res,
                        IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->AreFilesSame(first, second, res));
  }

  IOStatus LockFile(const std::string& f, const IOOptions& /*options*/,
                    FileLock** l, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->LockFile(f, l));
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->UnlockFile(l));
  }

  IOStatus GetTestDirectory(const IOOptions& /*options*/, std::string* path,
                            IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetTestDirectory(path));
  }
  IOStatus NewLogger(const std::string& fname, const IOOptions& /*options*/,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->NewLogger(fname, result));
  }

  void SanitizeFileOptions(FileOptions* opts) const override {
    target_->SanitizeEnvOptions(opts);
  }

  FileOptions OptimizeForLogRead(
      const FileOptions& file_options) const override {
    return target_->OptimizeForLogRead(file_options);
  }
  FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestRead(file_options);
  }
  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override {
    return target_->OptimizeForLogWrite(file_options, db_options);
  }
  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestWrite(file_options);
  }
  FileOptions OptimizeForCompactionTableWrite(
      const FileOptions& file_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return target_->OptimizeForCompactionTableWrite(file_options,
                                                    immutable_ops);
  }
  FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForCompactionTableRead(file_options, db_options);
  }
  FileOptions OptimizeForBlobFileRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForBlobFileRead(file_options, db_options);
  }

#ifdef GetFreeSpace
#undef GetFreeSpace
#endif
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& /*options*/,
                        uint64_t* diskfree, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetFreeSpace(path, diskfree));
  }
  IOStatus IsDirectory(const std::string& path, const IOOptions& /*options*/,
                       bool* is_dir, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->IsDirectory(path, is_dir));
  }

  std::string SerializeOptions(const ConfigOptions& /*config_options*/,
                               const std::string& /*prefix*/) const override {
    // We do not want the LegacyFileSystem to appear in the serialized output.
    // This clock is an internal class for those who do not implement one and
    // would be part of the Env.  As such, do not serialize it here.
    return "";
  }

 private:
  Env* target_;
};
}  // end anonymous namespace

Env::Env() : thread_status_updater_(nullptr) {
  file_system_ = std::make_shared<LegacyFileSystemWrapper>(this);
  system_clock_ = std::make_shared<LegacySystemClock>(this);
}

Env::Env(const std::shared_ptr<FileSystem>& fs)
    : thread_status_updater_(nullptr), file_system_(fs) {
  system_clock_ = std::make_shared<LegacySystemClock>(this);
}

Env::Env(const std::shared_ptr<FileSystem>& fs,
         const std::shared_ptr<SystemClock>& clock)
    : thread_status_updater_(nullptr), file_system_(fs), system_clock_(clock) {}

Env::~Env() = default;

Status Env::NewLogger(const std::string& fname,
                      std::shared_ptr<Logger>* result) {
  return NewEnvLogger(fname, this, result);
}

Status Env::CreateFromString(const ConfigOptions& config_options,
                             const std::string& value, Env** result) {
  Env* base = Env::Default();
  if (value.empty() || base->IsInstanceOf(value)) {
    *result = base;
    return Status::OK();
  } else {
    RegisterSystemEnvs();
    Env* env = *result;
    Status s = LoadStaticObject<Env>(config_options, value, &env);
    if (s.ok()) {
      *result = env;
    }
    return s;
  }
}

Status Env::CreateFromString(const ConfigOptions& config_options,
                             const std::string& value, Env** result,
                             std::shared_ptr<Env>* guard) {
  assert(result);
  assert(guard != nullptr);
  std::unique_ptr<Env> uniq;

  Env* env = *result;
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;

  Status status =
      Customizable::GetOptionsMap(config_options, env, value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  }
  Env* base = Env::Default();
  if (id.empty() || base->IsInstanceOf(id)) {
    env = base;
    status = Status::OK();
  } else {
    RegisterSystemEnvs();
    // First, try to load the Env as a unique object.
    status = config_options.registry->NewObject<Env>(id, &env, &uniq);
  }
  if (config_options.ignore_unsupported_options && status.IsNotSupported()) {
    status = Status::OK();
  } else if (status.ok()) {
    status = Customizable::ConfigureNewObject(config_options, env, opt_map);
  }
  if (status.ok()) {
    guard->reset(uniq.release());
    *result = env;
  }
  return status;
}

Status Env::CreateFromUri(const ConfigOptions& config_options,
                          const std::string& env_uri, const std::string& fs_uri,
                          Env** result, std::shared_ptr<Env>* guard) {
  *result = config_options.env;
  if (env_uri.empty() && fs_uri.empty()) {
    // Neither specified.  Use the default
    guard->reset();
    return Status::OK();
  } else if (!env_uri.empty() && !fs_uri.empty()) {
    // Both specified.  Cannot choose.  Return Invalid
    return Status::InvalidArgument("cannot specify both fs_uri and env_uri");
  } else if (fs_uri.empty()) {  // Only have an ENV URI.  Create an Env from it
    return CreateFromString(config_options, env_uri, result, guard);
  } else {
    std::shared_ptr<FileSystem> fs;
    Status s = FileSystem::CreateFromString(config_options, fs_uri, &fs);
    if (s.ok()) {
      guard->reset(new CompositeEnvWrapper(*result, fs));
      *result = guard->get();
    }
    return s;
  }
}

std::string Env::PriorityToString(Env::Priority priority) {
  switch (priority) {
    case Env::Priority::BOTTOM:
      return "Bottom";
    case Env::Priority::LOW:
      return "Low";
    case Env::Priority::HIGH:
      return "High";
    case Env::Priority::USER:
      return "User";
    case Env::Priority::TOTAL:
      assert(false);
  }
  return "Invalid";
}

std::string Env::IOActivityToString(IOActivity activity) {
  switch (activity) {
    case Env::IOActivity::kFlush:
      return "Flush";
    case Env::IOActivity::kCompaction:
      return "Compaction";
    case Env::IOActivity::kDBOpen:
      return "DBOpen";
    case Env::IOActivity::kGet:
      return "Get";
    case Env::IOActivity::kMultiGet:
      return "MultiGet";
    case Env::IOActivity::kDBIterator:
      return "DBIterator";
    case Env::IOActivity::kVerifyDBChecksum:
      return "VerifyDBChecksum";
    case Env::IOActivity::kVerifyFileChecksums:
      return "VerifyFileChecksums";
    case Env::IOActivity::kGetEntity:
      return "GetEntity";
    case Env::IOActivity::kMultiGetEntity:
      return "MultiGetEntity";
    case Env::IOActivity::kGetFileChecksumsFromCurrentManifest:
      return "GetFileChecksumsFromCurrentManifest";
    case Env::IOActivity::kLazyResolve:
      return "LazyResolve";
    case Env::IOActivity::kUnknown:
      return "Unknown";
    default:
      int activityIndex = static_cast<int>(activity);
      if (activityIndex >=
              static_cast<int>(Env::IOActivity::kFirstCustomIOActivity) &&
          activityIndex <=
              static_cast<int>(Env::IOActivity::kLastCustomIOActivity)) {
        std::stringstream ss;
        ss << std::hex << std::uppercase << activityIndex;
        return "CustomIOActivity" + ss.str();
      }
      return "Invalid";
  };
  assert(false);
  return "Invalid";
}

uint64_t Env::GetThreadID() const {
  std::hash<std::thread::id> hasher;
  return hasher(std::this_thread::get_id());
}

Status Env::ReuseWritableFile(const std::string& fname,
                              const std::string& old_fname,
                              std::unique_ptr<WritableFile>* result,
                              const EnvOptions& options) {
  Status s = RenameFile(old_fname, fname);
  if (!s.ok()) {
    return s;
  }
  return NewWritableFile(fname, result, options);
}

Status Env::SyncFile(const std::string& fname, const EnvOptions& options,
                     bool use_fsync) {
  std::unique_ptr<WritableFile> file_to_sync;
  Status status = ReopenWritableFile(fname, &file_to_sync, options);
  TEST_SYNC_POINT_CALLBACK("Env::SyncFile:Open", &status);
  if (status.ok()) {
    status = use_fsync ? file_to_sync->Fsync() : file_to_sync->Sync();
    Status close_status = file_to_sync->Close();
    if (status.ok()) {
      status = close_status;
    } else {
      close_status.PermitUncheckedError();
    }
  }
  return status;
}

Status Env::GetChildrenFileAttributes(const std::string& dir,
                                      std::vector<FileAttributes>* result) {
  assert(result != nullptr);
  std::vector<std::string> child_fnames;
  Status s = GetChildren(dir, &child_fnames);
  if (!s.ok()) {
    return s;
  }
  result->resize(child_fnames.size());
  size_t result_size = 0;
  for (size_t i = 0; i < child_fnames.size(); ++i) {
    const std::string path = dir + "/" + child_fnames[i];
    if (!(s = GetFileSize(path, &(*result)[result_size].size_bytes)).ok()) {
      if (FileExists(path).IsNotFound()) {
        // The file may have been deleted since we listed the directory
        continue;
      }
      return s;
    }
    (*result)[result_size].name = std::move(child_fnames[i]);
    result_size++;
  }
  result->resize(result_size);
  return Status::OK();
}

Status Env::GetHostNameString(std::string* result) {
  std::array<char, kMaxHostNameLen> hostname_buf{};
  Status s = GetHostName(hostname_buf.data(), hostname_buf.size());
  if (s.ok()) {
    hostname_buf[hostname_buf.size() - 1] = '\0';
    result->assign(hostname_buf.data());
  }
  return s;
}

std::string Env::GenerateUniqueId() {
  std::string result;
  bool success = port::GenerateRfcUuid(&result);
  if (!success) {
    // Fall back on our own way of generating a unique ID and adapt it to
    // RFC 4122 variant 1 version 4 (a random ID).
    // https://en.wikipedia.org/wiki/Universally_unique_identifier
    // We already tried GenerateRfcUuid so no need to try it again in
    // GenerateRawUniqueId
    constexpr bool exclude_port_uuid = true;
    uint64_t upper;
    uint64_t lower;
    GenerateRawUniqueId(&upper, &lower, exclude_port_uuid);

    // Set 4-bit version to 4
    upper = (upper & (~uint64_t{0xf000})) | 0x4000;
    // Set unary-encoded variant to 1 (0b10)
    lower = (lower & (~(uint64_t{3} << 62))) | (uint64_t{2} << 62);

    // Use 36 character format of RFC 4122
    result.resize(36U);
    char* buf = result.data();
    PutBaseChars<16>(&buf, 8, upper >> 32, /*!uppercase*/ false);
    *(buf++) = '-';
    PutBaseChars<16>(&buf, 4, upper >> 16, /*!uppercase*/ false);
    *(buf++) = '-';
    PutBaseChars<16>(&buf, 4, upper, /*!uppercase*/ false);
    *(buf++) = '-';
    PutBaseChars<16>(&buf, 4, lower >> 48, /*!uppercase*/ false);
    *(buf++) = '-';
    PutBaseChars<16>(&buf, 12, lower, /*!uppercase*/ false);
    assert(buf == &result[36]);

    // Verify variant 1 version 4
    assert(result[14] == '4');
    assert(result[19] == '8' || result[19] == '9' || result[19] == 'a' ||
           result[19] == 'b');
  }
  return result;
}

SequentialFile::~SequentialFile() = default;

RandomAccessFile::~RandomAccessFile() = default;

WritableFile::~WritableFile() = default;

MemoryMappedFileBuffer::~MemoryMappedFileBuffer() = default;

// This const variable can be used in public headers without introducing the
// possibility of ODR violations due to varying macro definitions.
const InfoLogLevel Logger::kDefaultLogLevel =
#ifdef NDEBUG
    INFO_LEVEL;
#else
    DEBUG_LEVEL;
#endif  // NDEBUG

Logger::~Logger() = default;

Status Logger::Close() {
  if (!closed_) {
    closed_ = true;
    return CloseImpl();
  } else {
    return Status::OK();
  }
}

Status Logger::CloseImpl() { return Status::NotSupported(); }

FileLock::~FileLock() = default;

void LogFlush(Logger* info_log) {
  if (info_log) {
    info_log->Flush();
  }
}

static void Logv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Log(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log, format, ap);
  va_end(ap);
}

void Logger::Logv(const InfoLogLevel log_level, const char* format,
                  va_list ap) {
  static const char* kInfoLogLevelNames[5] = {"DEBUG", "INFO", "WARN", "ERROR",
                                              "FATAL"};
  if (log_level < log_level_) {
    return;
  }

  if (log_level == InfoLogLevel::INFO_LEVEL) {
    // Doesn't print log level if it is INFO level.
    // This is to avoid unexpected performance regression after we add
    // the feature of log level. All the logs before we add the feature
    // are INFO level. We don't want to add extra costs to those existing
    // logging.
    Logv(format, ap);
  } else if (log_level == InfoLogLevel::HEADER_LEVEL) {
    LogHeader(format, ap);
  } else {
    char new_format[500];
    snprintf(new_format, sizeof(new_format) - 1, "[%s] %s",
             kInfoLogLevelNames[log_level], format);
    Logv(new_format, ap);
  }

  if (log_level >= InfoLogLevel::WARN_LEVEL &&
      log_level != InfoLogLevel::HEADER_LEVEL) {
    // Log messages with severity of warning or higher should be rare and are
    // sometimes followed by an unclean crash. We want to be sure important
    // messages are not lost in an application buffer when that happens.
    Flush();
  }
}

static void Logv(const InfoLogLevel log_level, Logger* info_log,
                 const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= log_level) {
    if (log_level == InfoLogLevel::HEADER_LEVEL) {
      info_log->LogHeader(format, ap);
    } else {
      info_log->Logv(log_level, format, ap);
    }
  }
}

void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
         ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log, format, ap);
  va_end(ap);
}

static void Headerv(Logger* info_log, const char* format, va_list ap) {
  if (info_log) {
    info_log->LogHeader(format, ap);
  }
}

void Header(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log, format, ap);
  va_end(ap);
}

static void Debugv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    info_log->Logv(InfoLogLevel::DEBUG_LEVEL, format, ap);
  }
}

void Debug(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log, format, ap);
  va_end(ap);
}

static void Infov(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Info(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log, format, ap);
  va_end(ap);
}

static void Warnv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::WARN_LEVEL) {
    info_log->Logv(InfoLogLevel::WARN_LEVEL, format, ap);
  }
}

void Warn(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log, format, ap);
  va_end(ap);
}

static void Errorv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::ERROR_LEVEL) {
    info_log->Logv(InfoLogLevel::ERROR_LEVEL, format, ap);
  }
}

void Error(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log, format, ap);
  va_end(ap);
}

static void Fatalv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::FATAL_LEVEL) {
    info_log->Logv(InfoLogLevel::FATAL_LEVEL, format, ap);
  }
}

void Fatal(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log, format, ap);
  va_end(ap);
}

void LogFlush(const std::shared_ptr<Logger>& info_log) {
  LogFlush(info_log.get());
}

void Log(const InfoLogLevel log_level, const std::shared_ptr<Logger>& info_log,
         const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log.get(), format, ap);
  va_end(ap);
}

void Header(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log.get(), format, ap);
  va_end(ap);
}

void Debug(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log.get(), format, ap);
  va_end(ap);
}

void Info(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log.get(), format, ap);
  va_end(ap);
}

void Warn(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log.get(), format, ap);
  va_end(ap);
}

void Error(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log.get(), format, ap);
  va_end(ap);
}

void Fatal(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log.get(), format, ap);
  va_end(ap);
}

void Log(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log.get(), format, ap);
  va_end(ap);
}

Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname,
                         bool should_sync, const IOOptions* io_options) {
  const auto& fs = env->GetFileSystem();
  return WriteStringToFile(fs.get(), data, fname, should_sync,
                           io_options ? *io_options : IOOptions());
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  const auto& fs = env->GetFileSystem();
  return ReadFileToString(fs.get(), fname, data);
}

namespace {  // anonymous namespace

void AssignEnvOptions(EnvOptions* env_options, const DBOptions& options) {
  env_options->use_mmap_reads = options.allow_mmap_reads;
  env_options->use_mmap_writes = options.allow_mmap_writes;
  env_options->use_direct_reads = options.use_direct_reads;
  env_options->set_fd_cloexec = options.is_fd_close_on_exec;
  env_options->bytes_per_sync = options.bytes_per_sync;
  env_options->compaction_readahead_size = options.compaction_readahead_size;
  env_options->rate_limiter = options.rate_limiter.get();
  env_options->writable_file_max_buffer_size =
      options.writable_file_max_buffer_size;
  env_options->allow_fallocate = options.allow_fallocate;
  env_options->strict_bytes_per_sync = options.strict_bytes_per_sync;
  options.env->SanitizeEnvOptions(env_options);
}

}  // namespace

EnvOptions Env::OptimizeForLogWrite(const EnvOptions& env_options,
                                    const DBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.bytes_per_sync = db_options.wal_bytes_per_sync;
  optimized_env_options.writable_file_max_buffer_size =
      db_options.writable_file_max_buffer_size;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForManifestWrite(const EnvOptions& env_options) const {
  return env_options;
}

EnvOptions Env::OptimizeForLogRead(const EnvOptions& env_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = false;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForManifestRead(const EnvOptions& env_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = false;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForCompactionTableWrite(
    const EnvOptions& env_options, const ImmutableDBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_writes =
      db_options.use_direct_io_for_flush_and_compaction;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForCompactionTableRead(
    const EnvOptions& env_options, const ImmutableDBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = db_options.use_direct_reads;
  return optimized_env_options;
}
EnvOptions Env::OptimizeForBlobFileRead(
    const EnvOptions& env_options, const ImmutableDBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = db_options.use_direct_reads;
  return optimized_env_options;
}

EnvOptions::EnvOptions(const DBOptions& options) {
  AssignEnvOptions(this, options);
}

EnvOptions::EnvOptions() {
  DBOptions options;
  AssignEnvOptions(this, options);
}

Status NewEnvLogger(const std::string& fname, Env* env,
                    std::shared_ptr<Logger>* result) {
  FileOptions options;
  // TODO: Tune the buffer size.
  options.writable_file_max_buffer_size = 1024 * 1024;
  std::unique_ptr<FSWritableFile> writable_file;
  const auto status = env->GetFileSystem()->NewWritableFile(
      fname, options, &writable_file, nullptr);
  if (!status.ok()) {
    return status;
  }

  *result = std::make_shared<EnvLogger>(std::move(writable_file), fname,
                                        options, env);
  return Status::OK();
}

const std::shared_ptr<FileSystem>& Env::GetFileSystem() const {
  return file_system_;
}

const std::shared_ptr<SystemClock>& Env::GetSystemClock() const {
  return system_clock_;
}
namespace {
static std::unordered_map<std::string, OptionTypeInfo> sc_wrapper_type_info = {
    {"target",
     OptionTypeInfo::AsCustomSharedPtr<SystemClock>(
         0, OptionVerificationType::kByName, OptionTypeFlags::kDontSerialize)},
};

}  // namespace
SystemClockWrapper::SystemClockWrapper(const std::shared_ptr<SystemClock>& t)
    : target_(t) {
  RegisterOptions("", &target_, &sc_wrapper_type_info);
}

Status SystemClockWrapper::PrepareOptions(const ConfigOptions& options) {
  if (target_ == nullptr) {
    target_ = SystemClock::Default();
  }
  return SystemClock::PrepareOptions(options);
}

std::string SystemClockWrapper::SerializeOptions(
    const ConfigOptions& config_options, const std::string& header) const {
  auto parent = SystemClock::SerializeOptions(config_options, "");
  if (config_options.IsShallow() || target_ == nullptr ||
      target_->IsInstanceOf(SystemClock::kDefaultName())) {
    return parent;
  } else {
    std::string result = header;
    if (!StartsWith(parent, OptionTypeInfo::kIdPropName())) {
      result.append(OptionTypeInfo::kIdPropName()).append("=");
    }
    result.append(parent);
    if (!EndsWith(result, config_options.delimiter)) {
      result.append(config_options.delimiter);
    }
    result.append("target=").append(target_->ToString(config_options));
    return result;
  }
}

static int RegisterBuiltinSystemClocks(ObjectLibrary& library,
                                       const std::string& /*arg*/) {
  library.AddFactory<SystemClock>(
      EmulatedSystemClock::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<SystemClock>* guard,
         std::string* /* errmsg */) {
        guard->reset(new EmulatedSystemClock(SystemClock::Default()));
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}

Status SystemClock::CreateFromString(const ConfigOptions& config_options,
                                     const std::string& value,
                                     std::shared_ptr<SystemClock>* result) {
  auto clock = SystemClock::Default();
  if (clock->IsInstanceOf(value)) {
    *result = clock;
    return Status::OK();
  } else {
    static std::once_flag once;
    std::call_once(once, [&]() {
      RegisterBuiltinSystemClocks(*(ObjectLibrary::Default().get()), "");
    });
    return LoadSharedObject<SystemClock>(config_options, value, result);
  }
}

bool SystemClock::TimedWait(port::CondVar* cv,
                            std::chrono::microseconds deadline) {
  return cv->TimedWait(deadline.count());
}
}  // namespace ROCKSDB_NAMESPACE
