// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "env/composite_env_wrapper.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
// The CompositeEnvWrapper class provides an interface that is compatible
// with the old monolithic Env API, and an implementation that wraps around
// the new Env that provides threading and other OS related functionality, and
// the new FileSystem API that provides storage functionality. By
// providing the old Env interface, it allows the rest of RocksDB code to
// be agnostic of whether the underlying Env implementation is a monolithic
// Env or an Env + FileSystem. In the former case, the user will specify
// Options::env only, whereas in the latter case, the user will specify
// Options::env and Options::file_system.

class CompositeSequentialFileWrapper : public SequentialFile {
 public:
  explicit CompositeSequentialFileWrapper(
      std::unique_ptr<FSSequentialFile>& target)
      : target_(std::move(target)) {}

  Status Read(size_t n, Slice* result, char* scratch) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Read(n, io_opts, result, scratch, &dbg);
  }
  Status Skip(uint64_t n) override { return target_->Skip(n); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                        char* scratch) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->PositionedRead(offset, n, io_opts, result, scratch, &dbg);
  }

 private:
  std::unique_ptr<FSSequentialFile> target_;
};

class CompositeRandomAccessFileWrapper : public RandomAccessFile {
 public:
  explicit CompositeRandomAccessFileWrapper(
      std::unique_ptr<FSRandomAccessFile>& target)
      : target_(std::move(target)) {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Read(offset, n, io_opts, result, scratch, &dbg);
  }
  Status MultiRead(ReadRequest* reqs, size_t num_reqs) override {
    IOOptions io_opts;
    IODebugContext dbg;
    std::vector<FSReadRequest> fs_reqs;
    Status status;

    fs_reqs.resize(num_reqs);
    for (size_t i = 0; i < num_reqs; ++i) {
      fs_reqs[i].offset = reqs[i].offset;
      fs_reqs[i].len = reqs[i].len;
      fs_reqs[i].scratch = reqs[i].scratch;
      fs_reqs[i].status = IOStatus::OK();
    }
    status = target_->MultiRead(fs_reqs.data(), num_reqs, io_opts, &dbg);
    for (size_t i = 0; i < num_reqs; ++i) {
      reqs[i].result = fs_reqs[i].result;
      reqs[i].status = fs_reqs[i].status;
    }
    return status;
  }
  Status Prefetch(uint64_t offset, size_t n) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Prefetch(offset, n, io_opts, &dbg);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }
  void Hint(AccessPattern pattern) override {
    target_->Hint((FSRandomAccessFile::AccessPattern)pattern);
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

  Status GetFileSize(uint64_t* size) override {
    return target_->GetFileSize(size);
  }

 private:
  std::unique_ptr<FSRandomAccessFile> target_;
};

class CompositeWritableFileWrapper : public WritableFile {
 public:
  explicit CompositeWritableFileWrapper(std::unique_ptr<FSWritableFile>& t)
      : target_(std::move(t)) {}

  Status Append(const Slice& data) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Append(data, io_opts, &dbg);
  }
  Status Append(const Slice& data,
                const DataVerificationInfo& verification_info) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Append(data, io_opts, verification_info, &dbg);
  }
  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->PositionedAppend(data, offset, io_opts, &dbg);
  }
  Status PositionedAppend(
      const Slice& data, uint64_t offset,
      const DataVerificationInfo& verification_info) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->PositionedAppend(data, offset, io_opts, verification_info,
                                     &dbg);
  }
  Status Truncate(uint64_t size) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Truncate(size, io_opts, &dbg);
  }
  Status Close() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Close(io_opts, &dbg);
  }
  Status Flush() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Flush(io_opts, &dbg);
  }
  Status Sync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Sync(io_opts, &dbg);
  }
  Status Fsync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Fsync(io_opts, &dbg);
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

  uint64_t GetFileSize() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->GetFileSize(io_opts, &dbg);
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

  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->RangeSync(offset, nbytes, io_opts, &dbg);
  }

  void PrepareWrite(size_t offset, size_t len) override {
    IOOptions io_opts;
    IODebugContext dbg;
    target_->PrepareWrite(offset, len, io_opts, &dbg);
  }

  Status Allocate(uint64_t offset, uint64_t len) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Allocate(offset, len, io_opts, &dbg);
  }

 private:
  std::unique_ptr<FSWritableFile> target_;
};

class CompositeRandomRWFileWrapper : public RandomRWFile {
 public:
  explicit CompositeRandomRWFileWrapper(std::unique_ptr<FSRandomRWFile>& target)
      : target_(std::move(target)) {}

  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status Write(uint64_t offset, const Slice& data) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Write(offset, data, io_opts, &dbg);
  }
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Read(offset, n, io_opts, result, scratch, &dbg);
  }
  Status Flush() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Flush(io_opts, &dbg);
  }
  Status Sync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Sync(io_opts, &dbg);
  }
  Status Fsync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Fsync(io_opts, &dbg);
  }
  Status Close() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Close(io_opts, &dbg);
  }

 private:
  std::unique_ptr<FSRandomRWFile> target_;
};

class CompositeDirectoryWrapper : public Directory {
 public:
  explicit CompositeDirectoryWrapper(std::unique_ptr<FSDirectory>& target)
      : target_(std::move(target)) {}

  Status Fsync() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->FsyncWithDirOptions(io_opts, &dbg, DirFsyncOptions());
  }

  Status Close() override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Close(io_opts, &dbg);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  std::unique_ptr<FSDirectory> target_;
};
}  // namespace

Status CompositeEnv::NewSequentialFile(const std::string& f,
                                       std::unique_ptr<SequentialFile>* r,
                                       const EnvOptions& options) {
  IODebugContext dbg;
  std::unique_ptr<FSSequentialFile> file;
  Status status;
  status =
      file_system_->NewSequentialFile(f, FileOptions(options), &file, &dbg);
  if (status.ok()) {
    r->reset(new CompositeSequentialFileWrapper(file));
  }
  return status;
}

Status CompositeEnv::NewRandomAccessFile(const std::string& f,
                                         std::unique_ptr<RandomAccessFile>* r,
                                         const EnvOptions& options) {
  IODebugContext dbg;
  std::unique_ptr<FSRandomAccessFile> file;
  Status status;
  status =
      file_system_->NewRandomAccessFile(f, FileOptions(options), &file, &dbg);
  if (status.ok()) {
    r->reset(new CompositeRandomAccessFileWrapper(file));
  }
  return status;
}

Status CompositeEnv::NewWritableFile(const std::string& f,
                                     std::unique_ptr<WritableFile>* r,
                                     const EnvOptions& options) {
  IODebugContext dbg;
  std::unique_ptr<FSWritableFile> file;
  Status status;
  status = file_system_->NewWritableFile(f, FileOptions(options), &file, &dbg);
  if (status.ok()) {
    r->reset(new CompositeWritableFileWrapper(file));
  }
  return status;
}

Status CompositeEnv::ReopenWritableFile(const std::string& fname,
                                        std::unique_ptr<WritableFile>* result,
                                        const EnvOptions& options) {
  IODebugContext dbg;
  Status status;
  std::unique_ptr<FSWritableFile> file;
  status = file_system_->ReopenWritableFile(fname, FileOptions(options), &file,
                                            &dbg);
  if (status.ok()) {
    result->reset(new CompositeWritableFileWrapper(file));
  }
  return status;
}

Status CompositeEnv::ReuseWritableFile(const std::string& fname,
                                       const std::string& old_fname,
                                       std::unique_ptr<WritableFile>* r,
                                       const EnvOptions& options) {
  IODebugContext dbg;
  Status status;
  std::unique_ptr<FSWritableFile> file;
  status = file_system_->ReuseWritableFile(fname, old_fname,
                                           FileOptions(options), &file, &dbg);
  if (status.ok()) {
    r->reset(new CompositeWritableFileWrapper(file));
  }
  return status;
}

Status CompositeEnv::NewRandomRWFile(const std::string& fname,
                                     std::unique_ptr<RandomRWFile>* result,
                                     const EnvOptions& options) {
  IODebugContext dbg;
  std::unique_ptr<FSRandomRWFile> file;
  Status status;
  status =
      file_system_->NewRandomRWFile(fname, FileOptions(options), &file, &dbg);
  if (status.ok()) {
    result->reset(new CompositeRandomRWFileWrapper(file));
  }
  return status;
}

Status CompositeEnv::NewDirectory(const std::string& name,
                                  std::unique_ptr<Directory>* result) {
  IOOptions io_opts;
  IODebugContext dbg;
  std::unique_ptr<FSDirectory> dir;
  Status status;
  status = file_system_->NewDirectory(name, io_opts, &dir, &dbg);
  if (status.ok()) {
    result->reset(new CompositeDirectoryWrapper(dir));
  }
  return status;
}

namespace {
static std::unordered_map<std::string, OptionTypeInfo> env_wrapper_type_info = {
    {"target",
     OptionTypeInfo(0, OptionType::kUnknown, OptionVerificationType::kByName,
                    OptionTypeFlags::kDontSerialize)
         .SetParseFunc([](const ConfigOptions& opts,
                          const std::string& /*name*/, const std::string& value,
                          void* addr) {
           auto target = static_cast<EnvWrapper::Target*>(addr);
           return Env::CreateFromString(opts, value, &(target->env),
                                        &(target->guard));
         })
         .SetEqualsFunc([](const ConfigOptions& opts,
                           const std::string& /*name*/, const void* addr1,
                           const void* addr2, std::string* mismatch) {
           const auto target1 = static_cast<const EnvWrapper::Target*>(addr1);
           const auto target2 = static_cast<const EnvWrapper::Target*>(addr2);
           if (target1->env != nullptr) {
             return target1->env->AreEquivalent(opts, target2->env, mismatch);
           } else {
             return (target2->env == nullptr);
           }
         })
         .SetPrepareFunc([](const ConfigOptions& opts,
                            const std::string& /*name*/, void* addr) {
           auto target = static_cast<EnvWrapper::Target*>(addr);
           if (target->guard.get() != nullptr) {
             target->env = target->guard.get();
           } else if (target->env == nullptr) {
             target->env = Env::Default();
           }
           return target->env->PrepareOptions(opts);
         })
         .SetValidateFunc([](const DBOptions& db_opts,
                             const ColumnFamilyOptions& cf_opts,
                             const std::string& /*name*/, const void* addr) {
           const auto target = static_cast<const EnvWrapper::Target*>(addr);
           if (target->env == nullptr) {
             return Status::InvalidArgument("Target Env not specified");
           } else {
             return target->env->ValidateOptions(db_opts, cf_opts);
           }
         })},
};
static std::unordered_map<std::string, OptionTypeInfo>
    composite_fs_wrapper_type_info = {
        {"file_system",
         OptionTypeInfo::AsCustomSharedPtr<FileSystem>(
             0, OptionVerificationType::kByName, OptionTypeFlags::kNone)},
};

static std::unordered_map<std::string, OptionTypeInfo>
    composite_clock_wrapper_type_info = {
        {"clock",
         OptionTypeInfo::AsCustomSharedPtr<SystemClock>(
             0, OptionVerificationType::kByName, OptionTypeFlags::kNone)},
};

}  // namespace

std::unique_ptr<Env> NewCompositeEnv(const std::shared_ptr<FileSystem>& fs) {
  return std::unique_ptr<Env>(new CompositeEnvWrapper(Env::Default(), fs));
}

CompositeEnvWrapper::CompositeEnvWrapper(Env* env,
                                         const std::shared_ptr<FileSystem>& fs,
                                         const std::shared_ptr<SystemClock>& sc)
    : CompositeEnv(fs, sc), target_(env) {
  RegisterOptions("", &target_, &env_wrapper_type_info);
  RegisterOptions("", &file_system_, &composite_fs_wrapper_type_info);
  RegisterOptions("", &system_clock_, &composite_clock_wrapper_type_info);
}

CompositeEnvWrapper::CompositeEnvWrapper(const std::shared_ptr<Env>& env,
                                         const std::shared_ptr<FileSystem>& fs,
                                         const std::shared_ptr<SystemClock>& sc)
    : CompositeEnv(fs, sc), target_(env) {
  RegisterOptions("", &target_, &env_wrapper_type_info);
  RegisterOptions("", &file_system_, &composite_fs_wrapper_type_info);
  RegisterOptions("", &system_clock_, &composite_clock_wrapper_type_info);
}

Status CompositeEnvWrapper::PrepareOptions(const ConfigOptions& options) {
  target_.Prepare();
  if (file_system_ == nullptr) {
    file_system_ = target_.env->GetFileSystem();
  }
  if (system_clock_ == nullptr) {
    system_clock_ = target_.env->GetSystemClock();
  }
  return Env::PrepareOptions(options);
}

std::string CompositeEnvWrapper::SerializeOptions(
    const ConfigOptions& config_options, const std::string& header) const {
  auto options = CompositeEnv::SerializeOptions(config_options, header);
  if (target_.env != nullptr && target_.env != Env::Default()) {
    options.append("target=");
    options.append(target_.env->ToString(config_options));
  }
  return options;
}

EnvWrapper::EnvWrapper(Env* t) : target_(t) {
  RegisterOptions("", &target_, &env_wrapper_type_info);
}

EnvWrapper::EnvWrapper(std::unique_ptr<Env>&& t) : target_(std::move(t)) {
  RegisterOptions("", &target_, &env_wrapper_type_info);
}

EnvWrapper::EnvWrapper(const std::shared_ptr<Env>& t) : target_(t) {
  RegisterOptions("", &target_, &env_wrapper_type_info);
}

EnvWrapper::~EnvWrapper() = default;

Status EnvWrapper::PrepareOptions(const ConfigOptions& options) {
  target_.Prepare();
  return Env::PrepareOptions(options);
}

std::string EnvWrapper::SerializeOptions(const ConfigOptions& config_options,
                                         const std::string& header) const {
  auto parent = Env::SerializeOptions(config_options, "");
  if (config_options.IsShallow() || target_.env == nullptr ||
      target_.env == Env::Default()) {
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
    result.append("target=").append(target_.env->ToString(config_options));
    return result;
  }
}

}  // namespace ROCKSDB_NAMESPACE
