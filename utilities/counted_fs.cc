//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/counted_fs.h"

#include <sstream>

#include "rocksdb/file_system.h"
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class CountedSequentialFile : public FSSequentialFileOwnerWrapper {
 private:
  CountedFileSystem* fs_;

 public:
  CountedSequentialFile(std::unique_ptr<FSSequentialFile>&& f,
                        CountedFileSystem* fs)
      : FSSequentialFileOwnerWrapper(std::move(f)), fs_(fs) {}

  // No copying allowed
  CountedSequentialFile(const CountedSequentialFile&) = delete;
  CountedSequentialFile& operator=(const CountedSequentialFile&) = delete;
  CountedSequentialFile(CountedSequentialFile&&) = delete;
  CountedSequentialFile& operator=(CountedSequentialFile&&) = delete;

  ~CountedSequentialFile() override { fs_->counters()->closes++; }

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override {
    IOStatus rv = target()->Read(n, options, result, scratch, dbg);
    fs_->counters()->reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override {
    IOStatus rv =
        target()->PositionedRead(offset, n, options, result, scratch, dbg);
    fs_->counters()->reads.RecordOp(rv, result->size());
    return rv;
  }
};

class CountedRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 private:
  CountedFileSystem* fs_;

 public:
  CountedRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& f,
                          CountedFileSystem* fs)
      : FSRandomAccessFileOwnerWrapper(std::move(f)), fs_(fs) {}

  // No copying allowed
  CountedRandomAccessFile(const CountedRandomAccessFile&) = delete;
  CountedRandomAccessFile& operator=(const CountedRandomAccessFile&) = delete;
  CountedRandomAccessFile(CountedRandomAccessFile&&) = delete;
  CountedRandomAccessFile& operator=(CountedRandomAccessFile&&) = delete;

  ~CountedRandomAccessFile() override { fs_->counters()->closes++; }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    IOStatus rv = target()->Read(offset, n, options, result, scratch, dbg);
    fs_->counters()->reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->MultiRead(reqs, num_reqs, options, dbg);
    for (size_t r = 0; r < num_reqs; r++) {
      fs_->counters()->reads.RecordOp(reqs[r].status, reqs[r].result.size());
    }
    return rv;
  }
};

class CountedWritableFile : public FSWritableFileOwnerWrapper {
 private:
  CountedFileSystem* fs_;

 public:
  CountedWritableFile(std::unique_ptr<FSWritableFile>&& f,
                      CountedFileSystem* fs)
      : FSWritableFileOwnerWrapper(std::move(f)), fs_(fs) {}

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    IOStatus rv = target()->Append(data, options, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& info,
                  IODebugContext* dbg) override {
    IOStatus rv = target()->Append(data, options, info, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    IOStatus rv = target()->PositionedAppend(data, offset, options, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& info,
                            IODebugContext* dbg) override {
    IOStatus rv = target()->PositionedAppend(data, offset, options, info, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Close(options, dbg);
    if (rv.ok()) {
      fs_->counters()->closes++;
    }
    return rv;
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Flush(options, dbg);
    if (rv.ok()) {
      fs_->counters()->flushes++;
    }
    return rv;
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Sync(options, dbg);
    if (rv.ok()) {
      fs_->counters()->syncs++;
    }
    return rv;
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Fsync(options, dbg);
    if (rv.ok()) {
      fs_->counters()->fsyncs++;
    }
    return rv;
  }

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override {
    IOStatus rv = target()->RangeSync(offset, nbytes, options, dbg);
    if (rv.ok()) {
      fs_->counters()->syncs++;
    }
    return rv;
  }
};

class CountedRandomRWFile : public FSRandomRWFileOwnerWrapper {
 private:
  mutable CountedFileSystem* fs_;

 public:
  CountedRandomRWFile(std::unique_ptr<FSRandomRWFile>&& f,
                      CountedFileSystem* fs)
      : FSRandomRWFileOwnerWrapper(std::move(f)), fs_(fs) {}
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override {
    IOStatus rv = target()->Write(offset, data, options, dbg);
    fs_->counters()->writes.RecordOp(rv, data.size());
    return rv;
  }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    IOStatus rv = target()->Read(offset, n, options, result, scratch, dbg);
    fs_->counters()->reads.RecordOp(rv, result->size());
    return rv;
  }

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Flush(options, dbg);
    if (rv.ok()) {
      fs_->counters()->flushes++;
    }
    return rv;
  }

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Sync(options, dbg);
    if (rv.ok()) {
      fs_->counters()->syncs++;
    }
    return rv;
  }

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Fsync(options, dbg);
    if (rv.ok()) {
      fs_->counters()->fsyncs++;
    }
    return rv;
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = target()->Close(options, dbg);
    if (rv.ok()) {
      fs_->counters()->closes++;
    }
    return rv;
  }
};

class CountedDirectory : public FSDirectoryWrapper {
 private:
  mutable CountedFileSystem* fs_;
  bool closed_ = false;

 public:
  CountedDirectory(std::unique_ptr<FSDirectory>&& f, CountedFileSystem* fs)
      : FSDirectoryWrapper(std::move(f)), fs_(fs) {}

  // No copying allowed
  CountedDirectory(const CountedDirectory&) = delete;
  CountedDirectory& operator=(const CountedDirectory&) = delete;
  CountedDirectory(CountedDirectory&&) = delete;
  CountedDirectory& operator=(CountedDirectory&&) = delete;

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = FSDirectoryWrapper::Fsync(options, dbg);
    if (rv.ok()) {
      fs_->counters()->dsyncs++;
    }
    return rv;
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus rv = FSDirectoryWrapper::Close(options, dbg);
    if (rv.ok()) {
      fs_->counters()->closes++;
      fs_->counters()->dir_closes++;
      closed_ = true;
    }
    return rv;
  }

  IOStatus FsyncWithDirOptions(const IOOptions& options, IODebugContext* dbg,
                               const DirFsyncOptions& dir_options) override {
    IOStatus rv =
        FSDirectoryWrapper::FsyncWithDirOptions(options, dbg, dir_options);
    if (rv.ok()) {
      fs_->counters()->dsyncs++;
    }
    return rv;
  }

  ~CountedDirectory() override {
    if (!closed_) {
      // TODO: fix DB+CF code to use explicit Close, not rely on destructor
      fs_->counters()->closes++;
      fs_->counters()->dir_closes++;
    }
  }
};
}  // anonymous namespace

std::string FileOpCounters::PrintCounters() const {
  std::stringstream ss;
  ss << "Num files opened: " << opens.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num files deleted: " << deletes.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num files renamed: " << renames.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Flush(): " << flushes.load(std::memory_order_relaxed) << std::endl;
  ss << "Num Sync(): " << syncs.load(std::memory_order_relaxed) << std::endl;
  ss << "Num Fsync(): " << fsyncs.load(std::memory_order_relaxed) << std::endl;
  ss << "Num Dir Fsync(): " << dsyncs.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Close(): " << closes.load(std::memory_order_relaxed) << std::endl;
  ss << "Num Dir Open(): " << dir_opens.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Dir Close(): " << dir_closes.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Read(): " << reads.ops.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num Append(): " << writes.ops.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num bytes read: " << reads.bytes.load(std::memory_order_relaxed)
     << std::endl;
  ss << "Num bytes written: " << writes.bytes.load(std::memory_order_relaxed)
     << std::endl;
  return ss.str();
}

CountedFileSystem::CountedFileSystem(const std::shared_ptr<FileSystem>& base)
    : FileSystemWrapper(base) {}

IOStatus CountedFileSystem::NewSequentialFile(
    const std::string& f, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* r, IODebugContext* dbg) {
  std::unique_ptr<FSSequentialFile> base;
  IOStatus s = target()->NewSequentialFile(f, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    r->reset(new CountedSequentialFile(std::move(base), this));
  }
  return s;
}

IOStatus CountedFileSystem::NewRandomAccessFile(
    const std::string& f, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* r, IODebugContext* dbg) {
  std::unique_ptr<FSRandomAccessFile> base;
  IOStatus s = target()->NewRandomAccessFile(f, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    r->reset(new CountedRandomAccessFile(std::move(base), this));
  }
  return s;
}

IOStatus CountedFileSystem::NewWritableFile(const std::string& f,
                                            const FileOptions& options,
                                            std::unique_ptr<FSWritableFile>* r,
                                            IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  IOStatus s = target()->NewWritableFile(f, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    r->reset(new CountedWritableFile(std::move(base), this));
  }
  return s;
}

IOStatus CountedFileSystem::ReopenWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  IOStatus s = target()->ReopenWritableFile(fname, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    result->reset(new CountedWritableFile(std::move(base), this));
  }
  return s;
}

IOStatus CountedFileSystem::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> base;
  IOStatus s =
      target()->ReuseWritableFile(fname, old_fname, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    result->reset(new CountedWritableFile(std::move(base), this));
  }
  return s;
}

IOStatus CountedFileSystem::NewRandomRWFile(
    const std::string& name, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  std::unique_ptr<FSRandomRWFile> base;
  IOStatus s = target()->NewRandomRWFile(name, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    result->reset(new CountedRandomRWFile(std::move(base), this));
  }
  return s;
}

IOStatus CountedFileSystem::NewDirectory(const std::string& name,
                                         const IOOptions& options,
                                         std::unique_ptr<FSDirectory>* result,
                                         IODebugContext* dbg) {
  std::unique_ptr<FSDirectory> base;
  IOStatus s = target()->NewDirectory(name, options, &base, dbg);
  if (s.ok()) {
    counters_.opens++;
    counters_.dir_opens++;
    result->reset(new CountedDirectory(std::move(base), this));
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
