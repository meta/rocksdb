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
#include <string>

#include "env/file_system_tracer.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

// SequentialFileReader is a wrapper on top of Env::SequentialFile. It handles
// Buffered (i.e when page cache is enabled) and Direct (with O_DIRECT / page
// cache disabled) reads appropriately, and also updates the IO stats.
class SequentialFileReader {
 private:
  void NotifyOnFileReadFinish(
      uint64_t offset, size_t length,
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const Status& status) const {
    FileOperationInfo info(FileOperationType::kRead, file_name_, start_ts,
                           finish_ts, status);
    info.offset = offset;
    info.length = length;

    for (auto& listener : listeners_) {
      listener->OnFileReadFinish(info);
    }
    info.status.PermitUncheckedError();
  }

  void AddFileIOListeners(
      const std::vector<std::shared_ptr<EventListener>>& listeners) {
    std::for_each(listeners.begin(), listeners.end(),
                  [this](const std::shared_ptr<EventListener>& e) {
                    if (e->ShouldBeNotifiedOnFileIO()) {
                      listeners_.emplace_back(e);
                    }
                  });
  }

  bool ShouldNotifyListeners() const { return !listeners_.empty(); }

  std::string file_name_;
  FSSequentialFilePtr file_;
  std::atomic<size_t> offset_{0};  // read offset
  std::vector<std::shared_ptr<EventListener>> listeners_{};
  RateLimiter* rate_limiter_;
  bool verify_and_reconstruct_read_;
  struct ReopenOptions {
    std::shared_ptr<FileSystem> fs;
    FileOptions file_options;
    size_t readahead_size;
    std::shared_ptr<IOTracer> io_tracer;
  };
  std::unique_ptr<ReopenOptions> reopen_options_;

 public:
  explicit SequentialFileReader(
      std::unique_ptr<FSSequentialFile>&& _file, const std::string& _file_name,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {},
      RateLimiter* rate_limiter =
          nullptr,  // TODO: migrate call sites to provide rate limiter
      bool verify_and_reconstruct_read = false)
      : file_name_(_file_name),
        file_(std::move(_file), io_tracer, _file_name),
        listeners_(),
        rate_limiter_(rate_limiter),
        verify_and_reconstruct_read_(verify_and_reconstruct_read) {
    AddFileIOListeners(listeners);
  }

  explicit SequentialFileReader(
      std::unique_ptr<FSSequentialFile>&& _file, const std::string& _file_name,
      size_t _readahead_size,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {},
      RateLimiter* rate_limiter =
          nullptr,  // TODO: migrate call sites to provide rate limiter
      bool verify_and_reconstruct_read = false)
      : file_name_(_file_name),
        file_(NewReadaheadSequentialFile(std::move(_file), _readahead_size),
              io_tracer, _file_name),
        listeners_(),
        rate_limiter_(rate_limiter),
        verify_and_reconstruct_read_(verify_and_reconstruct_read) {
    AddFileIOListeners(listeners);
  }
  static IOStatus Create(const std::shared_ptr<FileSystem>& fs,
                         const std::string& fname, const FileOptions& file_opts,
                         std::unique_ptr<SequentialFileReader>* reader,
                         IODebugContext* dbg, RateLimiter* rate_limiter);

  ~SequentialFileReader() = default;

  SequentialFileReader(const SequentialFileReader&) = delete;
  SequentialFileReader& operator=(const SequentialFileReader&) = delete;
  SequentialFileReader(SequentialFileReader&&) = delete;
  SequentialFileReader& operator=(SequentialFileReader&&) = delete;

  // `rate_limiter_priority` is used to charge the internal rate limiter when
  // enabled. The special value `Env::IO_TOTAL` makes this operation bypass the
  // rate limiter. The amount charged to the internal rate limiter is n, even
  // when less than n bytes are actually read (e.g. at end of file). To avoid
  // overcharging the rate limiter, the caller can use file size to cap n to
  // read until end of file.
  //
  // TODO(hx235): accept parameter `IOOptions` containing
  // `rate_limiter_priority` like RandomAccessFileReader::Read()
  IOStatus Read(size_t n, Slice* result, char* scratch,
                Env::IOPriority rate_limiter_priority);

  IOStatus Skip(uint64_t n);

  // Enables repositioning through a fresh file handle, including fresh
  // read-ahead buffers. The supplied options must match the original open.
  // Whether and when to reposition is decided by the reader using this file,
  // not here.
  void SetReopenOptions(const std::shared_ptr<FileSystem>& fs,
                        const FileOptions& file_options, size_t readahead_size,
                        const std::shared_ptr<IOTracer>& io_tracer);
  bool CanReopen() const { return reopen_options_ != nullptr; }
  IOStatus ReopenAt(uint64_t offset,
                    std::unique_ptr<SequentialFileReader>* reader) const;

  FSSequentialFile* file() { return file_.get(); }

  std::string file_name() { return file_name_; }

  bool use_direct_io() const { return file_->use_direct_io(); }

 private:
  // NewReadaheadSequentialFile provides a wrapper over SequentialFile to
  // always prefetch additional data with every read.
  static std::unique_ptr<FSSequentialFile> NewReadaheadSequentialFile(
      std::unique_ptr<FSSequentialFile>&& file, size_t readahead_size);
};
}  // namespace ROCKSDB_NAMESPACE
