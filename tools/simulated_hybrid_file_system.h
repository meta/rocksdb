//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <utility>

#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

// A FileSystem simulates hybrid file system by ingesting latency and limit
// IOPs.
// This class is only used for development purpose and should not be used
// in production.
// Right now we ingest 15ms latency and allow 100 requests per second when
// the file is for warm temperature.
// When the object is destroyed, the list of warm files are written to a
// file, which can be used to reopen a FileSystem and still recover the
// list. This is to allow the information to preserve between db_bench
// runs.
class SimulatedHybridFileSystem : public FileSystemWrapper {
 public:
  // metadata_file_name stores metadata of the files, so that it can be
  // loaded after process restarts. If the file doesn't exist, create
  // one. The file is written when the class is destroyed.
  // throughput_multiplier: multiplier of throughput. For example, 1 is to
  //      simulate single disk spindle. 4 is to simualte 4 disk spindles.
  // is_full_fs_warm: if true, all files are all included in slow I/O
  // simulation.
  SimulatedHybridFileSystem(const std::shared_ptr<FileSystem>& base,
                            const std::string& metadata_file_name,
                            int throughput_multiplier, bool is_full_fs_warm);

  ~SimulatedHybridFileSystem() override;

 public:
  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;
  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;
  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  const char* Name() const override { return name_.c_str(); }

 private:
  // Limit 100 requests per second. Rate limiter is designed to byte but
  // we use it as fixed bytes is one request.
  std::shared_ptr<RateLimiter> rate_limiter_;
  std::mutex mutex_;
  std::unordered_set<std::string> warm_file_set_;
  std::string metadata_file_name_;
  std::string name_;
  bool is_full_fs_warm_;
};

// Simulated random access file that can control IOPs and latency to simulate
// specific storage media
class SimulatedHybridRaf : public FSRandomAccessFileOwnerWrapper {
 public:
  SimulatedHybridRaf(std::unique_ptr<FSRandomAccessFile>&& t,
                     std::shared_ptr<RateLimiter> rate_limiter,
                     Temperature temperature)
      : FSRandomAccessFileOwnerWrapper(std::move(t)),
        rate_limiter_(rate_limiter),
        temperature_(temperature) {}

  ~SimulatedHybridRaf() override = default;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override;

 private:
  std::shared_ptr<RateLimiter> rate_limiter_;
  Temperature temperature_;

  void SimulateIOWait(int64_t num_requests) const;
};

class SimulatedWritableFile : public FSWritableFileWrapper {
 public:
  SimulatedWritableFile(std::unique_ptr<FSWritableFile>&& t,
                        std::shared_ptr<RateLimiter> rate_limiter)
      : FSWritableFileWrapper(t.get()),
        file_guard_(std::move(t)),
        rate_limiter_(rate_limiter) {}
  IOStatus Append(const Slice& data, const IOOptions&,
                  IODebugContext*) override;
  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override;
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override;

 private:
  std::unique_ptr<FSWritableFile> file_guard_;
  std::shared_ptr<RateLimiter> rate_limiter_;
  size_t unsynced_bytes = 0;

  void SimulateIOWait(int64_t num_requests) const;
};
}  // namespace ROCKSDB_NAMESPACE
