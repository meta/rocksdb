// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <fcntl.h>

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "options/options_helper.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "table/mock_table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

// In case defined by Windows headers
#undef small

namespace ROCKSDB_NAMESPACE {
class MockEnv;

namespace anon {
class AtomicCounter {
 public:
  explicit AtomicCounter(Env* env = NULL)
      : env_(env), cond_count_(&mu_), count_(0) {}

  void Increment() {
    MutexLock l(&mu_);
    count_++;
    cond_count_.SignalAll();
  }

  int Read() {
    MutexLock l(&mu_);
    return count_;
  }

  bool WaitFor(int count) {
    MutexLock l(&mu_);

    uint64_t start = env_->NowMicros();
    while (count_ < count) {
      uint64_t now = env_->NowMicros();
      cond_count_.TimedWait(now + /*1s*/ 1 * 1000 * 1000);
      if (env_->NowMicros() - start > /*10s*/ 10 * 1000 * 1000) {
        return false;
      }
      if (count_ < count) {
        GTEST_LOG_(WARNING) << "WaitFor is taking more time than usual";
      }
    }

    return true;
  }

  void Reset() {
    MutexLock l(&mu_);
    count_ = 0;
    cond_count_.SignalAll();
  }

 private:
  Env* env_;
  port::Mutex mu_;
  port::CondVar cond_count_;
  int count_;
};

struct OptionsOverride {
  std::shared_ptr<const FilterPolicy> filter_policy = nullptr;
  // These will be used only if filter_policy is set
  bool partition_filters = false;
  // Force using a default block cache. (Setting to false allows ASAN build
  // use a trivially small block cache for better UAF error detection.)
  bool full_block_cache = false;
  uint64_t metadata_block_size = 1024;

  // Used as a bit mask of individual enums in which to skip an XF test point
  int skip_policy = 0;

  // The default value for this option is changed from false to true.
  // Keeping the default to false for unit tests as old unit tests assume
  // this behavior. Tests for level_compaction_dynamic_level_bytes
  // will set the option to true explicitly.
  bool level_compaction_dynamic_level_bytes = false;
};

}  // namespace anon

enum SkipPolicy { kSkipNone = 0, kSkipNoSnapshot = 1, kSkipNoPrefix = 2 };

// Special Env used to delay background operations
class SpecialEnv : public EnvWrapper {
 public:
  explicit SpecialEnv(Env* base, bool time_elapse_only_sleep = false);

  static const char* kClassName() { return "SpecialEnv"; }
  const char* Name() const override { return kClassName(); }

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    class SSTableFile : public WritableFile {
     private:
      SpecialEnv* env_;
      std::unique_ptr<WritableFile> base_;

     public:
      SSTableFile(SpecialEnv* env, std::unique_ptr<WritableFile>&& base)
          : env_(env), base_(std::move(base)) {}
      Status Append(const Slice& data) override {
        if (env_->table_write_callback_) {
          (*env_->table_write_callback_)();
        }
        if (env_->drop_writes_.load(std::memory_order_acquire)) {
          // Drop writes on the floor
          return Status::OK();
        } else if (env_->no_space_.load(std::memory_order_acquire)) {
          return Status::NoSpace("No space left on device");
        } else {
          env_->bytes_written_ += data.size();
          return base_->Append(data);
        }
      }
      Status Append(
          const Slice& data,
          const DataVerificationInfo& /* verification_info */) override {
        return Append(data);
      }
      Status PositionedAppend(const Slice& data, uint64_t offset) override {
        if (env_->table_write_callback_) {
          (*env_->table_write_callback_)();
        }
        if (env_->drop_writes_.load(std::memory_order_acquire)) {
          // Drop writes on the floor
          return Status::OK();
        } else if (env_->no_space_.load(std::memory_order_acquire)) {
          return Status::NoSpace("No space left on device");
        } else {
          env_->bytes_written_ += data.size();
          return base_->PositionedAppend(data, offset);
        }
      }
      Status PositionedAppend(
          const Slice& data, uint64_t offset,
          const DataVerificationInfo& /* verification_info */) override {
        return PositionedAppend(data, offset);
      }
      Status Truncate(uint64_t size) override { return base_->Truncate(size); }
      Status RangeSync(uint64_t offset, uint64_t nbytes) override {
        Status s = base_->RangeSync(offset, nbytes);
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT_CALLBACK("SpecialEnv::SStableFile::RangeSync", &s);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
        return s;
      }
      Status Close() override {
// SyncPoint is not supported in Released Windows Mode.
#if !(defined NDEBUG) || !defined(OS_WIN)
        // Check preallocation size
        // preallocation size is never passed to base file.
        size_t preallocation_size = preallocation_block_size();
        TEST_SYNC_POINT_CALLBACK("DBTestWritableFile.GetPreallocationStatus",
                                 &preallocation_size);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
        Status s = base_->Close();
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT_CALLBACK("SpecialEnv::SStableFile::Close", &s);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
        return s;
      }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        while (env_->delay_sstable_sync_.load(std::memory_order_acquire)) {
          env_->SleepForMicroseconds(100000);
        }
        Status s;
        if (!env_->skip_fsync_) {
          s = base_->Sync();
        }
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT_CALLBACK("SpecialEnv::SStableFile::Sync", &s);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
        return s;
      }
      void SetIOPriority(Env::IOPriority pri) override {
        base_->SetIOPriority(pri);
      }
      Env::IOPriority GetIOPriority() override {
        return base_->GetIOPriority();
      }
      bool use_direct_io() const override { return base_->use_direct_io(); }
      Status Allocate(uint64_t offset, uint64_t len) override {
        return base_->Allocate(offset, len);
      }
      size_t GetUniqueId(char* id, size_t max_size) const override {
        return base_->GetUniqueId(id, max_size);
      }
      uint64_t GetFileSize() final { return base_->GetFileSize(); }
    };
    class ManifestFile : public WritableFile {
     public:
      ManifestFile(SpecialEnv* env, std::unique_ptr<WritableFile>&& b)
          : env_(env), base_(std::move(b)) {}
      Status Append(const Slice& data) override {
        if (env_->manifest_write_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated writer error");
        } else {
          return base_->Append(data);
        }
      }
      Status Append(
          const Slice& data,
          const DataVerificationInfo& /*verification_info*/) override {
        return Append(data);
      }

      Status Truncate(uint64_t size) override { return base_->Truncate(size); }
      Status Close() override { return base_->Close(); }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        if (env_->manifest_sync_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated sync error");
        } else {
          if (env_->skip_fsync_) {
            return Status::OK();
          } else {
            return base_->Sync();
          }
        }
      }
      uint64_t GetFileSize() override { return base_->GetFileSize(); }
      Status Allocate(uint64_t offset, uint64_t len) override {
        return base_->Allocate(offset, len);
      }

     private:
      SpecialEnv* env_;
      std::unique_ptr<WritableFile> base_;
    };
    class SpecialWalFile : public WritableFile {
     public:
      SpecialWalFile(SpecialEnv* env, std::unique_ptr<WritableFile>&& b)
          : env_(env), base_(std::move(b)) {
        env_->num_open_wal_file_.fetch_add(1);
      }
      ~SpecialWalFile() override { env_->num_open_wal_file_.fetch_add(-1); }
      Status Append(const Slice& data) override {
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT("SpecialEnv::SpecialWalFile::Append:1");
#endif
        Status s;
        if (env_->log_write_error_.load(std::memory_order_acquire)) {
          s = Status::IOError("simulated writer error");
        } else {
          int slowdown =
              env_->log_write_slowdown_.load(std::memory_order_acquire);
          if (slowdown > 0) {
            env_->SleepForMicroseconds(slowdown);
          }
          s = base_->Append(data);
        }
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT("SpecialEnv::SpecialWalFile::Append:2");
#endif
        return s;
      }
      Status Append(
          const Slice& data,
          const DataVerificationInfo& /* verification_info */) override {
        return Append(data);
      }
      Status Truncate(uint64_t size) override { return base_->Truncate(size); }
      void PrepareWrite(size_t offset, size_t len) override {
        base_->PrepareWrite(offset, len);
      }
      void SetPreallocationBlockSize(size_t size) override {
        base_->SetPreallocationBlockSize(size);
      }
      Status Close() override {
// SyncPoint is not supported in Released Windows Mode.
#if !(defined NDEBUG) || !defined(OS_WIN)
        // Check preallocation size
        size_t block_size;
        size_t last_allocated_block;
        base_->GetPreallocationStatus(&block_size, &last_allocated_block);
        TEST_SYNC_POINT_CALLBACK("DBTestWalFile.GetPreallocationStatus",
                                 &block_size);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)

        return base_->Close();
      }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        if (env_->corrupt_in_sync_) {
          EXPECT_OK(Append(std::string(33000, ' ')));
          return Status::IOError("Ingested Sync Failure");
        }
        if (env_->skip_fsync_) {
          return Status::OK();
        } else {
          return base_->Sync();
        }
      }
      bool IsSyncThreadSafe() const override {
        return env_->is_wal_sync_thread_safe_.load();
      }
      Status Allocate(uint64_t offset, uint64_t len) override {
        return base_->Allocate(offset, len);
      }
      uint64_t GetFileSize() final { return base_->GetFileSize(); }

     private:
      SpecialEnv* env_;
      std::unique_ptr<WritableFile> base_;
    };
    class OtherFile : public WritableFile {
     public:
      OtherFile(SpecialEnv* env, std::unique_ptr<WritableFile>&& b)
          : env_(env), base_(std::move(b)) {}
      Status Append(const Slice& data) override { return base_->Append(data); }
      Status Append(
          const Slice& data,
          const DataVerificationInfo& /*verification_info*/) override {
        return Append(data);
      }
      Status Truncate(uint64_t size) override { return base_->Truncate(size); }
      Status Close() override { return base_->Close(); }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        if (env_->skip_fsync_) {
          return Status::OK();
        } else {
          return base_->Sync();
        }
      }
      uint64_t GetFileSize() override { return base_->GetFileSize(); }
      Status Allocate(uint64_t offset, uint64_t len) override {
        return base_->Allocate(offset, len);
      }

     private:
      SpecialEnv* env_;
      std::unique_ptr<WritableFile> base_;
    };

    if (no_file_overwrite_.load(std::memory_order_acquire) &&
        target()->FileExists(f).ok()) {
      return Status::NotSupported("SpecialEnv::no_file_overwrite_ is true.");
    }

    if (non_writeable_rate_.load(std::memory_order_acquire) > 0) {
      uint32_t random_number;
      {
        MutexLock l(&rnd_mutex_);
        random_number = rnd_.Uniform(100);
      }
      if (random_number < non_writeable_rate_.load()) {
        return Status::IOError("simulated random write error");
      }
    }

    new_writable_count_++;

    if (non_writable_count_.load() > 0) {
      non_writable_count_--;
      return Status::IOError("simulated write error");
    }

    EnvOptions optimized = soptions;
    if (strstr(f.c_str(), "MANIFEST") != nullptr ||
        strstr(f.c_str(), "log") != nullptr) {
      optimized.use_mmap_writes = false;
      optimized.use_direct_writes = false;
    }

    Status s = target()->NewWritableFile(f, r, optimized);
    if (s.ok()) {
      if (strstr(f.c_str(), ".sst") != nullptr) {
        r->reset(new SSTableFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "MANIFEST") != nullptr) {
        r->reset(new ManifestFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "log") != nullptr) {
        r->reset(new SpecialWalFile(this, std::move(*r)));
      } else {
        r->reset(new OtherFile(this, std::move(*r)));
      }
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& soptions) override {
    class CountingFile : public RandomAccessFile {
     public:
      CountingFile(std::unique_ptr<RandomAccessFile>&& target,
                   anon::AtomicCounter* counter,
                   std::atomic<size_t>* bytes_read)
          : target_(std::move(target)),
            counter_(counter),
            bytes_read_(bytes_read) {}
      Status Read(uint64_t offset, size_t n, Slice* result,
                  char* scratch) const override {
        counter_->Increment();
        Status s = target_->Read(offset, n, result, scratch);
        *bytes_read_ += result->size();
        return s;
      }

      Status Prefetch(uint64_t offset, size_t n) override {
        Status s = target_->Prefetch(offset, n);
        *bytes_read_ += n;
        return s;
      }

      Status GetFileSize(uint64_t* s) override {
        return target_->GetFileSize(s);
      }

     private:
      std::unique_ptr<RandomAccessFile> target_;
      anon::AtomicCounter* counter_;
      std::atomic<size_t>* bytes_read_;
    };

    class RandomFailureFile : public RandomAccessFile {
     public:
      RandomFailureFile(std::unique_ptr<RandomAccessFile>&& target,
                        std::atomic<uint64_t>* failure_cnt, uint32_t fail_odd)
          : target_(std::move(target)),
            fail_cnt_(failure_cnt),
            fail_odd_(fail_odd) {}
      Status Read(uint64_t offset, size_t n, Slice* result,
                  char* scratch) const override {
        if (Random::GetTLSInstance()->OneIn(fail_odd_)) {
          fail_cnt_->fetch_add(1);
          return Status::IOError("random error");
        }
        return target_->Read(offset, n, result, scratch);
      }

      Status Prefetch(uint64_t offset, size_t n) override {
        return target_->Prefetch(offset, n);
      }

      Status GetFileSize(uint64_t* s) override {
        return target_->GetFileSize(s);
      }

     private:
      std::unique_ptr<RandomAccessFile> target_;
      std::atomic<uint64_t>* fail_cnt_;
      uint32_t fail_odd_;
    };

    Status s = target()->NewRandomAccessFile(f, r, soptions);
    random_file_open_counter_++;
    if (s.ok()) {
      if (count_random_reads_) {
        r->reset(new CountingFile(std::move(*r), &random_read_counter_,
                                  &random_read_bytes_counter_));
      } else if (rand_reads_fail_odd_ > 0) {
        r->reset(new RandomFailureFile(std::move(*r), &num_reads_fails_,
                                       rand_reads_fail_odd_));
      }
    }

    if (s.ok() && soptions.compaction_readahead_size > 0) {
      compaction_readahead_size_ = soptions.compaction_readahead_size;
    }
    return s;
  }

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& soptions) override {
    class CountingFile : public SequentialFile {
     public:
      CountingFile(std::unique_ptr<SequentialFile>&& target,
                   anon::AtomicCounter* counter)
          : target_(std::move(target)), counter_(counter) {}
      Status Read(size_t n, Slice* result, char* scratch) override {
        counter_->Increment();
        return target_->Read(n, result, scratch);
      }
      Status Skip(uint64_t n) override { return target_->Skip(n); }

     private:
      std::unique_ptr<SequentialFile> target_;
      anon::AtomicCounter* counter_;
    };

    Status s = target()->NewSequentialFile(f, r, soptions);
    if (s.ok() && count_sequential_reads_) {
      r->reset(new CountingFile(std::move(*r), &sequential_read_counter_));
    }
    return s;
  }

  void SleepForMicroseconds(int micros) override {
    sleep_counter_.Increment();
    if (no_slowdown_ || time_elapse_only_sleep_) {
      addon_microseconds_.fetch_add(micros);
    }
    if (!no_slowdown_) {
      target()->SleepForMicroseconds(micros);
    }
  }

  void MockSleepForMicroseconds(int64_t micros) {
    sleep_counter_.Increment();
    assert(no_slowdown_);
    addon_microseconds_.fetch_add(micros);
  }

  void MockSleepForSeconds(int64_t seconds) {
    sleep_counter_.Increment();
    assert(no_slowdown_);
    addon_microseconds_.fetch_add(seconds * 1000000);
  }

  Status GetCurrentTime(int64_t* unix_time) override {
    Status s;
    if (time_elapse_only_sleep_) {
      *unix_time = maybe_starting_time_;
    } else {
      s = target()->GetCurrentTime(unix_time);
    }
    if (s.ok()) {
      // mock microseconds elapsed to seconds of time
      *unix_time += addon_microseconds_.load() / 1000000;
    }
    return s;
  }

  uint64_t NowCPUNanos() override {
    now_cpu_count_.fetch_add(1);
    return target()->NowCPUNanos();
  }

  uint64_t NowNanos() override {
    return (time_elapse_only_sleep_ ? 0 : target()->NowNanos()) +
           addon_microseconds_.load() * 1000;
  }

  uint64_t NowMicros() override {
    return (time_elapse_only_sleep_ ? 0 : target()->NowMicros()) +
           addon_microseconds_.load();
  }

  Status DeleteFile(const std::string& fname) override {
    delete_count_.fetch_add(1);
    return target()->DeleteFile(fname);
  }

  void SetMockSleep(bool enabled = true) { no_slowdown_ = enabled; }

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    if (!skip_fsync_) {
      return target()->NewDirectory(name, result);
    } else {
      class NoopDirectory : public Directory {
       public:
        NoopDirectory() {}
        ~NoopDirectory() override {}

        Status Fsync() override { return Status::OK(); }
        Status Close() override { return Status::OK(); }
      };

      result->reset(new NoopDirectory());
      return Status::OK();
    }
  }

  Status RenameFile(const std::string& src, const std::string& dest) override {
    rename_count_.fetch_add(1);
    if (rename_error_.load(std::memory_order_acquire)) {
      return Status::NotSupported("Simulated `RenameFile()` error.");
    }
    return target()->RenameFile(src, dest);
  }

  // Something to return when mocking current time
  const int64_t maybe_starting_time_;

  Random rnd_;
  port::Mutex rnd_mutex_;  // Lock to pretect rnd_

  // sstable Sync() calls are blocked while this pointer is non-nullptr.
  std::atomic<bool> delay_sstable_sync_;

  // Drop writes on the floor while this pointer is non-nullptr.
  std::atomic<bool> drop_writes_;

  // Simulate no-space errors while this pointer is non-nullptr.
  std::atomic<bool> no_space_;

  // Simulate non-writable file system while this pointer is non-nullptr
  std::atomic<bool> non_writable_;

  // Force sync of manifest files to fail while this pointer is non-nullptr
  std::atomic<bool> manifest_sync_error_;

  // Force write to manifest files to fail while this pointer is non-nullptr
  std::atomic<bool> manifest_write_error_;

  // Force write to log files to fail while this pointer is non-nullptr
  std::atomic<bool> log_write_error_;

  // Force `RenameFile()` to fail while this pointer is non-nullptr
  std::atomic<bool> rename_error_{false};

  // Slow down every log write, in micro-seconds.
  std::atomic<int> log_write_slowdown_;

  // If true, returns Status::NotSupported for file overwrite.
  std::atomic<bool> no_file_overwrite_;

  // Number of WAL files that are still open for write.
  std::atomic<int> num_open_wal_file_;

  bool count_random_reads_;
  uint32_t rand_reads_fail_odd_ = 0;
  std::atomic<uint64_t> num_reads_fails_;
  anon::AtomicCounter random_read_counter_;
  std::atomic<size_t> random_read_bytes_counter_;
  std::atomic<int> random_file_open_counter_;

  bool count_sequential_reads_;
  anon::AtomicCounter sequential_read_counter_;

  anon::AtomicCounter sleep_counter_;

  std::atomic<int64_t> bytes_written_;

  std::atomic<int> sync_counter_;

  // If true, all fsync to files and directories are skipped.
  bool skip_fsync_ = false;

  // If true, ingest the corruption to file during sync.
  bool corrupt_in_sync_ = false;

  std::atomic<uint32_t> non_writeable_rate_;

  std::atomic<uint32_t> new_writable_count_;

  std::atomic<uint32_t> non_writable_count_;

  std::function<void()>* table_write_callback_;

  std::atomic<int> now_cpu_count_;

  std::atomic<int> delete_count_;

  std::atomic<int> rename_count_{0};

  std::atomic<bool> is_wal_sync_thread_safe_{true};

  std::atomic<size_t> compaction_readahead_size_{};

 private:  // accessing these directly is prone to error
  friend class DBTestBase;

  std::atomic<int64_t> addon_microseconds_{0};

  // Do not modify in the env of a running DB (could cause deadlock)
  std::atomic<bool> time_elapse_only_sleep_;

  bool no_slowdown_;
};

class FileTemperatureTestFS : public FileSystemWrapper {
 public:
  explicit FileTemperatureTestFS(const std::shared_ptr<FileSystem>& fs)
      : FileSystemWrapper(fs) {}

  static const char* kClassName() { return "FileTemperatureTestFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewSequentialFile(const std::string& fname, const FileOptions& opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    IOStatus s = target()->NewSequentialFile(fname, opts, result, dbg);
    uint64_t number;
    FileType type;
    if (ParseFileName(GetFileName(fname), &number, &type) &&
        type == kTableFile) {
      MutexLock lock(&mu_);
      requested_sst_file_temperatures_.emplace_back(number, opts.temperature);
      if (s.ok()) {
        if (opts.temperature != Temperature::kUnknown) {
          // Be extra picky and don't open if a wrong non-unknown temperature is
          // provided
          auto e = current_sst_file_temperatures_.find(number);
          if (e != current_sst_file_temperatures_.end() &&
              e->second != opts.temperature) {
            result->reset();
            return IOStatus::PathNotFound(
                "Read requested temperature " +
                temperature_to_string[opts.temperature] +
                " but stored with temperature " +
                temperature_to_string[e->second] + " for " + fname);
          }
        }
        *result = WrapWithTemperature<FSSequentialFileOwnerWrapper>(
            number, std::move(*result));
      }
    }
    return s;
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    IOStatus s = target()->NewRandomAccessFile(fname, opts, result, dbg);
    uint64_t number;
    FileType type;
    if (ParseFileName(GetFileName(fname), &number, &type) &&
        type == kTableFile) {
      MutexLock lock(&mu_);
      requested_sst_file_temperatures_.emplace_back(number, opts.temperature);
      if (s.ok()) {
        if (opts.temperature != Temperature::kUnknown) {
          // Be extra picky and don't open if a wrong non-unknown temperature is
          // provided
          auto e = current_sst_file_temperatures_.find(number);
          if (e != current_sst_file_temperatures_.end() &&
              e->second != opts.temperature) {
            result->reset();
            return IOStatus::PathNotFound(
                "Read requested temperature " +
                temperature_to_string[opts.temperature] +
                " but stored with temperature " +
                temperature_to_string[e->second] + " for " + fname);
          }
        }
        *result = WrapWithTemperature<FSRandomAccessFileOwnerWrapper>(
            number, std::move(*result));
      }
    }
    return s;
  }

  void PopRequestedSstFileTemperatures(
      std::vector<std::pair<uint64_t, Temperature>>* out = nullptr) {
    MutexLock lock(&mu_);
    if (out) {
      *out = std::move(requested_sst_file_temperatures_);
      assert(requested_sst_file_temperatures_.empty());
    } else {
      requested_sst_file_temperatures_.clear();
    }
  }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    uint64_t number;
    FileType type;
    if (ParseFileName(GetFileName(fname), &number, &type) &&
        type == kTableFile) {
      MutexLock lock(&mu_);
      current_sst_file_temperatures_[number] = opts.temperature;
    }
    return target()->NewWritableFile(fname, opts, result, dbg);
  }

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override {
    IOStatus ios = target()->DeleteFile(fname, options, dbg);
    if (ios.ok()) {
      uint64_t number;
      FileType type;
      if (ParseFileName(GetFileName(fname), &number, &type) &&
          type == kTableFile) {
        MutexLock lock(&mu_);
        current_sst_file_temperatures_.erase(number);
      }
    }
    return ios;
  }

  void CopyCurrentSstFileTemperatures(std::map<uint64_t, Temperature>* out) {
    MutexLock lock(&mu_);
    *out = current_sst_file_temperatures_;
  }

  size_t CountCurrentSstFilesWithTemperature(Temperature temp) {
    MutexLock lock(&mu_);
    size_t count = 0;
    for (const auto& e : current_sst_file_temperatures_) {
      if (e.second == temp) {
        ++count;
      }
    }
    return count;
  }

  std::map<Temperature, size_t> CountCurrentSstFilesByTemp() {
    MutexLock lock(&mu_);
    std::map<Temperature, size_t> ret;
    for (const auto& e : current_sst_file_temperatures_) {
      ret[e.second]++;
    }
    return ret;
  }

  void OverrideSstFileTemperature(uint64_t number, Temperature temp) {
    MutexLock lock(&mu_);
    current_sst_file_temperatures_[number] = temp;
  }

 protected:
  port::Mutex mu_;
  std::vector<std::pair<uint64_t, Temperature>>
      requested_sst_file_temperatures_;
  std::map<uint64_t, Temperature> current_sst_file_temperatures_;

  static std::string GetFileName(const std::string& fname) {
    auto filename = fname.substr(fname.find_last_of(kFilePathSeparator) + 1);
    // workaround only for Windows that the file path could contain both Windows
    // FilePathSeparator and '/'
    filename = filename.substr(filename.find_last_of('/') + 1);
    return filename;
  }

  template <class FileOwnerWrapperT, /*inferred*/ class FileT>
  std::unique_ptr<FileT> WrapWithTemperature(uint64_t number,
                                             std::unique_ptr<FileT>&& t) {
    class FileWithTemp : public FileOwnerWrapperT {
     public:
      FileWithTemp(FileTemperatureTestFS* fs, uint64_t number,
                   std::unique_ptr<FileT>&& t)
          : FileOwnerWrapperT(std::move(t)), fs_(fs), number_(number) {}

      Temperature GetTemperature() const override {
        MutexLock lock(&fs_->mu_);
        return fs_->current_sst_file_temperatures_[number_];
      }

     private:
      FileTemperatureTestFS* fs_;
      uint64_t number_;
    };
    return std::make_unique<FileWithTemp>(this, number, std::move(t));
  }
};

class OnFileDeletionListener : public EventListener {
 public:
  OnFileDeletionListener() : matched_count_(0) {}
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "OnFileDeletionListener"; }

  void SetExpectedFileName(const std::string file_name) {
    expected_file_name_ = file_name;
  }

  void VerifyMatchedCount(size_t expected_value) {
    ASSERT_EQ(matched_count_, expected_value);
  }

  void OnTableFileDeleted(const TableFileDeletionInfo& info) override {
    if (expected_file_name_ != "") {
      ASSERT_EQ(expected_file_name_, info.file_path);
      expected_file_name_ = "";
      matched_count_++;
    }
  }

 private:
  size_t matched_count_;
  std::string expected_file_name_;
};

class FlushCounterListener : public EventListener {
 public:
  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "FlushCounterListener"; }
  std::atomic<int> count{0};
  std::atomic<FlushReason> expected_flush_reason{FlushReason::kOthers};

  void OnFlushBegin(DB* /*db*/, const FlushJobInfo& flush_job_info) override {
    count++;
    ASSERT_EQ(expected_flush_reason.load(), flush_job_info.flush_reason);
  }
};

// A test merge operator mimics put but also fails if one of merge operands is
// "corrupted", "corrupted_try_merge", or "corrupted_must_merge".
class TestPutOperator : public MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    static const std::map<std::string, MergeOperator::OpFailureScope>
        bad_operand_to_op_failure_scope = {
            {"corrupted", MergeOperator::OpFailureScope::kDefault},
            {"corrupted_try_merge", MergeOperator::OpFailureScope::kTryMerge},
            {"corrupted_must_merge",
             MergeOperator::OpFailureScope::kMustMerge}};
    auto check_operand =
        [](Slice operand_val,
           MergeOperator::OpFailureScope* op_failure_scope) -> bool {
      auto iter = bad_operand_to_op_failure_scope.find(operand_val.ToString());
      if (iter != bad_operand_to_op_failure_scope.end()) {
        *op_failure_scope = iter->second;
        return false;
      }
      return true;
    };
    if (merge_in.existing_value != nullptr &&
        !check_operand(*merge_in.existing_value,
                       &merge_out->op_failure_scope)) {
      return false;
    }
    for (auto value : merge_in.operand_list) {
      if (!check_operand(value, &merge_out->op_failure_scope)) {
        return false;
      }
    }
    merge_out->existing_operand = merge_in.operand_list.back();
    return true;
  }

  const char* Name() const override { return "TestPutOperator"; }
};

/*
 * A cache wrapper that tracks certain CacheEntryRole's cache charge, its
 * peaks and increments
 *
 *        p0
 *       / \   p1
 *      /   \  /\
 *     /     \/  \
 *  a /       b   \
 * peaks = {p0, p1}
 * increments = {p1-a, p2-b}
 */
template <CacheEntryRole R>
class TargetCacheChargeTrackingCache : public CacheWrapper {
 public:
  explicit TargetCacheChargeTrackingCache(std::shared_ptr<Cache> target);

  const char* Name() const override { return "TargetCacheChargeTrackingCache"; }

  Status Insert(const Slice& key, ObjectPtr value,
                const CacheItemHelper* helper, size_t charge,
                Handle** handle = nullptr, Priority priority = Priority::LOW,
                const Slice& compressed = Slice(),
                CompressionType type = kNoCompression) override;

  using Cache::Release;
  bool Release(Handle* handle, bool erase_if_last_ref = false) override;

  std::size_t GetCacheCharge() { return cur_cache_charge_; }

  std::deque<std::size_t> GetChargedCachePeaks() { return cache_charge_peaks_; }

  std::size_t GetChargedCacheIncrementSum() {
    return cache_charge_increments_sum_;
  }

 private:
  static const Cache::CacheItemHelper* kCrmHelper;

  std::size_t cur_cache_charge_;
  std::size_t cache_charge_peak_;
  std::size_t cache_charge_increment_;
  bool last_peak_tracked_;
  std::deque<std::size_t> cache_charge_peaks_;
  std::size_t cache_charge_increments_sum_;
};

class DBTestBase : public testing::Test {
 public:
  // Sequence of option configurations to try
  enum OptionConfig : int {
    kDefault = 0,
    kBlockBasedTableWithPrefixHashIndex = 1,
    kBlockBasedTableWithWholeKeyHashIndex = 2,
    kPlainTableFirstBytePrefix = 3,
    kPlainTableCappedPrefix = 4,
    kPlainTableCappedPrefixNonMmap = 5,
    kPlainTableAllBytesPrefix = 6,
    kVectorRep = 7,
    kHashLinkList = 8,
    kMergePut = 9,
    kFilter = 10,
    kFullFilterWithNewTableReaderForCompactions = 11,
    kUncompressed = 12,
    kNumLevel_3 = 13,
    kDBLogDir = 14,
    kWalDirAndMmapReads = 15,
    kManifestFileSize = 16,
    kPerfOptions = 17,
    kHashSkipList = 18,
    kUniversalCompaction = 19,
    kUniversalCompactionMultiLevel = 20,
    kInfiniteMaxOpenFiles = 21,
    kCRC32cChecksum = 22,
    kFIFOCompaction = 23,
    kOptimizeFiltersForHits = 24,
    kRowCache = 25,
    kRecycleLogFiles = 26,
    kConcurrentSkipList = 27,
    kPipelinedWrite = 28,
    kConcurrentWALWrites = 29,
    kDirectIO,
    kLevelSubcompactions,
    kBlockBasedTableWithIndexRestartInterval,
    kBlockBasedTableWithPartitionedIndex,
    kBlockBasedTableWithPartitionedIndexFormat4,
    kBlockBasedTableWithLatestFormat,
    kPartitionedFilterWithNewTableReaderForCompactions,
    kUniversalSubcompactions,
    kUnorderedWrite,
    kBlockBasedTableWithBinarySearchWithFirstKeyIndex,
    // This must be the last line
    kEnd,
  };

 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  std::string alternative_db_log_dir_;
  MockEnv* mem_env_;
  Env* encrypted_env_;
  SpecialEnv* env_;
  std::shared_ptr<Env> env_read_only_;
  std::shared_ptr<Env> env_guard_;
  std::unique_ptr<DB> db_;
  std::vector<ColumnFamilyHandle*> handles_;

  int option_config_;
  Options last_options_;

  // Skip some options, as they may not be applicable to a specific test.
  // To add more skip constants, use values 4, 8, 16, etc.
  enum OptionSkip {
    kNoSkip = 0,
    kSkipDeletesFilterFirst = 1,
    kSkipUniversalCompaction = 2,
    kSkipMergePut = 4,
    kSkipPlainTable = 8,
    kSkipHashIndex = 16,
    kSkipNoSeekToLast = 32,
    kSkipFIFOCompaction = 128,
    kSkipMmapReads = 256,
    kSkipRowCache = 512,
  };

  const int kRangeDelSkipConfigs =
      // Plain tables do not support range deletions.
      kSkipPlainTable |
      // MmapReads disables the iterator pinning that RangeDelAggregator
      // requires.
      kSkipMmapReads |
      // Not compatible yet.
      kSkipRowCache;

  // `env_do_fsync` decides whether the special Env would do real
  // fsync for files and directories. Skipping fsync can speed up
  // tests, but won't cover the exact fsync logic.
  DBTestBase(const std::string path, bool env_do_fsync);

  ~DBTestBase() override;

  static std::string Key(int i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "key%06d", i);
    return std::string(buf);
  }

  // Expects valid key created by Key().
  static int IdFromKey(const std::string& key) {
    return std::stoi(key.substr(3));
  }

  static bool ShouldSkipOptions(int option_config, int skip_mask = kNoSkip);

  // Get the name of the option configuration
  static const char* GetOptionConfigName(int option_config);

  // Switch to a fresh database with the next option configuration to
  // test.  Return false if there are no more configurations to test.
  bool ChangeOptions(int skip_mask = kNoSkip);

  // Switch between different compaction styles.
  bool ChangeCompactOptions();

  // Switch between different WAL-realted options.
  bool ChangeWalOptions();

  // Switch between different filter policy
  // Jump from kDefault to kFilter to kFullFilter
  bool ChangeFilterOptions();

  // Switch between different DB options for file ingestion tests.
  bool ChangeOptionsForFileIngestionTest();

  // Return the current option configuration.
  Options CurrentOptions(const anon::OptionsOverride& options_override =
                             anon::OptionsOverride()) const;

  Options CurrentOptions(const Options& default_options,
                         const anon::OptionsOverride& options_override =
                             anon::OptionsOverride()) const;

  Options GetDefaultOptions() const;

  Options GetOptions(int option_config) const {
    return GetOptions(option_config, GetDefaultOptions());
  }

  Options GetOptions(int option_config, const Options& default_options,
                     const anon::OptionsOverride& options_override =
                         anon::OptionsOverride()) const;

  DBImpl* dbfull() { return static_cast_with_check<DBImpl>(db_.get()); }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const Options& options);

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options& options);

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<Options>& options);

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options& options);

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const std::vector<Options>& options);

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options& options);

  Status TryReopenReadOnlyWithColumnFamilies(
      const std::vector<std::string>& cfs, const std::vector<Options>& options);

  Status TryReopenReadOnlyWithColumnFamilies(
      const std::vector<std::string>& cfs, const Options& options);

  void Reopen(const Options& options);

  void Close();

  void DestroyAndReopen(const Options& options);

  void Destroy(const Options& options, bool delete_cf_paths = false);

  Status ReadOnlyReopen(const Options& options);

  // With a filesystem wrapper that fails on attempted write
  Status EnforcedReadOnlyReopen(const Options& options);

  Status TryReopen(const Options& options);

  bool IsDirectIOSupported();

  bool IsMemoryMappedAccessSupported() const;

  Status Flush(int cf = 0);

  Status Flush(const std::vector<int>& cf_ids);

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions());

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions());

  Status TimedPut(const Slice& k, const Slice& v, uint64_t write_unix_time,
                  WriteOptions wo = WriteOptions());

  Status TimedPut(int cf, const Slice& k, const Slice& v,
                  uint64_t write_unix_time, WriteOptions wo = WriteOptions());

  Status Merge(const Slice& k, const Slice& v,
               WriteOptions wo = WriteOptions());

  Status Merge(int cf, const Slice& k, const Slice& v,
               WriteOptions wo = WriteOptions());

  Status Delete(const std::string& k);

  Status Delete(int cf, const std::string& k);

  Status SingleDelete(const std::string& k);

  Status SingleDelete(int cf, const std::string& k);

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr,
                  bool use_coroutine = false);

  std::string Get(int cf, const std::string& k,
                  const Snapshot* snapshot = nullptr,
                  bool use_coroutine = false);

  Status Get(const std::string& k, PinnableSlice* v,
             bool use_coroutine = false);

  std::vector<std::string> MultiGet(std::vector<int> cfs,
                                    const std::vector<std::string>& k,
                                    const Snapshot* snapshot, bool batched,
                                    bool async = false,
                                    bool use_coroutine = false);

  std::vector<std::string> MultiGet(const std::vector<std::string>& k,
                                    const Snapshot* snapshot = nullptr,
                                    bool async = false,
                                    bool optimize_multiget_for_io = true,
                                    bool use_coroutine = false);

  void MultiGet(const std::vector<std::string>& k, const Snapshot* snapshot,
                bool async, bool optimize_multiget_for_io, bool use_coroutine,
                std::vector<PinnableSlice>& values,
                std::vector<Status>& statuses);

  Status CompactRange(const CompactRangeOptions& options,
                      std::optional<Slice> begin, std::optional<Slice> end);

  uint64_t GetNumSnapshots();

  uint64_t GetTimeOldestSnapshots();

  uint64_t GetSequenceOldestSnapshots();

  // Return a string that contains all key,value pairs in order,
  // formatted like "(k1->v1)(k2->v2)".
  std::string Contents(int cf = 0);

  std::string AllEntriesFor(const Slice& user_key, int cf = 0);

  // Similar to AllEntriesFor but this function also covers reopen with fifo.
  // Note that test cases with snapshots or entries in memtable should simply
  // use AllEntriesFor instead as snapshots and entries in memtable will
  // survive after db reopen.
  void CheckAllEntriesWithFifoReopen(const std::string& expected_value,
                                     const Slice& user_key, int cf,
                                     const std::vector<std::string>& cfs,
                                     const Options& options);

  int NumSortedRuns(int cf = 0);

  uint64_t TotalSize(int cf = 0);

  uint64_t SizeAtLevel(int level);

  size_t TotalLiveFiles(int cf = 0);

  size_t TotalLiveFilesAtPath(int cf, const std::string& path);

  size_t CountLiveFiles();

  int NumTableFilesAtLevel(int level, int cf = 0);

  int NumTableFilesAtLevel(int level, ColumnFamilyHandle* column_family,
                           DB* db = nullptr);

  double CompressionRatioAtLevel(int level, int cf = 0);

  int TotalTableFiles(int cf = 0, int levels = -1);

  std::vector<uint64_t> GetBlobFileNumbers();

  // Return spread of files per level
  std::string FilesPerLevel(int cf = 0);

  std::string FilesPerLevel(ColumnFamilyHandle* cfh, DB* db = nullptr);

  size_t CountFiles();

  Status CountFiles(size_t* count);

  std::vector<FileMetaData*> GetLevelFileMetadatas(int level, int cf = 0);

  Status Size(const Slice& start, const Slice& limit, uint64_t* size) {
    return Size(start, limit, 0, size);
  }

  Status Size(const Slice& start, const Slice& limit, int cf, uint64_t* size);

  void Compact(int cf, const Slice& start, const Slice& limit,
               uint32_t target_path_id);

  void Compact(int cf, const Slice& start, const Slice& limit);

  void Compact(const Slice& start, const Slice& limit);

  // Do n memtable compactions, each of which produces an sstable
  // covering the range [small,large].
  void MakeTables(int n, const std::string& small, const std::string& large,
                  int cf = 0);

  // Prevent pushing of new sstables into deeper levels by adding
  // tables that cover a specified range to all levels.
  void FillLevels(const std::string& smallest, const std::string& largest,
                  int cf);

  void MoveFilesToLevel(int level, int cf = 0);

  void MoveFilesToLevel(int level, ColumnFamilyHandle* column_family,
                        DB* db = nullptr);

  void DumpFileCounts(const char* label);

  std::string DumpSSTableList();

  static void GetSstFiles(Env* env, std::string path,
                          std::vector<std::string>* files);

  int GetSstFileCount(std::string path);

  // this will generate non-overlapping files since it keeps increasing key_idx
  void GenerateNewFile(Random* rnd, int* key_idx, bool nowait = false);

  void GenerateNewFile(int fd, Random* rnd, int* key_idx, bool nowait = false);

  static const int kNumKeysByGenerateNewRandomFile;
  static const int KNumKeysByGenerateNewFile = 100;

  void GenerateNewRandomFile(Random* rnd, bool nowait = false);

  std::string IterStatus(Iterator* iter);

  Options OptionsForLogIterTest();

  std::string DummyString(size_t len, char c = 'a');

  void VerifyIterLast(std::string expected_key, int cf = 0);

  // Used to test InplaceUpdate

  // If previous value is nullptr or delta is > than previous value,
  //   sets newValue with delta
  // If previous value is not empty,
  //   updates previous value with 'b' string of previous value size - 1.
  static UpdateStatus updateInPlaceSmallerSize(char* prevValue,
                                               uint32_t* prevSize, Slice delta,
                                               std::string* newValue);

  static UpdateStatus updateInPlaceSmallerVarintSize(char* prevValue,
                                                     uint32_t* prevSize,
                                                     Slice delta,
                                                     std::string* newValue);

  static UpdateStatus updateInPlaceLargerSize(char* prevValue,
                                              uint32_t* prevSize, Slice delta,
                                              std::string* newValue);

  static UpdateStatus updateInPlaceNoAction(char* prevValue, uint32_t* prevSize,
                                            Slice delta, std::string* newValue);

  // Utility method to test InplaceUpdate
  void validateNumberOfEntries(int numValues, int cf = 0);

  void CopyFile(const std::string& source, const std::string& destination,
                uint64_t size = 0);

  Status GetAllDataFiles(const FileType file_type,
                         std::unordered_map<std::string, uint64_t>* sst_files,
                         uint64_t* total_size = nullptr);

  std::vector<std::uint64_t> ListTableFiles(Env* env, const std::string& path);

  void VerifyDBFromMap(
      std::map<std::string, std::string> true_data,
      size_t* total_reads_res = nullptr, bool tailing_iter = false,
      ReadOptions* ro = nullptr, ColumnFamilyHandle* cf = nullptr,
      std::unordered_set<std::string>* not_found = nullptr) const;

  void VerifyDBInternal(
      std::vector<std::pair<std::string, std::string>> true_data);

  uint64_t GetNumberOfSstFilesForColumnFamily(DB* db,
                                              std::string column_family_name);

  uint64_t GetSstSizeHelper(Temperature temperature);

  uint64_t TestGetTickerCount(const Options& options, Tickers ticker_type) {
    return options.statistics->getTickerCount(ticker_type);
  }

  uint64_t TestGetAndResetTickerCount(const Options& options,
                                      Tickers ticker_type) {
    return options.statistics->getAndResetTickerCount(ticker_type);
  }
  // Short name for TestGetAndResetTickerCount
  uint64_t PopTicker(const Options& options, Tickers ticker_type) {
    return options.statistics->getAndResetTickerCount(ticker_type);
  }

  // Note: reverting this setting within the same test run is not yet
  // supported
  void SetTimeElapseOnlySleepOnReopen(DBOptions* options);

  void ResetTableProperties(TableProperties* tp) {
    tp->data_size = 0;
    tp->index_size = 0;
    tp->filter_size = 0;
    tp->raw_key_size = 0;
    tp->raw_value_size = 0;
    tp->num_data_blocks = 0;
    tp->num_data_blocks_compression_rejected = 0;
    tp->num_data_blocks_compression_bypassed = 0;
    tp->num_entries = 0;
    tp->num_deletions = 0;
    tp->num_merge_operands = 0;
    tp->num_range_deletions = 0;
    tp->standard_index_is_stub = 0;
  }

  void ParseTablePropertiesString(std::string tp_string, TableProperties* tp) {
    double dummy_double;
    std::replace(tp_string.begin(), tp_string.end(), ';', ' ');
    std::replace(tp_string.begin(), tp_string.end(), '=', ' ');
    ResetTableProperties(tp);
    int count = sscanf(
        tp_string.c_str(),
        "# data blocks %" SCNu64 " # data blocks compression rejected %" SCNu64
        " # data blocks compression bypassed %" SCNu64
        " # uniform blocks %" SCNu64 " # entries %" SCNu64
        " # deletions %" SCNu64 " # merge operands %" SCNu64
        " # range deletions %" SCNu64 " raw key size %" SCNu64
        " raw average key size %lf "
        " raw value size %" SCNu64
        " raw average value size %lf "
        " data block size %" SCNu64 " data uncompressed size %" SCNu64
        " index block size (user-key? %" SCNu64 ", delta-value? %" SCNu64
        ") %" SCNu64 " filter block size %" SCNu64,
        &tp->num_data_blocks, &tp->num_data_blocks_compression_rejected,
        &tp->num_data_blocks_compression_bypassed, &tp->num_uniform_blocks,
        &tp->num_entries, &tp->num_deletions, &tp->num_merge_operands,
        &tp->num_range_deletions, &tp->raw_key_size, &dummy_double,
        &tp->raw_value_size, &dummy_double, &tp->data_size,
        &tp->uncompressed_data_size, &tp->index_key_is_user_key,
        &tp->index_value_is_delta_encoded, &tp->index_size, &tp->filter_size);
    ASSERT_EQ(count, 18);

    const char* standard_index_is_stub =
        std::strstr(tp_string.c_str(), "standard index is stub ");
    if (standard_index_is_stub != nullptr) {
      ASSERT_EQ(
          std::sscanf(standard_index_is_stub, "standard index is stub %" SCNu64,
                      &tp->standard_index_is_stub),
          1);
    }
  }

 private:  // Prone to error on direct use
  void MaybeInstallTimeElapseOnlySleep(const DBOptions& options);

  bool time_elapse_only_sleep_on_reopen_ = false;
};

// For verifying that all files generated by current version have SST
// unique ids.
void VerifySstUniqueIds(const TablePropertiesCollection& props);

// Excludes kUnknown
extern const std::vector<Temperature> kKnownTemperatures;
Temperature RandomKnownTemperature();

}  // namespace ROCKSDB_NAMESPACE
