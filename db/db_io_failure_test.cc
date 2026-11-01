//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iomanip>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
namespace {
// A wrapper that allows injection of errors.
class CorruptionFS : public FileSystemWrapper {
 public:
  bool writable_file_error_;
  int num_writable_file_errors_;

  explicit CorruptionFS(const std::shared_ptr<FileSystem>& _target,
                        bool fs_buffer, bool verify_read)
      : FileSystemWrapper(_target),
        writable_file_error_(false),
        num_writable_file_errors_(0),
        corruption_trigger_(INT_MAX),
        read_count_(0),
        corrupt_offset_(0),
        corrupt_len_(0),
        rnd_(300),
        fs_buffer_(fs_buffer),
        verify_read_(verify_read) {}
  ~CorruptionFS() override {
    // Assert that the corruption was reset, which means it got triggered
    assert(corruption_trigger_ == INT_MAX || corrupt_len_ > 0);
  }
  const char* Name() const override { return "ErrorEnv"; }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    result->reset();
    if (writable_file_error_) {
      ++num_writable_file_errors_;
      return IOStatus::IOError(fname, "fake error");
    }
    return target()->NewWritableFile(fname, opts, result, dbg);
  }

  void SetCorruptionTrigger(const int trigger) {
    MutexLock l(&mutex_);
    corruption_trigger_ = trigger;
    read_count_ = 0;
    corrupt_fname_.clear();
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    class CorruptionRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
     public:
      CorruptionRandomAccessFile(CorruptionFS& fs, const std::string& fname,
                                 std::unique_ptr<FSRandomAccessFile>& file)
          : FSRandomAccessFileOwnerWrapper(std::move(file)),
            fs_(fs),
            fname_(fname) {}

      IOStatus Read(uint64_t offset, size_t len, const IOOptions& opts,
                    Slice* result, char* scratch,
                    IODebugContext* dbg) const override {
        IOStatus s = target()->Read(offset, len, opts, result, scratch, dbg);
        if (opts.verify_and_reconstruct_read) {
          fs_.MaybeResetOverlapWithCorruptedChunk(fname_, offset,
                                                  result->size());
          return s;
        }

        MutexLock l(&fs_.mutex_);
        if (s.ok() && ++fs_.read_count_ >= fs_.corruption_trigger_) {
          fs_.corruption_trigger_ = INT_MAX;
          char* data = const_cast<char*>(result->data());
          std::memcpy(
              data,
              fs_.rnd_.RandomString(static_cast<int>(result->size())).c_str(),
              result->size());
          fs_.SetCorruptedChunk(fname_, offset, result->size());
        }
        return s;
      }

      IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                         const IOOptions& options,
                         IODebugContext* dbg) override {
        for (size_t i = 0; i < num_reqs; ++i) {
          FSReadRequest& req = reqs[i];
          if (fs_.fs_buffer_) {
            // See https://github.com/meta/rocksdb/pull/13195 for why we
            // want to set up our test implementation for FSAllocationPtr this
            // way.
            char* internalData = new char[req.len];
            req.status = Read(req.offset, req.len, options, &req.result,
                              internalData, dbg);

            Slice* internalSlice = new Slice(internalData, req.len);
            FSAllocationPtr internalPtr(internalSlice, [](void* ptr) {
              delete[] static_cast<const char*>(
                  static_cast<Slice*>(ptr)->data_);
              delete static_cast<Slice*>(ptr);
            });
            req.fs_scratch = std::move(internalPtr);
          } else {
            req.status = Read(req.offset, req.len, options, &req.result,
                              req.scratch, dbg);
          }
        }
        return IOStatus::OK();
      }

      IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                        const IOOptions& /*options*/,
                        IODebugContext* /*dbg*/) override {
        return IOStatus::NotSupported("Prefetch");
      }

     private:
      CorruptionFS& fs_;
      std::string fname_;
    };

    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    EXPECT_OK(s);
    result->reset(new CorruptionRandomAccessFile(*this, fname, file));

    return s;
  }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    class CorruptionSequentialFile : public FSSequentialFileOwnerWrapper {
     public:
      CorruptionSequentialFile(CorruptionFS& fs, const std::string& fname,
                               std::unique_ptr<FSSequentialFile>& file)
          : FSSequentialFileOwnerWrapper(std::move(file)),
            fs_(fs),
            fname_(fname),
            offset_(0) {}

      IOStatus Read(size_t len, const IOOptions& opts, Slice* result,
                    char* scratch, IODebugContext* dbg) override {
        IOStatus s = target()->Read(len, opts, result, scratch, dbg);
        if (result->size() == 0 ||
            fname_.find("IDENTITY") != std::string::npos) {
          return s;
        }

        if (opts.verify_and_reconstruct_read) {
          fs_.MaybeResetOverlapWithCorruptedChunk(fname_, offset_,
                                                  result->size());
          return s;
        }

        MutexLock l(&fs_.mutex_);
        if (s.ok() && ++fs_.read_count_ >= fs_.corruption_trigger_) {
          fs_.corruption_trigger_ = INT_MAX;
          char* data = const_cast<char*>(result->data());
          std::memcpy(
              data,
              fs_.rnd_.RandomString(static_cast<int>(result->size())).c_str(),
              result->size());
          fs_.SetCorruptedChunk(fname_, offset_, result->size());
        }
        offset_ += result->size();
        return s;
      }

     private:
      CorruptionFS& fs_;
      std::string fname_;
      size_t offset_;
    };

    std::unique_ptr<FSSequentialFile> file;
    IOStatus s = target()->NewSequentialFile(fname, file_opts, &file, dbg);
    EXPECT_OK(s);
    result->reset(new CorruptionSequentialFile(*this, fname, file));

    return s;
  }

  void SupportedOps(int64_t& supported_ops) override {
    supported_ops = 1 << FSSupportedOps::kAsyncIO;
    if (fs_buffer_) {
      supported_ops |= 1 << FSSupportedOps::kFSBuffer;
    }
    if (verify_read_) {
      supported_ops |= 1 << FSSupportedOps::kVerifyAndReconstructRead;
    }
  }

  void SetCorruptedChunk(const std::string& fname, size_t offset, size_t len) {
    assert(corrupt_fname_.empty());

    corrupt_fname_ = fname;
    corrupt_offset_ = offset;
    corrupt_len_ = len;
  }

  void MaybeResetOverlapWithCorruptedChunk(const std::string& fname,
                                           size_t offset, size_t len) {
    if (fname == corrupt_fname_ &&
        ((offset <= corrupt_offset_ && (offset + len) > corrupt_offset_) ||
         (offset >= corrupt_offset_ &&
          offset < (corrupt_offset_ + corrupt_len_)))) {
      corrupt_fname_.clear();
    }
  }

  bool VerifyRetry() { return corrupt_len_ > 0 && corrupt_fname_.empty(); }

  int corruption_trigger() { return corruption_trigger_; }

 private:
  int corruption_trigger_;
  int read_count_;
  std::string corrupt_fname_;
  size_t corrupt_offset_;
  size_t corrupt_len_;
  Random rnd_;
  bool fs_buffer_;
  bool verify_read_;
  port::Mutex mutex_;
};
}  // anonymous namespace

class DBIOFailureTest : public DBTestBase {
 public:
  DBIOFailureTest() : DBTestBase("db_io_failure_test", /*env_do_fsync=*/true) {}
};

// Check that number of files does not grow when writes are dropped
TEST_F(DBIOFailureTest, DropWrites) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.paranoid_checks = false;
    Reopen(options);

    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    Compact("a", "z");
    const size_t num_files = CountFiles();
    // Force out-of-space errors
    env_->drop_writes_.store(true, std::memory_order_release);
    env_->sleep_counter_.Reset();
    env_->SetMockSleep();
    for (int i = 0; i < 5; i++) {
      if (option_config_ != kUniversalCompactionMultiLevel &&
          option_config_ != kUniversalSubcompactions) {
        for (int level = 0; level < dbfull()->NumberLevels(); level++) {
          if (level > 0 && level == dbfull()->NumberLevels() - 1) {
            break;
          }
          Status s =
              dbfull()->TEST_CompactRange(level, nullptr, nullptr, nullptr,
                                          true /* disallow trivial move */);
          ASSERT_TRUE(s.ok() || s.IsCorruption());
        }
      } else {
        Status s =
            dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
        ASSERT_TRUE(s.ok() || s.IsCorruption());
      }
    }

    std::string property_value;
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("5", property_value);

    env_->drop_writes_.store(false, std::memory_order_release);
    const size_t count = CountFiles();
    ASSERT_LT(count, num_files + 3);

    // Check that compaction attempts slept after errors
    // TODO @krad: Figure out why ASSERT_EQ 5 keeps failing in certain compiler
    // versions
    ASSERT_GE(env_->sleep_counter_.Read(), 4);
  } while (ChangeCompactOptions());
}

// Check background error counter bumped on flush failures.
TEST_F(DBIOFailureTest, DropWritesFlush) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.max_background_flushes = 1;
    Reopen(options);

    ASSERT_OK(Put("foo", "v1"));
    // Force out-of-space errors
    env_->drop_writes_.store(true, std::memory_order_release);

    std::string property_value;
    // Background error count is 0 now.
    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("0", property_value);

    // ASSERT file is too short
    ASSERT_TRUE(dbfull()->TEST_FlushMemTable(true).IsCorruption());

    ASSERT_TRUE(db_->GetProperty("rocksdb.background-errors", &property_value));
    ASSERT_EQ("1", property_value);

    env_->drop_writes_.store(false, std::memory_order_release);
  } while (ChangeCompactOptions());
}

// Check that CompactRange() returns failure if there is not enough space left
// on device
TEST_F(DBIOFailureTest, NoSpaceCompactRange) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.disable_auto_compactions = true;
    Reopen(options);

    // generate 5 tables
    for (int i = 0; i < 5; ++i) {
      ASSERT_OK(Put(Key(i), Key(i) + "v"));
      ASSERT_OK(Flush());
    }

    // Force out-of-space errors
    env_->no_space_.store(true, std::memory_order_release);

    Status s = dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                           true /* disallow trivial move */);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_TRUE(s.IsNoSpace());

    env_->no_space_.store(false, std::memory_order_release);
  } while (ChangeCompactOptions());
}

TEST_F(DBIOFailureTest, NonWritableFileSystem) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 4096;
    options.arena_block_size = 4096;
    options.env = env_;
    Reopen(options);
    ASSERT_OK(Put("foo", "v1"));
    env_->non_writeable_rate_.store(100);
    std::string big(100000, 'x');
    int errors = 0;
    for (int i = 0; i < 20; i++) {
      if (!Put("foo", big).ok()) {
        errors++;
        env_->SleepForMicroseconds(100000);
      }
    }
    ASSERT_GT(errors, 0);
    env_->non_writeable_rate_.store(0);
  } while (ChangeCompactOptions());
}

TEST_F(DBIOFailureTest, ManifestWriteError) {
  // Test for the following problem:
  // (a) Compaction produces file F
  // (b) Log record containing F is written to MANIFEST file, but Sync() fails
  // (c) GC deletes F
  // (d) After reopening DB, reads fail since deleted F is named in log record

  // We iterate twice.  In the second iteration, everything is the
  // same except the log record never makes it to the MANIFEST file.
  for (int iter = 0; iter < 2; iter++) {
    std::atomic<bool>* error_type = (iter == 0) ? &env_->manifest_sync_error_
                                                : &env_->manifest_write_error_;

    // Insert foo=>bar mapping
    Options options = CurrentOptions();
    options.env = env_;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.paranoid_checks = true;
    DestroyAndReopen(options);
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_EQ("bar", Get("foo"));

    // Memtable compaction (will succeed)
    ASSERT_OK(Flush());
    ASSERT_EQ("bar", Get("foo"));
    const int last = 2;
    MoveFilesToLevel(2);
    ASSERT_EQ(NumTableFilesAtLevel(last), 1);  // foo=>bar is now in last level

    // Merging compaction (will fail)
    error_type->store(true, std::memory_order_release);
    ASSERT_NOK(
        dbfull()->TEST_CompactRange(last, nullptr, nullptr));  // Should fail
    ASSERT_EQ("bar", Get("foo"));

    error_type->store(false, std::memory_order_release);

    // Since paranoid_checks=true, writes should fail
    ASSERT_NOK(Put("foo2", "bar2"));

    // Recovery: should not lose data
    ASSERT_EQ("bar", Get("foo"));

    // Try again with paranoid_checks=false
    Close();
    options.paranoid_checks = false;
    Reopen(options);

    // Merging compaction (will fail)
    error_type->store(true, std::memory_order_release);
    Status s =
        dbfull()->TEST_CompactRange(last, nullptr, nullptr);  // Should fail
    if (iter == 0) {
      ASSERT_OK(s);
    } else {
      ASSERT_TRUE(s.IsIOError());
    }
    ASSERT_EQ("bar", Get("foo"));

    // Recovery: should not lose data
    error_type->store(false, std::memory_order_release);
    Reopen(options);
    ASSERT_EQ("bar", Get("foo"));

    // Since paranoid_checks=false, writes should succeed
    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_EQ("bar", Get("foo"));
    ASSERT_EQ("bar2", Get("foo2"));
  }
}

TEST_F(DBIOFailureTest, PutFailsParanoid) {
  // Test the following:
  // (a) A random put fails in paranoid mode (simulate by sync fail)
  // (b) All other puts have to fail, even if writes would succeed
  // (c) All of that should happen ONLY if paranoid_checks = true

  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.store(true, std::memory_order_release);
  ASSERT_NOK(Put(1, "foo2", "bar2"));
  env_->log_write_error_.store(false, std::memory_order_release);
  // the next put should fail, too
  ASSERT_NOK(Put(1, "foo3", "bar3"));
  // but we're still able to read
  ASSERT_EQ("bar", Get(1, "foo"));

  // do the same thing with paranoid checks off
  options.paranoid_checks = false;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  // simulate error
  env_->log_write_error_.store(true, std::memory_order_release);
  ASSERT_NOK(Put(1, "foo2", "bar2"));
  env_->log_write_error_.store(false, std::memory_order_release);
  // the next put should NOT fail
  ASSERT_OK(Put(1, "foo3", "bar3"));
}
#if !(defined NDEBUG) || !defined(OS_WIN)
TEST_F(DBIOFailureTest, FlushSstRangeSyncError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.writable_file_max_buffer_size = 128 * 1024;
  options.bytes_per_sync = 128 * 1024;
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(10));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  const char* io_error_msg = "range sync dummy error";
  std::atomic<int> range_sync_called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::RangeSync", [&](void* arg) {
        if (range_sync_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError(io_error_msg);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  std::string rnd_str =
      rnd.RandomString(static_cast<int>(options.bytes_per_sync / 2));
  std::string rnd_str_512kb = rnd.RandomString(512 * 1024);

  ASSERT_OK(Put(1, "foo", "bar"));
  // First 1MB doesn't get range synced
  ASSERT_OK(Put(1, "foo0_0", rnd_str_512kb));
  ASSERT_OK(Put(1, "foo0_1", rnd_str_512kb));
  ASSERT_OK(Put(1, "foo1_1", rnd_str));
  ASSERT_OK(Put(1, "foo1_2", rnd_str));
  ASSERT_OK(Put(1, "foo1_3", rnd_str));
  ASSERT_OK(Put(1, "foo2", "bar"));
  ASSERT_OK(Put(1, "foo3_1", rnd_str));
  ASSERT_OK(Put(1, "foo3_2", rnd_str));
  ASSERT_OK(Put(1, "foo3_3", rnd_str));
  ASSERT_OK(Put(1, "foo4", "bar"));
  Status s = dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STREQ(s.getState(), io_error_msg);

  // Following writes should fail as flush failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar", Get(1, "foo"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_GE(1, range_sync_called.load());

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar", Get(1, "foo"));
}

TEST_F(DBIOFailureTest, CompactSstRangeSyncError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.writable_file_max_buffer_size = 128 * 1024;
  options.bytes_per_sync = 128 * 1024;
  options.level0_file_num_compaction_trigger = 2;
  options.target_file_size_base = 256 * 1024 * 1024;
  options.disable_auto_compactions = true;
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  std::string rnd_str =
      rnd.RandomString(static_cast<int>(options.bytes_per_sync / 2));
  std::string rnd_str_512kb = rnd.RandomString(512 * 1024);

  ASSERT_OK(Put(1, "foo", "bar"));
  // First 1MB doesn't get range synced
  ASSERT_OK(Put(1, "foo0_0", rnd_str_512kb));
  ASSERT_OK(Put(1, "foo0_1", rnd_str_512kb));
  ASSERT_OK(Put(1, "foo1_1", rnd_str));
  ASSERT_OK(Put(1, "foo1_2", rnd_str));
  ASSERT_OK(Put(1, "foo1_3", rnd_str));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo3_1", rnd_str));
  ASSERT_OK(Put(1, "foo3_2", rnd_str));
  ASSERT_OK(Put(1, "foo3_3", rnd_str));
  ASSERT_OK(Put(1, "foo4", "bar"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));

  const char* io_error_msg = "range sync dummy error";
  std::atomic<int> range_sync_called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::RangeSync", [&](void* arg) {
        if (range_sync_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError(io_error_msg);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->SetOptions(handles_[1],
                                 {
                                     {"disable_auto_compactions", "false"},
                                 }));
  Status s = dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STREQ(s.getState(), io_error_msg);

  // Following writes should fail as flush failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar", Get(1, "foo"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_GE(1, range_sync_called.load());

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar", Get(1, "foo"));
}

TEST_F(DBIOFailureTest, FlushSstCloseError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(2));

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  const char* io_error_msg = "close dummy error";
  std::atomic<int> close_called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::Close", [&](void* arg) {
        if (close_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError(io_error_msg);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  ASSERT_OK(Put(1, "foo", "bar2"));
  Status s = dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STREQ(s.getState(), io_error_msg);

  // Following writes should fail as flush failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar2", Get(1, "foo"));
  ASSERT_EQ("bar1", Get(1, "foo1"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar2", Get(1, "foo"));
  ASSERT_EQ("bar1", Get(1, "foo1"));
}

TEST_F(DBIOFailureTest, CompactionSstCloseError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "foo", "bar2"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "foo", "bar3"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  const char* io_error_msg = "close dummy error";
  std::atomic<int> close_called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::Close", [&](void* arg) {
        if (close_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError(io_error_msg);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->SetOptions(handles_[1],
                                 {
                                     {"disable_auto_compactions", "false"},
                                 }));
  Status s = dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STREQ(s.getState(), io_error_msg);

  // Following writes should fail as compaction failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar3", Get(1, "foo"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar3", Get(1, "foo"));
}

TEST_F(DBIOFailureTest, FlushSstSyncError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.use_fsync = false;
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(2));

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  const char* io_error_msg = "sync dummy error";
  std::atomic<int> sync_called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::Sync", [&](void* arg) {
        if (sync_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError(io_error_msg);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo1", "bar1"));
  ASSERT_OK(Put(1, "foo", "bar2"));
  Status s = dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STREQ(s.getState(), io_error_msg);

  // Following writes should fail as flush failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar2", Get(1, "foo"));
  ASSERT_EQ("bar1", Get(1, "foo1"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar2", Get(1, "foo"));
  ASSERT_EQ("bar1", Get(1, "foo1"));
}

TEST_F(DBIOFailureTest, CompactionSstSyncError) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.paranoid_checks = true;
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = true;
  options.use_fsync = false;

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "foo", "bar2"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "foo", "bar3"));
  ASSERT_OK(Put(1, "foo2", "bar"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  const char* io_error_msg = "sync dummy error";
  std::atomic<int> sync_called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SpecialEnv::SStableFile::Sync", [&](void* arg) {
        if (sync_called.fetch_add(1) == 0) {
          Status* st = static_cast<Status*>(arg);
          *st = Status::IOError(io_error_msg);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->SetOptions(handles_[1],
                                 {
                                     {"disable_auto_compactions", "false"},
                                 }));
  Status s = dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STREQ(s.getState(), io_error_msg);

  // Following writes should fail as compaction failed.
  ASSERT_NOK(Put(1, "foo2", "bar3"));
  ASSERT_EQ("bar3", Get(1, "foo"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ("bar3", Get(1, "foo"));
}
#endif  // !(defined NDEBUG) || !defined(OS_WIN)

class DBIOCorruptionTest
    : public DBIOFailureTest,
      public testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  DBIOCorruptionTest() : DBIOFailureTest() {
    BlockBasedTableOptions bbto;
    options_ = CurrentOptions();
    options_.statistics = CreateDBStatistics();

    base_env_ = env_;
    EXPECT_NE(base_env_, nullptr);
    fs_.reset(new CorruptionFS(base_env_->GetFileSystem(),
                               std::get<0>(GetParam()),
                               std::get<2>(GetParam())));
    env_guard_ = NewCompositeEnv(fs_);
    options_.env = env_guard_.get();
    bbto.num_file_reads_for_auto_readahead = 0;
    options_.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options_.disable_auto_compactions = true;
    options_.max_file_opening_threads = 0;

    Reopen(options_);
  }

  ~DBIOCorruptionTest() override {
    Close();
    db_ = nullptr;
  }

  Status ReopenDB() { return TryReopen(options_); }

  Statistics* stats() { return options_.statistics.get(); }

 protected:
  std::unique_ptr<Env> env_guard_;
  std::shared_ptr<CorruptionFS> fs_;
  Env* base_env_;
  Options options_;
};

TEST_P(DBIOCorruptionTest, GetReadCorruptionRetry) {
  CorruptionFS* fs =
      static_cast<CorruptionFS*>(env_guard_->GetFileSystem().get());

  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Flush());
  fs->SetCorruptionTrigger(1);

  std::string val;
  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  Status s = dbfull()->Get(ReadOptions(), "key1", &val);
  if (std::get<2>(GetParam())) {
    ASSERT_OK(s);
    ASSERT_EQ(val, "val1");
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 1);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT),
              1);
  } else {
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 0);
  }
}

TEST_P(DBIOCorruptionTest, IterReadCorruptionRetry) {
  CorruptionFS* fs =
      static_cast<CorruptionFS*>(env_guard_->GetFileSystem().get());

  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Flush());
  fs->SetCorruptionTrigger(1);

  ReadOptions ro;
  ro.readahead_size = 65536;
  ro.async_io = std::get<1>(GetParam());

  Iterator* iter = dbfull()->NewIterator(ro);
  iter->SeekToFirst();
  while (iter->status().ok() && iter->Valid()) {
    iter->Next();
  }
  if (std::get<2>(GetParam())) {
    ASSERT_OK(iter->status());
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 1);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT),
              1);
  } else {
    ASSERT_TRUE(iter->status().IsCorruption());
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 0);
  }
  delete iter;
}

TEST_P(DBIOCorruptionTest, MultiGetReadCorruptionRetry) {
  CorruptionFS* fs =
      static_cast<CorruptionFS*>(env_guard_->GetFileSystem().get());

  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  fs->SetCorruptionTrigger(1);

  std::vector<std::string> keystr{"key1", "key2"};
  std::vector<Slice> keys{Slice(keystr[0]), Slice(keystr[1])};
  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());
  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data());
  if (std::get<2>(GetParam())) {
    ASSERT_EQ(values[0].ToString(), "val1");
    ASSERT_EQ(values[1].ToString(), "val2");
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 1);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT),
              1);
  } else {
    ASSERT_TRUE(statuses[0].IsCorruption());
    ASSERT_TRUE(statuses[1].IsCorruption());
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 0);
  }
}

TEST_P(DBIOCorruptionTest, CompactionReadCorruptionRetry) {
  CorruptionFS* fs =
      static_cast<CorruptionFS*>(env_guard_->GetFileSystem().get());

  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Put("key3", "val3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  fs->SetCorruptionTrigger(1);
  Status s = dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  if (std::get<2>(GetParam())) {
    ASSERT_OK(s);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 1);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT),
              1);

    std::string val;
    ReadOptions ro;
    ro.async_io = std::get<1>(GetParam());
    ASSERT_OK(dbfull()->Get(ro, "key1", &val));
    ASSERT_EQ(val, "val1");
  } else {
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 0);
  }
}

TEST_P(DBIOCorruptionTest, FlushReadCorruptionRetry) {
  CorruptionFS* fs =
      static_cast<CorruptionFS*>(env_guard_->GetFileSystem().get());

  ASSERT_OK(Put("key1", "val1"));
  fs->SetCorruptionTrigger(1);
  Status s = Flush();
  if (std::get<2>(GetParam())) {
    ASSERT_OK(s);
    ASSERT_GT(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 1);
    ASSERT_GT(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT),
              1);

    std::string val;
    ReadOptions ro;
    ro.async_io = std::get<1>(GetParam());
    ASSERT_OK(dbfull()->Get(ro, "key1", &val));
    ASSERT_EQ(val, "val1");
  } else {
    ASSERT_NOK(s);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 0);
  }
}

TEST_P(DBIOCorruptionTest, ManifestCorruptionRetry) {
  CorruptionFS* fs =
      static_cast<CorruptionFS*>(env_guard_->GetFileSystem().get());

  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Recover:StartManifestRead",
      [&](void* /*arg*/) { fs->SetCorruptionTrigger(0); });
  SyncPoint::GetInstance()->EnableProcessing();

  if (std::get<2>(GetParam())) {
    ASSERT_OK(ReopenDB());
    ASSERT_GT(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 1);
    ASSERT_GT(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT),
              1);
  } else {
    ASSERT_EQ(ReopenDB(), Status::Corruption());
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 0);
  }
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBIOCorruptionTest, FooterReadCorruptionRetry) {
  Random rnd(300);
  bool retry = false;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ReadFooterFromFileInternal:0", [&](void* arg) {
        Slice* data = static_cast<Slice*>(arg);
        if (!retry) {
          std::memcpy(const_cast<char*>(data->data()),
                      rnd.RandomString(static_cast<int>(data->size())).c_str(),
                      data->size());
          retry = true;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key1", "val1"));
  Status s = Flush();
  if (std::get<2>(GetParam())) {
    ASSERT_OK(s);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 1);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT),
              1);

    std::string val;
    ReadOptions ro;
    ro.async_io = std::get<1>(GetParam());
    ASSERT_OK(dbfull()->Get(ro, "key1", &val));
    ASSERT_EQ(val, "val1");
  } else {
    ASSERT_NOK(s);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 0);
    ASSERT_GT(stats()->getTickerCount(SST_FOOTER_CORRUPTION_COUNT), 0);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBIOCorruptionTest, TablePropertiesCorruptionRetry) {
  Random rnd(300);
  bool retry = false;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ReadTablePropertiesHelper:0", [&](void* arg) {
        Slice* data = static_cast<Slice*>(arg);
        if (!retry) {
          std::memcpy(const_cast<char*>(data->data()),
                      rnd.RandomString(static_cast<int>(data->size())).c_str(),
                      data->size());
          retry = true;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key1", "val1"));
  Status s = Flush();
  if (std::get<2>(GetParam())) {
    ASSERT_OK(s);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 1);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT),
              1);

    std::string val;
    ReadOptions ro;
    ro.async_io = std::get<1>(GetParam());
    ASSERT_OK(dbfull()->Get(ro, "key1", &val));
    ASSERT_EQ(val, "val1");
  } else {
    ASSERT_NOK(s);
    ASSERT_EQ(stats()->getTickerCount(FILE_READ_CORRUPTION_RETRY_COUNT), 0);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBIOCorruptionTest, DBOpenReadCorruptionRetry) {
  if (!std::get<2>(GetParam())) {
    return;
  }
  CorruptionFS* fs =
      static_cast<CorruptionFS*>(env_guard_->GetFileSystem().get());

  for (int sst = 0; sst < 3; ++sst) {
    for (int key = 0; key < 100; ++key) {
      std::stringstream ss;
      ss << std::setw(3) << 100 * sst + key;
      ASSERT_OK(Put("key" + ss.str(), "val" + ss.str()));
    }
    ASSERT_OK(Flush());
  }
  Close();

  // DB open will create table readers unless we reduce the table cache
  // capacity.
  // SanitizeOptions will set max_open_files to minimum of 20. Table cache
  // is allocated with max_open_files - 10 as capacity. So override
  // max_open_files to 11 so table cache capacity will become 1. This will
  // prevent file open during DB open and force the file to be opened
  // during MultiGet
  SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = (int*)arg;
        *max_open_files = 11;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Progressively increase the IO count trigger for corruption, and verify
  // that it was retried
  int corruption_trigger = 1;
  fs->SetCorruptionTrigger(corruption_trigger);
  do {
    fs->SetCorruptionTrigger(corruption_trigger);
    ASSERT_OK(ReopenDB());
    for (int sst = 0; sst < 3; ++sst) {
      for (int key = 0; key < 100; ++key) {
        std::stringstream ss;
        ss << std::setw(3) << 100 * sst + key;
        ASSERT_EQ(Get("key" + ss.str()), "val" + ss.str());
      }
    }
    // Verify that the injected corruption was repaired
    ASSERT_TRUE(fs->VerifyRetry());
    corruption_trigger++;
  } while (fs->corruption_trigger() == INT_MAX);
}

// The parameters are - 1. Use FS provided buffer, 2. Use async IO ReadOption,
// 3. Retry with verify_and_reconstruct_read IOOption
INSTANTIATE_TEST_CASE_P(DBIOCorruptionTest, DBIOCorruptionTest,
                        testing::Combine(testing::Bool(), testing::Bool(),
                                         testing::Bool()));
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
