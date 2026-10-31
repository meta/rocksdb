//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <future>

#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/file_system.h"
#include "test_util/sync_point.h"
#include "util/defer.h"
#include "util/udt_util.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

#if defined(OS_LINUX)
#include <sys/statfs.h>
#endif

#if defined(OS_LINUX) && !defined(BTRFS_SUPER_MAGIC)
#define BTRFS_SUPER_MAGIC 0x9123683E
#endif

#if defined(OS_LINUX) && !defined(TMPFS_MAGIC)
#define TMPFS_MAGIC 0x01021994
#endif

#if defined(OS_LINUX) && !defined(OVERLAYFS_SUPER_MAGIC)
#define OVERLAYFS_SUPER_MAGIC 0x794c7630
#endif

#if defined(OS_LINUX) && !defined(ZFS_SUPER_MAGIC)
#define ZFS_SUPER_MAGIC 0x2fc12fc1
#endif

namespace ROCKSDB_NAMESPACE {
class DBWALTestBase : public DBTestBase {
 protected:
  explicit DBWALTestBase(const std::string& dir_name)
      : DBTestBase(dir_name, /*env_do_fsync=*/true) {}

  std::vector<uint64_t> ListWalNumbers() {
    std::vector<uint64_t> wal_numbers;
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& file : files) {
      uint64_t number = 0;
      FileType type = kWalFile;
      if (ParseFileName(file, &number, &type) && type == kWalFile) {
        wal_numbers.push_back(number);
      }
    }
    std::sort(wal_numbers.begin(), wal_numbers.end());
    return wal_numbers;
  }

  void CreateEmptyWal(uint64_t wal_number) {
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(LogFileName(dbname_, wal_number), &file,
                                    EnvOptions()));
    ASSERT_OK(file->Close());
  }

  void CreateWalWithContents(uint64_t wal_number, const Slice& contents) {
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(LogFileName(dbname_, wal_number), &file,
                                    EnvOptions()));
    ASSERT_OK(file->Append(contents));
    ASSERT_OK(file->Close());
  }

#if defined(ROCKSDB_PLATFORM_POSIX)
 public:
#if defined(ROCKSDB_FALLOCATE_PRESENT)
  bool IsFallocateSupported() {
    // Test fallocate support of running file system.
    // Skip this test if fallocate is not supported.
    std::string fname_test_fallocate = dbname_ + "/preallocate_testfile";
    int fd = -1;
    do {
      fd = open(fname_test_fallocate.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    } while (fd < 0 && errno == EINTR);
    assert(fd > 0);
    int alloc_status = fallocate(fd, 0, 0, 1);
    int err_number = errno;
    close(fd);
    assert(env_->DeleteFile(fname_test_fallocate) == Status::OK());
    if (err_number == ENOSYS || err_number == EOPNOTSUPP) {
      fprintf(stderr, "Skipped preallocated space check: %s\n",
              errnoStr(err_number).c_str());
      return false;
    }
    assert(alloc_status == 0);
    return true;
  }
#endif  // ROCKSDB_FALLOCATE_PRESENT

  uint64_t GetAllocatedFileSize(std::string file_name) {
    struct stat sbuf;
    int err = stat(file_name.c_str(), &sbuf);
    assert(err == 0);
    return sbuf.st_blocks * 512;
  }

#if defined(ROCKSDB_FALLOCATE_PRESENT)
  bool ShouldSkipAllocationCheck(const std::string& file_name) {
    (void)file_name;
#if defined(OS_LINUX)
    struct statfs fs_stat;
    if (statfs(file_name.c_str(), &fs_stat) == 0) {
      if (fs_stat.f_type ==
          static_cast<decltype(fs_stat.f_type)>(BTRFS_SUPER_MAGIC)) {
        return true;
      } else if (fs_stat.f_type ==
                 static_cast<decltype(fs_stat.f_type)>(ZFS_SUPER_MAGIC)) {
        return true;
      } else if (fs_stat.f_type ==
                 static_cast<decltype(fs_stat.f_type)>(TMPFS_MAGIC)) {
        return true;
      } else if (fs_stat.f_type ==
                 static_cast<decltype(fs_stat.f_type)>(OVERLAYFS_SUPER_MAGIC)) {
        return true;
      }
    }
#endif
    return false;
  }
#endif  // ROCKSDB_FALLOCATE_PRESENT
#endif  // ROCKSDB_PLATFORM_POSIX
};

class DBWALTest : public DBWALTestBase {
 public:
  DBWALTest() : DBWALTestBase("/db_wal_test") {}
};

// A SpecialEnv enriched to give more insight about deleted files
class EnrichedSpecialEnv : public SpecialEnv {
 public:
  explicit EnrichedSpecialEnv(Env* base) : SpecialEnv(base) {}
  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& soptions) override {
    InstrumentedMutexLock l(&env_mutex_);
    if (f == skipped_wal) {
      deleted_wal_reopened = true;
      if (IsWAL(f) && largest_deleted_wal.size() != 0 &&
          f.compare(largest_deleted_wal) <= 0) {
        gap_in_wals = true;
      }
    }
    return SpecialEnv::NewSequentialFile(f, r, soptions);
  }
  Status DeleteFile(const std::string& fname) override {
    if (IsWAL(fname)) {
      deleted_wal_cnt++;
      InstrumentedMutexLock l(&env_mutex_);
      // If this is the first WAL, remember its name and skip deleting it. We
      // remember its name partly because the application might attempt to
      // delete the file again.
      if (skipped_wal.size() != 0 && skipped_wal != fname) {
        if (largest_deleted_wal.size() == 0 ||
            largest_deleted_wal.compare(fname) < 0) {
          largest_deleted_wal = fname;
        }
      } else {
        skipped_wal = fname;
        return Status::OK();
      }
    }
    return SpecialEnv::DeleteFile(fname);
  }
  bool IsWAL(const std::string& fname) {
    // printf("iswal %s\n", fname.c_str());
    return fname.compare(fname.size() - 3, 3, "log") == 0;
  }

  InstrumentedMutex env_mutex_;
  // the wal whose actual delete was skipped by the env
  std::string skipped_wal;
  // the largest WAL that was requested to be deleted
  std::string largest_deleted_wal;
  // number of WALs that were successfully deleted
  std::atomic<size_t> deleted_wal_cnt = {0};
  // the WAL whose delete from fs was skipped is reopened during recovery
  std::atomic<bool> deleted_wal_reopened = {false};
  // whether a gap in the WALs was detected during recovery
  std::atomic<bool> gap_in_wals = {false};
};

class DBWALTestWithEnrichedEnv : public DBTestBase {
 public:
  DBWALTestWithEnrichedEnv()
      : DBTestBase("db_wal_test", /*env_do_fsync=*/true) {
    enriched_env_ = new EnrichedSpecialEnv(env_->target());
    auto options = CurrentOptions();
    options.env = enriched_env_;
    options.allow_2pc = true;
    Reopen(options);
    delete env_;
    // to be deleted by the parent class
    env_ = enriched_env_;
  }

 protected:
  EnrichedSpecialEnv* enriched_env_;
};

// Test that the recovery would successfully avoid the gaps between the logs.
// One known scenario that could cause this is that the application issue the
// WAL deletion out of order. For the sake of simplicity in the test, here we
// create the gap by manipulating the env to skip deletion of the first WAL but
// not the ones after it.
TEST_F(DBWALTestWithEnrichedEnv, SkipDeletedWALs) {
  auto options = last_options_;
  // To cause frequent WAL deletion
  options.write_buffer_size = 128;
  Reopen(options);

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::PurgeObsoleteFiles:End",
        "DBWALTestWithEnrichedEnv.SkipDeletedWALs:AfterFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions writeOpt = WriteOptions();
  for (int i = 0; i < 128 * 5; i++) {
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v1"));
  }
  FlushOptions fo;
  fo.wait = true;
  ASSERT_OK(db_->Flush(fo));

  TEST_SYNC_POINT("DBWALTestWithEnrichedEnv.SkipDeletedWALs:AfterFlush");

  // some wals are deleted
  ASSERT_NE(0, enriched_env_->deleted_wal_cnt);
  // but not the first one
  ASSERT_NE(0, enriched_env_->skipped_wal.size());

  // Test that the WAL that was not deleted will be skipped during recovery
  options = last_options_;
  Reopen(options);
  ASSERT_FALSE(enriched_env_->deleted_wal_reopened);
  ASSERT_FALSE(enriched_env_->gap_in_wals);

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBWALTest, WAL) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // Both value's should be present.
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v2", Get(1, "foo"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // again both values should be present.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RollLog) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    }
    ASSERT_OK(Put(1, "foo", "v4"));
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    }
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, SyncWALNotBlockWrite) {
  Options options = CurrentOptions();
  options.max_write_buffer_number = 4;
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo5", "bar5"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"WritableFileWriter::SyncWithoutFlush:1",
       "DBWALTest::SyncWALNotBlockWrite:1"},
      {"DBWALTest::SyncWALNotBlockWrite:2",
       "WritableFileWriter::SyncWithoutFlush:2"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread thread([&]() { ASSERT_OK(db_->SyncWAL()); });

  TEST_SYNC_POINT("DBWALTest::SyncWALNotBlockWrite:1");
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Put("foo3", "bar3"));
  FlushOptions fo;
  fo.wait = false;
  ASSERT_OK(db_->Flush(fo));
  ASSERT_OK(Put("foo4", "bar4"));

  TEST_SYNC_POINT("DBWALTest::SyncWALNotBlockWrite:2");

  thread.join();

  ASSERT_EQ(Get("foo1"), "bar1");
  ASSERT_EQ(Get("foo2"), "bar2");
  ASSERT_EQ(Get("foo3"), "bar3");
  ASSERT_EQ(Get("foo4"), "bar4");
  ASSERT_EQ(Get("foo5"), "bar5");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBWALTest, SyncWALNotWaitWrite) {
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo3", "bar3"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"SpecialEnv::SpecialWalFile::Append:1",
       "DBWALTest::SyncWALNotWaitWrite:1"},
      {"DBWALTest::SyncWALNotWaitWrite:2",
       "SpecialEnv::SpecialWalFile::Append:2"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread thread(
      [&]() { ASSERT_OK(Put("foo2", "bar2")); });
  // Moving this to SyncWAL before the actual fsync
  // TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:1");
  ASSERT_OK(db_->SyncWAL());
  // Moving this to SyncWAL after actual fsync
  // TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:2");

  thread.join();

  ASSERT_EQ(Get("foo1"), "bar1");
  ASSERT_EQ(Get("foo2"), "bar2");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBWALTest, Recover) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v5", Get(1, "baz"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Put(1, "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v4"));
    ASSERT_EQ("v4", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v5", Get(1, "baz"));
  } while (ChangeWalOptions());
}

class DBWALTestWithTimestamp
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<test::UserDefinedTimestampTestMode> {
 public:
  DBWALTestWithTimestamp()
      : DBBasicTestWithTimestampBase("db_wal_test_with_timestamp") {}

  Status CreateAndReopenWithTs(const std::vector<std::string>& cfs,
                               const Options& ts_options, bool persist_udt,
                               bool avoid_flush_during_recovery = false) {
    Options default_options = CurrentOptions();
    default_options.allow_concurrent_memtable_write =
        persist_udt ? true : false;
    DestroyAndReopen(default_options);
    CreateColumnFamilies(cfs, ts_options);
    return ReopenColumnFamiliesWithTs(cfs, ts_options, persist_udt,
                                      avoid_flush_during_recovery);
  }

  Status ReopenColumnFamiliesWithTs(const std::vector<std::string>& cfs,
                                    Options ts_options, bool persist_udt,
                                    bool avoid_flush_during_recovery = false) {
    Options default_options = CurrentOptions();
    default_options.create_if_missing = false;
    default_options.allow_concurrent_memtable_write =
        persist_udt ? true : false;
    default_options.avoid_flush_during_recovery = avoid_flush_during_recovery;
    ts_options.create_if_missing = false;

    std::vector<Options> cf_options(cfs.size(), ts_options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    cf_options.insert(cf_options.begin(), default_options);
    Close();
    return TryReopenWithColumnFamilies(cfs_plus_default, cf_options);
  }

  Status Put(uint32_t cf, const Slice& key, const Slice& ts,
             const Slice& value) {
    WriteOptions write_opts;
    return db_->Put(write_opts, handles_[cf], key, ts, value);
  }

  void CheckGet(const ReadOptions& read_opts, uint32_t cf, const Slice& key,
                const std::string& expected_value,
                const std::string& expected_ts) {
    std::string actual_value;
    std::string actual_ts;
    ASSERT_OK(
        db_->Get(read_opts, handles_[cf], key, &actual_value, &actual_ts));
    ASSERT_EQ(expected_value, actual_value);
    ASSERT_EQ(expected_ts, actual_ts);
  }
};

TEST_P(DBWALTestWithTimestamp, RecoverAndNoFlush) {
  // Set up the option that enables user defined timestmp size.
  std::string ts1;
  PutFixed64(&ts1, 1);
  Options ts_options;
  ts_options.create_if_missing = true;
  ts_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  // Test that user-defined timestamps are recovered from WAL regardless of
  // the value of this flag because UDTs are saved in WAL nonetheless.
  // We however need to explicitly disable flush during recovery by setting
  // `avoid_flush_during_recovery=true` so that we can avoid timestamps getting
  // stripped when the `persist_user_defined_timestamps` flag is false, so that
  // all written timestamps are available for testing user-defined time travel
  // read.
  bool persist_udt = test::ShouldPersistUDT(GetParam());
  ts_options.persist_user_defined_timestamps = persist_udt;
  bool avoid_flush_during_recovery = true;

  std::string full_history_ts_low;
  ReadOptions read_opts;
  do {
    Slice ts_slice = ts1;
    read_opts.timestamp = &ts_slice;
    ASSERT_OK(CreateAndReopenWithTs({"pikachu"}, ts_options, persist_udt,
                                    avoid_flush_during_recovery));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 0U);
    ASSERT_OK(Put(1, "foo", ts1, "v1"));
    ASSERT_OK(Put(1, "baz", ts1, "v5"));

    ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, ts_options, persist_udt,
                                         avoid_flush_during_recovery));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 0U);
    // Do a timestamped read with ts1 after second reopen.
    CheckGet(read_opts, 1, "foo", "v1", ts1);
    CheckGet(read_opts, 1, "baz", "v5", ts1);

    // Write more value versions for key "foo" and "bar" before and after second
    // reopen.
    std::string ts2;
    PutFixed64(&ts2, 2);
    ASSERT_OK(Put(1, "bar", ts2, "v2"));
    ASSERT_OK(Put(1, "foo", ts2, "v3"));

    ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, ts_options, persist_udt,
                                         avoid_flush_during_recovery));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 0U);
    std::string ts3;
    PutFixed64(&ts3, 3);
    ASSERT_OK(Put(1, "foo", ts3, "v4"));

    // All the key value pairs available for read:
    // "foo" -> [(ts1, "v1"), (ts2, "v3"), (ts3, "v4")]
    // "bar" -> [(ts2, "v2")]
    // "baz" -> [(ts1, "v5")]
    // Do a timestamped read with ts1 after third reopen.
    // read_opts.timestamp is set to ts1 for below reads
    CheckGet(read_opts, 1, "foo", "v1", ts1);
    std::string value;
    ASSERT_TRUE(db_->Get(read_opts, handles_[1], "bar", &value).IsNotFound());
    CheckGet(read_opts, 1, "baz", "v5", ts1);

    // Do a timestamped read with ts2 after third reopen.
    ts_slice = ts2;
    // read_opts.timestamp is set to ts2 for below reads.
    CheckGet(read_opts, 1, "foo", "v3", ts2);
    CheckGet(read_opts, 1, "bar", "v2", ts2);
    CheckGet(read_opts, 1, "baz", "v5", ts1);

    // Do a timestamped read with ts3 after third reopen.
    ts_slice = ts3;
    // read_opts.timestamp is set to ts3 for below reads.
    CheckGet(read_opts, 1, "foo", "v4", ts3);
    CheckGet(read_opts, 1, "bar", "v2", ts2);
    CheckGet(read_opts, 1, "baz", "v5", ts1);
    ASSERT_OK(db_->GetFullHistoryTsLow(handles_[1], &full_history_ts_low));
    ASSERT_TRUE(full_history_ts_low.empty());
  } while (ChangeWalOptions());
}

TEST_P(DBWALTestWithTimestamp, RecoverAndFlush) {
  // Set up the option that enables user defined timestamp size.
  std::string min_ts;
  std::string write_ts;
  PutFixed64(&min_ts, 0);
  PutFixed64(&write_ts, 1);
  Options ts_options;
  ts_options.create_if_missing = true;
  ts_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  bool persist_udt = test::ShouldPersistUDT(GetParam());
  ts_options.persist_user_defined_timestamps = persist_udt;

  std::string smallest_ukey_without_ts = "baz";
  std::string largest_ukey_without_ts = "foo";

  ASSERT_OK(CreateAndReopenWithTs({"pikachu"}, ts_options, persist_udt));
  // No flush, no sst files, because of no data.
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 0U);
  ASSERT_OK(Put(1, largest_ukey_without_ts, write_ts, "v1"));
  ASSERT_OK(Put(1, smallest_ukey_without_ts, write_ts, "v5"));

  ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, ts_options, persist_udt));
  // Memtable recovered from WAL flushed because `avoid_flush_during_recovery`
  // defaults to false, created one L0 file.
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 1U);

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(handles_[1], &level_to_files);
  std::string full_history_ts_low;
  ASSERT_OK(db_->GetFullHistoryTsLow(handles_[1], &full_history_ts_low));
  ASSERT_GT(level_to_files.size(), 1);
  // L0 only has one SST file.
  ASSERT_EQ(level_to_files[0].size(), 1);
  auto meta = level_to_files[0][0];
  if (persist_udt) {
    ASSERT_EQ(smallest_ukey_without_ts + write_ts, meta.smallest.user_key());
    ASSERT_EQ(largest_ukey_without_ts + write_ts, meta.largest.user_key());
    ASSERT_TRUE(full_history_ts_low.empty());
  } else {
    ASSERT_EQ(smallest_ukey_without_ts + min_ts, meta.smallest.user_key());
    ASSERT_EQ(largest_ukey_without_ts + min_ts, meta.largest.user_key());
    std::string effective_cutoff;
    Slice write_ts_slice = write_ts;
    GetFullHistoryTsLowFromU64CutoffTs(&write_ts_slice, &effective_cutoff);
    ASSERT_EQ(effective_cutoff, full_history_ts_low);
  }
}

// Param 0: test mode for the user-defined timestamp feature
INSTANTIATE_TEST_CASE_P(
    P, DBWALTestWithTimestamp,
    ::testing::Values(
        test::UserDefinedTimestampTestMode::kStripUserDefinedTimestamp,
        test::UserDefinedTimestampTestMode::kNormal));

TEST_F(DBWALTestWithTimestamp, EnableDisableUDT) {
  Options options;
  options.create_if_missing = true;
  options.comparator = BytewiseComparator();
  bool avoid_flush_during_recovery = true;
  ASSERT_OK(CreateAndReopenWithTs({"pikachu"}, options, true /* persist_udt */,
                                  avoid_flush_during_recovery));

  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "baz", "v5"));

  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = false;
  // Test handle timestamp size inconsistency in WAL when enabling user-defined
  // timestamps.
  ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, options,
                                       false /* persist_udt */,
                                       avoid_flush_during_recovery));

  std::string ts;
  PutFixed64(&ts, 0);
  Slice ts_slice = ts;
  ReadOptions read_opts;
  read_opts.timestamp = &ts_slice;
  // Pre-existing entries are treated as if they have the min timestamp.
  CheckGet(read_opts, 1, "foo", "v1", ts);
  CheckGet(read_opts, 1, "baz", "v5", ts);
  ts.clear();
  PutFixed64(&ts, 1);
  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "foo", ts, "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "baz", ts, "v6"));
  CheckGet(read_opts, 1, "foo", "v2", ts);
  CheckGet(read_opts, 1, "baz", "v6", ts);

  options.comparator = BytewiseComparator();
  // Open the column family again with the UDT feature disabled. Test handle
  // timestamp size inconsistency in WAL when disabling user-defined timestamps
  ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, options,
                                       true /* persist_udt */,
                                       avoid_flush_during_recovery));
  ASSERT_EQ("v2", Get(1, "foo"));
  ASSERT_EQ("v6", Get(1, "baz"));
}

TEST_F(DBWALTest, RecoverWithTableHandle) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.avoid_flush_during_recovery = false;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "foo", "v3"));
    ASSERT_OK(Put(1, "bar", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "big", std::string(100, 'a')));

    options = CurrentOptions();
    const int kSmallMaxOpenFiles = 13;
    if (option_config_ == kDBLogDir) {
      // Use this option to check not preloading files
      // Set the max open files to be small enough so no preload will
      // happen.
      options.max_open_files = kSmallMaxOpenFiles;
      // RocksDB sanitize max open files to at least 20. Modify it back.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
            int* max_open_files = static_cast<int*>(arg);
            *max_open_files = kSmallMaxOpenFiles;
          });

    } else if (option_config_ == kWalDirAndMmapReads) {
      // Use this option to check always loading all files.
      options.max_open_files = 100;
    } else {
      options.max_open_files = -1;
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

    std::vector<std::vector<FileMetaData>> files;
    dbfull()->TEST_GetFilesMetaData(handles_[1], &files);
    size_t total_files = 0;
    for (const auto& level : files) {
      total_files += level.size();
    }
    ASSERT_EQ(total_files, 3);
    for (const auto& level : files) {
      for (const auto& file : level) {
        if (options.max_open_files == kSmallMaxOpenFiles) {
          ASSERT_TRUE(file.fd.pinned_reader.Get() == nullptr);
        } else {
          ASSERT_TRUE(file.fd.pinned_reader.Get() != nullptr);
        }
      }
    }
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RecoverWithBlob) {
  // Write a value that's below the prospective size limit for blobs and another
  // one that's above. Note that blob files are not actually enabled at this
  // point.
  constexpr uint64_t min_blob_size = 10;

  constexpr char short_value[] = "short";
  static_assert(sizeof(short_value) - 1 < min_blob_size,
                "short_value too long");

  constexpr char long_value[] = "long_value";
  static_assert(sizeof(long_value) - 1 >= min_blob_size,
                "long_value too short");

  ASSERT_OK(Put("key1", short_value));
  ASSERT_OK(Put("key2", long_value));

  // There should be no files just yet since we haven't flushed.
  {
    VersionSet* const versions = dbfull()->GetVersionSet();
    ASSERT_NE(versions, nullptr);

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(cfd, nullptr);

    Version* const current = cfd->current();
    ASSERT_NE(current, nullptr);

    const VersionStorageInfo* const storage_info = current->storage_info();
    ASSERT_NE(storage_info, nullptr);

    ASSERT_EQ(storage_info->num_non_empty_levels(), 0);
    ASSERT_TRUE(storage_info->GetBlobFiles().empty());
  }

  // Reopen the database with blob files enabled. A new table file/blob file
  // pair should be written during recovery.
  Options options;
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;
  options.avoid_flush_during_recovery = false;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  ASSERT_EQ(Get("key1"), short_value);
  ASSERT_EQ(Get("key2"), long_value);

  VersionSet* const versions = dbfull()->GetVersionSet();
  ASSERT_NE(versions, nullptr);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const auto& l0_files = storage_info->LevelFiles(0);
  ASSERT_EQ(l0_files.size(), 1);

  const FileMetaData* const table_file = l0_files[0];
  ASSERT_NE(table_file, nullptr);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  const auto& blob_file = blob_files.front();
  ASSERT_NE(blob_file, nullptr);

  ASSERT_EQ(table_file->smallest.user_key(), "key1");
  ASSERT_EQ(table_file->largest.user_key(), "key2");
  ASSERT_EQ(table_file->fd.smallest_seqno, 1);
  ASSERT_EQ(table_file->fd.largest_seqno, 2);
  ASSERT_EQ(table_file->oldest_blob_file_number,
            blob_file->GetBlobFileNumber());

  ASSERT_EQ(blob_file->GetTotalBlobCount(), 1);

  const InternalStats* const internal_stats = cfd->internal_stats();
  ASSERT_NE(internal_stats, nullptr);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_FALSE(compaction_stats.empty());
  ASSERT_EQ(compaction_stats[0].bytes_written, table_file->fd.GetFileSize());
  ASSERT_EQ(compaction_stats[0].bytes_written_blob,
            blob_file->GetTotalBlobBytes());
  ASSERT_EQ(compaction_stats[0].num_output_files, 1);
  ASSERT_EQ(compaction_stats[0].num_output_files_blob, 1);

  const uint64_t* const cf_stats_value = internal_stats->TEST_GetCFStatsValue();
  ASSERT_EQ(cf_stats_value[InternalStats::BYTES_FLUSHED],
            compaction_stats[0].bytes_written +
                compaction_stats[0].bytes_written_blob);
}

TEST_F(DBWALTest, RecoverWithBlobMultiSST) {
  // Write several large (4 KB) values without flushing. Note that blob files
  // are not actually enabled at this point.
  std::string large_value(1 << 12, 'a');

  constexpr int num_keys = 64;

  for (int i = 0; i < num_keys; ++i) {
    ASSERT_OK(Put(Key(i), large_value));
  }

  // There should be no files just yet since we haven't flushed.
  {
    VersionSet* const versions = dbfull()->GetVersionSet();
    ASSERT_NE(versions, nullptr);

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(cfd, nullptr);

    Version* const current = cfd->current();
    ASSERT_NE(current, nullptr);

    const VersionStorageInfo* const storage_info = current->storage_info();
    ASSERT_NE(storage_info, nullptr);

    ASSERT_EQ(storage_info->num_non_empty_levels(), 0);
    ASSERT_TRUE(storage_info->GetBlobFiles().empty());
  }

  // Reopen the database with blob files enabled and write buffer size set to a
  // smaller value. Multiple table files+blob files should be written and added
  // to the Version during recovery.
  Options options;
  options.write_buffer_size = 1 << 16;  // 64 KB
  options.enable_blob_files = true;
  options.avoid_flush_during_recovery = false;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  for (int i = 0; i < num_keys; ++i) {
    ASSERT_EQ(Get(Key(i)), large_value);
  }

  VersionSet* const versions = dbfull()->GetVersionSet();
  ASSERT_NE(versions, nullptr);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const auto& l0_files = storage_info->LevelFiles(0);
  ASSERT_GT(l0_files.size(), 1);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_GT(blob_files.size(), 1);

  ASSERT_EQ(l0_files.size(), blob_files.size());
}

TEST_F(DBWALTest, WALWithChecksumHandoff) {
#ifndef ROCKSDB_ASSERT_STATUS_CHECKED
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  // Close the DB before the local Env is destroyed if an ASSERT exits early.
  Defer close_db_on_exit([this]() { Close(); });
  do {
    Options options = CurrentOptions();

    options.checksum_handoff_file_types.Add(FileType::kWalFile);
    options.env = fault_fs_env.get();
    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);

    CreateAndReopenWithCF({"pikachu"}, options);
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    // Both value's should be present.
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v2", Get(1, "foo"));

    writeOpt.disableWAL = true;
    // This put, data is persisted by Flush
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    writeOpt.disableWAL = false;
    // Data is persisted in the WAL
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "zoo", "v3"));
    ASSERT_OK(dbfull()->SyncWAL());
    // The hash does not match, write fails
    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
    writeOpt.disableWAL = false;
    ASSERT_NOK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    // Due to the write failure, Get should not find
    ASSERT_NE("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "zoo"));
    ASSERT_EQ("v3", Get(1, "bar"));

    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
    // Each write will be similated as corrupted.
    fault_fs->IngestDataCorruptionBeforeWrite();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v4"));
    writeOpt.disableWAL = false;
    ASSERT_NOK(dbfull()->Put(writeOpt, handles_[1], "foo", "v4"));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_NE("v4", Get(1, "foo"));
    ASSERT_NE("v4", Get(1, "bar"));
    fault_fs->NoDataCorruptionBeforeWrite();

    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
    // The file system does not provide checksum method and verification.
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v5"));
    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v5"));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v5", Get(1, "foo"));
    ASSERT_EQ("v5", Get(1, "bar"));

    Destroy(options);
  } while (ChangeWalOptions());
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
}

TEST_F(DBWALTest, LockWal) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    DestroyAndReopen(options);

    ASSERT_OK(Put("foo", "v"));
    ASSERT_OK(Put("bar", "v"));

    ASSERT_OK(db_->LockWAL());
    // Verify writes are stopped
    WriteOptions wopts;
    wopts.no_slowdown = true;
    Status s = db_->Put(wopts, "foo", "dontcare");
    ASSERT_TRUE(s.IsIncomplete());
    {
      VectorLogPtr wals;
      ASSERT_OK(db_->GetSortedWalFiles(wals));
      ASSERT_FALSE(wals.empty());
    }
    port::Thread worker([&]() {
      Status tmp_s = db_->Flush(FlushOptions());
      ASSERT_OK(tmp_s);
    });
    FlushOptions flush_opts;
    flush_opts.wait = false;
    s = db_->Flush(flush_opts);
    ASSERT_TRUE(s.IsTryAgain());
    ASSERT_OK(db_->UnlockWAL());
    ASSERT_OK(db_->Put(WriteOptions(), "foo", "dontcare"));

    worker.join();
  } while (ChangeWalOptions());
}

class DBRecoveryTestBlobError
    : public DBWALTest,
      public testing::WithParamInterface<std::string> {
 public:
  DBRecoveryTestBlobError() : sync_point_(GetParam()) {}

  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DBRecoveryTestBlobError, DBRecoveryTestBlobError,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileBuilder::WriteBlobToFile:AddRecord",
                            "BlobFileBuilder::WriteBlobToFile:AppendFooter"}));

TEST_P(DBRecoveryTestBlobError, RecoverWithBlobError) {
  // Write a value. Note that blob files are not actually enabled at this point.
  ASSERT_OK(Put("key", "blob"));

  // Reopen with blob files enabled but make blob file writing fail during
  // recovery.
  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* arg) {
    Status* const s = static_cast<Status*>(arg);
    assert(s);

    (*s) = Status::IOError(sync_point_);
  });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.enable_blob_files = true;
  options.avoid_flush_during_recovery = false;
  options.disable_auto_compactions = true;
  options.env = env_;

  ASSERT_NOK(TryReopen(options));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Make sure the files generated by the failed recovery have been deleted.
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  for (const auto& file : files) {
    uint64_t number = 0;
    FileType type = kTableFile;

    if (!ParseFileName(file, &number, &type)) {
      continue;
    }

    ASSERT_NE(type, kTableFile);
    ASSERT_NE(type, kBlobFile);
  }
}

TEST_F(DBWALTest, IgnoreRecoveredLog) {
  std::string backup_logs = dbname_ + "/backup_logs";

  do {
    // delete old files in backup_logs directory
    ASSERT_OK(env_->CreateDirIfMissing(backup_logs));
    std::vector<std::string> old_files;
    ASSERT_OK(env_->GetChildren(backup_logs, &old_files));
    for (auto& file : old_files) {
      ASSERT_OK(env_->DeleteFile(backup_logs + "/" + file));
    }
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.merge_operator = MergeOperators::CreateUInt64AddOperator();
    options.wal_dir = dbname_ + "/logs";
    DestroyAndReopen(options);

    // fill up the DB
    std::string one;
    std::string two;
    PutFixed64(&one, 1);
    PutFixed64(&two, 2);
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("foo"), Slice(one)));
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("foo"), Slice(one)));
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("bar"), Slice(one)));

    // copy the logs to backup
    std::vector<std::string> logs;
    ASSERT_OK(env_->GetChildren(options.wal_dir, &logs));
    for (auto& log : logs) {
      CopyFile(options.wal_dir + "/" + log, backup_logs + "/" + log);
    }

    // recover the DB
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));
    Close();

    // copy the logs from backup back to wal dir
    for (auto& log : logs) {
      CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
    }
    // this should ignore the log files, recovery should not happen again
    // if the recovery happens, the same merge operator would be called twice,
    // leading to incorrect results
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));
    Close();
    Destroy(options);
    Reopen(options);
    Close();

    // copy the logs from backup back to wal dir
    ASSERT_OK(env_->CreateDirIfMissing(options.wal_dir));
    for (auto& log : logs) {
      CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
    }
    // assert that we successfully recovered only from logs, even though we
    // destroyed the DB
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));

    // Recovery will fail if DB directory doesn't exist.
    Destroy(options);
    // copy the logs from backup back to wal dir
    ASSERT_OK(env_->CreateDirIfMissing(options.wal_dir));
    for (auto& log : logs) {
      CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
      // we won't be needing this file no more
      ASSERT_OK(env_->DeleteFile(backup_logs + "/" + log));
    }
    Status s = TryReopen(options);
    ASSERT_NOK(s);
    Destroy(options);
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RecoveryWithEmptyLog) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v3"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v3", Get(1, "foo"));
  } while (ChangeWalOptions());
}

#if !(defined NDEBUG) || !defined(OS_WIN)
TEST_F(DBWALTest, PreallocateBlock) {
  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1000 * 1000;
  options.max_total_wal_size = 0;

  size_t expected_preallocation_size = static_cast<size_t>(
      options.write_buffer_size + options.write_buffer_size / 10);

  DestroyAndReopen(options);

  std::atomic<int> called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWalFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        ASSERT_EQ(expected_preallocation_size, preallocation_size);
        called.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("", ""));
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(2, called.load());

  options.max_total_wal_size = 1000 * 1000;
  expected_preallocation_size = static_cast<size_t>(options.max_total_wal_size);
  Reopen(options);
  called.store(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWalFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        ASSERT_EQ(expected_preallocation_size, preallocation_size);
        called.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("", ""));
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(2, called.load());

  options.db_write_buffer_size = 800 * 1000;
  expected_preallocation_size =
      static_cast<size_t>(options.db_write_buffer_size);
  Reopen(options);
  called.store(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWalFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        ASSERT_EQ(expected_preallocation_size, preallocation_size);
        called.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("", ""));
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(2, called.load());

  expected_preallocation_size = 700 * 1000;
  std::shared_ptr<WriteBufferManager> write_buffer_manager =
      std::make_shared<WriteBufferManager>(static_cast<uint64_t>(700 * 1000));
  options.write_buffer_manager = write_buffer_manager;
  Reopen(options);
  called.store(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWalFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        ASSERT_EQ(expected_preallocation_size, preallocation_size);
        called.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("", ""));
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(2, called.load());
}
#endif  // !(defined NDEBUG) || !defined(OS_WIN)

TEST_F(DBWALTest, FullPurgePreservesRecycledLog) {
  // For github issue #1303
  for (int i = 0; i < 2; ++i) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.recycle_log_file_num = 2;
    options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
    if (i != 0) {
      options.wal_dir = alternative_wal_dir_;
    }

    DestroyAndReopen(options);
    ASSERT_OK(Put("foo", "v1"));
    VectorLogPtr log_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
    ASSERT_GT(log_files.size(), 0);
    ASSERT_OK(Flush());

    // Now the original WAL is in log_files[0] and should be marked for
    // recycling.
    // Verify full purge cannot remove this file.
    JobContext job_context(0);
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&job_context, true /* force */);
    dbfull()->TEST_UnlockMutex();
    dbfull()->PurgeObsoleteFiles(job_context);
    job_context.Clean();

    if (i == 0) {
      ASSERT_OK(
          env_->FileExists(LogFileName(dbname_, log_files[0]->LogNumber())));
    } else {
      ASSERT_OK(env_->FileExists(
          LogFileName(alternative_wal_dir_, log_files[0]->LogNumber())));
    }
  }
}

TEST_F(DBWALTest, FullPurgePreservesLogPendingReuse) {
  // Ensures full purge cannot delete a WAL while it's in the process of being
  // recycled. In particular, we force the full purge after a file has been
  // chosen for reuse, but before it has been renamed.
  for (int i = 0; i < 2; ++i) {
    Options options = CurrentOptions();
    options.recycle_log_file_num = 1;
    options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
    if (i != 0) {
      options.wal_dir = alternative_wal_dir_;
    }
    DestroyAndReopen(options);

    // The first flush creates a second log so writes can continue before the
    // flush finishes.
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Flush());
    // The first log can be recycled only after the flush closes its writer.
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

    // The second flush can recycle the first log. Sync points enforce the
    // full purge happens after choosing the log to recycle and before it is
    // renamed.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {"DBImpl::CreateWAL:BeforeReuseWritableFile1",
         "DBWALTest::FullPurgePreservesLogPendingReuse:PreFullPurge"},
        {"DBWALTest::FullPurgePreservesLogPendingReuse:PostFullPurge",
         "DBImpl::CreateWAL:BeforeReuseWritableFile2"},
    });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ROCKSDB_NAMESPACE::port::Thread thread([&]() {
      TEST_SYNC_POINT(
          "DBWALTest::FullPurgePreservesLogPendingReuse:PreFullPurge");
      ASSERT_OK(db_->EnableFileDeletions());
      TEST_SYNC_POINT(
          "DBWALTest::FullPurgePreservesLogPendingReuse:PostFullPurge");
    });
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Flush());
    thread.join();
  }
}

TEST_F(DBWALTest, RecycledWalKeepsSyncedWritesWhenOldWriterCloses) {
  // The flush pauses either before it removes WAL A from logs_, or after it
  // removed A but before it frees the writers it released.
  for (const char* pause_point : {"FindObsoleteFiles::PostMutexUnlock",
                                  "DBImpl::BackgroundCallFlush:FilesFound"}) {
    for (bool background_close : {false, true}) {
      for (bool sync_old_wal_before_flush : {false, true}) {
        SCOPED_TRACE(std::string("pause_point=") + pause_point +
                     " background_close_inactive_wals=" +
                     std::to_string(background_close) +
                     " sync_old_wal_before_flush=" +
                     std::to_string(sync_old_wal_before_flush));
        Options options = CurrentOptions();
        options.recycle_log_file_num = 1;
        options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
        options.max_background_flushes = 1;
        options.background_close_inactive_wals = background_close;
        DestroyAndReopen(options);
        ASSERT_OK(Put("a", "small"));

        // The flush of WAL A's memtable waits until A may have been synced as
        // an inactive WAL. It then puts A on the recycle list and pauses at
        // pause_point.
        std::atomic<bool> flush_started{false};
        std::promise<void> old_wal_synced;
        std::shared_future<void> old_wal_synced_future =
            old_wal_synced.get_future().share();
        std::atomic<bool> paused{false};
        std::promise<void> reached;
        std::promise<void> resume;
        std::shared_future<void> resume_future = resume.get_future().share();
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCallFlush:start", [&](void*) {
              if (!flush_started.exchange(true)) {
                old_wal_synced_future.wait();
              }
            });
        SyncPoint::GetInstance()->SetCallBack(pause_point, [&](void*) {
          if (!paused.exchange(true)) {
            reached.set_value();
            resume_future.wait();
          }
        });
        SyncPoint::GetInstance()->EnableProcessing();
        FlushOptions no_wait;
        no_wait.wait = false;
        ASSERT_OK(db_->Flush(no_wait));
        WriteOptions sync_write;
        sync_write.sync = true;
        if (sync_old_wal_before_flush) {
          ASSERT_OK(db_->Put(sync_write, "c", "small"));
        }
        old_wal_synced.set_value();
        reached.get_future().wait();

        // The next memtable switch reuses A as WAL C, and a synced write lands
        // in C past A's old size.
        ASSERT_OK(Put("b", "small"));
        ASSERT_OK(db_->Flush(no_wait));
        const std::string big_value(64 * 1024, 'v');
        ASSERT_OK(db_->Put(sync_write, "big", big_value));

        resume.set_value();
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
        ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();

        Reopen(options);
        EXPECT_EQ(big_value, Get("big"));
      }
    }
  }
}

TEST_F(DBWALTest, GetSortedWalFiles) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    VectorLogPtr log_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
    ASSERT_EQ(0, log_files.size());

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
    ASSERT_EQ(1, log_files.size());
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, GetCurrentWalFile) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

    std::unique_ptr<LogFile>* bad_log_file = nullptr;
    ASSERT_NOK(dbfull()->GetCurrentWalFile(bad_log_file));

    std::unique_ptr<LogFile> log_file;
    ASSERT_OK(dbfull()->GetCurrentWalFile(&log_file));

    // nothing has been written to the log yet
    ASSERT_EQ(log_file->StartSequence(), 0);
    ASSERT_EQ(log_file->SizeFileBytes(), 0);
    ASSERT_EQ(log_file->Type(), kAliveLogFile);
    ASSERT_GT(log_file->LogNumber(), 0);

    // add some data and verify that the file size actually moves foward
    ASSERT_OK(Put(0, "foo", "v1"));
    ASSERT_OK(Put(0, "foo2", "v2"));
    ASSERT_OK(Put(0, "foo3", "v3"));

    ASSERT_OK(dbfull()->GetCurrentWalFile(&log_file));

    ASSERT_EQ(log_file->StartSequence(), 0);
    ASSERT_GT(log_file->SizeFileBytes(), 0);
    ASSERT_EQ(log_file->Type(), kAliveLogFile);
    ASSERT_GT(log_file->LogNumber(), 0);

    // force log files to cycle and add some more data, then check if
    // log number moves forward

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    }

    ASSERT_OK(Put(0, "foo4", "v4"));
    ASSERT_OK(Put(0, "foo5", "v5"));
    ASSERT_OK(Put(0, "foo6", "v6"));

    ASSERT_OK(dbfull()->GetCurrentWalFile(&log_file));

    ASSERT_EQ(log_file->StartSequence(), 0);
    ASSERT_GT(log_file->SizeFileBytes(), 0);
    ASSERT_EQ(log_file->Type(), kAliveLogFile);
    ASSERT_GT(log_file->LogNumber(), 0);

  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RecoveryWithLogDataForSomeCFs) {
  // Test for regression of WAL cleanup missing files that don't contain data
  // for every column family.
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "foo", "v2"));
    uint64_t earliest_log_nums[2];
    for (int i = 0; i < 2; ++i) {
      if (i > 0) {
        ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
      }
      VectorLogPtr log_files;
      ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
      if (log_files.size() > 0) {
        earliest_log_nums[i] = log_files[0]->LogNumber();
      } else {
        earliest_log_nums[i] = std::numeric_limits<uint64_t>::max();
      }
    }
    // Check at least the first WAL was cleaned up during the recovery.
    ASSERT_LT(earliest_log_nums[0], earliest_log_nums[1]);
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RecoverWithLargeLog) {
  do {
    {
      Options options = CurrentOptions();
      CreateAndReopenWithCF({"pikachu"}, options);
      ASSERT_OK(Put(1, "big1", std::string(200000, '1')));
      ASSERT_OK(Put(1, "big2", std::string(200000, '2')));
      ASSERT_OK(Put(1, "small3", std::string(10, '3')));
      ASSERT_OK(Put(1, "small4", std::string(10, '4')));
      ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    }

    // Make sure that if we re-open with a small write buffer size that
    // we flush table files in the middle of a large log file.
    Options options;
    options.write_buffer_size = 100000;
    options = CurrentOptions(options);
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 3);
    ASSERT_EQ(std::string(200000, '1'), Get(1, "big1"));
    ASSERT_EQ(std::string(200000, '2'), Get(1, "big2"));
    ASSERT_EQ(std::string(10, '3'), Get(1, "small3"));
    ASSERT_EQ(std::string(10, '4'), Get(1, "small4"));
    ASSERT_GT(NumTableFilesAtLevel(0, 1), 1);
  } while (ChangeWalOptions());
}

// In https://reviews.facebook.net/D20661 we change
// recovery behavior: previously for each log file each column family
// memtable was flushed, even it was empty. Now it's changed:
// we try to create the smallest number of table files by merging
// updates from multiple logs
TEST_F(DBWALTest, RecoverCheckFileAmountWithSmallWriteBuffer) {
  Options options = CurrentOptions();
  options.write_buffer_size = 5000000;
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  // Since we will reopen DB with smaller write_buffer_size,
  // each key will go to new SST file
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));

  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  // Make 'dobrynia' to be flushed and new WAL file to be created
  ASSERT_OK(Put(2, Key(10), DummyString(7500000)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[2]));
  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(1));
    // Make sure 'dobrynia' was flushed: check sst files amount
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(1));
  }
  // New WAL file
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));

  options.write_buffer_size = 4096;
  options.arena_block_size = 4096;
  ReopenWithColumnFamilies({"default", "pikachu", "dobrynia", "nikitich"},
                           options);
  {
    // No inserts => default is empty
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(0));
    // First 4 keys goes to separate SSTs + 1 more SST for 2 smaller keys
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(5));
    // 1 SST for big key + 1 SST for small one
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(2));
    // 1 SST for all keys
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(1));
  }
}

// In https://reviews.facebook.net/D20661 we change
// recovery behavior: previously for each log file each column family
// memtable was flushed, even it wasn't empty. Now it's changed:
// we try to create the smallest number of table files by merging
// updates from multiple logs
TEST_F(DBWALTest, RecoverCheckFileAmount) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000;
  options.arena_block_size = 4 * 1024;
  options.avoid_flush_during_recovery = false;
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  // Make 'nikitich' memtable to be flushed
  ASSERT_OK(Put(3, Key(10), DummyString(1002400)));
  ASSERT_OK(Put(3, Key(1), DummyString(1)));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[3]));
  // 4 memtable are not flushed, 1 sst file
  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(1));
  }
  // Memtable for 'nikitich' has flushed, new WAL file has opened
  // 4 memtable still not flushed

  // Write to new WAL file
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  // Fill up 'nikitich' one more time
  ASSERT_OK(Put(3, Key(10), DummyString(1002400)));
  // make it flush
  ASSERT_OK(Put(3, Key(1), DummyString(1)));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[3]));
  // There are still 4 memtable not flushed, and 2 sst tables
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(2));
  }

  ReopenWithColumnFamilies({"default", "pikachu", "dobrynia", "nikitich"},
                           options);
  {
    std::vector<uint64_t> table_files = ListTableFiles(env_, dbname_);
    // Check, that records for 'default', 'dobrynia' and 'pikachu' from
    // first, second and third WALs  went to the same SST.
    // So, there is 6 SSTs: three  for 'nikitich', one for 'default', one for
    // 'dobrynia', one for 'pikachu'
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(3));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(1));
  }
}

TEST_F(DBWALTest, SyncMultipleLogs) {
  const uint64_t kNumBatches = 2;
  const int kBatchSize = 1000;

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.write_buffer_size = 4096;
  Reopen(options);

  WriteBatch batch;
  WriteOptions wo;
  wo.sync = true;

  for (uint64_t b = 0; b < kNumBatches; b++) {
    batch.Clear();
    for (int i = 0; i < kBatchSize; i++) {
      ASSERT_OK(batch.Put(Key(i), DummyString(128)));
    }

    ASSERT_OK(dbfull()->Write(wo, &batch));
  }

  ASSERT_OK(dbfull()->SyncWAL());
}

TEST_F(DBWALTest, DISABLED_RecycleMultipleWalsCrash) {
  Options options = CurrentOptions();
  options.max_write_buffer_number = 5;
  options.track_and_verify_wals_in_manifest = true;
  options.max_bgerror_resume_count = 0;  // manual resume
  options.recycle_log_file_num = 3;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  // Disable truncating recycled WALs to new size in posix env
  // (approximating a crash)
  SyncPoint::GetInstance()->SetCallBack(
      "PosixWritableFile::Close",
      [](void* arg) { *(static_cast<size_t*>(arg)) = 0; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Re-open with desired options
  DestroyAndReopen(options);
  Defer closer([this]() { Close(); });

  // Ensure WAL recycling wasn't sanitized away
  ASSERT_EQ(db_->GetOptions().recycle_log_file_num,
            options.recycle_log_file_num);

  // Prepare external files for later ingestion
  std::string sst_files_dir = dbname_ + "/sst_files/";
  ASSERT_OK(DestroyDir(env_, sst_files_dir));
  ASSERT_OK(env_->CreateDir(sst_files_dir));
  std::string external_file1 = sst_files_dir + "file1.sst";
  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(external_file1));
    ASSERT_OK(sst_file_writer.Put("external1", "ex1"));
    ExternalSstFileInfo file_info;
    ASSERT_OK(sst_file_writer.Finish(&file_info));
  }
  std::string external_file2 = sst_files_dir + "file2.sst";
  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(external_file2));
    ASSERT_OK(sst_file_writer.Put("external2", "ex2"));
    ExternalSstFileInfo file_info;
    ASSERT_OK(sst_file_writer.Finish(&file_info));
  }

  // Populate some WALs to be recycled such that there will be extra data
  // from an old incarnation of the WAL on recovery
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("ignore1", Random::GetTLSInstance()->RandomString(500)));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("ignore2", Random::GetTLSInstance()->RandomString(500)));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->ContinueBackgroundWork());
  ASSERT_OK(Flush());
  ASSERT_OK(Put("ignore3", Random::GetTLSInstance()->RandomString(500)));
  ASSERT_OK(Flush());

  // Verify expected log files (still there for recycling)
  std::vector<FileAttributes> files;
  int log_count = 0;
  ASSERT_OK(options.env->GetChildrenFileAttributes(dbname_, &files));
  for (const auto& f : files) {
    if (EndsWith(f.name, ".log")) {
      EXPECT_GT(f.size_bytes, 500);
      ++log_count;
    }
  }
  EXPECT_EQ(log_count, 3);

  // (Re-used recipe) Generate two inactive WALs and one active WAL, with a
  // gap in sequence numbers to interfere with recovery
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  // Need a gap in sequence numbers, so e.g. ingest external file
  // with an open snapshot
  {
    ManagedSnapshot snapshot(db_.get());
    ASSERT_OK(
        db_->IngestExternalFile({external_file1}, IngestExternalFileOptions()));
  }
  ASSERT_OK(Put("key3", "val3"));
  ASSERT_OK(db_->SyncWAL());
  // Need an SST file that is logically after that WAL, so that dropping WAL
  // data is not a valid point in time.
  {
    ManagedSnapshot snapshot(db_.get());
    ASSERT_OK(
        db_->IngestExternalFile({external_file2}, IngestExternalFileOptions()));
  }

  // Approximate a crash, with respect to recycled WAL data extending past
  // the end of the current WAL data (see SyncPoint callback above)
  Close();

  // Verify recycled log files haven't been truncated
  files.clear();
  log_count = 0;
  ASSERT_OK(options.env->GetChildrenFileAttributes(dbname_, &files));
  for (const auto& f : files) {
    if (EndsWith(f.name, ".log")) {
      EXPECT_GT(f.size_bytes, 500);
      ++log_count;
    }
  }
  EXPECT_EQ(log_count, 3);

  // Verify no data loss after reopen.
  Reopen(options);
  EXPECT_EQ("val1", Get("key1"));
  EXPECT_EQ("val2", Get("key2"));  // Passes because of adjacent seqnos
  EXPECT_EQ("ex1", Get("external1"));
  EXPECT_EQ("val3", Get("key3"));  // <- ONLY FAILURE! (Not a point in time)
  EXPECT_EQ("ex2", Get("external2"));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBWALTest, SyncWalPartialFailure) {
  class MyTestFileSystem : public FileSystemWrapper {
   public:
    explicit MyTestFileSystem(std::shared_ptr<FileSystem> base)
        : FileSystemWrapper(std::move(base)) {}

    static const char* kClassName() { return "MyTestFileSystem"; }
    const char* Name() const override { return kClassName(); }
    IOStatus NewWritableFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
      IOStatus s = target()->NewWritableFile(fname, file_opts, result, dbg);
      if (s.ok()) {
        *result =
            std::make_unique<MyTestWritableFile>(std::move(*result), *this);
      }
      return s;
    }

    Atomic<uint32_t> syncs_before_failure_{UINT32_MAX};

   protected:
    class MyTestWritableFile : public FSWritableFileOwnerWrapper {
     public:
      MyTestWritableFile(std::unique_ptr<FSWritableFile>&& file,
                         MyTestFileSystem& fs)
          : FSWritableFileOwnerWrapper(std::move(file)), fs_(fs) {}

      IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
        int prev_val = fs_.syncs_before_failure_.FetchSub(1);
        if (prev_val == 0) {
          return IOStatus::IOError("fault");
        } else {
          return target()->Sync(options, dbg);
        }
      }

     protected:
      MyTestFileSystem& fs_;
    };
  };

  Options options = CurrentOptions();
  options.max_write_buffer_number = 4;
  options.track_and_verify_wals_in_manifest = true;
  options.max_bgerror_resume_count = 0;  // manual resume

  auto custom_fs =
      std::make_shared<MyTestFileSystem>(options.env->GetFileSystem());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(custom_fs));
  options.env = fault_fs_env.get();
  Reopen(options);
  Defer closer([this]() { Close(); });

  // This is the simplest way to get
  // * one inactive WAL, synced
  // * one inactive WAL, not synced, and
  // * one active WAL, not synced
  // with a single thread, to exercise as much logic as we reasonably can.
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->SyncWAL());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key3", "val3"));

  // Allow 1 of the WALs to sync, but another won't
  custom_fs->syncs_before_failure_.Store(1);
  ASSERT_NOK(db_->SyncWAL());

  // Stuck in this state. (This could previously cause a segfault.)
  ASSERT_NOK(db_->SyncWAL());

  // Can't Resume because WAL write failure is considered non-recoverable,
  // regardless of the IOStatus itself. (Can/should be fixed?)
  ASSERT_NOK(db_->Resume());

  // Verify no data loss after reopen.
  // Also Close() could previously crash in this state.
  Reopen(options);
  ASSERT_EQ("val1", Get("key1"));
  ASSERT_EQ("val2", Get("key2"));
  ASSERT_EQ("val3", Get("key3"));
}

// Github issue 1339. Prior the fix we read sequence id from the first log to
// a local variable, then keep increase the variable as we replay logs,
// ignoring actual sequence id of the records. This is incorrect if some writes
// come with WAL disabled.
TEST_F(DBWALTest, PartOfWritesWithWALDisabled) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.env = fault_env.get();
  options.disable_auto_compactions = true;
  WriteOptions wal_on;
  WriteOptions wal_off;
  wal_on.sync = true;
  wal_on.disableWAL = false;
  wal_off.disableWAL = true;
  CreateAndReopenWithCF({"dummy"}, options);
  ASSERT_OK(Put(1, "dummy", "d1", wal_on));  // seq id 1
  ASSERT_OK(Put(1, "dummy", "d2", wal_off));
  ASSERT_OK(Put(1, "dummy", "d3", wal_off));
  ASSERT_OK(Put(0, "key", "v4", wal_on));  // seq id 4
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(0, "key", "v5", wal_on));  // seq id 5
  ASSERT_EQ("v5", Get(0, "key"));
  ASSERT_OK(dbfull()->FlushWAL(false));
  // Simulate a crash.
  fault_env->SetFilesystemActive(false);
  Close();
  fault_env->ResetState();
  ReopenWithColumnFamilies({"default", "dummy"}, options);
  // Prior to the fix, we may incorrectly recover "v5" with sequence id = 3.
  ASSERT_EQ("v5", Get(0, "key"));
  // Destroy DB before destruct fault_env.
  Destroy(options);
}

//
// Test WAL recovery for the various modes available
//
class RecoveryTestHelper {
 public:
  // Number of WAL files to generate
  static constexpr int kWALFilesCount = 10;
  // Starting number for the WAL file name like 00010.log
  static constexpr int kWALFileOffset = 10;
  // Keys to be written per WAL file
  static constexpr int kKeysPerWALFile = 133;
  // Size of the value
  static constexpr int kValueSize = 96;

  // Create WAL files with values filled in
  static void FillData(DBWALTestBase* test, const Options& options,
                       const size_t wal_count, size_t* count) {
    // Calling internal functions requires sanitized options.
    Options sanitized_options = SanitizeOptions(test->dbname_, options);
    const ImmutableDBOptions db_options(sanitized_options);

    *count = 0;

    std::shared_ptr<Cache> table_cache = NewLRUCache(50, 0);
    FileOptions file_options;
    WriteBufferManager write_buffer_manager(db_options.db_write_buffer_size);

    std::unique_ptr<VersionSet> versions;
    std::unique_ptr<WalManager> wal_manager;
    WriteController write_controller;

    versions.reset(new VersionSet(
        test->dbname_, &db_options, MutableDBOptions{options}, file_options,
        table_cache.get(), &write_buffer_manager, &write_controller,
        /*block_cache_tracer=*/nullptr,
        /*io_tracer=*/nullptr, /*db_id=*/"", /*db_session_id=*/"",
        options.daily_offpeak_time_utc,
        /*error_handler=*/nullptr, /*read_only=*/false));

    wal_manager.reset(
        new WalManager(db_options, file_options, /*io_tracer=*/nullptr));

    std::unique_ptr<log::Writer> current_log_writer;

    for (size_t j = kWALFileOffset; j < wal_count + kWALFileOffset; j++) {
      uint64_t current_log_number = j;
      std::string fname = LogFileName(test->dbname_, current_log_number);
      std::unique_ptr<WritableFileWriter> file_writer;
      ASSERT_OK(WritableFileWriter::Create(db_options.env->GetFileSystem(),
                                           fname, file_options, &file_writer,
                                           nullptr));
      log::Writer* log_writer =
          new log::Writer(std::move(file_writer), current_log_number,
                          db_options.recycle_log_file_num > 0, false,
                          db_options.wal_compression);
      ASSERT_OK(log_writer->AddCompressionTypeRecord(WriteOptions()));
      current_log_writer.reset(log_writer);

      WriteBatch batch;
      for (int i = 0; i < kKeysPerWALFile; i++) {
        std::string key = "key" + std::to_string((*count)++);
        std::string value = test->DummyString(kValueSize);
        ASSERT_NE(current_log_writer.get(), nullptr);
        uint64_t seq = versions->LastSequence() + 1;
        batch.Clear();
        ASSERT_OK(batch.Put(key, value));
        WriteBatchInternal::SetSequence(&batch, seq);
        ASSERT_OK(current_log_writer->AddRecord(
            WriteOptions(), WriteBatchInternal::Contents(&batch)));
        versions->SetLastAllocatedSequence(seq);
        versions->SetLastPublishedSequence(seq);
        versions->SetLastSequence(seq);
      }
    }
  }

  // Recreate and fill the store with some data
  static size_t FillData(DBWALTestBase* test, Options* options) {
    options->create_if_missing = true;
    test->DestroyAndReopen(*options);
    test->Close();

    size_t count = 0;
    FillData(test, *options, kWALFilesCount, &count);
    return count;
  }

  // Read back all the keys we wrote and return the number of keys found
  static size_t GetData(DBWALTestBase* test) {
    size_t count = 0;
    for (size_t i = 0; i < kWALFilesCount * kKeysPerWALFile; i++) {
      if (test->Get("key" + std::to_string(i)) != "NOT_FOUND") {
        ++count;
      }
    }
    return count;
  }

  // Manuall corrupt the specified WAL
  static void CorruptWAL(DBWALTestBase* test, const Options& options,
                         const double off, const double len,
                         const int wal_file_id, const bool trunc = false) {
    Env* env = options.env;
    std::string fname = LogFileName(test->dbname_, wal_file_id);
    uint64_t size;
    ASSERT_OK(env->GetFileSize(fname, &size));
    ASSERT_GT(size, 0);
#ifdef OS_WIN
    // Windows disk cache behaves differently. When we truncate
    // the original content is still in the cache due to the original
    // handle is still open. Generally, in Windows, one prohibits
    // shared access to files and it is not needed for WAL but we allow
    // it to induce corruption at various tests.
    test->Close();
#endif
    if (trunc) {
      ASSERT_OK(
          test::TruncateFile(env, fname, static_cast<uint64_t>(size * off)));
    } else {
      ASSERT_OK(test::CorruptFile(env, fname, static_cast<int>(size * off + 8),
                                  static_cast<int>(size * len), false));
    }
  }
};

TEST_F(DBWALTest, TrackAndVerifyWALsRecycleWAL) {
  Options options = CurrentOptions();
  options.avoid_flush_during_shutdown = true;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.recycle_log_file_num = 1;
  options.track_and_verify_wals = true;

  DestroyAndReopen(options);

  ASSERT_OK(Put("key_ignore", "wal_to_recycle"));
  ASSERT_OK(Put("key_ignore1", "wal_to_recycle"));
  ASSERT_OK(Put("key_ignore2", "wal_to_recycle"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("key_ignore", "wal_to_recycle"));
  ASSERT_OK(Put("key_ignore1", "wal_to_recycle"));
  ASSERT_OK(Put("key_ignore2", "wal_to_recycle"));
  ASSERT_OK(Flush());
  // The second WAL can be recycled only after the flush closes its writer.
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

  // Stop background flush to avoid deleting any WAL
  options.env->SetBackgroundThreads(1, Env::HIGH);
  test::SleepingBackgroundTask sleeping_task;
  options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                        &sleeping_task, Env::Priority::HIGH);

  // Recycle the first WAL
  ASSERT_OK(Put("key1", "old_value"));
  // Recycle the second WAL
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_OK(Put("key1", "new_value"));

  // Create a WAL hole on sequence number by truncating the first WAL to 0 byte
  VectorWalPtr log_files;
  ASSERT_OK(db_->GetSortedWalFiles(log_files));
  ASSERT_EQ(log_files.size(), 2);
  std::string log_name = LogFileName(dbname_, log_files.front()->LogNumber());
  Close();
  // Drop `Put("key1", "old_value")` in the first WAL
  ASSERT_OK(test::TruncateFile(options.env, log_name, 0 /* new_length */));

  {
    Status s = DB::Open(options, dbname_, &db_);

    ASSERT_OK(s);
  }

  ASSERT_EQ("wal_to_recycle", Get("key_ignore2"));
  ASSERT_EQ("NOT_FOUND", Get("key1"));

  Close();
}

class DBWALTrackAndVerifyWALsWithParamsTest
    : public DBWALTestBase,
      public ::testing::WithParamInterface<WALRecoveryMode> {
 public:
  DBWALTrackAndVerifyWALsWithParamsTest()
      : DBWALTestBase("/db_wal_track_and_verify_wals_with_params_test") {}
};

INSTANTIATE_TEST_CASE_P(
    DBWALTrackAndVerifyWALsWithParamsTest,
    DBWALTrackAndVerifyWALsWithParamsTest,
    ::testing::Values(WALRecoveryMode::kTolerateCorruptedTailRecords,
                      WALRecoveryMode::kAbsoluteConsistency,
                      WALRecoveryMode::kPointInTimeRecovery,
                      WALRecoveryMode::kSkipAnyCorruptedRecords));

TEST_P(DBWALTrackAndVerifyWALsWithParamsTest, Basic) {
  Options options = CurrentOptions();
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals = true;
  options.wal_recovery_mode = GetParam();

  // Stop background flush to avoid deleting any WAL
  options.env->SetBackgroundThreads(1, Env::HIGH);
  test::SleepingBackgroundTask sleeping_task;
  options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                        &sleeping_task, Env::Priority::HIGH);

  for (int i = 0; i < 5; i++) {
    DestroyAndReopen(options);

    ASSERT_OK(Put("key1", "old_value"));
    SequenceNumber last_seqno_recorded_in_fist_wal =
        dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(dbfull()->TEST_SwitchWAL());
    ASSERT_OK(Put("key1", "new_value"));

    VectorWalPtr log_files;
    ASSERT_OK(db_->GetSortedWalFiles(log_files));
    ASSERT_EQ(log_files.size(), 2);
    uint64_t first_log_number = log_files.front()->LogNumber();
    std::string first_log_name = LogFileName(dbname_, first_log_number);
    std::string second_log_name =
        LogFileName(dbname_, log_files.back()->LogNumber());

    if (i == 0) {
      // Delete the obsolete WAL and verify it will not be seen as a WAL hole
      sleeping_task.WakeUp();
      sleeping_task.WaitUntilDone();
      ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
      // Stop background flush to avoid deleting any WAL
      sleeping_task.Reset();
      options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                            &sleeping_task, Env::Priority::HIGH);
      Close();
    } else if (i == 1) {
      // Create a WAL hole on WAL number by deleting the first WAL and verify
      // the hole will be detected
      Close();
      ASSERT_OK(options.env->DeleteFile(first_log_name));
    } else if (i == 2) {
      // Create a WAL hole on sequence number by truncating the first WAL and
      // verify the hole will be detected
      Close();
      ASSERT_OK(
          test::TruncateFile(options.env, first_log_name, 0 /* new_length */));
    } else if (i == 3) {
      // Create a WAL hole on size difference by truncating the first WAL and
      // mocking a correct sequence number to force triggering corruption based
      // on size instead of sequence number and verify the hole will be detected
      Close();
      ASSERT_OK(
          test::TruncateFile(options.env, first_log_name, 0 /* new_length */));
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::UpdatePredecessorWALInfo", [&](void* arg) {
            std::pair<uint64_t, SequenceNumber*>* pair =
                static_cast<std::pair<uint64_t, SequenceNumber*>*>(arg);
            if (pair->first == first_log_number) {
              *(pair->second) = last_seqno_recorded_in_fist_wal;
            }
          });
      SyncPoint::GetInstance()->EnableProcessing();
    } else if (i == 4) {
      // Delete all wals and verify opening a DB with no WAL will be detected
      Close();
      ASSERT_OK(options.env->DeleteFile(first_log_name));
      ASSERT_OK(options.env->DeleteFile(second_log_name));
    }

    Status s;
    {
      s = DB::Open(options, dbname_, &db_);
    }

    if (i == 0) {
      ASSERT_OK(s);
      ASSERT_EQ("new_value", Get("key1"));
      continue;
    } else if (i == 3) {
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->DisableProcessing();
    } else if (i == 4) {
      ASSERT_TRUE(s.IsCorruption());
      ASSERT_TRUE(
          s.ToString().find("Opening an existing DB with no WAL files") !=
          std::string::npos);
      Close();
      continue;
    }

    if (options.wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
      ASSERT_OK(s);
      ASSERT_EQ("NOT_FOUND", Get("key1"));
    } else if (options.wal_recovery_mode ==
                   WALRecoveryMode::kAbsoluteConsistency ||
               options.wal_recovery_mode ==
                   WALRecoveryMode::kTolerateCorruptedTailRecords) {
      ASSERT_TRUE(s.IsCorruption());
      std::string msg;
      if (i == 1) {
        msg = "Missing WAL";
      } else if (i == 2) {
        msg = "Mismatched last sequence number recorded in the WAL";
      } else if (i == 3) {
        msg = "Mismatched size of the WAL";
      }
      ASSERT_TRUE(s.ToString().find(msg) != std::string::npos);
    } else {
      ASSERT_OK(s);
      ASSERT_EQ("new_value", Get("key1"));
    }

    Close();
  }
}

class DBWALTestWithParams
    : public DBWALTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, int, int, CompressionType, bool>> {
 public:
  DBWALTestWithParams() : DBWALTestBase("/db_wal_test_with_params") {}
};

INSTANTIATE_TEST_CASE_P(
    Wal, DBWALTestWithParams,
    ::testing::Combine(::testing::Bool(), ::testing::Range(0, 4, 1),
                       ::testing::Range(RecoveryTestHelper::kWALFileOffset,
                                        RecoveryTestHelper::kWALFileOffset +
                                            RecoveryTestHelper::kWALFilesCount,
                                        1),
                       ::testing::Values(CompressionType::kNoCompression,
                                         CompressionType::kZSTD),
                       ::testing::Bool()));

class DBWALTestWithParamsVaryingRecoveryMode
    : public DBWALTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, int, int, WALRecoveryMode, CompressionType>> {
 public:
  DBWALTestWithParamsVaryingRecoveryMode()
      : DBWALTestBase("/db_wal_test_with_params_mode") {}
};

INSTANTIATE_TEST_CASE_P(
    Wal, DBWALTestWithParamsVaryingRecoveryMode,
    ::testing::Combine(
        ::testing::Bool(), ::testing::Range(0, 4, 1),
        ::testing::Range(RecoveryTestHelper::kWALFileOffset,
                         RecoveryTestHelper::kWALFileOffset +
                             RecoveryTestHelper::kWALFilesCount,
                         1),
        ::testing::Values(WALRecoveryMode::kTolerateCorruptedTailRecords,
                          WALRecoveryMode::kAbsoluteConsistency,
                          WALRecoveryMode::kPointInTimeRecovery,
                          WALRecoveryMode::kSkipAnyCorruptedRecords),
        ::testing::Values(CompressionType::kNoCompression,
                          CompressionType::kZSTD)));

// Test scope:
// - We expect to open the data store when there is incomplete trailing writes
// at the end of any of the logs
// - We do not expect to open the data store for corruption
TEST_P(DBWALTestWithParams, kTolerateCorruptedTailRecords) {
  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file

  // Fill data for testing
  Options options = CurrentOptions();
  options.track_and_verify_wals = std::get<4>(GetParam());
  const size_t row_count = RecoveryTestHelper::FillData(this, &options);
  // test checksum failure or parsing
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .3,
                                 /*len%=*/.1, wal_file_id, trunc);

  options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  if (trunc) {
    options.create_if_missing = false;
    ASSERT_OK(TryReopen(options));
    const size_t recovered_row_count = RecoveryTestHelper::GetData(this);
    ASSERT_TRUE(corrupt_offset == 0 || recovered_row_count > 0);
    ASSERT_LT(recovered_row_count, row_count);
  } else {
    ASSERT_NOK(TryReopen(options));
  }
}

// Test scope:
// We don't expect the data store to be opened if there is any corruption
// (leading, middle or trailing -- incomplete writes or corruption)
TEST_P(DBWALTestWithParams, kAbsoluteConsistency) {
  // Verify clean slate behavior
  Options options = CurrentOptions();
  options.track_and_verify_wals = std::get<4>(GetParam());
  const size_t row_count = RecoveryTestHelper::FillData(this, &options);
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(RecoveryTestHelper::GetData(this), row_count);

  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file
  // WAL compression type
  CompressionType compression_type = std::get<3>(GetParam());
  options.wal_compression = compression_type;

  if (trunc && corrupt_offset == 0) {
    return;
  }

  // fill with new date
  RecoveryTestHelper::FillData(this, &options);
  // corrupt the wal
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .33,
                                 /*len%=*/.1, wal_file_id, trunc);
  // verify
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.create_if_missing = false;
  ASSERT_NOK(TryReopen(options));
}

// Test scope:
// We don't expect the data store to be opened if there is any inconsistency
// between WAL and SST files
TEST_F(DBWALTest, kPointInTimeRecoveryCFConsistency) {
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;

  // Create DB with multiple column families.
  CreateAndReopenWithCF({"one", "two"}, options);
  ASSERT_OK(Put(1, "key1", "val1"));
  ASSERT_OK(Put(2, "key2", "val2"));

  // Record the offset at this point
  Env* env = options.env;
  uint64_t wal_file_id = dbfull()->TEST_LogfileNumber();
  std::string fname = LogFileName(dbname_, wal_file_id);
  uint64_t offset_to_corrupt;
  ASSERT_OK(env->GetFileSize(fname, &offset_to_corrupt));
  ASSERT_GT(offset_to_corrupt, 0);

  ASSERT_OK(Put(1, "key3", "val3"));
  // Corrupt WAL at location of key3
  ASSERT_OK(test::CorruptFile(env, fname, static_cast<int>(offset_to_corrupt),
                              4, false));
  ASSERT_OK(Put(2, "key4", "val4"));
  ASSERT_OK(Put(1, "key5", "val5"));
  ASSERT_OK(Flush(2));

  // PIT recovery & verify
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  ASSERT_NOK(TryReopenWithColumnFamilies({"default", "one", "two"}, options));
}

TEST_F(DBWALTest, RaceInstallFlushResultsWithWalObsoletion) {
  Options options = CurrentOptions();
  options.env = env_;
  options.track_and_verify_wals_in_manifest = true;
  // The following make sure there are two bg flush threads.
  options.max_background_jobs = 8;

  DestroyAndReopen(options);

  const std::string cf1_name("cf1");
  CreateAndReopenWithCF({cf1_name}, options);
  assert(handles_.size() == 2);

  {
    dbfull()->TEST_LockMutex();
    ASSERT_LE(2, dbfull()->GetBGJobLimits().max_flushes);
    dbfull()->TEST_UnlockMutex();
  }

  ASSERT_OK(dbfull()->PauseBackgroundWork());

  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "foo", "value"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "value"));

  ASSERT_OK(dbfull()->TEST_FlushMemTable(
      /*wait=*/false, /*allow_write_stall=*/true, handles_[1]));

  ASSERT_OK(db_->Put(WriteOptions(), "foo", "value"));

  ASSERT_OK(dbfull()->TEST_FlushMemTable(
      /*wait=*/false, /*allow_write_stall=*/true, handles_[0]));

  bool called = false;
  std::atomic<int> bg_flush_threads{0};
  std::atomic<bool> wal_synced{false};
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void* /*arg*/) {
        int cur = bg_flush_threads.load();
        int desired = cur + 1;
        if (cur > 0 ||
            !bg_flush_threads.compare_exchange_strong(cur, desired)) {
          while (!wal_synced.load()) {
            // Wait until the other bg flush thread finishes committing WAL sync
            // operation to the MANIFEST.
          }
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableToOutputFile:CommitWal:1",
      [&](void* /*arg*/) { wal_synced.store(true); });
  // This callback will be called when the first bg flush thread reaches the
  // point before entering the MANIFEST write queue after flushing the SST
  // file.
  // The purpose of the sync points here is to ensure both bg flush threads
  // finish computing `min_wal_number_to_keep` before any of them updates the
  // `log_number` for the column family that's being flushed.
  SyncPoint::GetInstance()->SetCallBack(
      "MemTableList::TryInstallMemtableFlushResults:AfterComputeMinWalToKeep",
      [&](void* /*arg*/) {
        dbfull()->mutex()->AssertHeld();
        if (!called) {
          // We are the first bg flush thread in the MANIFEST write queue.
          // We set up the dependency between sync points for two threads that
          // will be executing the same code.
          // For the interleaving of events, see
          // https://github.com/meta/rocksdb/pull/9715.
          // bg flush thread1 will release the db mutex while in the MANIFEST
          // write queue. In the meantime, bg flush thread2 locks db mutex and
          // computes the min_wal_number_to_keep (before thread1 writes to
          // MANIFEST thus before cf1->log_number is updated). Bg thread2 joins
          // the MANIFEST write queue afterwards and bg flush thread1 proceeds
          // with writing to MANIFEST.
          called = true;
          SyncPoint::GetInstance()->LoadDependency({
              {"VersionSet::LogAndApply:WriteManifestStart",
               "DBWALTest::RaceInstallFlushResultsWithWalObsoletion:BgFlush2"},
              {"DBWALTest::RaceInstallFlushResultsWithWalObsoletion:BgFlush2",
               "VersionSet::LogAndApply:WriteManifest"},
          });
        } else {
          // The other bg flush thread has already been in the MANIFEST write
          // queue, and we are after.
          TEST_SYNC_POINT(
              "DBWALTest::RaceInstallFlushResultsWithWalObsoletion:BgFlush2");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->ContinueBackgroundWork());

  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));

  ASSERT_TRUE(called);

  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  std::unique_ptr<DB> db1;
  Status s = DB::OpenForReadOnly(options, dbname_, &db1);
  ASSERT_OK(s);
  assert(db1);
}

TEST_F(DBWALTest, FixSyncWalOnObseletedWalWithNewManifestCausingMissingWAL) {
  Options options = CurrentOptions();
  // Small size to force manifest creation
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  options.track_and_verify_wals_in_manifest = true;
  DestroyAndReopen(options);

  // Accumulate memtable m1 and create the 1st wal (i.e, 4.log)
  ASSERT_OK(Put(Key(1), ""));
  ASSERT_OK(Put(Key(2), ""));
  ASSERT_OK(Put(Key(3), ""));

  const std::string wal_file_path = db_->GetName() + "/000004.log";

  // Coerce the following sequence of events:
  // (1) Flush() marks 4.log to be obsoleted, 8.log to be the latest (i.e,
  // active) log and release the lock
  // (2) SyncWAL() proceeds with the lock. It
  // creates a new manifest and syncs all the inactive wals before the latest
  // (i.e, active log), which is 4.log. Note that SyncWAL() is not aware of the
  // fact that 4.log has marked as to be obseleted. Such wal
  // sync will then add a WAL addition record of 4.log to the new manifest
  // without any special treatment. Prior to the fix, there is no WAL deletion
  // record to offset it. (3) BackgroundFlush() will eventually purge 4.log.

  bool wal_synced = false;
  SyncPoint::GetInstance()->SetCallBack(
      "FindObsoleteFiles::PostMutexUnlock", [&](void*) {
        ASSERT_OK(env_->FileExists(wal_file_path));
        uint64_t pre_sync_wal_manifest_no =
            dbfull()->TEST_Current_Manifest_FileNo();
        ASSERT_OK(db_->SyncWAL());
        uint64_t post_sync_wal_manifest_no =
            dbfull()->TEST_Current_Manifest_FileNo();
        bool new_manifest_created =
            post_sync_wal_manifest_no == pre_sync_wal_manifest_no + 1;
        ASSERT_TRUE(new_manifest_created);
        wal_synced = true;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

  ASSERT_TRUE(wal_synced);
  // BackgroundFlush() purged 4.log
  // because the memtable associated with the WAL was flushed and new WAL was
  // created (i.e, 8.log)
  ASSERT_TRUE(env_->FileExists(wal_file_path).IsNotFound());

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // To verify the corruption of "Missing WAL with log number: 4" under
  // `options.track_and_verify_wals_in_manifest = true` is fixed.
  //
  // Before the fix, `db_->SyncWAL()` will sync and record WAL addtion of the
  // obseleted WAL 4.log in a new manifest without any special treament.
  // This will result in missing-wal corruption in DB::Reopen().
  Status s = TryReopen(options);
  EXPECT_OK(s);
}

// Test scope:
// - We expect to open data store under all circumstances
// - We expect only data upto the point where the first error was encountered
TEST_P(DBWALTestWithParams, kPointInTimeRecovery) {
  const int maxkeys =
      RecoveryTestHelper::kWALFilesCount * RecoveryTestHelper::kKeysPerWALFile;

  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file
  // WAL compression type
  CompressionType compression_type = std::get<3>(GetParam());

  // Fill data for testing
  Options options = CurrentOptions();
  options.track_and_verify_wals = std::get<4>(GetParam());
  options.wal_compression = compression_type;
  const size_t row_count = RecoveryTestHelper::FillData(this, &options);

  // Corrupt the wal
  // The offset here was 0.3 which cuts off right at the end of a
  // valid fragment after wal zstd compression checksum is enabled,
  // so changed the value to 0.33.
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .33,
                                 /*len%=*/.1, wal_file_id, trunc);

  // Verify
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));

  // Probe data for invariants
  size_t recovered_row_count = RecoveryTestHelper::GetData(this);
  ASSERT_LT(recovered_row_count, row_count);

  // Verify a prefix of keys were recovered. But not in the case of full WAL
  // truncation, because we have no way to know there was a corruption when
  // truncation happened on record boundaries (preventing recovery holes in
  // that case requires using `track_and_verify_wals_in_manifest`).
  if (!trunc || corrupt_offset != 0) {
    bool expect_data = true;
    for (size_t k = 0; k < maxkeys; ++k) {
      bool found = Get("key" + std::to_string(k)) != "NOT_FOUND";
      if (expect_data && !found) {
        expect_data = false;
      }
      ASSERT_EQ(found, expect_data);
    }
  }

  const size_t min = RecoveryTestHelper::kKeysPerWALFile *
                     (wal_file_id - RecoveryTestHelper::kWALFileOffset);
  ASSERT_GE(recovered_row_count, min);
  if (!trunc && corrupt_offset != 0) {
    const size_t max = RecoveryTestHelper::kKeysPerWALFile *
                       (wal_file_id - RecoveryTestHelper::kWALFileOffset + 1);
    ASSERT_LE(recovered_row_count, max);
  }
}

// Test scope:
// - We expect to open the data store under all scenarios
// - We expect to have recovered records past the corruption zone
TEST_P(DBWALTestWithParams, kSkipAnyCorruptedRecords) {
  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file
  // WAL compression type
  CompressionType compression_type = std::get<3>(GetParam());

  // Fill data for testing
  Options options = CurrentOptions();
  options.track_and_verify_wals = std::get<4>(GetParam());
  options.wal_compression = compression_type;
  const size_t row_count = RecoveryTestHelper::FillData(this, &options);

  // Corrupt the WAL
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .3,
                                 /*len%=*/.1, wal_file_id, trunc);

  // Verify behavior
  options.wal_recovery_mode = WALRecoveryMode::kSkipAnyCorruptedRecords;
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));

  // Probe data for invariants
  size_t recovered_row_count = RecoveryTestHelper::GetData(this);
  ASSERT_LT(recovered_row_count, row_count);

  if (!trunc) {
    ASSERT_TRUE(corrupt_offset != 0 || recovered_row_count > 0);
  }
}

TEST_F(DBWALTest, AvoidFlushDuringRecovery) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.avoid_flush_during_recovery = false;

  // Test with flush after recovery.
  Reopen(options);
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "v3"));
  ASSERT_OK(Put("bar", "v4"));
  ASSERT_EQ(1, TotalTableFiles());
  // Reopen DB. Check if WAL logs flushed.
  Reopen(options);
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v4", Get("bar"));
  ASSERT_EQ(2, TotalTableFiles());

  // Test without flush after recovery.
  options.avoid_flush_during_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v5"));
  ASSERT_OK(Put("bar", "v6"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "v7"));
  ASSERT_OK(Put("bar", "v8"));
  ASSERT_EQ(1, TotalTableFiles());
  // Reopen DB. WAL logs should not be flushed this time.
  Reopen(options);
  ASSERT_EQ("v7", Get("foo"));
  ASSERT_EQ("v8", Get("bar"));
  ASSERT_EQ(1, TotalTableFiles());

  // Force flush with allow_2pc.
  options.avoid_flush_during_recovery = true;
  options.allow_2pc = true;
  ASSERT_OK(Put("foo", "v9"));
  ASSERT_OK(Put("bar", "v10"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "v11"));
  ASSERT_OK(Put("bar", "v12"));
  Reopen(options);
  ASSERT_EQ("v11", Get("foo"));
  ASSERT_EQ("v12", Get("bar"));
  ASSERT_EQ(3, TotalTableFiles());
}

TEST_F(DBWALTest, WalCleanupAfterAvoidFlushDuringRecovery) {
  // Verifies WAL files that were present during recovery, but not flushed due
  // to avoid_flush_during_recovery, will be considered for deletion at a later
  // stage. We check at least one such file is deleted during Flush().
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.avoid_flush_during_recovery = true;
  Reopen(options);

  ASSERT_OK(Put("foo", "v1"));
  Reopen(options);
  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      // Flush() triggers deletion of obsolete tracked files
      ASSERT_OK(Flush());
    }
    VectorLogPtr log_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
    if (i == 0) {
      ASSERT_GT(log_files.size(), 0);
    } else {
      ASSERT_EQ(0, log_files.size());
    }
  }
}

TEST_F(DBWALTest, RecoverWithoutFlush) {
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.create_if_missing = false;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 64 * 1024 * 1024;

  size_t count = RecoveryTestHelper::FillData(this, &options);
  auto validateData = [this, count]() {
    for (size_t i = 0; i < count; i++) {
      ASSERT_NE(Get("key" + std::to_string(i)), "NOT_FOUND");
    }
  };
  Reopen(options);
  validateData();
  // Insert some data without flush
  ASSERT_OK(Put("foo", "foo_v1"));
  ASSERT_OK(Put("bar", "bar_v1"));
  Reopen(options);
  validateData();
  ASSERT_EQ(Get("foo"), "foo_v1");
  ASSERT_EQ(Get("bar"), "bar_v1");
  // Insert again and reopen
  ASSERT_OK(Put("foo", "foo_v2"));
  ASSERT_OK(Put("bar", "bar_v2"));
  Reopen(options);
  validateData();
  ASSERT_EQ(Get("foo"), "foo_v2");
  ASSERT_EQ(Get("bar"), "bar_v2");
  // manual flush and insert again
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("foo"), "foo_v2");
  ASSERT_EQ(Get("bar"), "bar_v2");
  ASSERT_OK(Put("foo", "foo_v3"));
  ASSERT_OK(Put("bar", "bar_v3"));
  Reopen(options);
  validateData();
  ASSERT_EQ(Get("foo"), "foo_v3");
  ASSERT_EQ(Get("bar"), "bar_v3");
}

TEST_F(DBWALTest, RecoverWithoutFlushMultipleCF) {
  const std::string kSmallValue = "v";
  const std::string kLargeValue = DummyString(1024);
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.create_if_missing = false;
  options.disable_auto_compactions = true;

  auto countWalFiles = [this]() {
    VectorLogPtr log_files;
    if (!dbfull()->GetSortedWalFiles(log_files).ok()) {
      return size_t{0};
    }
    return log_files.size();
  };

  // Create DB with multiple column families and multiple log files.
  CreateAndReopenWithCF({"one", "two"}, options);
  ASSERT_OK(Put(0, "key1", kSmallValue));
  ASSERT_OK(Put(1, "key2", kLargeValue));
  ASSERT_OK(Flush(1));
  ASSERT_EQ(1, countWalFiles());
  ASSERT_OK(Put(0, "key3", kSmallValue));
  ASSERT_OK(Put(2, "key4", kLargeValue));
  ASSERT_OK(Flush(2));
  ASSERT_EQ(2, countWalFiles());

  // Reopen, insert and flush.
  options.db_write_buffer_size = 64 * 1024 * 1024;
  ReopenWithColumnFamilies({"default", "one", "two"}, options);
  ASSERT_EQ(Get(0, "key1"), kSmallValue);
  ASSERT_EQ(Get(1, "key2"), kLargeValue);
  ASSERT_EQ(Get(0, "key3"), kSmallValue);
  ASSERT_EQ(Get(2, "key4"), kLargeValue);
  // Insert more data.
  ASSERT_OK(Put(0, "key5", kLargeValue));
  ASSERT_OK(Put(1, "key6", kLargeValue));
  ASSERT_EQ(3, countWalFiles());
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(2, "key7", kLargeValue));
  ASSERT_OK(dbfull()->FlushWAL(false));
  ASSERT_EQ(4, countWalFiles());

  // Reopen twice and validate.
  for (int i = 0; i < 2; i++) {
    ReopenWithColumnFamilies({"default", "one", "two"}, options);
    ASSERT_EQ(Get(0, "key1"), kSmallValue);
    ASSERT_EQ(Get(1, "key2"), kLargeValue);
    ASSERT_EQ(Get(0, "key3"), kSmallValue);
    ASSERT_EQ(Get(2, "key4"), kLargeValue);
    ASSERT_EQ(Get(0, "key5"), kLargeValue);
    ASSERT_EQ(Get(1, "key6"), kLargeValue);
    ASSERT_EQ(Get(2, "key7"), kLargeValue);
    ASSERT_EQ(4, countWalFiles());
  }
}

// In this test we are trying to do the following:
//   1. Create a DB with corrupted WAL log;
//   2. Open with avoid_flush_during_recovery = true;
//   3. Append more data without flushing, which creates new WAL log.
//   4. Open again. See if it can correctly handle previous corruption.
TEST_P(DBWALTestWithParamsVaryingRecoveryMode,
       RecoverFromCorruptedWALWithoutFlush) {
  const int kAppendKeys = 100;
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.create_if_missing = false;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 64 * 1024 * 1024;

  auto getAll = [this]() {
    std::vector<std::pair<std::string, std::string>> data;
    ReadOptions ropt;
    Iterator* iter = dbfull()->NewIterator(ropt);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      data.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());
    delete iter;
    return data;
  };

  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file
  WALRecoveryMode recovery_mode = std::get<3>(GetParam());
  // WAL compression type
  CompressionType compression_type = std::get<4>(GetParam());

  options.wal_recovery_mode = recovery_mode;
  options.wal_compression = compression_type;
  // Create corrupted WAL
  RecoveryTestHelper::FillData(this, &options);
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .3,
                                 /*len%=*/.1, wal_file_id, trunc);
  // Skip the test if DB won't open.
  if (!TryReopen(options).ok()) {
    ASSERT_TRUE(options.wal_recovery_mode ==
                    WALRecoveryMode::kAbsoluteConsistency ||
                (!trunc && options.wal_recovery_mode ==
                               WALRecoveryMode::kTolerateCorruptedTailRecords));
    return;
  }
  ASSERT_OK(TryReopen(options));
  // Append some more data.
  for (int k = 0; k < kAppendKeys; k++) {
    std::string key = "extra_key" + std::to_string(k);
    std::string value = DummyString(RecoveryTestHelper::kValueSize);
    ASSERT_OK(Put(key, value));
  }
  // Save data for comparison.
  auto data = getAll();
  // Reopen. Verify data.
  ASSERT_OK(TryReopen(options));
  auto actual_data = getAll();
  ASSERT_EQ(data, actual_data);
}

// Tests that total log size is recovered if we set
// avoid_flush_during_recovery=true.
// Flush should trigger if max_total_wal_size is reached.
TEST_F(DBWALTest, RestoreTotalLogSizeAfterRecoverWithoutFlush) {
  auto test_listener = std::make_shared<FlushCounterListener>();
  test_listener->expected_flush_reason = FlushReason::kWalFull;

  constexpr size_t kKB = 1024;
  constexpr size_t kMB = 1024 * 1024;
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.max_total_wal_size = 1 * kMB;
  options.listeners.push_back(test_listener);
  // Have to open DB in multi-CF mode to trigger flush when
  // max_total_wal_size is reached.
  CreateAndReopenWithCF({"one"}, options);
  // Write some keys and we will end up with one log file which is slightly
  // smaller than 1MB.
  std::string value_100k(100 * kKB, 'v');
  std::string value_300k(300 * kKB, 'v');
  ASSERT_OK(Put(0, "foo", "v1"));
  for (int i = 0; i < 9; i++) {
    ASSERT_OK(Put(1, "key" + std::to_string(i), value_100k));
  }
  // Get log files before reopen.
  VectorLogPtr log_files_before;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_before));
  ASSERT_EQ(1, log_files_before.size());
  uint64_t log_size_before = log_files_before[0]->SizeFileBytes();
  ASSERT_GT(log_size_before, 900 * kKB);
  ASSERT_LT(log_size_before, 1 * kMB);
  ReopenWithColumnFamilies({"default", "one"}, options);
  // Write one more value to make log larger than 1MB.
  ASSERT_OK(Put(1, "bar", value_300k));
  // Get log files again. A new log file will be opened.
  VectorLogPtr log_files_after_reopen;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_after_reopen));
  ASSERT_EQ(2, log_files_after_reopen.size());
  ASSERT_EQ(log_files_before[0]->LogNumber(),
            log_files_after_reopen[0]->LogNumber());
  ASSERT_GT(log_files_after_reopen[0]->SizeFileBytes() +
                log_files_after_reopen[1]->SizeFileBytes(),
            1 * kMB);
  // Write one more key to trigger flush.
  ASSERT_OK(Put(0, "foo", "v2"));
  for (auto* h : handles_) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(h));
  }
  // Flushed two column families.
  ASSERT_EQ(2, test_listener->count.load());
}

#if defined(ROCKSDB_PLATFORM_POSIX)
#if defined(ROCKSDB_FALLOCATE_PRESENT)
// Tests that we will truncate the preallocated space of the last log from
// previous.
TEST_F(DBWALTest, TruncateLastLogAfterRecoverWithoutFlush) {
  constexpr size_t kKB = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.avoid_flush_during_recovery = true;
  if (mem_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem environment");
    return;
  }
  if (!IsFallocateSupported()) {
    return;
  }

  DestroyAndReopen(options);
  size_t preallocated_size =
      dbfull()->TEST_GetWalPreallocateBlockSize(options.write_buffer_size);
  ASSERT_OK(Put("foo", "v1"));
  VectorLogPtr log_files_before;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_before));
  ASSERT_EQ(1, log_files_before.size());
  auto& file_before = log_files_before[0];
  ASSERT_LT(file_before->SizeFileBytes(), 1 * kKB);
  // The log file has preallocated space.
  {
    std::string fname = dbname_ + file_before->PathName();
    uint64_t allocated = GetAllocatedFileSize(fname);
    if (ShouldSkipAllocationCheck(fname)) {
      fprintf(stderr,
              "Skipping preallocation check on this filesystem for %s\n",
              fname.c_str());
    } else if (allocated < preallocated_size) {
      fprintf(stderr,
              "Warning: allocated size (%lu) less than preallocated size (%lu) "
              "for %s, skipping check. This may indicate the filesystem does "
              "not support preallocation or does not report it.\n",
              allocated, preallocated_size, fname.c_str());
    } else {
      ASSERT_GE(allocated, preallocated_size);
    }
  }
  Reopen(options);
  VectorLogPtr log_files_after;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_after));
  ASSERT_EQ(1, log_files_after.size());
  ASSERT_LT(log_files_after[0]->SizeFileBytes(), 1 * kKB);
  // The preallocated space should be truncated.
  {
    std::string fname = dbname_ + file_before->PathName();
    if (!ShouldSkipAllocationCheck(fname)) {
      ASSERT_LT(GetAllocatedFileSize(fname), preallocated_size);
    }
  }
}
// Tests that we will truncate the preallocated space of the last log from
// previous.
TEST_F(DBWALTest, TruncateLastLogAfterRecoverWithFlush) {
  constexpr size_t kKB = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.avoid_flush_during_recovery = false;
  options.avoid_flush_during_shutdown = true;
  if (mem_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem environment");
    return;
  }
  if (!IsFallocateSupported()) {
    return;
  }

  DestroyAndReopen(options);
  size_t preallocated_size =
      dbfull()->TEST_GetWalPreallocateBlockSize(options.write_buffer_size);
  ASSERT_OK(Put("foo", "v1"));
  VectorLogPtr log_files_before;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_before));
  ASSERT_EQ(1, log_files_before.size());
  auto& file_before = log_files_before[0];
  ASSERT_LT(file_before->SizeFileBytes(), 1 * kKB);
  {
    std::string fname = dbname_ + file_before->PathName();
    uint64_t allocated = GetAllocatedFileSize(fname);
    if (ShouldSkipAllocationCheck(fname)) {
      fprintf(stderr,
              "Skipping preallocation check on this filesystem for %s\n",
              fname.c_str());
    } else if (allocated < preallocated_size) {
      fprintf(stderr,
              "Warning: allocated size (%lu) less than preallocated size (%lu) "
              "for %s, skipping check. This may indicate the filesystem does "
              "not support preallocation or does not report it.\n",
              allocated, preallocated_size, fname.c_str());
    } else {
      ASSERT_GE(allocated, preallocated_size);
    }
  }
  // The log file has preallocated space.
  Close();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::PurgeObsoleteFiles:Begin",
        "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterRecover"},
       {"DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterTruncate",
        "DBImpl::DeleteObsoleteFileImpl::BeforeDeletion"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  port::Thread reopen_thread([&]() { Reopen(options); });

  TEST_SYNC_POINT(
      "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterRecover");
  // After the flush during Open, the log file should get deleted.  However,
  // if  the process is in a crash loop, the log file may not get
  // deleted and thte preallocated space will keep accumulating. So we need
  // to ensure it gets trtuncated.
  {
    std::string fname = dbname_ + file_before->PathName();
    if (!ShouldSkipAllocationCheck(fname)) {
      EXPECT_LT(GetAllocatedFileSize(fname), preallocated_size);
    }
  }
  TEST_SYNC_POINT(
      "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterTruncate");
  reopen_thread.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBWALTest, TruncateLastLogAfterRecoverWALEmpty) {
  Options options = CurrentOptions();
  options.env = env_;
  options.avoid_flush_during_recovery = false;
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem/non-encrypted  environment");
    return;
  }
  if (!IsFallocateSupported()) {
    return;
  }

  DestroyAndReopen(options);
  size_t preallocated_size =
      dbfull()->TEST_GetWalPreallocateBlockSize(options.write_buffer_size);
  Close();
  std::vector<std::string> filenames;
  std::string last_log;
  uint64_t last_log_num = 0;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  for (const auto& fname : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(fname, &number, &type, nullptr)) {
      if (type == kWalFile && number > last_log_num) {
        last_log = fname;
      }
    }
  }
  ASSERT_NE(last_log, "");
  last_log = dbname_ + '/' + last_log;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::PurgeObsoleteFiles:Begin",
        "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterRecover"},
       {"DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterTruncate",
        "DBImpl::DeleteObsoleteFileImpl::BeforeDeletion"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PosixWritableFile::Close",
      [](void* arg) { *(static_cast<size_t*>(arg)) = 0; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Preallocate space for the empty log file. This could happen if WAL data
  // was buffered in memory and the process crashed.
  std::unique_ptr<WritableFile> log_file;
  ASSERT_OK(env_->ReopenWritableFile(last_log, &log_file, EnvOptions()));
  log_file->SetPreallocationBlockSize(preallocated_size);
  log_file->PrepareWrite(0, 4096);
  log_file.reset();

  {
    uint64_t allocated = GetAllocatedFileSize(last_log);
    if (ShouldSkipAllocationCheck(last_log)) {
      fprintf(stderr,
              "Skipping preallocation check on this filesystem for %s\n",
              last_log.c_str());
    } else if (allocated < preallocated_size) {
      fprintf(stderr,
              "Warning: allocated size (%lu) less than preallocated size (%lu) "
              "for %s, skipping check. This may indicate the filesystem does "
              "not support preallocation or does not report it.\n",
              allocated, preallocated_size, last_log.c_str());
    } else {
      ASSERT_GE(allocated, preallocated_size);
    }
  }

  port::Thread reopen_thread([&]() { Reopen(options); });

  TEST_SYNC_POINT(
      "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterRecover");
  // The preallocated space should be truncated.
  if (!ShouldSkipAllocationCheck(last_log)) {
    EXPECT_LT(GetAllocatedFileSize(last_log), preallocated_size);
  }
  TEST_SYNC_POINT(
      "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterTruncate");
  reopen_thread.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBWALTest, ReadOnlyRecoveryNoTruncate) {
  constexpr size_t kKB = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.avoid_flush_during_recovery = true;
  if (mem_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem environment");
    return;
  }
  if (!IsFallocateSupported()) {
    return;
  }

  // create DB and close with file truncate disabled
  std::atomic_bool enable_truncate{false};

  SyncPoint::GetInstance()->SetCallBack("PosixWritableFile::Close",
                                        [&](void* arg) {
                                          if (!enable_truncate) {
                                            *(static_cast<size_t*>(arg)) = 0;
                                          }
                                        });
  SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);
  size_t preallocated_size =
      dbfull()->TEST_GetWalPreallocateBlockSize(options.write_buffer_size);
  ASSERT_OK(Put("foo", "v1"));
  VectorLogPtr log_files_before;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_before));
  ASSERT_EQ(1, log_files_before.size());
  auto& file_before = log_files_before[0];
  ASSERT_LT(file_before->SizeFileBytes(), 1 * kKB);
  // The log file has preallocated space.
  std::string fname = dbname_ + file_before->PathName();
  auto db_size = GetAllocatedFileSize(fname);
  {
    if (ShouldSkipAllocationCheck(fname)) {
      fprintf(stderr,
              "Skipping preallocation check on this filesystem for %s\n",
              fname.c_str());
    } else if (db_size < preallocated_size) {
      fprintf(stderr,
              "Warning: allocated size (%lu) less than preallocated size (%lu) "
              "for %s, skipping check. This may indicate the filesystem does "
              "not support preallocation or does not report it.\n",
              db_size, preallocated_size, fname.c_str());
    } else {
      ASSERT_GE(db_size, preallocated_size);
    }
  }
  Close();

  // enable truncate and open DB as readonly, the file should not be truncated
  // and DB size is not changed.
  enable_truncate = true;
  ASSERT_OK(ReadOnlyReopen(options));
  VectorLogPtr log_files_after;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_after));
  ASSERT_EQ(1, log_files_after.size());
  ASSERT_LT(log_files_after[0]->SizeFileBytes(), 1 * kKB);
  ASSERT_EQ(log_files_after[0]->PathName(), file_before->PathName());
  // The preallocated space should NOT be truncated.
  // the DB size is almost the same.
  {
    std::string fname2 = dbname_ + file_before->PathName();
    if (!ShouldSkipAllocationCheck(fname2)) {
      ASSERT_NEAR(GetAllocatedFileSize(fname2), db_size, db_size / 100);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // ROCKSDB_FALLOCATE_PRESENT
#endif  // ROCKSDB_PLATFORM_POSIX

// Reports a configurable preallocation capability and counts
// ReopenWritableFile() calls on WAL files.
class PreallocationTestFs : public FileSystemWrapper {
 public:
  PreallocationTestFs(const std::shared_ptr<FileSystem>& target,
                      bool preallocation_supported)
      : FileSystemWrapper(target),
        preallocation_supported_(preallocation_supported) {}

  const char* Name() const override { return "PreallocationTestFs"; }

  bool IsPreallocationSupported() override { return preallocation_supported_; }

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    uint64_t number;
    FileType type;
    if (ParseFileName(fname.substr(fname.find_last_of('/') + 1), &number,
                      &type) &&
        type == kWalFile) {
      wal_reopen_count_.FetchAddRelaxed(1);
    }
    return target()->ReopenWritableFile(fname, file_opts, result, dbg);
  }

  void SetPreallocationSupported(bool supported) {
    preallocation_supported_ = supported;
  }

  int GetWalReopenCount() const { return wal_reopen_count_.LoadRelaxed(); }

  void ResetWalReopenCount() { wal_reopen_count_.StoreRelaxed(0); }

 private:
  bool preallocation_supported_;
  RelaxedAtomic<int> wal_reopen_count_{0};
};

// Tests that the last WAL is reopened for truncation during recovery only if
// the FileSystem reports that it supports preallocation.
TEST_F(DBWALTest, SkipWalTruncateIfPreallocationNotSupported) {
  // The Env must outlive the loop since DestroyAndReopen() destroys the
  // previous DB using the previous iteration's options.
  auto test_fs = std::make_shared<PreallocationTestFs>(
      env_->GetFileSystem(), /*preallocation_supported=*/true);
  std::unique_ptr<Env> test_env(NewCompositeEnv(test_fs));
  for (bool preallocation_supported : {true, false}) {
    // avoid_flush_during_recovery = true restores the WAL as alive, while
    // false flushes the recovered data and leaves the WAL obsolete.
    for (bool avoid_flush_during_recovery : {true, false}) {
      SCOPED_TRACE(
          "preallocation_supported=" + std::to_string(preallocation_supported) +
          " avoid_flush_during_recovery=" +
          std::to_string(avoid_flush_during_recovery));
      Options options = CurrentOptions();
      options.env = test_env.get();
      options.avoid_flush_during_recovery = avoid_flush_during_recovery;
      options.avoid_flush_during_shutdown = true;
      test_fs->SetPreallocationSupported(preallocation_supported);
      DestroyAndReopen(options);
      ASSERT_OK(Put("foo", "v1"));
      Close();

      test_fs->ResetWalReopenCount();
      Reopen(options);
      ASSERT_EQ(preallocation_supported ? 1 : 0, test_fs->GetWalReopenCount());
      ASSERT_EQ("v1", Get("foo"));
      Close();
    }
  }
}

// Tests that a read-only open never reopens a WAL for truncation, even if the
// FileSystem supports preallocation.
TEST_F(DBWALTest, ReadOnlyOpenSkipsWalTruncate) {
  Options options = CurrentOptions();
  auto test_fs = std::make_shared<PreallocationTestFs>(
      env_->GetFileSystem(), /*preallocation_supported=*/true);
  std::unique_ptr<Env> test_env(NewCompositeEnv(test_fs));
  options.env = test_env.get();
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1"));
  Close();

  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v1", Get("foo"));
  Close();
  ASSERT_EQ(0, test_fs->GetWalReopenCount());

  // The same WAL is truncated by a read-write open.
  Reopen(options);
  ASSERT_EQ(1, test_fs->GetWalReopenCount());
  Close();
}

TEST_F(DBWALTest, WalInManifestButNotInSortedWals) {
  Options options = CurrentOptions();
  options.track_and_verify_wals_in_manifest = true;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;

  // Build a way to make wal files selectively go missing
  bool wals_go_missing = false;
  struct MissingWalFs : public FileSystemWrapper {
    MissingWalFs(const std::shared_ptr<FileSystem>& t,
                 bool* _wals_go_missing_flag)
        : FileSystemWrapper(t), wals_go_missing_flag(_wals_go_missing_flag) {}
    bool* wals_go_missing_flag;
    IOStatus GetChildren(const std::string& dir, const IOOptions& io_opts,
                         std::vector<std::string>* r,
                         IODebugContext* dbg) override {
      IOStatus s = target_->GetChildren(dir, io_opts, r, dbg);
      if (s.ok() && *wals_go_missing_flag) {
        for (size_t i = 0; i < r->size();) {
          if (EndsWith(r->at(i), ".log")) {
            r->erase(r->begin() + i);
          } else {
            ++i;
          }
        }
      }
      return s;
    }
    const char* Name() const override { return "MissingWalFs"; }
  };
  auto my_fs =
      std::make_shared<MissingWalFs>(env_->GetFileSystem(), &wals_go_missing);
  std::unique_ptr<Env> my_env(NewCompositeEnv(my_fs));
  options.env = my_env.get();

  CreateAndReopenWithCF({"blah"}, options);

  // Currently necessary to get a WAL tracked in manifest; see
  // https://github.com/meta/rocksdb/issues/10080
  ASSERT_OK(Put(0, "x", "y"));
  ASSERT_OK(db_->SyncWAL());
  ASSERT_OK(Put(1, "x", "y"));
  ASSERT_OK(db_->SyncWAL());
  ASSERT_OK(Flush(1));

  ASSERT_FALSE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());
  std::vector<std::unique_ptr<LogFile>> wals;
  ASSERT_OK(db_->GetSortedWalFiles(wals));
  wals_go_missing = true;
  ASSERT_NOK(db_->GetSortedWalFiles(wals));
  wals_go_missing = false;
  Close();
}

TEST_F(DBWALTest, WalTermTest) {
  Options options = CurrentOptions();
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));

  WriteOptions wo;
  wo.sync = true;
  wo.disableWAL = false;

  WriteBatch batch;
  ASSERT_OK(batch.Put("foo", "bar"));
  batch.MarkWalTerminationPoint();
  ASSERT_OK(batch.Put("foo2", "bar2"));

  ASSERT_OK(dbfull()->Write(wo, &batch));

  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
  ASSERT_EQ("bar", Get(1, "foo"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo2"));
}

TEST_F(DBWALTest, GetCompressedWalsAfterSync) {
  if (db_->GetOptions().wal_compression == kNoCompression) {
    ROCKSDB_GTEST_BYPASS("stream compression not present");
    return;
  }
  Options options = GetDefaultOptions();
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.create_if_missing = true;
  options.env = env_;
  options.avoid_flush_during_recovery = true;
  options.track_and_verify_wals_in_manifest = true;
  // Enable WAL compression so that the newly-created WAL will be non-empty
  // after DB open, even if point-in-time WAL recovery encounters no
  // corruption.
  options.wal_compression = kZSTD;
  DestroyAndReopen(options);

  // Write something to memtable and WAL so that wal_empty_ will be false after
  // next DB::Open().
  ASSERT_OK(Put("a", "v"));

  Reopen(options);

  // New WAL is created, thanks to !wal_empty_.
  ASSERT_OK(dbfull()->TEST_SwitchWAL());

  ASSERT_OK(Put("b", "v"));

  ASSERT_OK(db_->SyncWAL());

  VectorLogPtr wals;
  Status s = dbfull()->GetSortedWalFiles(wals);
  ASSERT_OK(s);
}

TEST_F(DBWALTest, EmptyWalReopenTest) {
  Options options = CurrentOptions();
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);

  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));

  {
    std::vector<std::string> files;
    int num_wal_files = 0;
    ASSERT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& file : files) {
      uint64_t number = 0;
      FileType type = kWalFile;
      if (ParseFileName(file, &number, &type) && type == kWalFile) {
        num_wal_files++;
      }
    }

    ASSERT_EQ(num_wal_files, 1);
  }
}

TEST_F(DBWALTest, AsyncWalPrecreateWaitsForPendingFile) {
  Options options = CurrentOptions();
  options.env = env_;
  options.statistics = CreateDBStatistics();
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  // Goal: cover the race where foreground WAL rotation reaches the switch
  // before the background precreate publishes its result. The sync points hold
  // the async worker after it has created the file but before it marks it
  // ready, then verify the foreground writer waits instead of allocating a
  // higher WAL number.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:BeforePublish",
       "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:WorkerBlocked"},
      {"DBWALTest::AsyncWalPrecreateWaitsForPendingFile:AllowPublish",
       "DBImpl::BGWorkAsyncWALPrecreate:Publish"},
      {"DBImpl::WaitForAsyncWALPrecreate:BeforeWait",
       "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:ForegroundWaiting"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:WorkerBlocked");

  ASSERT_OK(Put("fill", "value"));
  Status writer_status;
  port::Thread writer(
      [&]() { writer_status = dbfull()->TEST_SwitchMemtable(); });

  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:ForegroundWaiting");
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:AllowPublish");
  writer.join();
  ASSERT_OK(writer_status);
  ASSERT_OK(Put("trigger", "value"));
  ASSERT_EQ("value", Get("trigger"));
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_WAITED));
  ASSERT_EQ(0, options.statistics->getTickerCount(WAL_PRECREATE_MISS));
}

TEST_F(DBWALTest, AsyncWalPrecreateConsumesReadyFile) {
  Options options = CurrentOptions();
  options.env = env_;
  options.statistics = CreateDBStatistics();
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  // Goal: exercise the no-wait consumption path. The background task publishes
  // a prepared future WAL before the foreground memtable switch, and the switch
  // must install exactly that reserved WAL number instead of allocating
  // another.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::AsyncWalPrecreateConsumesReadyFile:PrecreateDone"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateConsumesReadyFile:PrecreateDone");

  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_GE(wal_numbers.size(), 2);
  const uint64_t prepared_wal_number = wal_numbers.back();
  ASSERT_NE(prepared_wal_number, dbfull()->TEST_GetCurrentLogNumber());

  ASSERT_OK(Put("fill", "value"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_EQ(prepared_wal_number, dbfull()->TEST_GetCurrentLogNumber());
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
  ASSERT_EQ(0, options.statistics->getTickerCount(WAL_PRECREATE_MISS));
  ASSERT_EQ(0, options.statistics->getTickerCount(WAL_PRECREATE_WAITED));
}

TEST_F(DBWALTest, AsyncWalPrecreateRecoveryToleratesActualFutureWal) {
  Options options = CurrentOptions();
  options.env = env_;
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.track_and_verify_wals = true;
  options.track_and_verify_wals_in_manifest = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;

  // Goal: cover the actual async precreation path, not a hand-forged empty WAL.
  // The background task opens the future WAL, clean close releases the
  // unpublished writer and leaves the empty future WAL behind, and recovery
  // must tolerate that file while replaying the preceding WAL with real user
  // data.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::AsyncWalPrecreateRecoveryToleratesActualFutureWal:"
       "PrecreateDone"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  DestroyAndReopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateRecoveryToleratesActualFutureWal:"
      "PrecreateDone");

  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_GE(wal_numbers.size(), 2);
  const uint64_t future_wal_number = wal_numbers.back();
  uint64_t future_wal_size = 0;
  ASSERT_OK(env_->GetFileSize(LogFileName(dbname_, future_wal_number),
                              &future_wal_size));
  ASSERT_EQ(0, future_wal_size);

  ASSERT_OK(Put("key", "value"));
  Close();

  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("value", Get("key"));
  ASSERT_OK(Put("after-recovery", "value2"));
  ASSERT_EQ("value2", Get("after-recovery"));
}

TEST_F(DBWALTest, AsyncWalPrecreateDeletesFileOnStartFailure) {
  Options options = CurrentOptions();
  options.env = env_;
  options.statistics = CreateDBStatistics();
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  // Goal: force failure after consuming a prepared WAL but before it becomes a
  // logical WAL. The failed, unpublished file must be deleted and its writer
  // destroyed outside mutex_.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::AsyncWalPrecreateDeletesFileOnStartFailure:PrecreateDone"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateDeletesFileOnStartFailure:PrecreateDone");
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::StartWALFile:AfterCompressionTypeRecord", [](void* arg) {
        *static_cast<IOStatus*>(arg) =
            IOStatus::IOError("injected async WAL start failure");
      });
  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_GE(wal_numbers.size(), 2);
  const uint64_t prepared_wal_number = wal_numbers.back();

  ASSERT_OK(Put("fill", "value"));
  ASSERT_NOK(dbfull()->TEST_SwitchMemtable());
  ASSERT_TRUE(
      env_->FileExists(LogFileName(dbname_, prepared_wal_number)).IsNotFound());
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
}

TEST_F(DBWALTest, AsyncWalPrecreateFailureFallsBackToSyncCreation) {
  Options options = CurrentOptions();
  options.env = env_;
  options.statistics = CreateDBStatistics();
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  // Goal: fail the background precreate file open after normal DB open has
  // created the current WAL. The foreground switch should observe no prepared
  // WAL, create one synchronously, and expose both failure and miss counters.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::AsyncWalPrecreateFailureFallsBackToSyncCreation:"
       "PrecreateDone"},
  });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Open:AfterDeleteFiles",
      [&](void*) { env_->non_writable_count_ = 1; });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([&] {
    env_->non_writable_count_ = 0;
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateFailureFallsBackToSyncCreation:"
      "PrecreateDone");
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_FAILED));

  ASSERT_OK(Put("fill", "value"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_MISS));
}

TEST_F(DBWALTest, ReadOnlyErrorIfWalFileExistsIgnoresEmptyWals) {
  Options options = CurrentOptions();
  options.env = env_;
  DestroyAndReopen(options);
  Close();

  // Goal: preserve error_if_wal_file_exists for WALs containing data while
  // allowing empty future WALs left by async precreation. This creates empty
  // WAL files directly so the test isolates the read-only open contract.
  const std::vector<uint64_t> existing_wal_numbers = ListWalNumbers();
  const uint64_t empty_wal_number =
      existing_wal_numbers.empty() ? 1 : existing_wal_numbers.back() + 1;
  CreateEmptyWal(empty_wal_number);

  DBOptions db_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName,
                               ColumnFamilyOptions(options));
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(db_options, dbname_, column_families, &handles,
                                &read_only_db,
                                true /* error_if_wal_file_exists */));
  for (ColumnFamilyHandle* handle : handles) {
    ASSERT_OK(read_only_db->DestroyColumnFamilyHandle(handle));
  }
  read_only_db.reset();
  handles.clear();

  CreateWalWithContents(empty_wal_number + 1, Slice("not empty"));
  ASSERT_NOK(DB::OpenForReadOnly(db_options, dbname_, column_families, &handles,
                                 &read_only_db,
                                 true /* error_if_wal_file_exists */));
}

TEST_F(DBWALTest, RecoveryToleratesEmptyFutureWal) {
  Options options = CurrentOptions();
  options.env = env_;
  options.track_and_verify_wals = true;
  options.track_and_verify_wals_in_manifest = true;
  options.avoid_flush_during_recovery = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", "value"));
  Close();

  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_FALSE(wal_numbers.empty());
  const uint64_t future_wal_number = wal_numbers.back() + 1;
  CreateEmptyWal(future_wal_number);

  // Goal: model a crash after async WAL precreation but before consumption.
  // Recovery should process the zero-byte future WAL, mark its file number
  // used, and keep replaying the real preceding WAL without reporting a WAL
  // chain or MANIFEST-tracking error.
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("value", Get("key"));
  ASSERT_OK(Put("after-recovery", "value2"));
  ASSERT_EQ("value2", Get("after-recovery"));
}

TEST_F(DBWALTest, RecoveryFlushSwitchWALOnEmptyMemtable) {
  Options options = CurrentOptions();
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  options.env = fault_fs_env.get();
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  // Make sure the memtable switch in recovery flush happened after test checks
  // the memtable is empty.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBWALTest.RecoveryFlushSwitchWALOnEmptyMemtable:"
        "AfterCheckMemtableEmpty",
        "RecoverFromRetryableBGIOError:BeforeStart"}});
  SyncPoint::GetInstance()->EnableProcessing();
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataWrite, 7 /* seed*/, 1 /* one_in */,
      true /* retryable */, false /* has_data_loss*/);
  fault_fs->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataWrite);

  WriteOptions wo;
  wo.sync = true;
  Status s = Put("k", "old_v", wo);
  ASSERT_TRUE(s.IsIOError());
  // To verify the key is not in memtable nor SST
  ASSERT_TRUE(static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
                  ->cfd()
                  ->mem()
                  ->IsEmpty());
  ASSERT_EQ("NOT_FOUND", Get("k"));
  TEST_SYNC_POINT(
      "DBWALTest.RecoveryFlushSwitchWALOnEmptyMemtable:"
      "AfterCheckMemtableEmpty");
  SyncPoint::GetInstance()->DisableProcessing();

  fault_fs->DisableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataWrite);

  // Keep trying write until recovery of the previous IO error finishes
  while (!s.ok()) {
    options.env->SleepForMicroseconds(1000);
    s = Put("k", "new_v");
  }

  // If recovery flush didn't switch WAL, we will end up having two duplicate
  // WAL entries with same seqno and same key that violate assertion during WAL
  // recovery and fail DB reopen
  options.avoid_flush_during_recovery = false;
  Reopen(options);

  ASSERT_EQ("new_v", Get("k"));
  Destroy(options);
}

TEST_F(DBWALTest, WALWriteErrorNoRecovery) {
  Options options = CurrentOptions();
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  options.env = fault_fs_env.get();
  options.manual_wal_flush = true;
  DestroyAndReopen(options);
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kWrite, 7 /* seed*/, 1 /* one_in */,
      true /* retryable */, false /* has_data_loss*/);
  fault_fs->EnableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  ASSERT_OK(Put("k", "v"));
  Status s;
  s = db_->FlushWAL(false);
  ASSERT_TRUE(s.IsIOError());
  s = dbfull()->TEST_GetBGError();
  ASSERT_EQ(s.severity(), Status::Severity::kFatalError);
  ASSERT_FALSE(dbfull()->TEST_IsRecoveryInProgress());
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
  Destroy(options);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
