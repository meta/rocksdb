//  Copyright (c) 2024-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

#ifdef OS_LINUX

class DBFollowerTest : public DBTestBase {
 public:
  // Create directories for leader and follower
  // Create the leader DB object
  DBFollowerTest() : DBTestBase("/db_follower_test", /*env_do_fsync*/ false) {
    follower_name_ = dbname_ + "/follower";
    db_parent_ = dbname_;
    Close();
    Destroy(CurrentOptions());
    EXPECT_EQ(env_->CreateDirIfMissing(dbname_), Status::OK());
    dbname_ = dbname_ + "/leader";
    Reopen(CurrentOptions());
  }

  ~DBFollowerTest() override {
    follower_.reset();
    EXPECT_EQ(DestroyDB(follower_name_, CurrentOptions()), Status::OK());
    Destroy(CurrentOptions());
    dbname_ = db_parent_;
  }

 protected:
  class DBFollowerTestFS : public FileSystemWrapper {
   public:
    explicit DBFollowerTestFS(const std::shared_ptr<FileSystem>& target)
        : FileSystemWrapper(target),
          cv_(&mutex_),
          barrier_(false),
          count_(0),
          reinit_count_(0) {}

    const char* Name() const override { return "DBFollowerTestFS"; }

    IOStatus NewSequentialFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSSequentialFile>* result,
                               IODebugContext* dbg = nullptr) override {
      class DBFollowerTestSeqFile : public FSSequentialFileWrapper {
       public:
        DBFollowerTestSeqFile(DBFollowerTestFS* fs,
                              std::unique_ptr<FSSequentialFile>&& file,
                              uint64_t /*size*/)
            : FSSequentialFileWrapper(file.get()),
              fs_(fs),
              file_(std::move(file)) {}

        IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                      char* scratch, IODebugContext* dbg) override {
          fs_->BarrierWait();
          return target()->Read(n, options, result, scratch, dbg);
        }

       private:
        DBFollowerTestFS* fs_;
        std::unique_ptr<FSSequentialFile> file_;
      };

      std::unique_ptr<FSSequentialFile> file;
      IOStatus s = target()->NewSequentialFile(fname, file_opts, &file, dbg);

      if (s.ok() && test::GetFileType(fname) == kDescriptorFile) {
        uint64_t size = 0;
        EXPECT_EQ(target()->GetFileSize(fname, IOOptions(), &size, nullptr),
                  IOStatus::OK());
        result->reset(new DBFollowerTestSeqFile(this, std::move(file), size));
      } else {
        *result = std::move(file);
      }
      return s;
    }

    void BarrierInit(int count) {
      MutexLock l(&mutex_);
      barrier_ = true;
      count_ = count;
    }

    void BarrierWait() {
      MutexLock l(&mutex_);
      if (!barrier_) {
        return;
      }
      if (--count_ == 0) {
        if (reinit_count_ > 0) {
          count_ = reinit_count_;
          reinit_count_ = 0;
        } else {
          barrier_ = false;
        }
        cv_.SignalAll();
      } else {
        cv_.Wait();
      }
    }

    void BarrierWaitAndReinit(int count) {
      MutexLock l(&mutex_);
      if (!barrier_) {
        return;
      }
      reinit_count_ = count;
      if (--count_ == 0) {
        if (reinit_count_ > 0) {
          count_ = reinit_count_;
          reinit_count_ = 0;
        } else {
          barrier_ = false;
        }
        cv_.SignalAll();
      } else {
        cv_.Wait();
      }
    }

   private:
    port::Mutex mutex_;
    port::CondVar cv_;
    bool barrier_;
    int count_;
    int reinit_count_;
  };

  class DBFollowerTestSstPartitioner : public SstPartitioner {
   public:
    explicit DBFollowerTestSstPartitioner(uint64_t max_keys)
        : max_keys_(max_keys), num_keys_(0) {}

    const char* Name() const override { return "DBFollowerTestSstPartitioner"; }

    PartitionerResult ShouldPartition(
        const PartitionerRequest& /*request*/) override {
      if (++num_keys_ > max_keys_) {
        num_keys_ = 0;
        return PartitionerResult::kRequired;
      } else {
        return PartitionerResult::kNotRequired;
      }
    }

    bool CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                          const Slice& /*largest_user_key*/) override {
      return true;
    }

   private:
    uint64_t max_keys_;
    uint64_t num_keys_;
  };

  class DBFollowerTestSstPartitionerFactory : public SstPartitionerFactory {
   public:
    explicit DBFollowerTestSstPartitionerFactory(uint64_t max_keys)
        : max_keys_(max_keys) {}

    std::unique_ptr<SstPartitioner> CreatePartitioner(
        const SstPartitioner::Context& /*context*/) const override {
      std::unique_ptr<SstPartitioner> partitioner;
      partitioner.reset(new DBFollowerTestSstPartitioner(max_keys_));
      return partitioner;
    }

    const char* Name() const override {
      return "DBFollowerTestSstPartitionerFactory";
    }

   private:
    uint64_t max_keys_;
  };

  Status OpenAsFollower() {
    Options opts = CurrentOptions();
    if (!follower_env_) {
      follower_env_ = NewCompositeEnv(
          std::make_shared<DBFollowerTestFS>(env_->GetFileSystem()));
    }
    opts.env = follower_env_.get();
    opts.follower_refresh_catchup_period_ms = 100;
    return DB::OpenAsFollower(opts, follower_name_, dbname_, &follower_);
  }

  std::string FollowerGet(const std::string& k) {
    ReadOptions options;
    options.verify_checksums = true;
    std::string result;
    Status s = follower()->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  void VerifyFollowerBatchValue(const std::string& key,
                                const std::string& expected) {
    std::vector<Iterator*> iterators;
    ASSERT_OK(follower()->NewIterators(
        ReadOptions(), {follower()->DefaultColumnFamily()}, &iterators));
    ASSERT_EQ(1U, iterators.size());
    std::unique_ptr<Iterator> iterator(iterators[0]);
    iterator->Seek(key);
    ASSERT_OK(iterator->status());
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ(expected, iterator->value());
  }

  DB* follower() { return follower_.get(); }
  DBFollowerTestFS* follower_fs() {
    return static_cast<DBFollowerTestFS*>(follower_env_->GetFileSystem().get());
  }

  void CheckDirs() {
    std::vector<std::string> db_children;
    std::vector<std::string> follower_children;
    EXPECT_OK(env_->GetChildren(dbname_, &db_children));
    EXPECT_OK(env_->GetChildren(follower_name_, &follower_children));

    std::set<uint64_t> db_filenums;
    std::set<uint64_t> follower_filenums;
    for (auto& name : db_children) {
      if (test::GetFileType(name) != kTableFile) {
        continue;
      }
      db_filenums.insert(test::GetFileNumber(name));
    }
    for (auto& name : follower_children) {
      if (test::GetFileType(name) != kTableFile) {
        continue;
      }
      follower_filenums.insert(test::GetFileNumber(name));
    }
    db_filenums.merge(follower_filenums);
    EXPECT_EQ(follower_filenums.size(), db_filenums.size());
  }

 private:
  std::string follower_name_;
  std::string db_parent_;
  std::unique_ptr<Env> follower_env_;
  std::unique_ptr<DB> follower_;
};

TEST_F(DBFollowerTest, Basic) {
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());

  ASSERT_OK(OpenAsFollower());
  std::string val;
  ASSERT_OK(follower()->Get(ReadOptions(), "k1", &val));
  ASSERT_EQ(val, "v1");
  CheckDirs();
}

TEST_F(DBFollowerTest, NewIteratorsRefreshesAfterCatchUp) {
  ASSERT_OK(Put("key", "v1"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:Begin1", "Leader::Start"},
      {"Leader::Done", "DBImplFollower::TryCatchupWithLeader:Begin2"},
      {"DBImplFollower::TryCatchupWithLeader:End", "Follower::WaitForCatchup"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(OpenAsFollower());
  VerifyFollowerBatchValue("key", "v1");

  TEST_SYNC_POINT("Leader::Start");
  ASSERT_OK(Put("key", "v2"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT("Leader::Done");
  TEST_SYNC_POINT("Follower::WaitForCatchup");

  VerifyFollowerBatchValue("key", "v2");

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearTrace();
}

TEST_F(DBFollowerTest, Flush) {
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:Begin1", "Leader::Start"},
      {"Leader::Done", "DBImplFollower::TryCatchupWithLeader:Begin2"},
      {"DBImplFollower::TryCatchupWithLeader:End", "Follower::WaitForCatchup"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(OpenAsFollower());
  TEST_SYNC_POINT("Leader::Start");
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT("Leader::Done");

  TEST_SYNC_POINT("Follower::WaitForCatchup");
  std::string val;
  ASSERT_OK(follower()->Get(ReadOptions(), "k1", &val));
  ASSERT_EQ(val, "v1");
  CheckDirs();

  SyncPoint::GetInstance()->DisableProcessing();
}

// This test creates 4 L0 files, immediately followed by a compaction to L1.
// The follower replays the 4 flush records from the MANIFEST unsuccessfully,
// and then successfully recovers a Version from the compaction record
TEST_F(DBFollowerTest, RetryCatchup) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  Reopen(opts);

  ASSERT_OK(OpenAsFollower());
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:Begin1", "Leader::Start"},
      {"DBImpl::BackgroundCompaction:Start",
       "DBImplFollower::TryCatchupWithLeader:Begin2"},
      {"VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:"
       "Begin1",
       "DBImpl::BackgroundCompaction:BeforeCompaction"},
      {"DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles",
       "VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:"
       "Begin2"},
      {"DBImplFollower::TryCatchupWithLeader:End", "Follower::WaitForCatchup"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  TEST_SYNC_POINT("Leader::Start");
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k1", "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k1", "v4"));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));

  TEST_SYNC_POINT("Follower::WaitForCatchup");
  ASSERT_EQ(FollowerGet("k1"), "v4");
  CheckDirs();

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

// This test validates the same as the previous test, except there is a
// MANIFEST rollover between the flushes and compaction. The follower
// does not switch to a new MANIFEST in ReadAndApply. So it would require
// another round of refresh before catching up.
TEST_F(DBFollowerTest, RetryCatchupManifestRollover) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  Reopen(opts);

  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k1", "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(OpenAsFollower());
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:Begin1", "Leader::Start"},
      {"Leader::Flushed", "DBImplFollower::TryCatchupWithLeader:Begin2"},
      {"VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:"
       "Begin1",
       "Leader::Done"},
      {"DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles",
       "VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:"
       "Begin2"},
      {"DBImplFollower::TryCatchupWithLeader:End",
       "Follower::WaitForCatchup:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  TEST_SYNC_POINT("Leader::Start");
  ASSERT_OK(Put("k1", "v4"));
  ASSERT_OK(Flush());

  TEST_SYNC_POINT("Leader::Flushed");
  TEST_SYNC_POINT("Leader::Done");
  Reopen(opts);
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));

  TEST_SYNC_POINT("Follower::WaitForCatchup:1");
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:End",
       "Follower::WaitForCatchup:2"},
  });
  TEST_SYNC_POINT("Follower::WaitForCatchup:2");
  ASSERT_EQ(FollowerGet("k1"), "v4");

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBFollowerTest, CatchUpKeepsViewWhenNewManifestListsDeletedFiles) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.max_manifest_file_size = 1;
  opts.max_manifest_space_amp_pct = 0;
  Reopen(opts);
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:Begin1", "Leader::Start"},
      {"Leader::Done", "DBImplFollower::TryCatchupWithLeader:Begin2"},
      {"DBImplFollower::TryCatchupWithLeader:End",
       "Follower::WaitForCatchup:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(OpenAsFollower());
  ASSERT_EQ(FollowerGet("foo"), "v1");

  std::atomic<bool> compacted{false};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:Begin1",
      [&](void*) {
        if (compacted.exchange(true)) {
          return;
        }
        std::vector<LiveFileMetaData> tables;
        db_->GetLiveFilesMetaData(&tables);
        CompactRangeOptions compact_options;
        compact_options.bottommost_level_compaction =
            BottommostLevelCompaction::kForce;
        ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
        ASSERT_OK(dbfull()->TEST_WaitForPurge());
        for (const auto& table : tables) {
          const Status table_status =
              env_->FileExists(MakeTableFileName(dbname_, table.file_number));
          ASSERT_TRUE(table_status.IsNotFound()) << table_status.ToString();
        }
      });

  TEST_SYNC_POINT("Leader::Start");
  // The second flush rolls the MANIFEST again, so that the newest one lists
  // the file of the first flush in its snapshot.
  ASSERT_OK(Put("bar", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("baz", "v1"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT("Leader::Done");

  TEST_SYNC_POINT("Follower::WaitForCatchup:1");
  ASSERT_TRUE(compacted.load());
  ASSERT_EQ(FollowerGet("foo"), "v1");

  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:End",
       "Follower::WaitForCatchup:2"},
  });
  TEST_SYNC_POINT("Follower::WaitForCatchup:2");
  ASSERT_EQ(FollowerGet("foo"), "v1");
  ASSERT_EQ(FollowerGet("bar"), "v1");
  ASSERT_EQ(FollowerGet("baz"), "v1");

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

// This test creates 4 L0 files and compacts them. The follower, during catchup,
// successfully instantiates 4 Versions corresponding to the 4 files (but
// doesn't install them yet), followed by deleting those 4 and adding a new
// file from compaction. The test verifies that the 4 L0 files are deleted
// correctly by the follower.
// We use the Barrier* functions to ensure that the follower first sees the 4
// L0 files and is able to link them, and then sees the compaction that
// obsoletes those L0 files (so those L0 files are intermediates that it has
// to explicitly delete). Suppose we don't have any barriers, its possible
// the follower reads the L0 records and compaction records from the MANIFEST
// in one read, which means those L0 files would have already been deleted
// by the leader and the follower cannot link to them.
TEST_F(DBFollowerTest, IntermediateObsoleteFiles) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  Reopen(opts);
  ASSERT_OK(OpenAsFollower());

  follower_fs()->BarrierInit(2);
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k1", "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k1", "v4"));
  ASSERT_OK(Flush());
  follower_fs()->BarrierWaitAndReinit(2);

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
  follower_fs()->BarrierWait();

  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:End",
       "Follower::WaitForCatchup:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  TEST_SYNC_POINT("Follower::WaitForCatchup:1");
  CheckDirs();
  ASSERT_EQ(FollowerGet("k1"), "v4");
}

// This test verifies a scenario where the follower can recover a Version
// partially (i.e some of the additions cannot be found), and the files
// that are found are obsoleted by a subsequent VersionEdit.
TEST_F(DBFollowerTest, PartialVersionRecovery) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.sst_partitioner_factory =
      std::make_shared<DBFollowerTestSstPartitionerFactory>(1);
  Reopen(opts);

  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Put("k2", "v1"));
  ASSERT_OK(Put("k3", "v1"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  ASSERT_OK(Put("k1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k3", "v2"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_OK(OpenAsFollower());
  ASSERT_OK(dbfull()->SetOptions(dbfull()->DefaultColumnFamily(),
                                 {{"max_compaction_bytes", "1"}}));

  follower_fs()->BarrierInit(2);
  Slice key("k1");
  ASSERT_OK(dbfull()->TEST_CompactRange(1, &key, &key, nullptr, true));

  follower_fs()->BarrierWaitAndReinit(2);

  // The second compaction input overlaps the previous compaction outputs
  // by one file. This file is never added to  VersionStorageInfo since it
  // was added and deleted before the catch up completes. We later verify that
  // the follower correctly deleted this file.
  key = Slice("k3");
  ASSERT_OK(dbfull()->TEST_CompactRange(1, &key, &key, nullptr, true));
  follower_fs()->BarrierWait();

  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:End",
       "Follower::WaitForCatchup:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  TEST_SYNC_POINT("Follower::WaitForCatchup:1");
  CheckDirs();
  ASSERT_EQ(FollowerGet("k1"), "v2");
  ASSERT_EQ(FollowerGet("k2"), "v1");
  ASSERT_EQ(FollowerGet("k3"), "v2");
  SyncPoint::GetInstance()->DisableProcessing();
}

// This test verifies a scenario similar to the PartialVersionRecovery, except
// with a MANIFEST rollover in between. When there is a rollover, the
// follower's attempt ends without installing a new Version. The next catch up
// attempt will recover a full Version.
TEST_F(DBFollowerTest, PartialVersionRecoveryWithRollover) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.sst_partitioner_factory =
      std::make_shared<DBFollowerTestSstPartitionerFactory>(1);
  Reopen(opts);

  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Put("k2", "v1"));
  ASSERT_OK(Put("k3", "v1"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  ASSERT_OK(Put("k1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k3", "v2"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  opts.max_compaction_bytes = 1;
  Reopen(opts);

  ASSERT_OK(OpenAsFollower());

  follower_fs()->BarrierInit(2);
  Slice key("k1");
  ASSERT_OK(dbfull()->TEST_CompactRange(1, &key, &key, nullptr, true));

  follower_fs()->BarrierWaitAndReinit(2);
  Reopen(opts);
  key = Slice("k3");
  ASSERT_OK(dbfull()->TEST_CompactRange(1, &key, &key, nullptr, true));
  follower_fs()->BarrierWait();

  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:Begin1",
       "Follower::WaitForCatchup:1"},
      {"Follower::WaitForCatchup:2",
       "DBImplFollower::TryCatchupWithLeader:Begin2"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  TEST_SYNC_POINT("Follower::WaitForCatchup:1");
  TEST_SYNC_POINT("Follower::WaitForCatchup:2");
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplFollower::TryCatchupWithLeader:End",
       "Follower::WaitForCatchup:3"},
  });
  TEST_SYNC_POINT("Follower::WaitForCatchup:3");
  CheckDirs();
  ASSERT_EQ(FollowerGet("k1"), "v2");
  ASSERT_EQ(FollowerGet("k2"), "v1");
  ASSERT_EQ(FollowerGet("k3"), "v2");
  SyncPoint::GetInstance()->DisableProcessing();
}
#endif
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
