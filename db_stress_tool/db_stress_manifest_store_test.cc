//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#include "db_stress_tool/db_stress_manifest_store.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

namespace {

using Fault = MockManifestStoreData::Fault;

// A DB row with one field, because a claim parses the DB row fields.
ManifestRow DbRow(const std::string& value) {
  return ManifestRow{ManifestRowKey{ManifestRowType::kDb, 0, 0},
                     EncodeManifestRowFields({ManifestRowField{1, value}})};
}

ManifestRow FileRow(uint64_t number, const std::string& fields) {
  return ManifestRow{ManifestRowKey{ManifestRowType::kFile, 0, number}, fields};
}

// A generation request that expects commit sequence 1 and holds `rows`.
ManifestGeneration Generation(std::vector<ManifestRow> rows) {
  ManifestGeneration request;
  request.expected_commit_sequence = 1;
  request.commit_id = "generation";
  request.db_row = DbRow("db");
  request.rows = std::move(rows);
  return request;
}

// Collects the rows of a scan as (file number, fields).
class CollectVisitor : public ManifestRowVisitor {
 public:
  Status OnDbRow(const ManifestDbRowResult& db_row) override {
    db_row_ = db_row;
    return Status::OK();
  }
  Status OnRow(const ManifestRow& row) override {
    rows_.emplace_back(row.key.number, row.fields);
    return Status::OK();
  }

  ManifestDbRowResult db_row_;
  std::vector<std::pair<uint64_t, std::string>> rows_;
};

}  // namespace

class FileManifestStoreTest : public testing::Test {
 protected:
  FileManifestStoreTest()
      : env_(Env::Default()),
        dir_(test::PerThreadDBPath(env_, "file_manifest_store")) {
    EXPECT_OK(FileManifestStore::DestroyStore(env_, dir_));
  }
  ~FileManifestStoreTest() override {
    EXPECT_OK(FileManifestStore::DestroyStore(env_, dir_));
  }

  std::unique_ptr<FileManifestStore> NewStore(bool writer,
                                              uint64_t fault_one_in = 0) {
    return std::make_unique<FileManifestStore>(env_, dir_, writer, fault_one_in,
                                               /*seed=*/301);
  }

  // Plays a process restart: the next object loads the state from the file.
  void Restart() { FileManifestStore::ForgetState(dir_); }

  // Creates the DB row with file 10, so the commit sequence is 1.
  void CreateDb(FileManifestStore* store) {
    ManifestCommit commit;
    commit.expected_commit_sequence = 0;
    commit.commit_id = "create";
    commit.db_row = DbRow("db");
    commit.upserts.push_back(FileRow(10, "f10"));
    ASSERT_OK(store->Commit(commit));
  }

  // Adds file `number` with an expected commit sequence of `expected`.
  static IOStatus AddFile(FileManifestStore* store, uint64_t expected,
                          uint64_t number) {
    ManifestCommit commit;
    commit.expected_commit_sequence = expected;
    commit.commit_id = "add-" + std::to_string(number);
    commit.db_row = DbRow("db");
    commit.upserts.push_back(FileRow(number, "f" + std::to_string(number)));
    return store->Commit(commit);
  }

  uint64_t CommitSequence() {
    auto reader = NewStore(/*writer=*/false);
    ManifestDbRowResult db_row;
    EXPECT_OK(reader->ReadDbRow(&db_row));
    return db_row.state.commit_sequence;
  }

  std::vector<std::pair<uint64_t, std::string>> ScanRows() {
    auto reader = NewStore(/*writer=*/false);
    CollectVisitor visitor;
    EXPECT_OK(reader->Scan(ManifestScanOptions(), &visitor));
    return visitor.rows_;
  }

  // Forces the next fault of `store` (see SetNextFaultForTesting()).
  static void SetNextFault(FileManifestStore* store, Fault fault) {
    store->SetNextFaultForTesting(fault);
  }

  static void CheckInjected(const IOStatus& s, Status::Code code) {
    EXPECT_EQ(code, s.code()) << s.ToString();
    EXPECT_NE(s.ToString().find(FaultInjectionTestFS::kInjected),
              std::string::npos)
        << s.ToString();
  }

  Env* env_;
  std::string dir_;
};

TEST_F(FileManifestStoreTest, StateSurvivesRestart) {
  {
    auto store = NewStore(/*writer=*/true);
    ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
    ASSERT_OK(AddFile(store.get(), 1, 11));
  }
  Restart();
  auto reader = NewStore(/*writer=*/false);
  CollectVisitor visitor;
  ASSERT_OK(reader->Scan(ManifestScanOptions(), &visitor));
  EXPECT_EQ(2, visitor.db_row_.state.commit_sequence);
  EXPECT_EQ("add-11", visitor.db_row_.state.last_commit_id);
  EXPECT_EQ(DbRow("db").fields, visitor.db_row_.db_row.fields);
  const std::vector<std::pair<uint64_t, std::string>> expected = {{10, "f10"},
                                                                  {11, "f11"}};
  EXPECT_EQ(expected, visitor.rows_);
  // A reader only reads.
  EXPECT_EQ(Status::Code::kNotSupported, AddFile(reader.get(), 2, 12).code());
}

TEST_F(FileManifestStoreTest, NewWriterFencesOlderWriter) {
  auto first = NewStore(/*writer=*/true);
  ASSERT_NO_FATAL_FAILURE(CreateDb(first.get()));
  auto second = NewStore(/*writer=*/true);
  ASSERT_GT(*second->epoch(), *first->epoch());
  ManifestDbRowResult db_row;
  ManifestStoreOpenOptions open_opts;
  open_opts.commit_id = "claim";
  ASSERT_OK(second->Open(open_opts, &db_row));
  EXPECT_TRUE(
      AddFile(first.get(), db_row.state.commit_sequence, 11).IsIOFenced());
  ASSERT_OK(AddFile(second.get(), db_row.state.commit_sequence, 11));

  // After a restart, a new writer gets an epoch above the stored epoch.
  const uint64_t stored_epoch = second->StoredEpoch();
  first.reset();
  second.reset();
  Restart();
  auto third = NewStore(/*writer=*/true);
  EXPECT_EQ(stored_epoch, third->StoredEpoch());
  EXPECT_GT(*third->epoch(), stored_epoch);
  EXPECT_FALSE(NewStore(/*writer=*/false)->epoch().has_value());
}

TEST_F(FileManifestStoreTest, WrongCommitSequenceIsBusy) {
  auto store = NewStore(/*writer=*/true);
  ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
  EXPECT_TRUE(AddFile(store.get(), 5, 11).IsBusy());
  EXPECT_TRUE(AddFile(store.get(), 0, 11).IsBusy());
  EXPECT_EQ(1, CommitSequence());
}

TEST_F(FileManifestStoreTest, GenerationReplacesRows) {
  {
    auto store = NewStore(/*writer=*/true);
    ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
    uint64_t generation = 0;
    ASSERT_OK(store->WriteGeneration(
        Generation(
            {FileRow(20, "f20"), FileRow(21, "f21"), FileRow(22, "f22")}),
        &generation));
    EXPECT_GT(generation, 0);
  }
  Restart();
  const std::vector<std::pair<uint64_t, std::string>> expected = {
      {20, "f20"}, {21, "f21"}, {22, "f22"}};
  EXPECT_EQ(expected, ScanRows());
  EXPECT_EQ(2, CommitSequence());
}

TEST_F(FileManifestStoreTest, CommitFaults) {
  struct Case {
    Fault fault;
    Status::Code code;
    bool applied;
  };
  const Case kCases[] = {
      {Fault::kTryAgain, Status::Code::kTryAgain, false},
      {Fault::kTimedOutNotApplied, Status::Code::kTimedOut, false},
      {Fault::kTimedOutApplied, Status::Code::kTimedOut, true},
  };
  for (const Case& c : kCases) {
    SCOPED_TRACE(static_cast<int>(c.fault));
    {
      auto store = NewStore(/*writer=*/true);
      ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
      SetNextFault(store.get(), c.fault);
      CheckInjected(AddFile(store.get(), 1, 11), c.code);
    }
    // The outcome is in the state file.
    Restart();
    EXPECT_EQ(c.applied ? 2 : 1, CommitSequence());
    ASSERT_OK(FileManifestStore::DestroyStore(env_, dir_));
  }
}

TEST_F(FileManifestStoreTest, LateApply) {
  auto store = NewStore(/*writer=*/true);
  ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
  SetNextFault(store.get(), Fault::kTimedOutLateApply);
  CheckInjected(AddFile(store.get(), 1, 11), Status::Code::kTimedOut);
  // The next call applies the commit, and the state file has it.
  EXPECT_EQ(2, CommitSequence());
  store.reset();
  Restart();
  EXPECT_EQ(2, CommitSequence());
}

TEST_F(FileManifestStoreTest, LateApplyLostByRestart) {
  auto store = NewStore(/*writer=*/true);
  ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
  SetNextFault(store.get(), Fault::kTimedOutLateApply);
  CheckInjected(AddFile(store.get(), 1, 11), Status::Code::kTimedOut);
  // A process kill before the next call loses the late apply.
  store.reset();
  Restart();
  EXPECT_EQ(1, CommitSequence());
}

TEST_F(FileManifestStoreTest, GenerationFaults) {
  const Fault kFaults[] = {Fault::kLoseGenerationRow,
                           Fault::kCorruptGenerationRow};
  for (Fault fault : kFaults) {
    SCOPED_TRACE(static_cast<int>(fault));
    {
      auto store = NewStore(/*writer=*/true);
      ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
      SetNextFault(store.get(), fault);
      uint64_t generation = 0;
      CheckInjected(store->WriteGeneration(
                        Generation({FileRow(20, "f20"), FileRow(21, "f21")}),
                        &generation),
                    Status::Code::kTryAgain);
    }
    Restart();
    const std::vector<std::pair<uint64_t, std::string>> expected = {
        {10, "f10"}};
    EXPECT_EQ(expected, ScanRows());
    ASSERT_OK(FileManifestStore::DestroyStore(env_, dir_));
  }
}

TEST_F(FileManifestStoreTest, RandomFaultsOnlyWhenEnabled) {
  auto store = NewStore(/*writer=*/true, /*fault_one_in=*/2);
  // Faults start disabled.
  ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
  for (uint64_t i = 0; i < 20; ++i) {
    ASSERT_OK(AddFile(store.get(), CommitSequence(), 100 + i));
  }
  store->SetFaultsEnabled(true);
  int failures = 0;
  for (uint64_t i = 0; i < 100; ++i) {
    const uint64_t before = CommitSequence();
    IOStatus s = AddFile(store.get(), before, 200 + i);
    const uint64_t after = CommitSequence();
    if (s.ok()) {
      EXPECT_EQ(before + 1, after);
      continue;
    }
    ++failures;
    EXPECT_TRUE(s.IsTryAgain() || s.IsTimedOut()) << s.ToString();
    EXPECT_NE(s.ToString().find(FaultInjectionTestFS::kInjected),
              std::string::npos);
    EXPECT_TRUE(after == before || after == before + 1);
  }
  // Each call fails with a chance of 1 in 2. So among 100 calls, at least one
  // fails and at least one succeeds, except with a tiny chance.
  EXPECT_GT(failures, 0);
  EXPECT_LT(failures, 100);
}

// A commit that changes a WAL row, or no row other than the DB row, never gets
// an injected fault, also with a fault at every call. That includes a commit
// that also changes a file row. A fault set for testing stays for the next
// commit that changes file rows and no WAL row.
TEST_F(FileManifestStoreTest, WalTrackingCommitsGetNoFault) {
  auto store = NewStore(/*writer=*/true, /*fault_one_in=*/1);
  ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
  store->SetFaultsEnabled(true);
  // The WAL row that the store has, 0 for none.
  uint64_t stored_wal = 0;
  for (uint64_t i = 0; i < 10; ++i) {
    ManifestCommit commit;
    commit.expected_commit_sequence = CommitSequence();
    commit.commit_id = "wal-" + std::to_string(i);
    commit.db_row = DbRow("db");
    // Every third commit changes only the DB row. The others add a WAL row and
    // delete the previous one, and the ones with an odd `i` also add a file
    // row.
    if (i % 3 != 2 && i % 2 == 1) {
      commit.upserts.push_back(
          ManifestRow{ManifestRowKey{ManifestRowType::kFile, 0, 100 + i}, "f"});
    }
    if (i % 3 != 2) {
      commit.upserts.push_back(
          ManifestRow{ManifestRowKey{ManifestRowType::kWal, 0, 20 + i}, "w"});
      if (stored_wal != 0) {
        commit.deletes.push_back(
            ManifestRowKey{ManifestRowType::kWal, 0, stored_wal});
      }
      stored_wal = 20 + i;
    }
    ASSERT_OK(store->Commit(commit));
  }
  store->SetFaultsEnabled(false);
  SetNextFault(store.get(), Fault::kTryAgain);
  ManifestCommit wal_commit;
  wal_commit.expected_commit_sequence = CommitSequence();
  wal_commit.commit_id = "wal-forced";
  wal_commit.db_row = DbRow("db");
  wal_commit.upserts.push_back(
      ManifestRow{ManifestRowKey{ManifestRowType::kWal, 0, 40}, "w"});
  ASSERT_OK(store->Commit(wal_commit));
  CheckInjected(AddFile(store.get(), CommitSequence(), 11), Status::kTryAgain);
}

TEST_F(FileManifestStoreTest, Destroy) {
  uint64_t stored_epoch = 0;
  {
    auto store = NewStore(/*writer=*/true);
    ASSERT_NO_FATAL_FAILURE(CreateDb(store.get()));
    ASSERT_OK(store->Destroy());
    stored_epoch = store->StoredEpoch();
  }
  Restart();
  {
    auto reader = NewStore(/*writer=*/false);
    ManifestDbRowResult db_row;
    EXPECT_TRUE(reader->ReadDbRow(&db_row).IsNotFound());
    // Destroy() keeps the epoch, so an old writer stays fenced.
    EXPECT_EQ(stored_epoch, reader->StoredEpoch());
  }
  ASSERT_OK(FileManifestStore::DestroyStore(env_, dir_));
  EXPECT_TRUE(env_->FileExists(dir_).IsNotFound());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <cstdio>

int main() {
  fprintf(stderr, "SKIPPED as db_stress requires GFLAGS\n");
  return 0;
}
#endif  // GFLAGS
