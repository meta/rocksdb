//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/version_builder.h"

#include <cstring>
#include <initializer_list>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/sst_file_writer.h"
#include "table/unique_id_impl.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "util/string_util.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class VersionBuilderTest : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  VersionStorageInfo vstorage_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::vector<uint64_t> size_being_compacted_;

  VersionBuilderTest()
      : ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        ioptions_(options_),
        mutable_cf_options_(options_),
        vstorage_(&icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
                  nullptr, false, EpochNumberRequirement::kMustPresent,
                  ioptions_.clock, options_.bottommost_file_compaction_delay,
                  OffpeakTimeOption(options_.daily_offpeak_time_utc),
                  PeriodicCompactionPhaseParams{}),
        file_num_(1) {
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    size_being_compacted_.resize(options_.num_levels);
  }

  ~VersionBuilderTest() override {
    for (int i = 0; i < vstorage_.num_levels(); i++) {
      for (auto* f : vstorage_.LevelFiles(i)) {
        if (--f->refs == 0) {
          delete f;
        }
      }
    }
  }

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  void Add(int level, uint64_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           uint64_t num_entries = 0, uint64_t num_deletions = 0,
           bool sampled = false, SequenceNumber smallest_seqno = 0,
           SequenceNumber largest_seqno = 0,
           uint64_t oldest_blob_file_number = kInvalidBlobFileNumber,
           uint64_t epoch_number = kUnknownEpochNumber) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData(
        file_number, path_id, file_size, GetInternalKey(smallest, smallest_seq),
        GetInternalKey(largest, largest_seq), smallest_seqno, largest_seqno,
        /* marked_for_compact */ false, Temperature::kUnknown,
        oldest_blob_file_number, kUnknownOldestAncestorTime,
        kUnknownFileCreationTime, epoch_number, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
        /* user_defined_timestamps_persisted */ true, /* min timestamp */ "",
        /* max timestamp */ "");
    f->compensated_file_size = file_size;
    f->num_entries = num_entries;
    f->num_deletions = num_deletions;
    vstorage_.AddFile(level, f);
    if (sampled) {
      f->init_stats_from_file = true;
      vstorage_.UpdateAccumulatedStats(f);
    }
  }

  void AddBlob(uint64_t blob_file_number, uint64_t total_blob_count,
               uint64_t total_blob_bytes, std::string checksum_method,
               std::string checksum_value,
               BlobFileMetaData::LinkedSsts linked_ssts,
               uint64_t garbage_blob_count, uint64_t garbage_blob_bytes) {
    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, total_blob_count, total_blob_bytes,
        std::move(checksum_method), std::move(checksum_value));
    auto meta =
        BlobFileMetaData::Create(std::move(shared_meta), std::move(linked_ssts),
                                 garbage_blob_count, garbage_blob_bytes);

    vstorage_.AddBlobFile(std::move(meta));
  }

  void AddDummyFile(uint64_t table_file_number, uint64_t blob_file_number,
                    uint64_t epoch_number) {
    constexpr int level = 0;
    constexpr char smallest[] = "bar";
    constexpr char largest[] = "foo";
    constexpr uint64_t file_size = 100;
    constexpr uint32_t path_id = 0;
    constexpr SequenceNumber smallest_seq = 0;
    constexpr SequenceNumber largest_seq = 0;
    constexpr uint64_t num_entries = 0;
    constexpr uint64_t num_deletions = 0;
    constexpr bool sampled = false;

    Add(level, table_file_number, smallest, largest, file_size, path_id,
        smallest_seq, largest_seq, num_entries, num_deletions, sampled,
        smallest_seq, largest_seq, blob_file_number, epoch_number);
  }

  void AddDummyFileToEdit(VersionEdit* edit, uint64_t table_file_number,
                          uint64_t blob_file_number, uint64_t epoch_number) {
    assert(edit);

    constexpr int level = 0;
    constexpr uint32_t path_id = 0;
    constexpr uint64_t file_size = 100;
    constexpr char smallest[] = "bar";
    constexpr char largest[] = "foo";
    constexpr SequenceNumber smallest_seqno = 100;
    constexpr SequenceNumber largest_seqno = 300;
    constexpr bool marked_for_compaction = false;

    edit->AddFile(
        level, table_file_number, path_id, file_size, GetInternalKey(smallest),
        GetInternalKey(largest), smallest_seqno, largest_seqno,
        marked_for_compaction, Temperature::kUnknown, blob_file_number,
        kUnknownOldestAncestorTime, kUnknownFileCreationTime, epoch_number,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2,
        0, 0, /* user_defined_timestamps_persisted */ true);
  }

  void UpdateVersionStorageInfo(VersionStorageInfo* vstorage) {
    assert(vstorage);

    vstorage->PrepareForVersionAppend(ioptions_, mutable_cf_options_);
    vstorage->SetFinalized();
  }

  void UpdateVersionStorageInfo() { UpdateVersionStorageInfo(&vstorage_); }
};

class VersionBuilderDBTest : public DBTestBase {
 public:
  VersionBuilderDBTest()
      : DBTestBase("version_builder_db_test", /*env_do_fsync=*/false) {}
};

// Regression test: when LogAndApply fails after VersionBuilder pinned a
// table reader for a newly-added file (via LoadTableHandlers), the table
// cache entry must be evicted by VersionBuilder::UnrefFile when the
// FileMetaData is destroyed.
//
// This test faithfully reproduces the crash-test failure mode
// ("File N is not live nor quarantined" from TEST_VerifyNoObsoleteFilesCached
// inside Close on ASAN):
//
//   1. IngestExternalFile submits an AddFile edit through LogAndApply.
//   2. LoadTableHandlers inside ProcessManifestWrites pins a table reader
//      for the new file, creating a table cache entry.
//   3. SyncManifest fails (fault injected). The error handler adds the
//      ingested file to its quarantine list.
//   4. A subsequent successful LogAndApply (via manual Resume() and then
//      Flush) clears the quarantine (version_set.cc:
//      `ClearFilesToQuarantine`).
//   5. Metadata read fault injection prevents FindObsoleteFiles from
//      scanning the directory, so the on-disk orphan is not discovered.
//   6. On DB Close, TEST_VerifyNoObsoleteFilesCached iterates the table
//      cache and finds an entry for a file that is neither live nor
//      quarantined -> assertion fires (without the fix).
//
// The IngestExternalFile path issues a few no-op manifest writes before
// the actual AddFile edit; we arm the injection only after
// LoadTableHandlers has actually pinned a reader for the ingested file.
TEST_F(VersionBuilderDBTest,
       IngestExternalFileEvictsTableCacheOnLogAndApplyFailure) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));

  Options options = CurrentOptions();
  options.env = fault_env.get();
  options.disable_auto_compactions = true;
  // Disable auto-recovery so recovery runs synchronously via our explicit
  // Resume() call -- no background thread to race with test teardown.
  options.max_bgerror_resume_count = 0;
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  // RAII: ensure the DB is closed before fault_env goes out of scope,
  // even if an ASSERT_* below early-returns.
  Defer closer([this]() { Close(); });

  // Seed the memtable with data so a subsequent Flush actually produces a
  // successful LogAndApply (and thus clears the quarantine).
  ASSERT_OK(Put("k1", "v1"));

  // Build an external SST through the public SstFileWriter API.
  const std::string ext_file = dbname_ + "/ingest_me.sst";
  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(ext_file));
  ASSERT_OK(writer.Put("k2", "v2"));
  ExternalSstFileInfo file_info;
  ASSERT_OK(writer.Finish(&file_info));

  // Arm the injection only once we have observed VersionBuilder actually
  // pin a table reader for the ingested file (i.e. after FindTable runs).
  std::atomic<bool> injection_armed{false};
  std::atomic<bool> injected_once{false};
  SyncPoint::GetInstance()->SetCallBack("TableCache::FindTable:0",
                                        [&](void*) { injection_armed = true; });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (!injection_armed.load()) {
          return;
        }
        if (injected_once.exchange(true)) {
          return;
        }
        auto* ptr = static_cast<IOStatus*>(arg);
        assert(ptr);
        *ptr = IOStatus::IOError("injected");
        // Retryable IOError -> soft error, recoverable via Resume().
        ptr->SetRetryable(true);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  IngestExternalFileOptions ifo;
  Status s = db_->IngestExternalFile({ext_file}, ifo);
  ASSERT_NOK(s);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Manual synchronous recovery: clear the soft bg error, then do a
  // successful LogAndApply (via Flush) which triggers
  // ClearFilesToQuarantine (version_set.cc line ~6591). This puts the
  // ingested file into the "not live nor quarantined" state.
  ASSERT_OK(dbfull()->Resume());
  ASSERT_OK(Flush());

  // Prevent the FindObsoleteFiles full-directory scan from finding the
  // orphan on disk. Mirrors the crash test's open_metadata_read_fault_one_in.
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataRead, /*seed=*/0, /*one_in=*/1,
      /*retryable=*/false, /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataRead);
  Defer disable_fault([&] {
    fault_fs->DisableThreadLocalErrorInjection(
        FaultInjectionIOType::kMetadataRead);
  });

  // The RAII Defer above will run Close() on scope exit. Close's
  // TEST_VerifyNoObsoleteFilesCached (active on ASAN) iterates the table
  // cache. Without the D99759696 fix, an entry for the ingested file
  // remains -- a file that is neither live nor quarantined -- and the
  // assertion fires. With the fix, VersionBuilder::UnrefFile evicts the
  // entry when the discarded FileMetaData is destroyed.
}

void UnrefFilesInVersion(VersionStorageInfo* new_vstorage) {
  for (int i = 0; i < new_vstorage->num_levels(); i++) {
    for (auto* f : new_vstorage->LevelFiles(i)) {
      if (--f->refs == 0) {
        delete f;
      }
    }
  }
}

TEST_F(VersionBuilderTest, ApplyAndSaveTo) {
  Add(0, 1U, "150", "200", 100U, /*path_id*/ 0,
      /*smallest_seq*/ 100, /*largest_seq*/ 100,
      /*num_entries*/ 0, /*num_deletions*/ 0,
      /*sampled*/ false, /*smallest_seqno*/ 0,
      /*largest_seqno*/ 0,
      /*oldest_blob_file_number*/ kInvalidBlobFileNumber,
      /*epoch_number*/ 1);

  Add(1, 66U, "150", "200", 100U);
  Add(1, 88U, "201", "300", 100U);

  Add(2, 6U, "150", "179", 100U);
  Add(2, 7U, "180", "220", 100U);
  Add(2, 8U, "221", "300", 100U);

  Add(3, 26U, "150", "170", 100U);
  Add(3, 27U, "171", "179", 100U);
  Add(3, 28U, "191", "220", 100U);
  Add(3, 29U, "221", "300", 100U);

  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 666, 0, 100U, GetInternalKey("301"), GetInternalKey("350"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.DeleteFile(3, 27U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, nullptr, false,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  ASSERT_EQ(400U, new_vstorage.NumLevelBytes(2));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(3));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, false, 200U, 200U,
      /*oldest_blob_file_number*/ kInvalidBlobFileNumber,
      /*epoch_number*/ 2);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, false, 100U, 100U,
      /*oldest_blob_file_number*/ kInvalidBlobFileNumber,
      /*epoch_number*/ 1);

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);

  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      3, 666, 0, 100U, GetInternalKey("301"), GetInternalKey("350"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);

  version_edit.DeleteFile(0, 1U);
  version_edit.DeleteFile(0, 88U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, nullptr, false,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(0));
  ASSERT_EQ(100U, new_vstorage.NumLevelBytes(3));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, false, 200U, 200U,
      /*oldest_blob_file_number*/ kInvalidBlobFileNumber,
      /*epoch_number*/ 2);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, false, 100U, 100U,
      /*oldest_blob_file_number*/ kInvalidBlobFileNumber,
      /*epoch_number*/ 1);

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);

  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      4, 666, 0, 100U, GetInternalKey("301"), GetInternalKey("350"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.DeleteFile(0, 1U);
  version_edit.DeleteFile(0, 88U);
  version_edit.DeleteFile(4, 6U);
  version_edit.DeleteFile(4, 7U);
  version_edit.DeleteFile(4, 8U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, nullptr, false,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(0));
  ASSERT_EQ(100U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyMultipleAndSaveTo) {
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 666, 0, 100U, GetInternalKey("301"), GetInternalKey("350"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.AddFile(
      2, 676, 0, 100U, GetInternalKey("401"), GetInternalKey("450"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.AddFile(
      2, 636, 0, 100U, GetInternalKey("601"), GetInternalKey("650"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.AddFile(
      2, 616, 0, 100U, GetInternalKey("501"), GetInternalKey("550"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.AddFile(
      2, 606, 0, 100U, GetInternalKey("701"), GetInternalKey("750"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, nullptr, false,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  ASSERT_EQ(500U, new_vstorage.NumLevelBytes(2));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyDeleteAndSaveTo) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);

  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, nullptr, false,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 666, 0, 100U, GetInternalKey("301"), GetInternalKey("350"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.AddFile(
      2, 676, 0, 100U, GetInternalKey("401"), GetInternalKey("450"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.AddFile(
      2, 636, 0, 100U, GetInternalKey("601"), GetInternalKey("650"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.AddFile(
      2, 616, 0, 100U, GetInternalKey("501"), GetInternalKey("550"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit.AddFile(
      2, 606, 0, 100U, GetInternalKey("701"), GetInternalKey("750"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  ASSERT_OK(version_builder.Apply(&version_edit));

  VersionEdit version_edit2;
  version_edit.AddFile(
      2, 808, 0, 100U, GetInternalKey("901"), GetInternalKey("950"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);
  version_edit2.DeleteFile(2, 616);
  version_edit2.DeleteFile(2, 636);
  version_edit.AddFile(
      2, 806, 0, 100U, GetInternalKey("801"), GetInternalKey("850"), 200, 200,
      false, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);

  ASSERT_OK(version_builder.Apply(&version_edit2));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(2));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyFileDeletionIncorrectLevel) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr uint64_t file_size = 100;

  Add(level, file_number, smallest, largest, file_size);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr int incorrect_level = 3;

  edit.DeleteFile(incorrect_level, file_number);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot delete table file #2345 from level 3 since "
                          "it is on level 1"));
}

TEST_F(VersionBuilderTest, ApplyFileDeletionNotInLSMTree) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr int level = 3;
  constexpr uint64_t file_number = 1234;

  edit.DeleteFile(level, file_number);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot delete table file #1234 from level 3 since "
                          "it is not in the LSM tree"));
}

TEST_F(VersionBuilderTest, ApplyFileDeletionAndAddition) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr uint64_t file_size = 10000;
  constexpr uint32_t path_id = 0;
  constexpr SequenceNumber smallest_seq = 100;
  constexpr SequenceNumber largest_seq = 500;
  constexpr uint64_t num_entries = 0;
  constexpr uint64_t num_deletions = 0;
  constexpr bool sampled = false;
  constexpr SequenceNumber smallest_seqno = 1;
  constexpr SequenceNumber largest_seqno = 1000;

  Add(level, file_number, smallest, largest, file_size, path_id, smallest_seq,
      largest_seq, num_entries, num_deletions, sampled, smallest_seqno,
      largest_seqno);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit deletion;

  deletion.DeleteFile(level, file_number);

  ASSERT_OK(builder.Apply(&deletion));

  VersionEdit addition;

  constexpr bool marked_for_compaction = false;

  addition.AddFile(
      level, file_number, path_id, file_size,
      GetInternalKey(smallest, smallest_seq),
      GetInternalKey(largest, largest_seq), smallest_seqno, largest_seqno,
      marked_for_compaction, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);

  ASSERT_OK(builder.Apply(&addition));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  ASSERT_EQ(new_vstorage.GetFileLocation(file_number).GetLevel(), level);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAlreadyInBase) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr uint64_t file_size = 10000;

  Add(level, file_number, smallest, largest, file_size);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr int new_level = 2;
  constexpr uint32_t path_id = 0;
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  edit.AddFile(
      new_level, file_number, path_id, file_size, GetInternalKey(smallest),
      GetInternalKey(largest), smallest_seqno, largest_seqno,
      marked_for_compaction, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot add table file #2345 to level 2 since it is "
                          "already in the LSM tree on level 1"));
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAlreadyApplied) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr int level = 3;
  constexpr uint64_t file_number = 2345;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 10000;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  edit.AddFile(
      level, file_number, path_id, file_size, GetInternalKey(smallest),
      GetInternalKey(largest), smallest_seqno, largest_seqno,
      marked_for_compaction, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);

  ASSERT_OK(builder.Apply(&edit));

  VersionEdit other_edit;

  constexpr int new_level = 2;

  other_edit.AddFile(
      new_level, file_number, path_id, file_size, GetInternalKey(smallest),
      GetInternalKey(largest), smallest_seqno, largest_seqno,
      marked_for_compaction, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);

  const Status s = builder.Apply(&other_edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot add table file #2345 to level 2 since it is "
                          "already in the LSM tree on level 3"));
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAndDeletion) {
  UpdateVersionStorageInfo();

  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 10000;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit addition;

  addition.AddFile(
      level, file_number, path_id, file_size, GetInternalKey(smallest),
      GetInternalKey(largest), smallest_seqno, largest_seqno,
      marked_for_compaction, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, kUnknownEpochNumber,
      kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0,
      0, /* user_defined_timestamps_persisted */ true);

  ASSERT_OK(builder.Apply(&addition));

  VersionEdit deletion;

  deletion.DeleteFile(level, file_number);

  ASSERT_OK(builder.Apply(&deletion));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  ASSERT_FALSE(new_vstorage.GetFileLocation(file_number).IsValid());

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyBlobFileAddition) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] =
      "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c\x52"
      "\x5c\xbd";

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  // Add dummy table file to ensure the blob file is referenced.
  constexpr uint64_t table_file_number = 1;
  AddDummyFileToEdit(&edit, table_file_number, blob_file_number,
                     1 /*epoch_number*/);

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 1);

  const auto new_meta = new_vstorage.GetBlobFileMetaData(blob_file_number);

  ASSERT_NE(new_meta, nullptr);
  ASSERT_EQ(new_meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(new_meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(new_meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(new_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(new_meta->GetChecksumValue(), checksum_value);
  ASSERT_EQ(new_meta->GetLinkedSsts(),
            BlobFileMetaData::LinkedSsts{table_file_number});
  ASSERT_EQ(new_meta->GetGarbageBlobCount(), 0);
  ASSERT_EQ(new_meta->GetGarbageBlobBytes(), 0);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyBlobFileAdditionAlreadyInBase) {
  // Attempt to add a blob file that is already present in the base version.

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] =
      "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c\x52"
      "\x5c\xbd";
  constexpr uint64_t garbage_blob_count = 123;
  constexpr uint64_t garbage_blob_bytes = 456789;

  AddBlob(blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
          checksum_value, BlobFileMetaData::LinkedSsts(), garbage_blob_count,
          garbage_blob_bytes);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Blob file #1234 already added"));
}

TEST_F(VersionBuilderTest, ApplyBlobFileAdditionAlreadyApplied) {
  // Attempt to add the same blob file twice using version edits.

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] =
      "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c\x52"
      "\x5c\xbd";

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  ASSERT_OK(builder.Apply(&edit));

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Blob file #1234 already added"));
}

TEST_F(VersionBuilderTest, ApplyBlobFileGarbageFileInBase) {
  // Increase the amount of garbage for a blob file present in the base version.

  constexpr uint64_t table_file_number = 1;
  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] =
      "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c\x52"
      "\x5c\xbd";
  constexpr uint64_t garbage_blob_count = 123;
  constexpr uint64_t garbage_blob_bytes = 456789;

  AddBlob(blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
          checksum_value, BlobFileMetaData::LinkedSsts{table_file_number},
          garbage_blob_count, garbage_blob_bytes);

  const auto meta = vstorage_.GetBlobFileMetaData(blob_file_number);
  ASSERT_NE(meta, nullptr);

  // Add dummy table file to ensure the blob file is referenced.
  AddDummyFile(table_file_number, blob_file_number, 1 /*epoch_number*/);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr uint64_t new_garbage_blob_count = 456;
  constexpr uint64_t new_garbage_blob_bytes = 111111;

  edit.AddBlobFileGarbage(blob_file_number, new_garbage_blob_count,
                          new_garbage_blob_bytes);

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 1);

  const auto new_meta = new_vstorage.GetBlobFileMetaData(blob_file_number);

  ASSERT_NE(new_meta, nullptr);
  ASSERT_EQ(new_meta->GetSharedMeta(), meta->GetSharedMeta());
  ASSERT_EQ(new_meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(new_meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(new_meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(new_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(new_meta->GetChecksumValue(), checksum_value);
  ASSERT_EQ(new_meta->GetLinkedSsts(),
            BlobFileMetaData::LinkedSsts{table_file_number});
  ASSERT_EQ(new_meta->GetGarbageBlobCount(),
            garbage_blob_count + new_garbage_blob_count);
  ASSERT_EQ(new_meta->GetGarbageBlobBytes(),
            garbage_blob_bytes + new_garbage_blob_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyBlobFileGarbageFileAdditionApplied) {
  // Increase the amount of garbage for a blob file added using a version edit.

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit addition;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] =
      "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c\x52"
      "\x5c\xbd";

  addition.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                       checksum_method, checksum_value);

  // Add dummy table file to ensure the blob file is referenced.
  constexpr uint64_t table_file_number = 1;
  AddDummyFileToEdit(&addition, table_file_number, blob_file_number,
                     1 /*epoch_number*/);

  ASSERT_OK(builder.Apply(&addition));

  constexpr uint64_t garbage_blob_count = 123;
  constexpr uint64_t garbage_blob_bytes = 456789;

  VersionEdit garbage;

  garbage.AddBlobFileGarbage(blob_file_number, garbage_blob_count,
                             garbage_blob_bytes);

  ASSERT_OK(builder.Apply(&garbage));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 1);

  const auto new_meta = new_vstorage.GetBlobFileMetaData(blob_file_number);

  ASSERT_NE(new_meta, nullptr);
  ASSERT_EQ(new_meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(new_meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(new_meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(new_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(new_meta->GetChecksumValue(), checksum_value);
  ASSERT_EQ(new_meta->GetLinkedSsts(),
            BlobFileMetaData::LinkedSsts{table_file_number});
  ASSERT_EQ(new_meta->GetGarbageBlobCount(), garbage_blob_count);
  ASSERT_EQ(new_meta->GetGarbageBlobBytes(), garbage_blob_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyBlobFileGarbageFileNotFound) {
  // Attempt to increase the amount of garbage for a blob file that is
  // neither in the base version, nor was it added using a version edit.

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t garbage_blob_count = 5678;
  constexpr uint64_t garbage_blob_bytes = 999999;

  edit.AddBlobFileGarbage(blob_file_number, garbage_blob_count,
                          garbage_blob_bytes);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Blob file #1234 not found"));
}

TEST_F(VersionBuilderTest, DroppedBlobFiles) {
  // Table file 1 references blob file 10, which is in the base version.
  AddBlob(/* blob_file_number */ 10, /* total_blob_count */ 100,
          /* total_blob_bytes */ 1000, /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 0, /* garbage_blob_bytes */ 0);
  AddDummyFile(/* table_file_number */ 1, /* blob_file_number */ 10,
               /* epoch_number */ 1);
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;
  constexpr bool force_consistency_checks = false;

  auto save_new_version = [&](VersionBuilder* builder,
                              VersionStorageInfo* base_vstorage) {
    auto vstorage = std::make_unique<VersionStorageInfo>(
        &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
        base_vstorage, force_consistency_checks,
        EpochNumberRequirement::kMightMissing, nullptr, 0,
        OffpeakTimeOption(options_.daily_offpeak_time_utc),
        PeriodicCompactionPhaseParams{});
    EXPECT_OK(builder->SaveTo(vstorage.get()));
    UpdateVersionStorageInfo(vstorage.get());
    return vstorage;
  };

  // Blob file 5 is older than every blob file a table file references and
  // blob file 10 is still in the version, so only blob file 12 stays dropped.
  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);
  VersionEdit edit;
  for (const uint64_t blob_file_number :
       std::initializer_list<uint64_t>{5, 12, 10, 12}) {
    edit.AddDroppedBlobFile(blob_file_number);
  }
  ASSERT_OK(builder.Apply(&edit));
  const std::unique_ptr<VersionStorageInfo> new_vstorage =
      save_new_version(&builder, &vstorage_);
  ASSERT_EQ(new_vstorage->GetDroppedBlobFiles(), std::vector<uint64_t>{12});
  ASSERT_TRUE(new_vstorage->IsDroppedBlobFile(12));
  ASSERT_FALSE(new_vstorage->IsDroppedBlobFile(10));

  // Dropped blob files carry over while a table file can reference them.
  VersionBuilder second_builder(env_options, &ioptions_, table_cache,
                                new_vstorage.get(), version_set);
  VersionEdit second_edit;
  AddDummyFileToEdit(&second_edit, /* table_file_number */ 2,
                     /* blob_file_number */ 10, /* epoch_number */ 2);
  ASSERT_OK(second_builder.Apply(&second_edit));
  const std::unique_ptr<VersionStorageInfo> second_vstorage =
      save_new_version(&second_builder, new_vstorage.get());
  ASSERT_EQ(second_vstorage->GetDroppedBlobFiles(), std::vector<uint64_t>{12});

  // Once every table file only references newer blob files, blob file 12 can
  // no longer be referenced and is forgotten.
  VersionBuilder third_builder(env_options, &ioptions_, table_cache,
                               second_vstorage.get(), version_set);
  VersionEdit third_edit;
  third_edit.DeleteFile(/* level */ 0, /* file_number */ 1);
  third_edit.DeleteFile(/* level */ 0, /* file_number */ 2);
  third_edit.AddBlobFile(/* blob_file_number */ 20, /* total_blob_count */ 100,
                         /* total_blob_bytes */ 1000,
                         /* checksum_method */ std::string(),
                         /* checksum_value */ std::string());
  AddDummyFileToEdit(&third_edit, /* table_file_number */ 3,
                     /* blob_file_number */ 20, /* epoch_number */ 3);
  ASSERT_OK(third_builder.Apply(&third_edit));
  const std::unique_ptr<VersionStorageInfo> third_vstorage =
      save_new_version(&third_builder, second_vstorage.get());
  ASSERT_TRUE(third_vstorage->GetDroppedBlobFiles().empty());

  UnrefFilesInVersion(third_vstorage.get());
  UnrefFilesInVersion(second_vstorage.get());
  UnrefFilesInVersion(new_vstorage.get());
}

TEST_F(VersionBuilderTest, BlobFileGarbageOverflow) {
  // Test that VersionEdits that would result in the count/total size of garbage
  // exceeding the count/total size of all blobs are rejected.

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit addition;

  constexpr uint64_t blob_file_number = 1234;
  constexpr uint64_t total_blob_count = 5678;
  constexpr uint64_t total_blob_bytes = 999999;
  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] =
      "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c\x52"
      "\x5c\xbd";

  addition.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                       checksum_method, checksum_value);

  // Add dummy table file to ensure the blob file is referenced.
  constexpr uint64_t table_file_number = 1;
  AddDummyFileToEdit(&addition, table_file_number, blob_file_number,
                     1 /*epoch_number*/);

  ASSERT_OK(builder.Apply(&addition));

  {
    // Garbage blob count overflow
    constexpr uint64_t garbage_blob_count = 5679;
    constexpr uint64_t garbage_blob_bytes = 999999;

    VersionEdit garbage;

    garbage.AddBlobFileGarbage(blob_file_number, garbage_blob_count,
                               garbage_blob_bytes);

    const Status s = builder.Apply(&garbage);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(
        std::strstr(s.getState(), "Garbage overflow for blob file #1234"));
  }

  {
    // Garbage blob bytes overflow
    constexpr uint64_t garbage_blob_count = 5678;
    constexpr uint64_t garbage_blob_bytes = 1000000;

    VersionEdit garbage;

    garbage.AddBlobFileGarbage(blob_file_number, garbage_blob_count,
                               garbage_blob_bytes);

    const Status s = builder.Apply(&garbage);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(
        std::strstr(s.getState(), "Garbage overflow for blob file #1234"));
  }
}

TEST_F(VersionBuilderTest, SaveBlobFilesTo) {
  // Add three blob files to base version.
  for (uint64_t i = 1; i <= 3; ++i) {
    const uint64_t table_file_number = 2 * i;
    const uint64_t blob_file_number = 2 * i + 1;
    const uint64_t total_blob_count = i * 1000;
    const uint64_t total_blob_bytes = i * 1000000;
    const uint64_t garbage_blob_count = i * 100;
    const uint64_t garbage_blob_bytes = i * 20000;

    AddBlob(blob_file_number, total_blob_count, total_blob_bytes,
            /* checksum_method */ std::string(),
            /* checksum_value */ std::string(),
            BlobFileMetaData::LinkedSsts{table_file_number}, garbage_blob_count,
            garbage_blob_bytes);
  }

  // Add dummy table files to ensure the blob files are referenced.
  // Note: files are added to L0, so they have to be added in reverse order
  // (newest first).
  for (uint64_t i = 3; i >= 1; --i) {
    const uint64_t table_file_number = 2 * i;
    const uint64_t blob_file_number = 2 * i + 1;

    AddDummyFile(table_file_number, blob_file_number, i /*epoch_number*/);
  }

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  // Add some garbage to the second and third blob files. The second blob file
  // remains valid since it does not consist entirely of garbage yet. The third
  // blob file is all garbage after the edit and will not be part of the new
  // version. The corresponding dummy table file is also removed for
  // consistency.
  edit.AddBlobFileGarbage(/* blob_file_number */ 5,
                          /* garbage_blob_count */ 200,
                          /* garbage_blob_bytes */ 100000);
  edit.AddBlobFileGarbage(/* blob_file_number */ 7,
                          /* garbage_blob_count */ 2700,
                          /* garbage_blob_bytes */ 2940000);
  edit.DeleteFile(/* level */ 0, /* file_number */ 6);

  // Add a fourth blob file.
  edit.AddBlobFile(/* blob_file_number */ 9, /* total_blob_count */ 4000,
                   /* total_blob_bytes */ 4000000,
                   /* checksum_method */ std::string(),
                   /* checksum_value */ std::string());

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 3);

  const auto meta3 = new_vstorage.GetBlobFileMetaData(/* blob_file_number */ 3);

  ASSERT_NE(meta3, nullptr);
  ASSERT_EQ(meta3->GetBlobFileNumber(), 3);
  ASSERT_EQ(meta3->GetTotalBlobCount(), 1000);
  ASSERT_EQ(meta3->GetTotalBlobBytes(), 1000000);
  ASSERT_EQ(meta3->GetGarbageBlobCount(), 100);
  ASSERT_EQ(meta3->GetGarbageBlobBytes(), 20000);

  const auto meta5 = new_vstorage.GetBlobFileMetaData(/* blob_file_number */ 5);

  ASSERT_NE(meta5, nullptr);
  ASSERT_EQ(meta5->GetBlobFileNumber(), 5);
  ASSERT_EQ(meta5->GetTotalBlobCount(), 2000);
  ASSERT_EQ(meta5->GetTotalBlobBytes(), 2000000);
  ASSERT_EQ(meta5->GetGarbageBlobCount(), 400);
  ASSERT_EQ(meta5->GetGarbageBlobBytes(), 140000);

  const auto meta9 = new_vstorage.GetBlobFileMetaData(/* blob_file_number */ 9);

  ASSERT_NE(meta9, nullptr);
  ASSERT_EQ(meta9->GetBlobFileNumber(), 9);
  ASSERT_EQ(meta9->GetTotalBlobCount(), 4000);
  ASSERT_EQ(meta9->GetTotalBlobBytes(), 4000000);
  ASSERT_EQ(meta9->GetGarbageBlobCount(), 0);
  ASSERT_EQ(meta9->GetGarbageBlobBytes(), 0);

  // Delete the first table file, which makes the first blob file obsolete
  // since it's at the head and unreferenced.
  VersionBuilder second_builder(env_options, &ioptions_, table_cache,
                                &new_vstorage, version_set);

  VersionEdit second_edit;
  second_edit.DeleteFile(/* level */ 0, /* file_number */ 2);

  ASSERT_OK(second_builder.Apply(&second_edit));

  VersionStorageInfo new_vstorage_2(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &new_vstorage,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(second_builder.SaveTo(&new_vstorage_2));

  UpdateVersionStorageInfo(&new_vstorage_2);

  const auto& newer_blob_files = new_vstorage_2.GetBlobFiles();
  ASSERT_EQ(newer_blob_files.size(), 2);

  const auto newer_meta3 =
      new_vstorage_2.GetBlobFileMetaData(/* blob_file_number */ 3);

  ASSERT_EQ(newer_meta3, nullptr);

  // Blob file #5 is referenced by table file #4, and blob file #9 is
  // unreferenced. After deleting table file #4, all blob files will become
  // unreferenced and will therefore be obsolete.
  VersionBuilder third_builder(env_options, &ioptions_, table_cache,
                               &new_vstorage_2, version_set);
  VersionEdit third_edit;
  third_edit.DeleteFile(/* level */ 0, /* file_number */ 4);

  ASSERT_OK(third_builder.Apply(&third_edit));

  VersionStorageInfo new_vstorage_3(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
      &new_vstorage_2, force_consistency_checks,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(third_builder.SaveTo(&new_vstorage_3));

  UpdateVersionStorageInfo(&new_vstorage_3);

  ASSERT_TRUE(new_vstorage_3.GetBlobFiles().empty());

  UnrefFilesInVersion(&new_vstorage_3);
  UnrefFilesInVersion(&new_vstorage_2);
  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, SaveBlobFilesToConcurrentJobs) {
  // When multiple background jobs (flushes/compactions) are executing in
  // parallel, it is possible for the VersionEdit adding blob file K to be
  // applied *after* the VersionEdit adding blob file N (for N > K). This test
  // case makes sure this is handled correctly.

  // Add blob file #4 (referenced by table file #3) to base version.
  constexpr uint64_t base_table_file_number = 3;
  constexpr uint64_t base_blob_file_number = 4;
  constexpr uint64_t base_total_blob_count = 100;
  constexpr uint64_t base_total_blob_bytes = 1 << 20;

  constexpr char checksum_method[] = "SHA1";
  constexpr char checksum_value[] = "\xfa\xce\xb0\x0c";
  constexpr uint64_t garbage_blob_count = 0;
  constexpr uint64_t garbage_blob_bytes = 0;

  AddDummyFile(base_table_file_number, base_blob_file_number,
               1 /*epoch_number*/);
  AddBlob(base_blob_file_number, base_total_blob_count, base_total_blob_bytes,
          checksum_method, checksum_value,
          BlobFileMetaData::LinkedSsts{base_table_file_number},
          garbage_blob_count, garbage_blob_bytes);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  // Add blob file #2 (referenced by table file #1).
  constexpr int level = 0;
  constexpr uint64_t table_file_number = 1;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 1 << 12;
  constexpr char smallest[] = "key1";
  constexpr char largest[] = "key987";
  constexpr SequenceNumber smallest_seqno = 0;
  constexpr SequenceNumber largest_seqno = 0;
  constexpr bool marked_for_compaction = false;

  constexpr uint64_t blob_file_number = 2;
  static_assert(blob_file_number < base_blob_file_number,
                "Added blob file should have a smaller file number");

  constexpr uint64_t total_blob_count = 234;
  constexpr uint64_t total_blob_bytes = 1 << 22;

  edit.AddFile(
      level, table_file_number, path_id, file_size, GetInternalKey(smallest),
      GetInternalKey(largest), smallest_seqno, largest_seqno,
      marked_for_compaction, Temperature::kUnknown, blob_file_number,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime, 2 /*epoch_number*/,
      checksum_value, checksum_method, kNullUniqueId64x2, 0, 0,
      /* user_defined_timestamps_persisted */ true);
  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  ASSERT_OK(builder.Apply(&edit));

  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  const auto& new_blob_files = new_vstorage.GetBlobFiles();
  ASSERT_EQ(new_blob_files.size(), 2);

  const auto base_meta =
      new_vstorage.GetBlobFileMetaData(base_blob_file_number);

  ASSERT_NE(base_meta, nullptr);
  ASSERT_EQ(base_meta->GetBlobFileNumber(), base_blob_file_number);
  ASSERT_EQ(base_meta->GetTotalBlobCount(), base_total_blob_count);
  ASSERT_EQ(base_meta->GetTotalBlobBytes(), base_total_blob_bytes);
  ASSERT_EQ(base_meta->GetGarbageBlobCount(), garbage_blob_count);
  ASSERT_EQ(base_meta->GetGarbageBlobBytes(), garbage_blob_bytes);
  ASSERT_EQ(base_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(base_meta->GetChecksumValue(), checksum_value);

  const auto added_meta = new_vstorage.GetBlobFileMetaData(blob_file_number);

  ASSERT_NE(added_meta, nullptr);
  ASSERT_EQ(added_meta->GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(added_meta->GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(added_meta->GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(added_meta->GetGarbageBlobCount(), garbage_blob_count);
  ASSERT_EQ(added_meta->GetGarbageBlobBytes(), garbage_blob_bytes);
  ASSERT_EQ(added_meta->GetChecksumMethod(), checksum_method);
  ASSERT_EQ(added_meta->GetChecksumValue(), checksum_value);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForBlobFiles) {
  // Initialize base version. The first table file points to a valid blob file
  // in this version; the second one does not refer to any blob files.

  Add(/* level */ 1, /* file_number */ 1, /* smallest */ "150",
      /* largest */ "200", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 100, /* largest_seq */ 100,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 100, /* largest_seqno */ 100,
      /* oldest_blob_file_number */ 16);
  Add(/* level */ 1, /* file_number */ 23, /* smallest */ "201",
      /* largest */ "300", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 200, /* largest_seq */ 200,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 200, /* largest_seqno */ 200,
      kInvalidBlobFileNumber);

  AddBlob(/* blob_file_number */ 16, /* total_blob_count */ 1000,
          /* total_blob_bytes */ 1000000,
          /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 500, /* garbage_blob_bytes */ 300000);

  UpdateVersionStorageInfo();

  // Add a new table file that points to the existing blob file, and add a
  // new table file--blob file pair.
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  edit.AddFile(/* level */ 1, /* file_number */ 606, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("701"),
               /* largest */ GetInternalKey("750"), /* smallest_seqno */ 200,
               /* largest_seqno */ 200, /* marked_for_compaction */ false,
               Temperature::kUnknown,
               /* oldest_blob_file_number */ 16, kUnknownOldestAncestorTime,
               kUnknownFileCreationTime, kUnknownEpochNumber,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName,
               kNullUniqueId64x2, 0, 0,
               /* user_defined_timestamps_persisted */ true);

  edit.AddFile(/* level */ 1, /* file_number */ 700, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("801"),
               /* largest */ GetInternalKey("850"), /* smallest_seqno */ 200,
               /* largest_seqno */ 200, /* marked_for_compaction */ false,
               Temperature::kUnknown,
               /* oldest_blob_file_number */ 1000, kUnknownOldestAncestorTime,
               kUnknownFileCreationTime, kUnknownEpochNumber,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName,
               kNullUniqueId64x2, 0, 0,
               /* user_defined_timestamps_persisted */ true);
  edit.AddBlobFile(/* blob_file_number */ 1000, /* total_blob_count */ 2000,
                   /* total_blob_bytes */ 200000,
                   /* checksum_method */ std::string(),
                   /* checksum_value */ std::string());

  ASSERT_OK(builder.Apply(&edit));

  // Save to a new version in order to trigger consistency checks.
  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForBlobFilesInconsistentLinks) {
  // Initialize base version. Links between the table file and the blob file
  // are inconsistent.

  Add(/* level */ 1, /* file_number */ 1, /* smallest */ "150",
      /* largest */ "200", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 100, /* largest_seq */ 100,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 100, /* largest_seqno */ 100,
      /* oldest_blob_file_number */ 256);

  AddBlob(/* blob_file_number */ 16, /* total_blob_count */ 1000,
          /* total_blob_bytes */ 1000000,
          /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 500, /* garbage_blob_bytes */ 300000);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  // Save to a new version in order to trigger consistency checks.
  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  const Status s = builder.SaveTo(&new_vstorage);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(
      s.getState(),
      "Links are inconsistent between table files and blob file #16"));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForBlobFilesAllGarbage) {
  // Initialize base version. The table file points to a blob file that is
  // all garbage.

  Add(/* level */ 1, /* file_number */ 1, /* smallest */ "150",
      /* largest */ "200", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 100, /* largest_seq */ 100,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 100, /* largest_seqno */ 100,
      /* oldest_blob_file_number */ 16);

  AddBlob(/* blob_file_number */ 16, /* total_blob_count */ 1000,
          /* total_blob_bytes */ 1000000,
          /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 1000, /* garbage_blob_bytes */ 1000000);

  UpdateVersionStorageInfo();

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  // Save to a new version in order to trigger consistency checks.
  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  const Status s = builder.SaveTo(&new_vstorage);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(
      std::strstr(s.getState(), "Blob file #16 consists entirely of garbage"));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForBlobFilesAllGarbageLinkedSsts) {
  // Initialize base version, with a table file pointing to a blob file
  // that has no garbage at this point.

  Add(/* level */ 1, /* file_number */ 1, /* smallest */ "150",
      /* largest */ "200", /* file_size */ 100,
      /* path_id */ 0, /* smallest_seq */ 100, /* largest_seq */ 100,
      /* num_entries */ 0, /* num_deletions */ 0,
      /* sampled */ false, /* smallest_seqno */ 100, /* largest_seqno */ 100,
      /* oldest_blob_file_number */ 16);

  AddBlob(/* blob_file_number */ 16, /* total_blob_count */ 1000,
          /* total_blob_bytes */ 1000000,
          /* checksum_method */ std::string(),
          /* checksum_value */ std::string(), BlobFileMetaData::LinkedSsts{1},
          /* garbage_blob_count */ 0, /* garbage_blob_bytes */ 0);

  UpdateVersionStorageInfo();

  // Mark the entire blob file garbage but do not remove the linked SST.
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  VersionEdit edit;

  edit.AddBlobFileGarbage(/* blob_file_number */ 16,
                          /* garbage_blob_count */ 1000,
                          /* garbage_blob_bytes */ 1000000);

  ASSERT_OK(builder.Apply(&edit));

  // Save to a new version in order to trigger consistency checks.
  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  const Status s = builder.SaveTo(&new_vstorage);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(
      std::strstr(s.getState(), "Blob file #16 consists entirely of garbage"));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, MaintainLinkedSstsForBlobFiles) {
  // Initialize base version. Table files 1..10 are linked to blob files 1..5,
  // while table files 11..20 are not linked to any blob files.

  for (uint64_t i = 1; i <= 10; ++i) {
    std::ostringstream oss;
    oss << std::setw(2) << std::setfill('0') << i;

    const std::string key = oss.str();

    Add(/* level */ 1, /* file_number */ i, /* smallest */ key.c_str(),
        /* largest */ key.c_str(), /* file_size */ 100,
        /* path_id */ 0, /* smallest_seq */ i * 100, /* largest_seq */ i * 100,
        /* num_entries */ 0, /* num_deletions */ 0,
        /* sampled */ false, /* smallest_seqno */ i * 100,
        /* largest_seqno */ i * 100,
        /* oldest_blob_file_number */ ((i - 1) % 5) + 1);
  }

  for (uint64_t i = 1; i <= 5; ++i) {
    AddBlob(/* blob_file_number */ i, /* total_blob_count */ 2000,
            /* total_blob_bytes */ 2000000,
            /* checksum_method */ std::string(),
            /* checksum_value */ std::string(),
            BlobFileMetaData::LinkedSsts{i, i + 5},
            /* garbage_blob_count */ 1000, /* garbage_blob_bytes */ 1000000);
  }

  for (uint64_t i = 11; i <= 20; ++i) {
    std::ostringstream oss;
    oss << std::setw(2) << std::setfill('0') << i;

    const std::string key = oss.str();

    Add(/* level */ 1, /* file_number */ i, /* smallest */ key.c_str(),
        /* largest */ key.c_str(), /* file_size */ 100,
        /* path_id */ 0, /* smallest_seq */ i * 100, /* largest_seq */ i * 100,
        /* num_entries */ 0, /* num_deletions */ 0,
        /* sampled */ false, /* smallest_seqno */ i * 100,
        /* largest_seqno */ i * 100, kInvalidBlobFileNumber);
  }

  UpdateVersionStorageInfo();

  {
    const auto& blob_files = vstorage_.GetBlobFiles();
    ASSERT_EQ(blob_files.size(), 5);

    const std::vector<BlobFileMetaData::LinkedSsts> expected_linked_ssts{
        {1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10}};

    for (size_t i = 0; i < 5; ++i) {
      const auto meta =
          vstorage_.GetBlobFileMetaData(/* blob_file_number */ i + 1);
      ASSERT_NE(meta, nullptr);
      ASSERT_EQ(meta->GetLinkedSsts(), expected_linked_ssts[i]);
    }
  }

  VersionEdit edit;

  // Add an SST that references a blob file.
  edit.AddFile(
      /* level */ 1, /* file_number */ 21, /* path_id */ 0,
      /* file_size */ 100, /* smallest */ GetInternalKey("21", 2100),
      /* largest */ GetInternalKey("21", 2100), /* smallest_seqno */ 2100,
      /* largest_seqno */ 2100, /* marked_for_compaction */ false,
      Temperature::kUnknown,
      /* oldest_blob_file_number */ 1, kUnknownOldestAncestorTime,
      kUnknownFileCreationTime, kUnknownEpochNumber, kUnknownFileChecksum,
      kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
      /* user_defined_timestamps_persisted */ true);

  // Add an SST that does not reference any blob files.
  edit.AddFile(
      /* level */ 1, /* file_number */ 22, /* path_id */ 0,
      /* file_size */ 100, /* smallest */ GetInternalKey("22", 2200),
      /* largest */ GetInternalKey("22", 2200), /* smallest_seqno */ 2200,
      /* largest_seqno */ 2200, /* marked_for_compaction */ false,
      Temperature::kUnknown, kInvalidBlobFileNumber, kUnknownOldestAncestorTime,
      kUnknownFileCreationTime, kUnknownEpochNumber, kUnknownFileChecksum,
      kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
      /* user_defined_timestamps_persisted */ true);

  // Delete a file that references a blob file.
  edit.DeleteFile(/* level */ 1, /* file_number */ 6);

  // Delete a file that does not reference any blob files.
  edit.DeleteFile(/* level */ 1, /* file_number */ 16);

  // Trivially move a file that references a blob file. Note that we save
  // the original BlobFileMetaData object so we can check that no new object
  // gets created.
  auto meta3 = vstorage_.GetBlobFileMetaData(/* blob_file_number */ 3);

  edit.DeleteFile(/* level */ 1, /* file_number */ 3);
  edit.AddFile(/* level */ 2, /* file_number */ 3, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("03", 300),
               /* largest */ GetInternalKey("03", 300),
               /* smallest_seqno */ 300,
               /* largest_seqno */ 300, /* marked_for_compaction */ false,
               Temperature::kUnknown,
               /* oldest_blob_file_number */ 3, kUnknownOldestAncestorTime,
               kUnknownFileCreationTime, kUnknownEpochNumber,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName,
               kNullUniqueId64x2, 0, 0,
               /* user_defined_timestamps_persisted */ true);

  // Trivially move a file that does not reference any blob files.
  edit.DeleteFile(/* level */ 1, /* file_number */ 13);
  edit.AddFile(/* level */ 2, /* file_number */ 13, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("13", 1300),
               /* largest */ GetInternalKey("13", 1300),
               /* smallest_seqno */ 1300,
               /* largest_seqno */ 1300, /* marked_for_compaction */ false,
               Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               kUnknownEpochNumber, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
               /* user_defined_timestamps_persisted */ true);

  // Add one more SST file that references a blob file, then promptly
  // delete it in a second version edit before the new version gets saved.
  // This file should not show up as linked to the blob file in the new version.
  edit.AddFile(/* level */ 1, /* file_number */ 23, /* path_id */ 0,
               /* file_size */ 100, /* smallest */ GetInternalKey("23", 2300),
               /* largest */ GetInternalKey("23", 2300),
               /* smallest_seqno */ 2300,
               /* largest_seqno */ 2300, /* marked_for_compaction */ false,
               Temperature::kUnknown,
               /* oldest_blob_file_number */ 5, kUnknownOldestAncestorTime,
               kUnknownFileCreationTime, kUnknownEpochNumber,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName,
               kNullUniqueId64x2, 0, 0,
               /* user_defined_timestamps_persisted */ true);

  VersionEdit edit2;

  edit2.DeleteFile(/* level */ 1, /* file_number */ 23);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);

  ASSERT_OK(builder.Apply(&edit));
  ASSERT_OK(builder.Apply(&edit2));

  constexpr bool force_consistency_checks = true;
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      force_consistency_checks, EpochNumberRequirement::kMightMissing, nullptr,
      0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  {
    const auto& blob_files = new_vstorage.GetBlobFiles();
    ASSERT_EQ(blob_files.size(), 5);

    const std::vector<BlobFileMetaData::LinkedSsts> expected_linked_ssts{
        {1, 21}, {2, 7}, {3, 8}, {4, 9}, {5, 10}};

    for (size_t i = 0; i < 5; ++i) {
      const auto meta =
          new_vstorage.GetBlobFileMetaData(/* blob_file_number */ i + 1);
      ASSERT_NE(meta, nullptr);
      ASSERT_EQ(meta->GetLinkedSsts(), expected_linked_ssts[i]);
    }

    // Make sure that no new BlobFileMetaData got created for the blob file
    // affected by the trivial move.
    ASSERT_EQ(new_vstorage.GetBlobFileMetaData(/* blob_file_number */ 3),
              meta3);
  }

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForFileDeletedTwice) {
  Add(0, 1U, "150", "200", 100, /*path_id*/ 0,
      /*smallest_seq*/ 100, /*largest_seq*/ 100,
      /*num_entries*/ 0, /*num_deletions*/ 0,
      /*sampled*/ false, /*smallest_seqno*/ 0,
      /*largest_seqno*/ 0,
      /*oldest_blob_file_number*/ kInvalidBlobFileNumber,
      /*epoch_number*/ 1);

  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.DeleteFile(0, 1U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;
  constexpr VersionSet* version_set = nullptr;

  VersionBuilder version_builder(env_options, &ioptions_, table_cache,
                                 &vstorage_, version_set);
  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, nullptr,
      true /* force_consistency_checks */,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  UpdateVersionStorageInfo(&new_vstorage);

  VersionBuilder version_builder2(env_options, &ioptions_, table_cache,
                                  &new_vstorage, version_set);
  VersionStorageInfo new_vstorage2(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, nullptr,
      true /* force_consistency_checks */,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_NOK(version_builder2.Apply(&version_edit));

  UnrefFilesInVersion(&new_vstorage);
  UnrefFilesInVersion(&new_vstorage2);
}

TEST_F(VersionBuilderTest, CheckConsistencyForL0FilesSortedByEpochNumber) {
  Status s;
  // To verify files of same epoch number of overlapping ranges are caught as
  // corrupted
  VersionEdit version_edit_1;
  version_edit_1.AddFile(
      /* level */ 0, /* file_number */ 1U, /* path_id */ 0,
      /* file_size */ 100, /* smallest */ GetInternalKey("a", 1),
      /* largest */ GetInternalKey("c", 3), /* smallest_seqno */ 1,
      /* largest_seqno */ 3, /* marked_for_compaction */ false,
      Temperature::kUnknown,
      /* oldest_blob_file_number */ kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime,
      1 /* epoch_number */, kUnknownFileChecksum, kUnknownFileChecksumFuncName,
      kNullUniqueId64x2, 0, 0, /* user_defined_timestamps_persisted */ true);
  version_edit_1.AddFile(
      /* level */ 0, /* file_number */ 2U, /* path_id */ 0,
      /* file_size */ 100, /* smallest */ GetInternalKey("b", 2),
      /* largest */ GetInternalKey("d", 4), /* smallest_seqno */ 2,
      /* largest_seqno */ 4, /* marked_for_compaction */ false,
      Temperature::kUnknown,
      /* oldest_blob_file_number */ kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime,
      1 /* epoch_number */, kUnknownFileChecksum, kUnknownFileChecksumFuncName,
      kNullUniqueId64x2, 0, 0, /* user_defined_timestamps_persisted */ true);

  VersionBuilder version_builder_1(EnvOptions(), &ioptions_,
                                   nullptr /* table_cache */, &vstorage_,
                                   nullptr /* file_metadata_cache_res_mgr */);
  VersionStorageInfo new_vstorage_1(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
      nullptr /* src_vstorage */, true /* force_consistency_checks */,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(version_builder_1.Apply(&version_edit_1));
  s = version_builder_1.SaveTo(&new_vstorage_1);
  EXPECT_TRUE(s.IsCorruption());
  EXPECT_TRUE(std::strstr(
      s.getState(), "L0 files of same epoch number but overlapping range"));
  UnrefFilesInVersion(&new_vstorage_1);

  // To verify L0 files not sorted by epoch_number are caught as corrupted
  VersionEdit version_edit_2;
  version_edit_2.AddFile(
      /* level */ 0, /* file_number */ 1U, /* path_id */ 0,
      /* file_size */ 100, /* smallest */ GetInternalKey("a", 1),
      /* largest */ GetInternalKey("a", 1), /* smallest_seqno */ 1,
      /* largest_seqno */ 1, /* marked_for_compaction */ false,
      Temperature::kUnknown,
      /* oldest_blob_file_number */ kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime,
      1 /* epoch_number */, kUnknownFileChecksum, kUnknownFileChecksumFuncName,
      kNullUniqueId64x2, 0, 0, /* user_defined_timestamps_persisted */ true);
  version_edit_2.AddFile(
      /* level */ 0, /* file_number */ 2U, /* path_id */ 0,
      /* file_size */ 100, /* smallest */ GetInternalKey("b", 2),
      /* largest */ GetInternalKey("b", 2), /* smallest_seqno */ 2,
      /* largest_seqno */ 2, /* marked_for_compaction */ false,
      Temperature::kUnknown,
      /* oldest_blob_file_number */ kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime,
      2 /* epoch_number */, kUnknownFileChecksum, kUnknownFileChecksumFuncName,
      kNullUniqueId64x2, 0, 0, /* user_defined_timestamps_persisted */ true);

  VersionBuilder version_builder_2(EnvOptions(), &ioptions_,
                                   nullptr /* table_cache */, &vstorage_,
                                   nullptr /* file_metadata_cache_res_mgr */);
  VersionStorageInfo new_vstorage_2(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
      nullptr /* src_vstorage */, true /* force_consistency_checks */,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});

  ASSERT_OK(version_builder_2.Apply(&version_edit_2));
  s = version_builder_2.SaveTo(&new_vstorage_2);
  ASSERT_TRUE(s.ok());

  const std::vector<FileMetaData*>& l0_files = new_vstorage_2.LevelFiles(0);
  ASSERT_EQ(l0_files.size(), 2);
  // Manually corrupt L0 files's epoch_number
  l0_files[0]->epoch_number = 1;
  l0_files[1]->epoch_number = 2;

  // To surface corruption error by applying dummy version edit
  VersionEdit dummy_version_edit;
  VersionBuilder dummy_version_builder(
      EnvOptions(), &ioptions_, nullptr /* table_cache */, &vstorage_,
      nullptr /* file_metadata_cache_res_mgr */);
  ASSERT_OK(dummy_version_builder.Apply(&dummy_version_edit));
  s = dummy_version_builder.SaveTo(&new_vstorage_2);
  EXPECT_TRUE(s.IsCorruption());
  EXPECT_TRUE(std::strstr(s.getState(), "L0 files are not sorted properly"));

  UnrefFilesInVersion(&new_vstorage_2);
}

TEST_F(VersionBuilderTest, EstimatedActiveKeys) {
  const uint32_t kTotalSamples = 20;
  const uint32_t kNumLevels = 5;
  const uint32_t kFilesPerLevel = 8;
  const uint32_t kNumFiles = kNumLevels * kFilesPerLevel;
  const uint32_t kEntriesPerFile = 1000;
  const uint32_t kDeletionsPerFile = 100;
  for (uint32_t i = 0; i < kNumFiles; ++i) {
    Add(static_cast<int>(i / kFilesPerLevel), i + 1,
        std::to_string((i + 100) * 1000).c_str(),
        std::to_string((i + 100) * 1000 + 999).c_str(), 100U, 0, 100, 100,
        kEntriesPerFile, kDeletionsPerFile, (i < kTotalSamples));
  }
  // minus 2X for the number of deletion entries because:
  // 1x for deletion entry does not count as a data entry.
  // 1x for each deletion entry will actually remove one data entry.
  ASSERT_EQ(vstorage_.GetEstimatedActiveKeys(),
            (kEntriesPerFile - 2 * kDeletionsPerFile) * kNumFiles);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
