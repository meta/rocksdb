//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/version_builder.h"

#include <atomic>
#include <cstring>
#include <functional>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>

#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/db_test_util.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/sst_file_writer.h"
#include "table/unique_id_impl.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
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

  // Builds the addition record of relocation file `file_number`. The file holds
  // one value from each origin in `origin_file_numbers`. Its served files (the
  // old file numbers that may be routed to it) are those origins plus
  // `extra_generations`.
  Status MakeRelocationAddition(
      uint64_t file_number, const std::vector<uint64_t>& origin_file_numbers,
      BlobRelocationFileAddition* addition,
      const std::vector<uint64_t>& extra_generations = {}) {
    if (addition == nullptr) {
      return Status::InvalidArgument("Null relocation addition output");
    }
    BlobRelocationDirectoryBuilder builder;
    for (uint64_t origin_file_number : origin_file_numbers) {
      Status status = builder.Add(origin_file_number,
                                  /*original_value_offset=*/11,
                                  /*stored_value_size=*/17, kNoCompression);
      if (status.ok()) {
        status = builder.AddServedFile(origin_file_number);
      }
      for (uint64_t generation : extra_generations) {
        if (status.ok()) {
          status = builder.AddServedFile(generation);
        }
      }
      if (!status.ok()) {
        return status;
      }
    }
    BlobRelocationDirectory directory;
    Status status = builder.Finish(&directory);
    if (!status.ok()) {
      return status;
    }
    std::string encoded_directory;
    directory.EncodeTo(&encoded_directory);
    *addition = BlobRelocationFileAddition(
        file_number, /*file_size=*/4096, "crc32c", "checksum",
        std::move(encoded_directory), /*tail_size=*/1);
    return addition->Validate();
  }

  Status AddRelocationToBase(const BlobRelocationFileAddition& addition) {
    Status status = addition.Validate();
    if (!status.ok()) {
      return status;
    }
    vstorage_.SetBlobIndirectionFormatVersion(
        kBlobIndirectionManifestFormatVersion);
    vstorage_.AddBlobRelocationFile(
        BlobRelocationFileMetaData::Create(addition));
    return Status::OK();
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

  void AddBlobReferenceDescriptorToLastFile(VersionEdit* edit) {
    assert(edit != nullptr);
    assert(!edit->GetMutableNewFiles().empty());
    BlobReferenceMetadataDescriptor descriptor;
    descriptor.column_family_id = edit->GetColumnFamily();
    descriptor.sst_file_number =
        edit->GetMutableNewFiles().back().second.fd.GetNumber();
    descriptor.index_handle = BlockHandle(/*offset=*/10, /*size=*/20);
    descriptor.EncodeTo(
        &edit->GetMutableNewFiles().back().second.blob_reference_descriptor);
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

TEST_F(VersionBuilderTest, ApplyChecksBaseConsistencyOnce) {
  // The base Version does not change during a builder's life. So once the check
  // passes, the builder does not check it again, even when it applies many
  // edits.
  Add(2, 1U, "150", "200", 100U);
  UpdateVersionStorageInfo();
  std::atomic<int> base_checks{0};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::Apply:CheckBaseConsistency",
      [&](void*) { ++base_checks; });
  SyncPoint::GetInstance()->EnableProcessing();

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, &ioptions_,
                                 /*table_cache=*/nullptr, &vstorage_,
                                 /*version_set=*/nullptr);
  for (uint64_t i = 0; i < 5; ++i) {
    VersionEdit edit;
    const std::string smallest = std::to_string(300 + 100 * i);
    const std::string largest = std::to_string(350 + 100 * i);
    edit.AddFile(2, 10 + i, 0, 100U, GetInternalKey(smallest.c_str()),
                 GetInternalKey(largest.c_str()), 200, 200, false,
                 Temperature::kUnknown, kInvalidBlobFileNumber,
                 kUnknownOldestAncestorTime, kUnknownFileCreationTime,
                 kUnknownEpochNumber, kUnknownFileChecksum,
                 kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
                 /* user_defined_timestamps_persisted */ true);
    ASSERT_OK(version_builder.Apply(&edit));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_EQ(base_checks.load(), 1);

  VersionStorageInfo new_vstorage(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, nullptr, false,
      EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));
  UpdateVersionStorageInfo(&new_vstorage);
  EXPECT_EQ(new_vstorage.NumLevelFiles(2), 6);
  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyReportsInconsistentBaseOnEveryApply) {
  // Overlapping L1 files make the base inconsistent. Every Apply reports it,
  // because a failed check is not remembered as passed.
  Add(1, 1U, "100", "300", 100U);
  Add(1, 2U, "200", "400", 100U);
  EnvOptions env_options;
  VersionBuilder version_builder(env_options, &ioptions_,
                                 /*table_cache=*/nullptr, &vstorage_,
                                 /*version_set=*/nullptr);
  VersionEdit edit;
  ASSERT_EQ(version_builder.Apply(&edit).code(), Status::kCorruption);
  ASSERT_EQ(version_builder.Apply(&edit).code(), Status::kCorruption);
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

TEST_F(VersionBuilderTest, IndirectOriginBlobFileRequiresFormatFence) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionEdit addition;
  addition.AddBlobFile(/*blob_file_number=*/100, /*total_blob_count=*/1,
                       /*total_blob_bytes=*/10, /*checksum_method=*/{},
                       /*checksum_value=*/{}, /*is_indirect_origin=*/true);
  AddDummyFileToEdit(&addition, /*table_file_number=*/101,
                     /*blob_file_number=*/100, /*epoch_number=*/1);

  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    ASSERT_TRUE(builder.Apply(&addition).IsCorruption());
  }

  addition.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  AddBlobReferenceDescriptorToLastFile(&addition);
  VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                         &vstorage_, /*version_set=*/nullptr);
  ASSERT_OK(builder.Apply(&addition));

  VersionStorageInfo output(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(builder.SaveTo(&output));
  const std::shared_ptr<BlobFileMetaData> meta =
      output.GetBlobFileMetaData(/*blob_file_number=*/100);
  ASSERT_NE(meta, nullptr);
  ASSERT_TRUE(meta->IsIndirectOrigin());
  ASSERT_OK(output.ValidateBlobRelocationState());
  UnrefFilesInVersion(&output);
}

TEST_F(VersionBuilderTest, FencedL0EpochRunRejectsNonNeighborOverlap) {
  // An L0 epoch run is a set of L0 files with one epoch number. The L0 order
  // puts the files of one epoch in largest-seqno order, not key order, and the
  // neighbor check compares only files next to each other. In this example,
  // the file numbers, keys and seqnos are only examples. Epoch 9 has three
  // files, newest first:
  //   file 11  keys [a..c]  largest seqno 300
  //   file 12  keys [m..p]  largest seqno 200
  //   file 13  keys [b..d]  largest seqno 100
  // Files 11 and 13 overlap, but they are not neighbors. With the format fence,
  // the Version must be rejected. Without the fence the check is skipped, as
  // in base RocksDB. A run whose files do not overlap installs with the fence.
  UpdateVersionStorageInfo();
  EnvOptions env_options;
  struct RunFile {
    uint64_t file_number;
    const char* smallest;
    const char* largest;
    SequenceNumber largest_seqno;
  };
  const auto try_install = [&](bool fenced, const std::vector<RunFile>& files) {
    VersionEdit edit;
    if (fenced) {
      edit.SetBlobIndirectionFormatVersion(
          kBlobIndirectionManifestFormatVersion);
    }
    for (const RunFile& file : files) {
      edit.AddFile(/*level=*/0, file.file_number, /*file_path_id=*/0,
                   /*file_size=*/100, GetInternalKey(file.smallest),
                   GetInternalKey(file.largest), /*smallest_seqno=*/50,
                   file.largest_seqno, /*marked_for_compaction=*/false,
                   Temperature::kUnknown, kInvalidBlobFileNumber,
                   kUnknownOldestAncestorTime, kUnknownFileCreationTime,
                   /*epoch_number=*/9, kUnknownFileChecksum,
                   kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
                   /*user_defined_timestamps_persisted=*/true);
      if (fenced) {
        edit.GetMutableNewFiles().back().second.blob_reference_legacy_metadata =
            true;
      }
    }
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    Status status = builder.Apply(&edit);
    if (!status.ok()) {
      return status;
    }
    VersionStorageInfo output(
        &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
        /*force_consistency_checks=*/true, EpochNumberRequirement::kMustPresent,
        /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
        OffpeakTimeOption(options_.daily_offpeak_time_utc),
        PeriodicCompactionPhaseParams{});
    status = builder.SaveTo(&output);
    UnrefFilesInVersion(&output);
    return status;
  };
  const std::vector<RunFile> overlapping = {
      {11, "a", "c", 300}, {12, "m", "p", 200}, {13, "b", "d", 100}};
  const Status rejected = try_install(/*fenced=*/true, overlapping);
  ASSERT_EQ(rejected.code(), Status::Code::kCorruption) << rejected.ToString();
  ASSERT_NE(rejected.ToString().find(
                "L0 files of same epoch number but overlapping range #11"),
            std::string::npos)
      << rejected.ToString();
  ASSERT_OK(try_install(/*fenced=*/false, overlapping));
  ASSERT_OK(try_install(
      /*fenced=*/true,
      {{11, "a", "c", 300}, {12, "m", "p", 200}, {13, "e", "g", 100}}));
}

TEST_F(VersionBuilderTest, PhysicalFileLossRejectsNormalBlobFile) {
  // A loss record names only a relocation file. A missing normal or
  // indirect-origin blob file uses the base dropped-file record instead
  // (best_efforts_blob_db_repair). So a loss record that names a normal or an
  // indirect-origin blob file in the edit that adds the file is Corruption. A
  // later loss record that names an indirect-origin blob file (file 100 in this
  // test) is Corruption too, and a later loss record that names a relocation
  // file (file 200) is accepted.
  UpdateVersionStorageInfo();
  EnvOptions env_options;
  const auto make_edit = [&](bool is_indirect_origin) {
    VersionEdit edit;
    edit.SetBlobIndirectionFormatVersion(kBlobIndirectionManifestFormatVersion);
    edit.AddBlobFile(/*blob_file_number=*/100, /*total_blob_count=*/1,
                     /*total_blob_bytes=*/10, /*checksum_method=*/{},
                     /*checksum_value=*/{}, is_indirect_origin);
    AddDummyFileToEdit(
        &edit, /*table_file_number=*/101,
        /*blob_file_number=*/is_indirect_origin ? kInvalidBlobFileNumber : 100,
        /*epoch_number=*/1);
    if (is_indirect_origin) {
      AddBlobReferenceDescriptorToLastFile(&edit);
    } else {
      edit.GetMutableNewFiles().back().second.blob_reference_legacy_metadata =
          true;
    }
    return edit;
  };
  for (const bool is_indirect_origin : {true, false}) {
    SCOPED_TRACE("is_indirect_origin=" + std::to_string(is_indirect_origin));
    VersionEdit lost = make_edit(is_indirect_origin);
    lost.MarkBlobPhysicalFileLost(100);
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    const Status status = builder.Apply(&lost);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find("names no live target"),
              std::string::npos);
  }

  VersionEdit added = make_edit(/*is_indirect_origin=*/true);
  BlobRelocationFileAddition relocation;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{90}, &relocation));
  added.AddBlobRelocationFile(relocation);
  const auto make_later = [](uint64_t lost_file_number) {
    VersionEdit later;
    later.SetBlobIndirectionFormatVersion(
        kBlobIndirectionManifestFormatVersion);
    later.MarkBlobPhysicalFileLost(lost_file_number);
    return later;
  };
  VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                         &vstorage_, /*version_set=*/nullptr);
  ASSERT_OK(builder.Apply(&added));
  VersionEdit later_origin = make_later(100);
  const Status status = builder.Apply(&later_origin);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(status.ToString().find("names no live target"), std::string::npos);
  VersionEdit later_relocation = make_later(200);
  ASSERT_OK(builder.Apply(&later_relocation));
}

TEST_F(VersionBuilderTest, ImplicitNormalBlobPruningReportsPhysicalDeletion) {
  constexpr uint64_t kBlob = 100;
  constexpr uint64_t kSst = 101;
  AddDummyFile(kSst, kBlob, /*epoch_number=*/1);
  vstorage_.LevelFiles(0).front()->blob_reference_legacy_metadata = true;
  AddBlob(kBlob, /*total_blob_count=*/1, /*total_blob_bytes=*/100,
          /*checksum_method=*/{}, /*checksum_value=*/{}, {kSst},
          /*garbage_blob_count=*/0, /*garbage_blob_bytes=*/0);
  vstorage_.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  // A fenced Version's file table holds its blob files.
  vstorage_.SyncBlobFileTable({}, /*rebuild=*/true, /*cache_res_mgr=*/nullptr);
  UpdateVersionStorageInfo();
  VersionEdit edit;
  edit.DeleteFile(0, kSst);
  // No BlobFileGarbage or blob file retirement record accompanies this SST
  // deletion.
  ASSERT_TRUE(edit.GetBlobFileGarbages().empty());
  EnvOptions env_options;
  VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                         &vstorage_, /*version_set=*/nullptr);
  ASSERT_OK(builder.Apply(&edit));
  VersionStorageInfo output(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(builder.SaveTo(&output));
  ASSERT_TRUE(output.GetBlobFiles().empty());
  ASSERT_EQ(output.RemovedBlobFiles(), std::vector<uint64_t>{kBlob});
  UnrefFilesInVersion(&output);
}

TEST_F(VersionBuilderTest, FormatFenceRequiresExactSstMetadata) {
  AddDummyFile(/*table_file_number=*/200,
               /*blob_file_number=*/kInvalidBlobFileNumber,
               /*epoch_number=*/2);
  UpdateVersionStorageInfo();
  EnvOptions env_options;

  VersionEdit missing;
  missing.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  AddDummyFileToEdit(&missing, /*table_file_number=*/101,
                     /*blob_file_number=*/kInvalidBlobFileNumber,
                     /*epoch_number=*/1);
  VersionBuilder missing_builder(env_options, &ioptions_,
                                 /*table_cache=*/nullptr, &vstorage_,
                                 /*version_set=*/nullptr);
  ASSERT_TRUE(missing_builder.Apply(&missing).IsCorruption());

  missing.GetMutableNewFiles().back().second.blob_reference_legacy_metadata =
      true;
  VersionBuilder legacy_builder(env_options, &ioptions_,
                                /*table_cache=*/nullptr, &vstorage_,
                                /*version_set=*/nullptr);
  ASSERT_OK(legacy_builder.Apply(&missing));

  missing.GetMutableNewFiles().back().second.blob_reference_legacy_metadata =
      false;
  AddBlobReferenceDescriptorToLastFile(&missing);
  VersionBuilder complete_builder(env_options, &ioptions_,
                                  /*table_cache=*/nullptr, &vstorage_,
                                  /*version_set=*/nullptr);
  ASSERT_OK(complete_builder.Apply(&missing));

  vstorage_.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  VersionEdit legacy_move;
  legacy_move.DeleteFile(/*level=*/0, /*file=*/200);
  AddDummyFileToEdit(&legacy_move, /*table_file_number=*/200,
                     /*blob_file_number=*/kInvalidBlobFileNumber,
                     /*epoch_number=*/2);
  VersionBuilder move_builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                              &vstorage_, /*version_set=*/nullptr);
  ASSERT_OK(move_builder.Apply(&legacy_move));

  // A move without a descriptor is allowed only when the current copy has no
  // descriptor and no coarse frontier either. Otherwise the move would drop
  // the exact or coarse metadata of SST 200.
  VersionEdit with_descriptor;
  AddDummyFileToEdit(&with_descriptor, /*table_file_number=*/200,
                     /*blob_file_number=*/kInvalidBlobFileNumber,
                     /*epoch_number=*/2);
  AddBlobReferenceDescriptorToLastFile(&with_descriptor);
  ASSERT_EQ(vstorage_.LevelFiles(0).size(), 1U);
  vstorage_.LevelFiles(0).front()->blob_reference_descriptor =
      with_descriptor.GetNewFiles().back().second.blob_reference_descriptor;
  VersionBuilder exact_move_builder(env_options, &ioptions_,
                                    /*table_cache=*/nullptr, &vstorage_,
                                    /*version_set=*/nullptr);
  const Status status = exact_move_builder.Apply(&legacy_move);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(status.ToString().find("drops the exact or coarse blob reference"),
            std::string::npos);

  // The same move of a coarse SST (for example, frontier blob file 300) is
  // also rejected, because the moved copy would become legacy and stop
  // pinning the indirect origins from file 300 on.
  vstorage_.LevelFiles(0).front()->blob_reference_descriptor.clear();
  vstorage_.LevelFiles(0).front()->oldest_coarse_indirect_origin_file_number =
      300;
  VersionBuilder coarse_move_builder(env_options, &ioptions_,
                                     /*table_cache=*/nullptr, &vstorage_,
                                     /*version_set=*/nullptr);
  const Status coarse_status = coarse_move_builder.Apply(&legacy_move);
  ASSERT_TRUE(coarse_status.IsCorruption()) << coarse_status.ToString();
  ASSERT_NE(
      coarse_status.ToString().find("drops the exact or coarse blob reference"),
      std::string::npos);
}

TEST_F(VersionBuilderTest, ApplyBlobRelocationStateAtomically) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                         &vstorage_, /*version_set=*/nullptr);
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &addition));

  VersionEdit edit;
  edit.SetBlobIndirectionFormatVersion(kBlobIndirectionManifestFormatVersion);
  edit.AddBlobRelocationFile(addition);
  edit.SetBlobRelocationRoute(BlobRelocationRoute(
      /*source_file_number=*/100, /*physical_file_number=*/200));
  ASSERT_OK(builder.Apply(&edit));
  // Applying the same edit again is accepted and changes nothing, as in a
  // MANIFEST replay.
  ASSERT_OK(builder.Apply(&edit));

  VersionStorageInfo output(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr,
      /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(builder.SaveTo(&output));
  ASSERT_EQ(output.GetBlobIndirectionFormatVersion(),
            kBlobIndirectionManifestFormatVersion);
  ASSERT_NE(output.GetBlobRelocationFileMetaData(200), nullptr);
  ASSERT_EQ(output.GetBlobRelocationRouteTarget(/*source_file_number=*/100),
            200U);
  ASSERT_EQ(output.GetBlobRelocationRouteTargetCount(200), 1U);
  ASSERT_OK(output.ValidateBlobRelocationState());
}

TEST_F(VersionBuilderTest, NewVersionKeepsRelocationGarbageUntilCensusPublish) {
  // The census publishes the garbage of the relocation files for one Version
  // only. A Version built from it starts with the same value, so the blob
  // garbage properties do not drop to 0 after every flush or compaction. The
  // read value never exceeds the record bytes of the current relocation files.
  // In this example, relocation file 200 serves values of origin file 100, and
  // the census found 10 bytes of garbage in it.
  constexpr uint64_t kRelocationFileNumber = 200;
  constexpr uint64_t kGarbageBytes = 10;
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(kRelocationFileNumber,
                                   /*origin_file_numbers=*/{100}, &addition));
  ASSERT_GE(addition.GetTotalRecordBytes(), kGarbageBytes);
  ASSERT_OK(AddRelocationToBase(addition));
  UpdateVersionStorageInfo();
  vstorage_.SetBlobRelocationGarbageBytes(kGarbageBytes);
  ASSERT_EQ(vstorage_.BlobRelocationGarbageBytesForRead(), kGarbageBytes);

  const auto make_output = [&](VersionStorageInfo* base) {
    return std::make_unique<VersionStorageInfo>(
        &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, base,
        /*force_consistency_checks=*/false,
        EpochNumberRequirement::kMightMissing,
        /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
        OffpeakTimeOption(options_.daily_offpeak_time_utc),
        PeriodicCompactionPhaseParams{});
  };
  EnvOptions env_options;

  // An edit that does not change the relocation files keeps the value.
  std::unique_ptr<VersionStorageInfo> next = make_output(&vstorage_);
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit edit;
    ASSERT_OK(builder.Apply(&edit));
    ASSERT_OK(builder.SaveTo(next.get()));
  }
  UpdateVersionStorageInfo(next.get());
  EXPECT_EQ(next->BlobRelocationGarbageBytesForRead(), kGarbageBytes);
  EXPECT_EQ(next->GetBlobStats().total_garbage_size, kGarbageBytes);

  // An edit that deletes the relocation file leaves no record bytes, so the
  // copied value reads as 0.
  std::unique_ptr<VersionStorageInfo> after_delete = make_output(next.get());
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           next.get(), /*version_set=*/nullptr);
    VersionEdit edit;
    edit.DeleteBlobRelocationFile(kRelocationFileNumber);
    ASSERT_OK(builder.Apply(&edit));
    ASSERT_OK(builder.SaveTo(after_delete.get()));
  }
  UpdateVersionStorageInfo(after_delete.get());
  EXPECT_EQ(after_delete->GetBlobRelocationFileMetaData(kRelocationFileNumber),
            nullptr);
  EXPECT_EQ(after_delete->BlobRelocationGarbageBytesForRead(), 0U);
  EXPECT_EQ(after_delete->GetBlobStats().total_garbage_size, 0U);
}

TEST_F(VersionBuilderTest, RetiredAnchorRootDoesNotDropNormalBlobFiles) {
  // SST #101 names indirect origin #100 as its oldest blob file and also holds
  // values in normal blob file #150, which no SST links. Normal file #160
  // becomes fully garbage. The blob file retirement of #100 leaves no blob file
  // with linked SSTs. Both the Version that installs that blob file retirement
  // and a later Version must keep #150 and drop #160.
  constexpr uint64_t kSourceFileNumber = 100;
  constexpr uint64_t kTableFileNumber = 101;
  constexpr uint64_t kLiveNormalFileNumber = 150;
  constexpr uint64_t kGarbageNormalFileNumber = 160;
  constexpr uint64_t kRelocationFileNumber = 200;
  vstorage_.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  AddDummyFile(kTableFileNumber, kSourceFileNumber, /*epoch_number=*/1);
  auto shared = SharedBlobFileMetaData::Create(
      kSourceFileNumber, /*total_blob_count=*/2,
      /*total_blob_bytes=*/100, /*checksum_method=*/{},
      /*checksum_value=*/{}, /*is_indirect_origin=*/true);
  vstorage_.AddBlobFile(BlobFileMetaData::Create(
      std::move(shared), BlobFileMetaData::LinkedSsts{kTableFileNumber},
      /*garbage_blob_count=*/1, /*garbage_blob_bytes=*/40));
  AddBlob(kLiveNormalFileNumber, /*total_blob_count=*/10,
          /*total_blob_bytes=*/1000, /*checksum_method=*/{},
          /*checksum_value=*/{}, BlobFileMetaData::LinkedSsts{},
          /*garbage_blob_count=*/0, /*garbage_blob_bytes=*/0);
  AddBlob(kGarbageNormalFileNumber, /*total_blob_count=*/5,
          /*total_blob_bytes=*/500, /*checksum_method=*/{},
          /*checksum_value=*/{}, BlobFileMetaData::LinkedSsts{},
          /*garbage_blob_count=*/4, /*garbage_blob_bytes=*/400);
  // A fenced Version's file table holds its blob files.
  vstorage_.SyncBlobFileTable({}, /*rebuild=*/true, /*cache_res_mgr=*/nullptr);
  UpdateVersionStorageInfo();

  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(kRelocationFileNumber, {kSourceFileNumber},
                                   &addition));

  VersionEdit retire;
  retire.AddBlobRelocationFile(addition);
  retire.SetBlobRelocationRoute(
      BlobRelocationRoute(kSourceFileNumber, kRelocationFileNumber));
  retire.RetireBlobFile(kSourceFileNumber);
  retire.AddBlobFileGarbage(kGarbageNormalFileNumber,
                            /*garbage_blob_count=*/1,
                            /*garbage_blob_bytes=*/100);

  const auto make_output = [&](VersionStorageInfo* base) {
    return std::make_unique<VersionStorageInfo>(
        &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, base,
        /*force_consistency_checks=*/false,
        EpochNumberRequirement::kMightMissing,
        /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
        OffpeakTimeOption(options_.daily_offpeak_time_utc),
        PeriodicCompactionPhaseParams{});
  };

  EnvOptions env_options;
  std::unique_ptr<VersionStorageInfo> retired = make_output(&vstorage_);
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    ASSERT_OK(builder.Apply(&retire));
    ASSERT_OK(builder.SaveTo(retired.get()));
  }
  UpdateVersionStorageInfo(retired.get());

  // Any later Version is built from a base where nothing links a blob file.
  std::unique_ptr<VersionStorageInfo> next = make_output(retired.get());
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           retired.get(), /*version_set=*/nullptr);
    VersionEdit unrelated;
    ASSERT_OK(builder.Apply(&unrelated));
    ASSERT_OK(builder.SaveTo(next.get()));
  }
  UpdateVersionStorageInfo(next.get());

  for (VersionStorageInfo* storage : {retired.get(), next.get()}) {
    ASSERT_EQ(storage->GetBlobFileMetaData(kSourceFileNumber), nullptr);
    ASSERT_NE(storage->GetBlobFileMetaData(kLiveNormalFileNumber), nullptr);
    ASSERT_EQ(storage->GetBlobFileMetaData(kGarbageNormalFileNumber), nullptr);
  }
  UnrefFilesInVersion(next.get());
  UnrefFilesInVersion(retired.get());
}

TEST_F(VersionBuilderTest,
       IndirectOriginApproximateGarbageDoesNotAuthorizeRetirement) {
  constexpr uint64_t kSourceFileNumber = 100;
  constexpr uint64_t kTableFileNumber = 101;
  vstorage_.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  AddDummyFile(kTableFileNumber, kSourceFileNumber, /*epoch_number=*/1);
  auto shared = SharedBlobFileMetaData::Create(
      kSourceFileNumber, /*total_blob_count=*/1,
      /*total_blob_bytes=*/100, /*checksum_method=*/{},
      /*checksum_value=*/{}, /*is_indirect_origin=*/true);
  vstorage_.AddBlobFile(BlobFileMetaData::Create(
      std::move(shared), BlobFileMetaData::LinkedSsts{kTableFileNumber},
      /*garbage_blob_count=*/0, /*garbage_blob_bytes=*/0));
  // A fenced Version's file table holds its blob files.
  vstorage_.SyncBlobFileTable({}, /*rebuild=*/true, /*cache_res_mgr=*/nullptr);
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                         &vstorage_, /*version_set=*/nullptr);
  VersionEdit approximate;
  approximate.AddBlobFileGarbage(kSourceFileNumber,
                                 /*garbage_blob_count=*/1,
                                 /*garbage_blob_bytes=*/100);
  ASSERT_OK(builder.Apply(&approximate));

  VersionStorageInfo output(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/true, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(builder.SaveTo(&output));
  const std::shared_ptr<BlobFileMetaData> retained =
      output.GetBlobFileMetaData(kSourceFileNumber);
  ASSERT_NE(retained, nullptr);
  EXPECT_TRUE(retained->IsIndirectOrigin());
  EXPECT_EQ(retained->GetGarbageBlobCount(), 1U);
  EXPECT_EQ(retained->GetGarbageBlobBytes(), 100U);
  UnrefFilesInVersion(&output);
}

TEST_F(VersionBuilderTest, IndirectBlobGCSuppressionAdvancesCandidate) {
  auto first_shared = SharedBlobFileMetaData::Create(
      /*blob_file_number=*/100, /*total_blob_count=*/10,
      /*total_blob_bytes=*/1000, /*checksum_method=*/{},
      /*checksum_value=*/{}, /*is_indirect_origin=*/true);
  auto second_shared = SharedBlobFileMetaData::Create(
      /*blob_file_number=*/200, /*total_blob_count=*/10,
      /*total_blob_bytes=*/1000, /*checksum_method=*/{},
      /*checksum_value=*/{}, /*is_indirect_origin=*/true);
  vstorage_.AddBlobFile(BlobFileMetaData::Create(
      std::move(first_shared), BlobFileMetaData::LinkedSsts{},
      /*garbage_blob_count=*/8, /*garbage_blob_bytes=*/800));
  vstorage_.AddBlobFile(BlobFileMetaData::Create(
      std::move(second_shared), BlobFileMetaData::LinkedSsts{},
      /*garbage_blob_count=*/6, /*garbage_blob_bytes=*/600));
  BlobRelocationFileAddition relocation_addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/300,
                                   /*origin_file_numbers=*/{50},
                                   &relocation_addition));
  vstorage_.AddBlobRelocationFile(
      BlobRelocationFileMetaData::Create(relocation_addition));
  vstorage_.AddBlobRelocationRoute(
      BlobRelocationRoute(/*old_file_number=*/50,
                          /*target_file_number=*/300));

  vstorage_.SetBlobGcCandidates({100, 200, 300});
  EXPECT_EQ(vstorage_.BlobPhysicalFileForIndirectBlobGC(), 100U);
  std::vector<uint64_t> candidates;
  vstorage_.GetBlobPhysicalFilesForIndirectBlobGC(/*max_files=*/3, &candidates);
  EXPECT_EQ(candidates, (std::vector<uint64_t>{100, 200, 300}));
  EXPECT_EQ(vstorage_.GetBlobRelocationRouteTargetCount(300), 1U);

  // A job that tried some candidates and could not finish them defers them.
  // DeferBlobFilesForIndirectBlobGC() moves them to the end of the list, so the
  // next job looks at the other candidates first.
  vstorage_.DeferBlobFilesForIndirectBlobGC({100, 200});
  vstorage_.GetBlobPhysicalFilesForIndirectBlobGC(/*max_files=*/3, &candidates);
  EXPECT_EQ(candidates, (std::vector<uint64_t>{300, 100, 200}));
  vstorage_.SetBlobGcCandidates({100, 200, 300});

  // Suppress a candidate that is not first in the list.
  // SuppressBlobFileForIndirectBlobGC() removes it and shifts the later
  // candidates forward, so the list never has holes.
  vstorage_.SuppressBlobFileForIndirectBlobGC(200);
  vstorage_.GetBlobPhysicalFilesForIndirectBlobGC(/*max_files=*/3, &candidates);
  EXPECT_EQ(candidates, (std::vector<uint64_t>{100, 300}));
  EXPECT_EQ(vstorage_.BlobPhysicalFileForIndirectBlobGC(), 100U);

  vstorage_.SuppressBlobFileForIndirectBlobGC(100);
  EXPECT_EQ(vstorage_.BlobPhysicalFileForIndirectBlobGC(), 300U);

  vstorage_.SuppressBlobFileForIndirectBlobGC(300);
  EXPECT_EQ(vstorage_.BlobPhysicalFileForIndirectBlobGC(),
            kInvalidBlobFileNumber);
}

TEST_F(VersionBuilderTest, BlobRelocationMetadataPinsPhysicalFile) {
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &addition));
  int destruction_count = 0;
  std::shared_ptr<BlobRelocationFileMetaData> metadata =
      BlobRelocationFileMetaData::Create(
          addition, [&destruction_count](BlobRelocationFileMetaData* value) {
            ++destruction_count;
            delete value;
          });
  std::unique_ptr<VersionStorageInfo> first =
      std::make_unique<VersionStorageInfo>(
          &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
          /*src_vstorage=*/nullptr, /*force_consistency_checks=*/false,
          EpochNumberRequirement::kMightMissing, /*clock=*/nullptr,
          /*bottommost_file_compaction_delay=*/0,
          OffpeakTimeOption(options_.daily_offpeak_time_utc),
          PeriodicCompactionPhaseParams{});
  first->SetBlobIndirectionFormatVersion(kBlobIndirectionManifestFormatVersion);
  first->AddBlobRelocationFile(metadata);

  std::unique_ptr<VersionStorageInfo> second =
      std::make_unique<VersionStorageInfo>(
          &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
          /*src_vstorage=*/nullptr, /*force_consistency_checks=*/false,
          EpochNumberRequirement::kMightMissing, /*clock=*/nullptr,
          /*bottommost_file_compaction_delay=*/0,
          OffpeakTimeOption(options_.daily_offpeak_time_utc),
          PeriodicCompactionPhaseParams{});
  second->SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  second->AddBlobRelocationFile(
      first->GetBlobRelocationFileMetaData(/*file_number=*/200));

  metadata.reset();
  first.reset();
  ASSERT_EQ(destruction_count, 0);
  second.reset();
  ASSERT_EQ(destruction_count, 1);
}

TEST_F(VersionBuilderTest, InvalidBlobRelocationEditHasNoPartialEffect) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                         &vstorage_, /*version_set=*/nullptr);
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &addition));

  VersionEdit edit;
  edit.SetBlobIndirectionFormatVersion(kBlobIndirectionManifestFormatVersion);
  edit.AddBlobRelocationFile(addition);
  edit.SetBlobRelocationRoute(
      BlobRelocationRoute(/*source_file_number=*/100,
                          /*physical_file_number=*/999));
  ASSERT_TRUE(builder.Apply(&edit).IsCorruption());

  VersionStorageInfo output(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr,
      /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(builder.SaveTo(&output));
  ASSERT_FALSE(output.HasBlobIndirectionFormat());
  ASSERT_TRUE(output.GetBlobRelocationFiles().empty());
  ASSERT_TRUE(output.GetBlobRelocationRouteCount() == 0);
}

TEST_F(VersionBuilderTest, BlobRelocationRejectsDuplicateRecords) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &addition));

  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit duplicate;
    duplicate.SetBlobIndirectionFormatVersion(
        kBlobIndirectionManifestFormatVersion);
    duplicate.AddBlobRelocationFile(addition);
    duplicate.AddBlobRelocationFile(addition);
    ASSERT_TRUE(builder.Apply(&duplicate).IsCorruption());
  }
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit duplicate_route;
    duplicate_route.SetBlobIndirectionFormatVersion(
        kBlobIndirectionManifestFormatVersion);
    duplicate_route.AddBlobRelocationFile(addition);
    const BlobRelocationRoute route(/*source_file_number=*/100,
                                    /*physical_file_number=*/200);
    duplicate_route.SetBlobRelocationRoute(route);
    duplicate_route.SetBlobRelocationRoute(route);
    ASSERT_TRUE(builder.Apply(&duplicate_route).IsCorruption());
  }
}

TEST_F(VersionBuilderTest, BlobRelocationRejectsPhysicalFileNumberCollisions) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &addition));

  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit edit;
    edit.SetBlobIndirectionFormatVersion(kBlobIndirectionManifestFormatVersion);
    edit.AddBlobRelocationFile(addition);
    AddDummyFileToEdit(&edit, /*table_file_number=*/200,
                       /*blob_file_number=*/kInvalidBlobFileNumber,
                       /*epoch_number=*/1);
    ASSERT_TRUE(builder.Apply(&edit).IsCorruption());
  }
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit edit;
    edit.SetBlobIndirectionFormatVersion(kBlobIndirectionManifestFormatVersion);
    edit.AddBlobRelocationFile(addition);
    edit.AddBlobFile(/*blob_file_number=*/200, /*total_blob_count=*/1,
                     /*total_blob_bytes=*/10, /*checksum_method=*/{},
                     /*checksum_value=*/{});
    ASSERT_TRUE(builder.Apply(&edit).IsCorruption());
  }
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit install;
    install.SetBlobIndirectionFormatVersion(
        kBlobIndirectionManifestFormatVersion);
    install.AddBlobRelocationFile(addition);
    ASSERT_OK(builder.Apply(&install));

    VersionEdit deletion;
    deletion.DeleteBlobRelocationFile(/*file_number=*/200);
    ASSERT_OK(builder.Apply(&deletion));

    VersionEdit blob_reuse;
    blob_reuse.AddBlobFile(/*blob_file_number=*/200, /*total_blob_count=*/1,
                           /*total_blob_bytes=*/10, /*checksum_method=*/{},
                           /*checksum_value=*/{});
    ASSERT_TRUE(builder.Apply(&blob_reuse).IsCorruption());

    VersionEdit table_reuse;
    AddDummyFileToEdit(&table_reuse, /*table_file_number=*/200,
                       /*blob_file_number=*/kInvalidBlobFileNumber,
                       /*epoch_number=*/1);
    ASSERT_TRUE(builder.Apply(&table_reuse).IsCorruption());
  }
}

TEST_F(VersionBuilderTest, BlobRelocationFileDeletionNeedsLiveFile) {
  // As for an SST deletion, a relocation file deletion must name a live
  // relocation file of the edit's column family. The base Version here has no
  // relocation file 200, so a deletion of file 200 is Corruption. A deletion
  // after file 200 is added is accepted. A repeated deletion of a file that
  // the builder already deleted is accepted too, because MANIFEST replay and
  // catch-up can repeat a record.
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &addition));
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit deletion;
    deletion.SetBlobIndirectionFormatVersion(
        kBlobIndirectionManifestFormatVersion);
    deletion.DeleteBlobRelocationFile(/*file_number=*/200);
    const Status status = builder.Apply(&deletion);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find(
                  "Blob relocation file deletion names no live file"),
              std::string::npos)
        << status.ToString();
  }
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit install;
    install.SetBlobIndirectionFormatVersion(
        kBlobIndirectionManifestFormatVersion);
    install.AddBlobRelocationFile(addition);
    ASSERT_OK(builder.Apply(&install));
    VersionEdit deletion;
    deletion.DeleteBlobRelocationFile(/*file_number=*/200);
    ASSERT_OK(builder.Apply(&deletion));
    VersionEdit repeated_deletion;
    repeated_deletion.DeleteBlobRelocationFile(/*file_number=*/200);
    ASSERT_OK(builder.Apply(&repeated_deletion));
  }
}

TEST_F(VersionBuilderTest, BlobRelocationFileNumberChecksPerColumnFamily) {
  // VersionBuilder checks file numbers only against the state of the edit's
  // own column family (see "File protection" in
  // db/blob/blob_relocation_manifest.h). This test lists each check of a
  // relocation file number, a new SST number, a new blob file number, a route
  // target and a route deletion, with its error text.
  //
  // The example base Version is fenced and has SST 10, normal blob file 50,
  // relocation file 200, and a route from old file 100 to file 200.
  vstorage_.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  AddDummyFile(/*table_file_number=*/10,
               /*blob_file_number=*/kInvalidBlobFileNumber,
               /*epoch_number=*/1);
  AddBlob(/*blob_file_number=*/50, /*total_blob_count=*/1,
          /*total_blob_bytes=*/100, /*checksum_method=*/{},
          /*checksum_value=*/{}, BlobFileMetaData::LinkedSsts{},
          /*garbage_blob_count=*/0, /*garbage_blob_bytes=*/0);
  BlobRelocationFileAddition relocation;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &relocation));
  ASSERT_OK(AddRelocationToBase(relocation));
  vstorage_.AddBlobRelocationRoute(BlobRelocationRoute(
      /*source_file_number=*/100, /*physical_file_number=*/200));
  UpdateVersionStorageInfo();

  const auto add_relocation = [&](uint64_t file_number,
                                  const std::vector<uint64_t>& origins,
                                  VersionEdit* edit) {
    BlobRelocationFileAddition addition;
    ASSERT_OK(MakeRelocationAddition(file_number, origins, &addition));
    edit->AddBlobRelocationFile(addition);
  };
  const auto add_blob_file = [](uint64_t file_number, VersionEdit* edit) {
    edit->AddBlobFile(file_number, /*total_blob_count=*/1,
                      /*total_blob_bytes=*/10, /*checksum_method=*/{},
                      /*checksum_value=*/{});
  };
  struct Case {
    const char* name;
    std::function<void(VersionEdit*)> make_edit;
    // An empty text means that the edit is accepted.
    const char* error;
  };
  const std::vector<Case> cases = {
      {"relocation file 10 is an SST",
       [&](VersionEdit* edit) { add_relocation(10, {5}, edit); },
       "Blob relocation file conflicts with a table file"},
      {"relocation file 50 is a normal blob file",
       [&](VersionEdit* edit) { add_relocation(50, {5}, edit); },
       "Blob relocation file conflicts with a normal blob file"},
      {"relocation file 200 again with other contents",
       [&](VersionEdit* edit) { add_relocation(200, {90}, edit); },
       "Conflicting blob relocation file addition"},
      {"relocation file 200 again with the same contents (as at replay)",
       [&](VersionEdit* edit) { edit->AddBlobRelocationFile(relocation); }, ""},
      {"new SST 200 is a relocation file",
       [&](VersionEdit* edit) {
         AddDummyFileToEdit(edit, /*table_file_number=*/200,
                            /*blob_file_number=*/kInvalidBlobFileNumber,
                            /*epoch_number=*/2);
         // A new SST of a fenced column family needs exact blob reference
         // metadata.
         AddBlobReferenceDescriptorToLastFile(edit);
       },
       "Table file conflicts with a blob relocation file"},
      {"new blob file 200 is a relocation file",
       [&](VersionEdit* edit) { add_blob_file(200, edit); },
       "Normal blob file conflicts with a relocation file"},
      {"new blob file 100 has a route",
       [&](VersionEdit* edit) { add_blob_file(100, edit); },
       "Normal blob file conflicts with a route"},
      {"route target 999 does not exist",
       [&](VersionEdit* edit) {
         edit->SetBlobRelocationRoute(BlobRelocationRoute(
             /*source_file_number=*/150, /*physical_file_number=*/999));
       },
       "Invalid blob relocation route target"},
      {"route 100 deleted and relocation file 100 added in one edit",
       [&](VersionEdit* edit) {
         edit->DeleteBlobRelocationRoute(100);
         add_relocation(100, {5}, edit);
       },
       "Blob relocation file conflicts with a route"},
      {"route 100 deleted and blob file 100 added in one edit",
       [&](VersionEdit* edit) {
         edit->DeleteBlobRelocationRoute(100);
         add_blob_file(100, edit);
       },
       "Normal blob file conflicts with a route"},
      {"relocation file 100 has a route",
       [&](VersionEdit* edit) { add_relocation(100, {5}, edit); },
       "Blob relocation file conflicts with a route"},
      {"route 150 deleted but it does not exist",
       [&](VersionEdit* edit) { edit->DeleteBlobRelocationRoute(150); },
       "Blob relocation route deletion names no route"},
      {"route 100 deleted (it exists)",
       [&](VersionEdit* edit) { edit->DeleteBlobRelocationRoute(100); }, ""},
      {"route target 50 is a normal blob file",
       [&](VersionEdit* edit) {
         edit->SetBlobRelocationRoute(BlobRelocationRoute(
             /*source_file_number=*/150, /*physical_file_number=*/50));
       },
       "Invalid blob relocation route target"},
  };
  EnvOptions env_options;
  for (const Case& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    VersionEdit edit;
    test_case.make_edit(&edit);
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    const Status status = builder.Apply(&edit);
    if (std::string(test_case.error).empty()) {
      ASSERT_OK(status);
    } else {
      ASSERT_TRUE(status.IsCorruption()) << status.ToString();
      ASSERT_NE(status.ToString().find(test_case.error), std::string::npos)
          << status.ToString();
    }
  }

  // Route 100 deleted by one edit, then relocation file 100 added by a later
  // edit of the same builder (for example in one LogAndApply batch).
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit deletion;
    deletion.DeleteBlobRelocationRoute(100);
    ASSERT_OK(builder.Apply(&deletion));
    VersionEdit addition;
    add_relocation(100, {5}, &addition);
    const Status status = builder.Apply(&addition);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(
        status.ToString().find("Blob relocation file conflicts with a route"),
        std::string::npos)
        << status.ToString();
  }
  // The same route deletion applied twice to one builder, as at replay or
  // catch-up, is accepted.
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit first;
    first.DeleteBlobRelocationRoute(100);
    ASSERT_OK(builder.Apply(&first));
    VersionEdit repeated;
    repeated.DeleteBlobRelocationRoute(100);
    ASSERT_OK(builder.Apply(&repeated));
  }
}

TEST_F(VersionBuilderTest, BlobRelocationRejectsSetAndDeleteSameKey) {
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &addition));
  ASSERT_OK(AddRelocationToBase(addition));
  vstorage_.AddBlobRelocationRoute(BlobRelocationRoute(
      /*source_file_number=*/100, /*physical_file_number=*/200));
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit edit;
    edit.AddBlobRelocationFile(addition);
    edit.DeleteBlobRelocationFile(/*file_number=*/200);
    ASSERT_TRUE(builder.Apply(&edit).IsCorruption());
  }
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit edit;
    edit.SetBlobRelocationRoute(BlobRelocationRoute(
        /*source_file_number=*/100, /*physical_file_number=*/200));
    edit.DeleteBlobRelocationRoute(/*source_file_number=*/100);
    ASSERT_TRUE(builder.Apply(&edit).IsCorruption());
  }
}

TEST_F(VersionBuilderTest, BlobRelocationRouteReplacementAndDeletion) {
  BlobRelocationFileAddition first_addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100},
                                   &first_addition));
  ASSERT_OK(AddRelocationToBase(first_addition));
  vstorage_.AddBlobRelocationRoute(BlobRelocationRoute(
      /*source_file_number=*/100, /*physical_file_number=*/200));
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder replacement_builder(env_options, &ioptions_,
                                     /*table_cache=*/nullptr, &vstorage_,
                                     /*version_set=*/nullptr);
  BlobRelocationFileAddition second_addition;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/300,
                                   /*origin_file_numbers=*/{100},
                                   &second_addition));
  VersionEdit replacement;
  replacement.AddBlobRelocationFile(second_addition);
  replacement.DeleteBlobRelocationFile(/*file_number=*/200);
  replacement.SetBlobRelocationRoute(BlobRelocationRoute(
      /*source_file_number=*/100, /*physical_file_number=*/300));
  ASSERT_OK(replacement_builder.Apply(&replacement));

  VersionStorageInfo replaced(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr,
      /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(replacement_builder.SaveTo(&replaced));
  ASSERT_EQ(replaced.GetBlobRelocationFileMetaData(200), nullptr);
  ASSERT_NE(replaced.GetBlobRelocationFileMetaData(300), nullptr);
  ASSERT_EQ(replaced.GetBlobRelocationRouteTargetCount(200), 0U);
  ASSERT_EQ(replaced.GetBlobRelocationRouteTargetCount(300), 1U);

  VersionBuilder deletion_builder(env_options, &ioptions_,
                                  /*table_cache=*/nullptr, &replaced,
                                  /*version_set=*/nullptr);
  VersionEdit deletion;
  deletion.DeleteBlobRelocationRoute(/*source_file_number=*/100);
  deletion.DeleteBlobRelocationFile(/*file_number=*/300);
  ASSERT_OK(deletion_builder.Apply(&deletion));
  VersionStorageInfo deleted(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &replaced,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr,
      /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(deletion_builder.SaveTo(&deleted));
  ASSERT_TRUE(deleted.GetBlobRelocationFiles().empty());
  ASSERT_TRUE(deleted.GetBlobRelocationRouteCount() == 0);
  // A saved Version keeps only live files and routes, not a record of
  // deletions. As for an SST, a new builder on top of it rejects a deletion of
  // relocation file 300, because file 300 is not live there.
  {
    VersionBuilder fresh_builder(env_options, &ioptions_,
                                 /*table_cache=*/nullptr, &deleted,
                                 /*version_set=*/nullptr);
    VersionEdit file_deletion;
    file_deletion.DeleteBlobRelocationFile(/*file_number=*/300);
    const Status status = fresh_builder.Apply(&file_deletion);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find(
                  "Blob relocation file deletion names no live file"),
              std::string::npos)
        << status.ToString();
  }
  // A builder that applied the deletions remembers them. It accepts the same
  // deletions again (as in a replay), and it rejects any reuse of file number
  // 300 and a new route from 100.
  VersionBuilder replay_builder(env_options, &ioptions_,
                                /*table_cache=*/nullptr, &replaced,
                                /*version_set=*/nullptr);
  ASSERT_OK(replay_builder.Apply(&deletion));
  ASSERT_OK(replay_builder.Apply(&deletion));

  VersionEdit relocation_reuse;
  relocation_reuse.AddBlobRelocationFile(second_addition);
  ASSERT_TRUE(replay_builder.Apply(&relocation_reuse).IsCorruption());

  VersionEdit blob_reuse;
  blob_reuse.AddBlobFile(/*blob_file_number=*/300, /*total_blob_count=*/1,
                         /*total_blob_bytes=*/10, /*checksum_method=*/{},
                         /*checksum_value=*/{});
  ASSERT_TRUE(replay_builder.Apply(&blob_reuse).IsCorruption());

  VersionEdit table_reuse;
  AddDummyFileToEdit(&table_reuse, /*table_file_number=*/300,
                     /*blob_file_number=*/kInvalidBlobFileNumber,
                     /*epoch_number=*/1);
  ASSERT_TRUE(replay_builder.Apply(&table_reuse).IsCorruption());

  VersionEdit route_reuse;
  route_reuse.SetBlobRelocationRoute(BlobRelocationRoute(
      /*source_file_number=*/100, /*physical_file_number=*/300));
  ASSERT_TRUE(replay_builder.Apply(&route_reuse).IsCorruption());
}

TEST_F(VersionBuilderTest, BlobRelocationUnchangedVersionSharesLiveState) {
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(200, {100}, &addition));
  ASSERT_OK(AddRelocationToBase(addition));
  for (uint64_t generation = 1000; generation < 2000; ++generation) {
    vstorage_.AddBlobRelocationRoute(BlobRelocationRoute(generation, 200));
  }
  UpdateVersionStorageInfo();
  ASSERT_OK(vstorage_.ValidateBlobRelocationState());
  vstorage_.ComputeBlobRelocationStateFingerprint();

  EnvOptions env_options;
  VersionBuilder builder(env_options, &ioptions_, nullptr, &vstorage_, nullptr);
  VersionStorageInfo unchanged(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      false, EpochNumberRequirement::kMightMissing, nullptr, 0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(builder.SaveTo(&unchanged));
  ASSERT_EQ(unchanged.GetBlobRelocationState(),
            vstorage_.GetBlobRelocationState());
  ASSERT_EQ(unchanged.GetBlobRelocationStateFingerprintHigh(),
            vstorage_.GetBlobRelocationStateFingerprintHigh());

  VersionEdit deletion;
  deletion.DeleteBlobRelocationRoute(1000);
  VersionBuilder changed_builder(env_options, &ioptions_, nullptr, &unchanged,
                                 nullptr);
  ASSERT_OK(changed_builder.Apply(&deletion));
  VersionStorageInfo changed(&icmp_, ucmp_, options_.num_levels,
                             kCompactionStyleLevel, &unchanged, false,
                             EpochNumberRequirement::kMightMissing, nullptr, 0,
                             OffpeakTimeOption(options_.daily_offpeak_time_utc),
                             PeriodicCompactionPhaseParams{});
  ASSERT_OK(changed_builder.SaveTo(&changed));
  ASSERT_NE(changed.GetBlobRelocationState(),
            unchanged.GetBlobRelocationState());
  ASSERT_EQ(changed.GetBlobRelocationRouteTarget(1000), kInvalidBlobFileNumber);
  ASSERT_EQ(unchanged.GetBlobRelocationRouteTarget(1000), 200U);
  ASSERT_NE(changed.GetBlobRelocationStateFingerprintHigh(),
            unchanged.GetBlobRelocationStateFingerprintHigh());
  ASSERT_EQ(changed.GetBlobRelocationRouteTargetCount(200), 999U);
  ASSERT_EQ(unchanged.GetBlobRelocationRouteTargetCount(200), 1000U);
}

TEST_F(VersionBuilderTest, BlobRelocationFingerprintIsOrderFree) {
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(200, {100, 101}, &addition));
  const std::shared_ptr<BlobRelocationFileMetaData> file =
      BlobRelocationFileMetaData::Create(addition);
  BlobRelocationState first;
  first.SetFile(file);
  first.SetRoute(BlobRelocationRoute(1000, 200));
  first.SetRoute(BlobRelocationRoute(1065, 200));
  first.Seal();
  uint64_t first_high;
  uint64_t first_low;
  first.ComputeFingerprint(1, &first_high, &first_low);
  BlobRelocationState second;
  second.SetRoute(BlobRelocationRoute(1065, 200));
  second.SetRoute(BlobRelocationRoute(1000, 300));
  second.SetFile(file);
  // Re-pointing, adding and removing a route leave the same final digest.
  second.SetRoute(BlobRelocationRoute(1000, 200));
  second.SetRoute(BlobRelocationRoute(1001, 200));
  second.DeleteRoute(1001);
  second.Seal();
  uint64_t second_high;
  uint64_t second_low;
  second.ComputeFingerprint(1, &second_high, &second_low);
  ASSERT_EQ(first_high, second_high);
  ASSERT_EQ(first_low, second_low);
  // The digest covers the route target and the loss marker.
  BlobRelocationState copied(first);
  copied.SetRoute(BlobRelocationRoute(1065, 300));
  copied.Seal();
  uint64_t copied_high;
  uint64_t copied_low;
  copied.ComputeFingerprint(1, &copied_high, &copied_low);
  ASSERT_NE(copied_high, first_high);
  BlobRelocationState lost(first);
  lost.SetFile(BlobRelocationFileMetaData::CreateMarkedLost(file));
  lost.Seal();
  uint64_t lost_high;
  uint64_t lost_low;
  lost.ComputeFingerprint(1, &lost_high, &lost_low);
  ASSERT_NE(lost_high, first_high);
  ASSERT_TRUE(lost.HasLostFiles());
  ASSERT_TRUE(BlobFileTable::IsLost(lost.table().Find(200)));
  ASSERT_FALSE(BlobFileTable::IsLost(first.table().Find(200)));
  ASSERT_EQ(first.FindRouteTarget(1000), 200U);
  ASSERT_EQ(first.RouteCount(200), 2U);
}

TEST_F(VersionBuilderTest, BlobRelocationFingerprintCoversFileFields) {
  // Each field of a relocation file that identifies it (size, checksum,
  // directory digest) changes the fingerprint.
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(200, {100}, &addition));
  const auto fingerprint = [](const BlobRelocationFileAddition& file) {
    BlobRelocationState state;
    state.SetFile(BlobRelocationFileMetaData::Create(file));
    state.Seal();
    uint64_t high = 0;
    uint64_t low = 0;
    state.ComputeFingerprint(1, &high, &low);
    return std::make_pair(high, low);
  };
  const auto base = fingerprint(BlobRelocationFileAddition(
      200, addition.GetFileSize(), "crc32c", "aaaa", addition.GetTotalEntries(),
      addition.GetTotalRecordBytes(), addition.GetDirectoryDigest(),
      addition.GetTailSize()));
  EXPECT_NE(base,
            fingerprint(BlobRelocationFileAddition(
                200, addition.GetFileSize() + 1, "crc32c", "aaaa",
                addition.GetTotalEntries(), addition.GetTotalRecordBytes(),
                addition.GetDirectoryDigest(), addition.GetTailSize())));
  EXPECT_NE(base,
            fingerprint(BlobRelocationFileAddition(
                200, addition.GetFileSize(), "crc32c", "aaab",
                addition.GetTotalEntries(), addition.GetTotalRecordBytes(),
                addition.GetDirectoryDigest(), addition.GetTailSize())));
  EXPECT_NE(base,
            fingerprint(BlobRelocationFileAddition(
                200, addition.GetFileSize(), "crc32c", "aaaa",
                addition.GetTotalEntries(), addition.GetTotalRecordBytes(),
                addition.GetDirectoryDigest() ^ 1, addition.GetTailSize())));
}

TEST_F(VersionBuilderTest, BlobRelocationRetentionLeavesTableToItsCharge) {
  // A census change log entry that holds a newer state is charged only for
  // memory that no other reservation charges. The file table is charged by
  // ownership to the file metadata reservation, so a route change on a large
  // table adds no table bytes to the entry.
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(200, {100}, &addition));
  BlobRelocationState base;
  base.SetFile(BlobRelocationFileMetaData::Create(addition));
  for (uint64_t source = 1000; source < 11000; ++source) {
    base.SetRoute(BlobRelocationRoute(source, 200));
  }
  base.Seal();
  BlobRelocationState changed(base);
  changed.DeleteRoute(1000);
  changed.Seal();
  ASSERT_GT(changed.table().ApproximateMemoryUsage(), 100000U);
  EXPECT_LT(changed.IncrementalRetentionBytes(base, /*changed_files=*/{}),
            1024U);

  // A changed relocation file is counted, old and new side.
  BlobRelocationFileAddition other;
  ASSERT_OK(MakeRelocationAddition(300, {150}, &other));
  BlobRelocationState with_file(base);
  with_file.SetFile(BlobRelocationFileMetaData::Create(other));
  with_file.Seal();
  EXPECT_GT(with_file.IncrementalRetentionBytes(base, {300}),
            with_file.IncrementalRetentionBytes(base, {}));
}

TEST_F(VersionBuilderTest, AcceptedLossMarksRelocationFile) {
  // Relocation file #200 holds the values of old file #100, which is routed
  // to it.
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(200, {100}, &addition));
  ASSERT_OK(AddRelocationToBase(addition));
  vstorage_.AddBlobRelocationRoute(BlobRelocationRoute(100, 200));
  UpdateVersionStorageInfo();
  ASSERT_OK(vstorage_.ValidateBlobRelocationState());
  vstorage_.ComputeBlobRelocationStateFingerprint();

  EnvOptions env_options;
  const auto apply = [&](VersionEdit* edit, VersionStorageInfo* base,
                         VersionStorageInfo* output) {
    VersionBuilder builder(env_options, &ioptions_, nullptr, base, nullptr);
    Status status = builder.Apply(edit);
    if (status.ok() && output != nullptr) {
      status = builder.SaveTo(output);
    }
    return status;
  };
  const auto new_storage = [&](VersionStorageInfo* base) {
    return std::make_unique<VersionStorageInfo>(
        &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, base, false,
        EpochNumberRequirement::kMightMissing, nullptr, 0,
        OffpeakTimeOption(options_.daily_offpeak_time_utc),
        PeriodicCompactionPhaseParams{});
  };
  {
    // A loss record is rejected for a duplicate file number, file number 0, an
    // unknown file, and a file that the same edit deletes.
    for (const std::vector<uint64_t>& losses :
         std::vector<std::vector<uint64_t>>{{200, 200}, {0}, {999}}) {
      VersionEdit edit;
      for (uint64_t file_number : losses) {
        edit.MarkBlobPhysicalFileLost(file_number);
      }
      ASSERT_TRUE(apply(&edit, &vstorage_, nullptr).IsCorruption());
    }
    VersionEdit deleted;
    deleted.DeleteBlobRelocationRoute(100);
    deleted.DeleteBlobRelocationFile(200);
    deleted.MarkBlobPhysicalFileLost(200);
    ASSERT_TRUE(apply(&deleted, &vstorage_, nullptr).IsCorruption());
  }

  VersionEdit loss;
  loss.MarkBlobPhysicalFileLost(200);
  std::unique_ptr<VersionStorageInfo> lost = new_storage(&vstorage_);
  ASSERT_OK(apply(&loss, &vstorage_, lost.get()));
  lost->ComputeBlobRelocationStateFingerprint();
  ASSERT_TRUE(lost->GetBlobRelocationFileMetaData(200)->IsMarkedLost());
  ASSERT_TRUE(lost->HasLostBlobRelocationFiles());
  ASSERT_TRUE(BlobFileTable::IsLost(lost->GetBlobFileTable().Find(200)));
  ASSERT_NE(lost->GetBlobRelocationStateFingerprintHigh(),
            vstorage_.GetBlobRelocationStateFingerprintHigh());
  // A reference whose generation number is the routed old file number and one
  // flattened to the file are both lost; a reference with another generation
  // number is not.
  const auto index_of = [](uint64_t generation, uint64_t origin) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(&encoded, generation,
                                  {.origin_file_number = origin,
                                   .original_value_offset = 11,
                                   .stored_value_size = 17,
                                   .compression = kNoCompression,
                                   .stable_checksum = 1,
                                   .stored_checksum = 2});
    BlobIndex index;
    EXPECT_OK(index.DecodeFrom(encoded));
    return index;
  };
  ASSERT_TRUE(lost->IsBlobReferenceLost(index_of(100, 100)));
  ASSERT_TRUE(lost->IsBlobReferenceLost(index_of(200, 100)));
  ASSERT_FALSE(lost->IsBlobReferenceLost(index_of(150, 150)));
  UpdateVersionStorageInfo(lost.get());

  // Deleting the lost file (after its route) clears the loss count.
  VersionEdit deletion;
  deletion.DeleteBlobRelocationRoute(100);
  deletion.DeleteBlobRelocationFile(200);
  std::unique_ptr<VersionStorageInfo> deleted = new_storage(lost.get());
  ASSERT_OK(apply(&deletion, lost.get(), deleted.get()));
  ASSERT_FALSE(deleted->HasLostBlobRelocationFiles());
}

TEST_F(VersionBuilderTest, BlobRelocationStateKeepsFileTotals) {
  // BlobRelocationState keeps two totals over its relocation files: the file
  // sizes and the record bytes from each file's directory. It updates them when
  // files are set and deleted. When a sum would overflow, it sets
  // AggregateOverflow() and stops changing the totals.
  const auto make_file = [&](uint64_t file_number,
                             const std::vector<uint64_t>& origins,
                             uint64_t file_size) {
    BlobRelocationFileAddition addition;
    EXPECT_OK(MakeRelocationAddition(file_number, origins, &addition));
    addition = BlobRelocationFileAddition(
        file_number, file_size, "crc32c", "checksum",
        addition.GetTotalEntries(), addition.GetTotalRecordBytes(),
        addition.GetDirectoryDigest(), /*tail_size=*/1);
    if (file_size != std::numeric_limits<uint64_t>::max()) {
      EXPECT_OK(addition.Validate());
    }
    return BlobRelocationFileMetaData::Create(addition);
  };
  BlobRelocationState state;
  const auto first = make_file(200, {100}, /*file_size=*/4096);
  const auto second = make_file(201, {101, 102}, /*file_size=*/1000);
  state.SetFile(first);
  state.SetFile(second);
  EXPECT_EQ(state.PhysicalFileBytes(), 5096U);
  EXPECT_EQ(state.RecordBytes(),
            first->GetTotalRecordBytes() + second->GetTotalRecordBytes());
  state.DeleteFile(200);
  EXPECT_EQ(state.PhysicalFileBytes(), 1000U);
  EXPECT_EQ(state.RecordBytes(), second->GetTotalRecordBytes());
  EXPECT_FALSE(state.AggregateOverflow());
  state.SetFile(make_file(202, {103}, std::numeric_limits<uint64_t>::max()));
  EXPECT_TRUE(state.AggregateOverflow());
  EXPECT_EQ(state.PhysicalFileBytes(), 1000U);
}

TEST_F(VersionBuilderTest, BlobRelocationSupportsMaximumFileNumber) {
  vstorage_.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  UpdateVersionStorageInfo();

  constexpr uint64_t kMaxFileNumber = kMaxBlobRelocationFileNumber;
  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(kMaxFileNumber,
                                   /*origin_file_numbers=*/{100}, &addition));
  // Numbers above kMaxBlobRelocationFileNumber are rejected, for relocation
  // files and for both ends of a route.
  BlobRelocationFileAddition too_large;
  ASSERT_TRUE(MakeRelocationAddition(kMaxFileNumber + 1,
                                     /*origin_file_numbers=*/{100}, &too_large)
                  .IsCorruption());
  ASSERT_TRUE(
      BlobRelocationRoute(kMaxFileNumber + 1, 200).Validate().IsCorruption());
  ASSERT_TRUE(
      BlobRelocationRoute(100, kMaxFileNumber + 1).Validate().IsCorruption());
  ASSERT_OK(BlobRelocationRoute(100, kMaxFileNumber).Validate());

  EnvOptions env_options;
  VersionBuilder addition_builder(env_options, &ioptions_,
                                  /*table_cache=*/nullptr, &vstorage_,
                                  /*version_set=*/nullptr);
  VersionEdit addition_edit;
  addition_edit.AddBlobRelocationFile(addition);
  ASSERT_OK(addition_builder.Apply(&addition_edit));
  VersionStorageInfo live(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(addition_builder.SaveTo(&live));
  ASSERT_NE(live.GetBlobRelocationFileMetaData(kMaxFileNumber), nullptr);

  VersionBuilder deletion_builder(env_options, &ioptions_,
                                  /*table_cache=*/nullptr, &live,
                                  /*version_set=*/nullptr);
  VersionEdit deletion_edit;
  deletion_edit.DeleteBlobRelocationFile(kMaxFileNumber);
  ASSERT_OK(deletion_builder.Apply(&deletion_edit));
  VersionStorageInfo retired(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &live,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(deletion_builder.SaveTo(&retired));
  ASSERT_EQ(retired.GetBlobRelocationFileMetaData(kMaxFileNumber), nullptr);
  ASSERT_TRUE(retired.GetBlobRelocationFiles().empty());
}

TEST_F(VersionBuilderTest, BlobFileRetirementRemovesCollectedFile) {
  // This test models an indirect blob GC install edit. Indirect-origin blob
  // file #100 still has live values, which go to relocation file #200.
  // Indirect-origin blob file #110 has no live values left.
  constexpr uint64_t kSourceFileNumber = 100;
  constexpr uint64_t kEmptyFileNumber = 110;
  constexpr uint64_t kRelocationFileNumber = 200;
  constexpr uint64_t kTableFileNumber = 101;
  vstorage_.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  AddDummyFile(kTableFileNumber, kSourceFileNumber, /*epoch_number=*/1);
  for (uint64_t file_number : {kSourceFileNumber, kEmptyFileNumber}) {
    auto shared = SharedBlobFileMetaData::Create(
        file_number, /*total_blob_count=*/2,
        /*total_blob_bytes=*/100, /*checksum_method=*/{},
        /*checksum_value=*/{}, /*is_indirect_origin=*/true);
    vstorage_.AddBlobFile(BlobFileMetaData::Create(
        std::move(shared),
        file_number == kSourceFileNumber
            ? BlobFileMetaData::LinkedSsts{kTableFileNumber}
            : BlobFileMetaData::LinkedSsts{},
        /*garbage_blob_count=*/1, /*garbage_blob_bytes=*/40));
  }
  // A normal blob file written with enable_blob_indirection off.
  constexpr uint64_t kLegacyFileNumber = 120;
  AddBlob(kLegacyFileNumber, /*total_blob_count=*/2, /*total_blob_bytes=*/100,
          /*checksum_method=*/{}, /*checksum_value=*/{},
          BlobFileMetaData::LinkedSsts{}, /*garbage_blob_count=*/1,
          /*garbage_blob_bytes=*/40);
  // The format fence is set on this Version, so its file table must list its
  // blob files.
  vstorage_.SyncBlobFileTable({}, /*rebuild=*/true, /*cache_res_mgr=*/nullptr);
  UpdateVersionStorageInfo();

  BlobRelocationFileAddition addition;
  ASSERT_OK(MakeRelocationAddition(kRelocationFileNumber, {kSourceFileNumber},
                                   &addition));
  EnvOptions env_options;
  {
    // A route from blob file #100 is rejected while #100 is still live (the
    // edit has no blob file retirement for it).
    VersionBuilder missing_retirement_builder(
        env_options, &ioptions_, /*table_cache=*/nullptr, &vstorage_,
        /*version_set=*/nullptr);
    VersionEdit missing_retirement;
    missing_retirement.AddBlobRelocationFile(addition);
    missing_retirement.SetBlobRelocationRoute(
        BlobRelocationRoute(kSourceFileNumber, kRelocationFileNumber));
    ASSERT_TRUE(
        missing_retirement_builder.Apply(&missing_retirement).IsCorruption());
  }
  {
    // A blob file retirement must name a live indirect-origin blob file. #105
    // does not exist.
    VersionBuilder unknown_builder(env_options, &ioptions_,
                                   /*table_cache=*/nullptr, &vstorage_,
                                   /*version_set=*/nullptr);
    VersionEdit unknown;
    unknown.RetireBlobFile(/*file_number=*/105);
    ASSERT_TRUE(unknown_builder.Apply(&unknown).IsCorruption());
  }
  {
    // A blob file retirement of a blob file written with
    // enable_blob_indirection off is rejected.
    VersionBuilder legacy_builder(env_options, &ioptions_,
                                  /*table_cache=*/nullptr, &vstorage_,
                                  /*version_set=*/nullptr);
    VersionEdit legacy;
    legacy.RetireBlobFile(kLegacyFileNumber);
    ASSERT_TRUE(legacy_builder.Apply(&legacy).IsCorruption());
  }
  {
    // The same file twice in one edit is rejected.
    VersionBuilder duplicate_builder(env_options, &ioptions_,
                                     /*table_cache=*/nullptr, &vstorage_,
                                     /*version_set=*/nullptr);
    VersionEdit duplicate;
    duplicate.RetireBlobFile(kEmptyFileNumber);
    duplicate.RetireBlobFile(kEmptyFileNumber);
    const Status status = duplicate_builder.Apply(&duplicate);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find("Duplicate blob file retirement"),
              std::string::npos)
        << status.ToString();
  }

  // The GC install edit adds relocation file #200, routes #100 to it, and
  // records the blob file retirement of #100 and #110. #110 has no live values,
  // so it needs no route.
  VersionEdit edit;
  edit.AddBlobRelocationFile(addition);
  edit.SetBlobRelocationRoute(
      BlobRelocationRoute(kSourceFileNumber, kRelocationFileNumber));
  edit.RetireBlobFile(kSourceFileNumber);
  edit.RetireBlobFile(kEmptyFileNumber);

  VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                         &vstorage_, /*version_set=*/nullptr);
  ASSERT_OK(builder.Apply(&edit));
  // A blob file retirement record holds only the file number. A second record
  // for #100 in a later edit of the same builder (as in a replay) changes
  // nothing.
  VersionEdit replay;
  replay.RetireBlobFile(kSourceFileNumber);
  ASSERT_OK(builder.Apply(&replay));

  VersionStorageInfo output(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/false, EpochNumberRequirement::kMightMissing,
      /*clock=*/nullptr, /*bottommost_file_compaction_delay=*/0,
      OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(builder.SaveTo(&output));
  ASSERT_EQ(output.GetBlobFileMetaData(kSourceFileNumber), nullptr);
  ASSERT_EQ(output.GetBlobFileMetaData(kEmptyFileNumber), nullptr);
  ASSERT_NE(output.GetBlobRelocationFileMetaData(kRelocationFileNumber),
            nullptr);
  ASSERT_EQ(output.GetBlobRelocationRouteTarget(kSourceFileNumber),
            kRelocationFileNumber);
  ASSERT_EQ(output.GetBlobRelocationRouteTarget(kEmptyFileNumber),
            kInvalidBlobFileNumber);
  ASSERT_EQ(output.GetBlobFileTable().Find(kSourceFileNumber),
            BlobFileTable::RouteValue(kRelocationFileNumber));
  ASSERT_EQ(output.GetBlobFileTable().Find(kEmptyFileNumber), 0U);
  ASSERT_OK(output.ValidateBlobRelocationState());
  UpdateVersionStorageInfo(&output);

  {
    // The saved Version no longer has #110, so a second blob file retirement of
    // #110 is rejected.
    VersionBuilder again_builder(env_options, &ioptions_,
                                 /*table_cache=*/nullptr, &output,
                                 /*version_set=*/nullptr);
    VersionEdit again;
    again.RetireBlobFile(kEmptyFileNumber);
    ASSERT_TRUE(again_builder.Apply(&again).IsCorruption());
  }
  {
    // #100 is now the old file number of a route, so a new blob file or
    // relocation file may not use that number.
    VersionBuilder reuse_builder(env_options, &ioptions_,
                                 /*table_cache=*/nullptr, &output,
                                 /*version_set=*/nullptr);
    VersionEdit blob_reuse;
    blob_reuse.AddBlobFile(kSourceFileNumber, /*total_blob_count=*/1,
                           /*total_blob_bytes=*/10, /*checksum_method=*/{},
                           /*checksum_value=*/{},
                           /*is_indirect_origin=*/true);
    ASSERT_TRUE(reuse_builder.Apply(&blob_reuse).IsCorruption());

    BlobRelocationFileAddition relocation_reuse;
    ASSERT_OK(
        MakeRelocationAddition(kSourceFileNumber, {50}, &relocation_reuse));
    VersionBuilder relocation_reuse_builder(env_options, &ioptions_,
                                            /*table_cache=*/nullptr, &output,
                                            /*version_set=*/nullptr);
    VersionEdit relocation_edit;
    relocation_edit.AddBlobRelocationFile(relocation_reuse);
    ASSERT_TRUE(
        relocation_reuse_builder.Apply(&relocation_edit).IsCorruption());
  }
  {
    // Late approximate garbage for the routed file is a no-op.
    VersionBuilder post_retirement_builder(env_options, &ioptions_,
                                           /*table_cache=*/nullptr, &output,
                                           /*version_set=*/nullptr);
    VersionEdit post_retirement_garbage;
    post_retirement_garbage.AddBlobFileGarbage(kSourceFileNumber,
                                               /*garbage_blob_count=*/1,
                                               /*garbage_blob_bytes=*/60);
    ASSERT_OK(post_retirement_builder.Apply(&post_retirement_garbage));
  }
  UnrefFilesInVersion(&output);
}

TEST_F(VersionBuilderTest, BlobRelocationRouteEndpoints) {
  // A route goes from an old file number that is not a live file to a live
  // relocation file.
  BlobRelocationFileAddition target;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &target));
  ASSERT_OK(AddRelocationToBase(target));
  BlobRelocationFileAddition other;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/300,
                                   /*origin_file_numbers=*/{100}, &other));
  ASSERT_OK(AddRelocationToBase(other));
  AddBlob(/*blob_file_number=*/50, /*total_blob_count=*/1,
          /*total_blob_bytes=*/100, /*checksum_method=*/"",
          /*checksum_value=*/"", BlobFileMetaData::LinkedSsts(),
          /*garbage_blob_count=*/0, /*garbage_blob_bytes=*/0);
  UpdateVersionStorageInfo();
  EnvOptions env_options;
  const struct {
    uint64_t source;
    uint64_t target;
    bool ok;
  } cases[] = {
      {/*source=*/100, /*target=*/200, /*ok=*/true},
      // The target must be a live relocation file.
      {100, 999, false},
      {100, 50, false},
      // The source must not be a live file.
      {300, 200, false},
      {50, 200, false},
  };
  for (const auto& test_case : cases) {
    SCOPED_TRACE(std::to_string(test_case.source) + "->" +
                 std::to_string(test_case.target));
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit edit;
    edit.SetBlobRelocationRoute(
        BlobRelocationRoute(test_case.source, test_case.target));
    const Status status = builder.Apply(&edit);
    if (test_case.ok) {
      ASSERT_OK(status);
    } else {
      ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    }
  }
}

TEST_F(VersionBuilderTest, BlobRelocationRejectsRouteChains) {
  // The base Version has route 100 -> 200. No route may point to a file that
  // has a route itself (one hop). A relocation file may not be deleted while
  // routes point to it.
  BlobRelocationFileAddition target;
  ASSERT_OK(MakeRelocationAddition(/*file_number=*/200,
                                   /*origin_file_numbers=*/{100}, &target));
  ASSERT_OK(AddRelocationToBase(target));
  vstorage_.AddBlobRelocationRoute(
      BlobRelocationRoute(/*source_file_number=*/100,
                          /*physical_file_number=*/200));
  UpdateVersionStorageInfo();
  ASSERT_OK(vstorage_.ValidateBlobRelocationState());

  EnvOptions env_options;
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit to_route_key;
    to_route_key.SetBlobRelocationRoute(
        BlobRelocationRoute(/*source_file_number=*/400,
                            /*physical_file_number=*/100));
    ASSERT_TRUE(builder.Apply(&to_route_key).IsCorruption());
  }
  {
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit delete_target;
    delete_target.DeleteBlobRelocationFile(/*file_number=*/200);
    ASSERT_TRUE(builder.Apply(&delete_target).IsCorruption());
  }
  {
    // An indirect blob GC job collects relocation file 200 into 300. The edit
    // deletes 200, adds route 200 -> 300 and re-points route 100 to 300. Every
    // route still has one hop.
    BlobRelocationFileAddition later_target;
    ASSERT_OK(MakeRelocationAddition(/*file_number=*/300,
                                     /*origin_file_numbers=*/{100},
                                     &later_target, {200}));
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit collect;
    collect.AddBlobRelocationFile(later_target);
    collect.DeleteBlobRelocationFile(/*file_number=*/200);
    collect.SetBlobRelocationRoute(BlobRelocationRoute(200, 300));
    collect.SetBlobRelocationRoute(BlobRelocationRoute(100, 300));
    ASSERT_OK(builder.Apply(&collect));
    VersionStorageInfo collected(
        &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
        /*force_consistency_checks=*/false,
        EpochNumberRequirement::kMightMissing, /*clock=*/nullptr,
        /*bottommost_file_compaction_delay=*/0,
        OffpeakTimeOption(options_.daily_offpeak_time_utc),
        PeriodicCompactionPhaseParams{});
    ASSERT_OK(builder.SaveTo(&collected));
    ASSERT_EQ(collected.GetBlobRelocationRouteTarget(100), 300U);
    ASSERT_EQ(collected.GetBlobRelocationRouteTarget(200), 300U);
    ASSERT_EQ(collected.GetBlobRelocationRouteTargetCount(300), 2U);
    ASSERT_OK(collected.ValidateBlobRelocationState());
  }
  {
    // Adding route 200 -> 300 without re-pointing 100 leaves route
    // 100 -> 200 pointing to deleted file 200, so the edit is rejected.
    BlobRelocationFileAddition later_target;
    ASSERT_OK(MakeRelocationAddition(/*file_number=*/300,
                                     /*origin_file_numbers=*/{100},
                                     &later_target, {200}));
    VersionBuilder builder(env_options, &ioptions_, /*table_cache=*/nullptr,
                           &vstorage_, /*version_set=*/nullptr);
    VersionEdit partial;
    partial.AddBlobRelocationFile(later_target);
    partial.DeleteBlobRelocationFile(/*file_number=*/200);
    partial.SetBlobRelocationRoute(BlobRelocationRoute(200, 300));
    ASSERT_TRUE(builder.Apply(&partial).IsCorruption());
  }
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
  // blob file 10 is in the version, so only blob files 12 and 15 stay
  // dropped.
  VersionBuilder builder(env_options, &ioptions_, table_cache, &vstorage_,
                         version_set);
  VersionEdit edit;
  for (const uint64_t blob_file_number :
       std::vector<uint64_t>{15, 5, 12, 10, 12}) {
    edit.AddDroppedBlobFile(blob_file_number);
  }
  ASSERT_OK(builder.Apply(&edit));
  const std::unique_ptr<VersionStorageInfo> new_vstorage =
      save_new_version(&builder, &vstorage_);
  ASSERT_EQ(new_vstorage->GetDroppedBlobFiles(),
            (std::vector<uint64_t>{12, 15}));
  ASSERT_TRUE(new_vstorage->IsDroppedBlobFile(12));
  ASSERT_TRUE(new_vstorage->IsDroppedBlobFile(15));
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
  ASSERT_EQ(second_vstorage->GetDroppedBlobFiles(),
            (std::vector<uint64_t>{12, 15}));

  // Once every table file only references newer blob files, blob files 12
  // and 15 can no longer be referenced and are forgotten.
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

TEST_F(VersionBuilderTest, SaveBlobFilesToWithMissingOldestReference) {
  AddDummyFile(/*table_file_number=*/9, /*blob_file_number=*/7,
               /*epoch_number=*/2);
  AddDummyFile(/*table_file_number=*/1, /*blob_file_number=*/3,
               /*epoch_number=*/1);
  AddBlob(3, 10, 1000, std::string(), std::string(),
          BlobFileMetaData::LinkedSsts{1}, 0, 0);
  AddBlob(11, 10, 1000, std::string(), std::string(),
          BlobFileMetaData::LinkedSsts{}, 0, 0);
  AddBlob(12, 10, 1000, std::string(), std::string(),
          BlobFileMetaData::LinkedSsts{}, 0, 0);
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder builder(env_options, &ioptions_, nullptr, &vstorage_, nullptr);
  VersionEdit edit;
  edit.DeleteFile(/*level=*/0, /*file_number=*/1);
  edit.AddBlobFileGarbage(3, 10, 1000);
  edit.AddBlobFileGarbage(12, 10, 1000);
  ASSERT_OK(builder.Apply(&edit));

  VersionStorageInfo retained(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &vstorage_,
      /*force_consistency_checks=*/true, EpochNumberRequirement::kMightMissing,
      nullptr, 0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(builder.SaveTo(&retained));
  UpdateVersionStorageInfo(&retained);
  ASSERT_EQ(retained.GetBlobFiles().size(), 1);
  ASSERT_EQ(retained.GetBlobFileMetaData(3), nullptr);
  ASSERT_EQ(retained.GetBlobFileMetaData(7), nullptr);
  ASSERT_EQ(retained.GetBlobFileMetaData(12), nullptr);
  const std::shared_ptr<BlobFileMetaData> healthy =
      retained.GetBlobFileMetaData(11);
  ASSERT_NE(healthy, nullptr);
  ASSERT_TRUE(healthy->GetLinkedSsts().empty());
  ASSERT_EQ(healthy->GetGarbageBlobCount(), 0);
  ASSERT_EQ(healthy->GetGarbageBlobBytes(), 0);

  VersionBuilder cleanup_builder(env_options, &ioptions_, nullptr, &retained,
                                 nullptr);
  VersionEdit cleanup;
  cleanup.DeleteFile(/*level=*/0, /*file_number=*/9);
  ASSERT_OK(cleanup_builder.Apply(&cleanup));

  VersionStorageInfo unreferenced(
      &icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel, &retained,
      /*force_consistency_checks=*/true, EpochNumberRequirement::kMightMissing,
      nullptr, 0, OffpeakTimeOption(options_.daily_offpeak_time_utc),
      PeriodicCompactionPhaseParams{});
  ASSERT_OK(cleanup_builder.SaveTo(&unreferenced));
  ASSERT_TRUE(unreferenced.GetBlobFiles().empty());

  UnrefFilesInVersion(&unreferenced);
  UnrefFilesInVersion(&retained);
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

// Tests for BlobFileTable, the packed file table of a Version. Two tests apply
// random edits and compare the table with a std::map. The others check 64-bit
// keys, shard growth and sharing, tagged values and memory charging. The
// parameter selects the plain key scan (true) or the SSE2 scan when available
// (false).
class BlobFileTableTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override { BlobFileTable::TEST_UsePlainScan(GetParam()); }
  void TearDown() override { BlobFileTable::TEST_UsePlainScan(false); }

  static uint64_t RandomValue(Random64* rnd) {
    switch (rnd->Uniform(3)) {
      case 0:
        return BlobFileTable::RouteValue(1 + rnd->Uniform(1 << 20));
      case 1:
        // A fake aligned blob file pointer; the table never reads it.
        return (1 + rnd->Uniform(1 << 20)) << 3;
      default:
        return ((1 + rnd->Uniform(1 << 20)) << 3) | 2 | (rnd->OneIn(2) ? 4 : 0);
    }
  }

  // file number -> (value, route count).
  using Expected = std::map<uint64_t, std::pair<uint64_t, uint32_t>>;

  static void CheckEqual(const BlobFileTable& table, const Expected& expected,
                         const std::vector<uint64_t>& probes) {
    ASSERT_EQ(table.size(), expected.size());
    for (const auto& entry : expected) {
      uint32_t count = 0;
      ASSERT_EQ(table.Find(entry.first, &count), entry.second.first)
          << entry.first;
      ASSERT_EQ(count, entry.second.second) << entry.first;
    }
    for (uint64_t probe : probes) {
      const auto it = expected.find(probe);
      ASSERT_EQ(table.Find(probe), it != expected.end() ? it->second.first : 0)
          << probe;
    }
    Expected visited;
    table.ForEach([&](uint64_t file_number, uint64_t value, uint32_t count) {
      ASSERT_TRUE(
          visited.emplace(file_number, std::make_pair(value, count)).second);
    });
    ASSERT_EQ(visited, expected);
    ASSERT_EQ(table.Find(kInvalidBlobFileNumber), 0);
  }

  // Applies random batches of sets and erases over keys below `key_limit`, plus
  // keys above 2^32 when `wide_keys` is true. It keeps copies of some old
  // tables ("pinned") and checks at the end that later edits did not change
  // them (the table is copy-on-write).
  static void RunRandomEdits(uint64_t seed, uint64_t key_limit,
                             bool wide_keys) {
    SCOPED_TRACE("seed=" + std::to_string(seed));
    Random64 rnd(seed);
    const auto random_key = [&]() {
      if (wide_keys && rnd.OneIn(3)) {
        return (uint64_t{1} << 32) + rnd.Uniform(key_limit);
      }
      return 1 + rnd.Uniform(key_limit);
    };
    BlobFileTable table;
    Expected expected;
    std::vector<std::pair<BlobFileTable, Expected>> pinned;
    for (int round = 0; round < 60; ++round) {
      std::vector<BlobFileTable::Op> ops;
      // Grow in the first rounds, then shrink, so shards split and merge.
      const bool erase_heavy = round >= 40;
      const size_t batch = 1 + rnd.Uniform(round % 7 == 0 ? 2000 : 100);
      for (size_t i = 0; i < batch; ++i) {
        uint64_t key = random_key();
        if (!expected.empty() && rnd.OneIn(erase_heavy ? 1 : 3)) {
          // Erase or re-set an existing key.
          auto it = expected.lower_bound(key);
          if (it == expected.end()) {
            it = expected.begin();
          }
          key = it->first;
        }
        const bool erase = rnd.OneIn(erase_heavy ? 1 : 4);
        const uint64_t value = erase ? 0 : RandomValue(&rnd);
        // Only relocation files carry a route count; some counts are large.
        const uint32_t count =
            BlobFileTable::IsRelocationFile(value)
                ? (rnd.OneIn(8) ? std::numeric_limits<uint32_t>::max() - 1
                                : static_cast<uint32_t>(rnd.Uniform(1000)))
                : 0;
        ops.push_back({key, value, count});
        if (erase) {
          expected.erase(key);
        } else {
          expected[key] = std::make_pair(value, count);
        }
      }
      table.Apply(ops);
      std::vector<uint64_t> probes;
      for (int i = 0; i < 64; ++i) {
        probes.push_back(random_key());
      }
      ASSERT_NO_FATAL_FAILURE(CheckEqual(table, expected, probes));
      if (rnd.OneIn(4)) {
        pinned.emplace_back(table, expected);
      }
    }
    for (const auto& old : pinned) {
      ASSERT_NO_FATAL_FAILURE(CheckEqual(old.first, old.second, {}));
    }
  }
};

TEST_P(BlobFileTableTest, MatchesMapOverRandomEdits) {
  for (uint64_t seed = 1; seed <= 8; ++seed) {
    ASSERT_NO_FATAL_FAILURE(RunRandomEdits(seed, /*key_limit=*/5000,
                                           /*wide_keys=*/false));
  }
}

TEST_P(BlobFileTableTest, MatchesMapWithWideKeys) {
  for (uint64_t seed = 101; seed <= 108; ++seed) {
    ASSERT_NO_FATAL_FAILURE(RunRandomEdits(seed, /*key_limit=*/5000,
                                           /*wide_keys=*/true));
  }
}

TEST_P(BlobFileTableTest, WideShardKeysShareLow32Bits) {
  // 5 and 2^32 + 5 have the same low 32 bits. Whichever shard holds 2^32 + 5
  // is wide, and a lookup must compare all 64 bits.
  const uint64_t high = (uint64_t{1} << 32) + 5;
  BlobFileTable table;
  std::vector<BlobFileTable::Op> ops{{5, BlobFileTable::RouteValue(7)},
                                     {high, BlobFileTable::RouteValue(9)}};
  // Enough other keys that some share a shard with 5 or 2^32 + 5.
  for (uint64_t key = 6; key < 200; ++key) {
    ops.push_back({key, BlobFileTable::RouteValue(key + 1)});
  }
  table.Apply(ops);
  ASSERT_EQ(table.Find(5), BlobFileTable::RouteValue(7));
  ASSERT_EQ(table.Find(high), BlobFileTable::RouteValue(9));
  ASSERT_EQ(table.Find((uint64_t{2} << 32) + 5), 0);
  table.Apply({{5, 0}});
  ASSERT_EQ(table.Find(5), 0);
  ASSERT_EQ(table.Find(high), BlobFileTable::RouteValue(9));
  table.Apply({{5, BlobFileTable::RouteValue(11)}, {high, 0}});
  ASSERT_EQ(table.Find(5), BlobFileTable::RouteValue(11));
  ASSERT_EQ(table.Find(high), 0);
}

TEST_P(BlobFileTableTest, ShardsGrowShrinkAndShare) {
  BlobFileTable table;
  std::vector<BlobFileTable::Op> ops;
  for (uint64_t key = 1; key <= 10000; ++key) {
    ops.push_back({key, BlobFileTable::RouteValue(key + 1)});
  }
  table.Apply(ops);
  ASSERT_EQ(table.size(), 10000);
  const size_t full_bytes = table.ApproximateMemoryUsage();
  // 10000 entries make 512 shards: the smallest power of two with at most
  // kTargetEntriesPerShard (32) entries per shard on average. An entry needs 16
  // bytes (4-byte key, 8-byte value, 4-byte route count). Shard headers,
  // padding and the top array add more, so the estimate is about 18 bytes per
  // entry (see the memory example in blob_relocation_manifest.h). The bounds
  // below allow 12 to 32 bytes.
  ASSERT_GE(full_bytes, 10000 * (4 + 8));
  ASSERT_LE(full_bytes, 10000 * 32);

  // A copy shares every shard. An edit of the copy leaves the original
  // unchanged.
  BlobFileTable copy(table);
  copy.Apply({{5000, 0}, {20000, BlobFileTable::RouteValue(7)}});
  ASSERT_EQ(table.Find(5000), BlobFileTable::RouteValue(5001));
  ASSERT_EQ(table.Find(20000), 0);
  ASSERT_EQ(copy.Find(5000), 0);
  ASSERT_EQ(copy.Find(20000), BlobFileTable::RouteValue(7));

  // Erasing almost everything rebuilds with fewer shards.
  ops.clear();
  for (uint64_t key = 1; key <= 10000; ++key) {
    if (key % 1000 != 0) {
      ops.push_back({key, 0});
    }
  }
  table.Apply(ops);
  ASSERT_EQ(table.size(), 10);
  ASSERT_LT(table.ApproximateMemoryUsage(), 1024);
  for (uint64_t key = 1000; key <= 10000; key += 1000) {
    ASSERT_EQ(table.Find(key), BlobFileTable::RouteValue(key + 1));
  }
  ASSERT_EQ(copy.size(), 10000);
  ASSERT_EQ(copy.Find(1), BlobFileTable::RouteValue(2));
}

TEST_P(BlobFileTableTest, TaggedValues) {
  alignas(8) static const char fake[16] = {};
  const auto* blob = reinterpret_cast<const BlobFileMetaData*>(fake);
  const auto* relocation =
      reinterpret_cast<const BlobRelocationFileMetaData*>(fake + 8);
  const uint64_t blob_value = BlobFileTable::BlobFileValue(blob);
  const uint64_t relocation_value =
      BlobFileTable::RelocationFileValue(relocation, /*lost=*/true);
  const uint64_t route_value = BlobFileTable::RouteValue(uint64_t{1} << 40);
  ASSERT_TRUE(BlobFileTable::IsBlobFile(blob_value));
  ASSERT_FALSE(BlobFileTable::IsRelocationFile(blob_value));
  ASSERT_EQ(BlobFileTable::AsBlobFile(blob_value), blob);
  ASSERT_TRUE(BlobFileTable::IsRelocationFile(relocation_value));
  ASSERT_TRUE(BlobFileTable::IsLost(relocation_value));
  ASSERT_EQ(BlobFileTable::AsRelocationFile(relocation_value), relocation);
  ASSERT_TRUE(BlobFileTable::IsRoute(route_value));
  ASSERT_FALSE(BlobFileTable::IsBlobFile(route_value));
  ASSERT_EQ(BlobFileTable::RouteTarget(route_value), uint64_t{1} << 40);
}

TEST_P(BlobFileTableTest, ChargesMemoryOnce) {
  std::shared_ptr<CacheReservationManager> manager =
      std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<
              CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>>(
              NewLRUCache(64 << 20)));
  {
    BlobFileTable table;
    table.SetCacheReservationManager(manager);
    std::vector<BlobFileTable::Op> ops;
    for (uint64_t key = 1; key <= 5000; ++key) {
      ops.push_back({key, BlobFileTable::RouteValue(key + 1)});
    }
    table.Apply(ops);
    ASSERT_EQ(manager->GetTotalMemoryUsed(), table.ApproximateMemoryUsage());
    {
      // A copy charges its own top array but shares the shards.
      BlobFileTable copy(table);
      copy.Apply({{1, 0}});
      ASSERT_GT(manager->GetTotalMemoryUsed(), table.ApproximateMemoryUsage());
      ASSERT_LT(manager->GetTotalMemoryUsed(),
                table.ApproximateMemoryUsage() * 3 / 2);
    }
    ASSERT_EQ(manager->GetTotalMemoryUsed(), table.ApproximateMemoryUsage());
  }
  ASSERT_EQ(manager->GetTotalMemoryUsed(), 0);
}

TEST_P(BlobFileTableTest, ChargeStaysUntilLastHolderFrees) {
  // Each table copy (one per Version, or one held by a census change log entry)
  // shares unchanged shards. Whichever copy is freed first, every shard that a
  // remaining copy holds stays charged. When one copy is left, the charge
  // equals its memory.
  std::shared_ptr<CacheReservationManager> manager =
      std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<
              CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>>(
              NewLRUCache(64 << 20)));
  auto first = std::make_unique<BlobFileTable>();
  first->SetCacheReservationManager(manager);
  std::vector<BlobFileTable::Op> ops;
  for (uint64_t key = 1; key <= 5000; ++key) {
    ops.push_back({key, BlobFileTable::RouteValue(key + 1)});
  }
  first->Apply(ops);
  auto second = std::make_unique<BlobFileTable>(*first);
  second->Apply({{1, 0}, {2, 0}});
  auto third = std::make_unique<BlobFileTable>(*second);
  third->Apply({{3, 0}});
  // Shared shards are charged once, so the total charge is below the sum of
  // the memory of the three tables.
  const size_t all_three = manager->GetTotalMemoryUsed();
  ASSERT_LT(all_three, first->ApproximateMemoryUsage() +
                           second->ApproximateMemoryUsage() +
                           third->ApproximateMemoryUsage());
  ASSERT_GE(all_three, first->ApproximateMemoryUsage());
  // Free the middle copy first, then the oldest, out of order.
  second.reset();
  ASSERT_GE(manager->GetTotalMemoryUsed(), first->ApproximateMemoryUsage());
  ASSERT_GE(manager->GetTotalMemoryUsed(), third->ApproximateMemoryUsage());
  first.reset();
  ASSERT_EQ(manager->GetTotalMemoryUsed(), third->ApproximateMemoryUsage());
  third.reset();
  ASSERT_EQ(manager->GetTotalMemoryUsed(), 0);
}

INSTANTIATE_TEST_CASE_P(BlobFileTableTest, BlobFileTableTest, testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
