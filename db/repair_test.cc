//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "db/blob/blob_log_format.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "file/file_util.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/wal_iterator.h"
#include "table/unique_id_impl.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class RepairTest : public DBTestBase {
 public:
  RepairTest() : DBTestBase("repair_test", /*env_do_fsync=*/true) {}

  Status GetFirstSstPath(std::string* first_sst_path) {
    assert(first_sst_path != nullptr);
    first_sst_path->clear();
    uint64_t manifest_size;
    std::vector<std::string> files;
    Status s = db_->GetLiveFiles(files, &manifest_size);
    if (s.ok()) {
      auto sst_iter =
          std::find_if(files.begin(), files.end(), [](const std::string& file) {
            uint64_t number;
            FileType type;
            bool ok = ParseFileName(file, &number, &type);
            return ok && type == kTableFile;
          });
      *first_sst_path = sst_iter == files.end() ? "" : dbname_ + *sst_iter;
    }
    return s;
  }

  void ReopenWithSstIdVerify() {
    std::atomic_int verify_passed{0};
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTable::Open::PassedVerifyUniqueId", [&](void* arg) {
          // override job status
          auto id = static_cast<UniqueId64x2*>(arg);
          assert(*id != kNullUniqueId64x2);
          verify_passed++;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    auto options = CurrentOptions();
    options.verify_sst_unique_id_in_manifest = true;
    Reopen(options);

    ASSERT_GT(verify_passed, 0);
    SyncPoint::GetInstance()->DisableProcessing();
  }

  // Reads the path and the bytes of every file in the DB directory and in its
  // lost/ subdirectory into `*files`. A test compares two snapshots to check
  // that RepairDB changed, added or removed no file. The snapshot skips the
  // info log files (LOG and LOG.old.*), because RepairDB starts a new info log
  // even when it refuses to repair.
  void SnapshotDbDir(std::map<std::string, std::string>* files) {
    files->clear();
    const InfoLogPrefix info_log_prefix(/*has_log_dir=*/false, dbname_);
    for (const std::string& dir : {dbname_, dbname_ + "/lost"}) {
      std::vector<std::string> names;
      const Status status = env_->GetChildren(dir, &names);
      if (status.IsNotFound()) {
        continue;
      }
      ASSERT_OK(status);
      for (const std::string& name : names) {
        uint64_t number = 0;
        FileType type;
        if (name == "." || name == ".." ||
            (ParseFileName(name, &number, info_log_prefix.prefix, &type) &&
             type == kInfoLogFile)) {
          continue;
        }
        const std::string path = dir + "/" + name;
        bool is_dir = false;
        ASSERT_OK(env_->IsDirectory(path, &is_dir));
        if (is_dir) {
          (*files)[path] = "<directory>";
          continue;
        }
        ASSERT_OK(ReadFileToString(env_, path, &(*files)[path]));
      }
    }
  }

  // Reads the header version of every blob file in the DB directory into
  // `*versions`.
  void GetBlobFileHeaderVersions(std::vector<uint32_t>* versions) {
    versions->clear();
    std::unordered_map<std::string, uint64_t> blob_files;
    ASSERT_OK(GetAllDataFiles(kBlobFile, &blob_files));
    for (const auto& blob_file : blob_files) {
      std::string bytes;
      ASSERT_OK(ReadFileToString(env_, blob_file.first, &bytes));
      ASSERT_GE(bytes.size(), BlobLogHeader::kSize);
      BlobLogHeader header;
      ASSERT_OK(header.DecodeFrom(Slice(bytes.data(), BlobLogHeader::kSize)));
      versions->push_back(header.version);
    }
  }

  // Deletes every file in the DB directory whose type is in `types`.
  void DeleteDbFiles(const std::vector<FileType>& types) {
    std::vector<std::string> names;
    ASSERT_OK(env_->GetChildren(dbname_, &names));
    for (const std::string& name : names) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(name, &number, &type) &&
          std::find(types.begin(), types.end(), type) != types.end()) {
        ASSERT_OK(env_->DeleteFile(dbname_ + "/" + name));
      }
    }
  }
};

TEST_F(RepairTest, RefusesBlobIndirectionDbAndChangesNoFile) {
  for (int scenario = 0; scenario != 3; ++scenario) {
    Options options = CurrentOptions();
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", "value"));
    ASSERT_OK(Flush());
    Close();
    std::vector<std::string> names;
    ASSERT_OK(env_->GetChildren(dbname_, &names));
    std::vector<std::pair<std::string, std::string>> original;
    for (const std::string& name : names) {
      uint64_t number = 0;
      FileType type;
      if (!ParseFileName(name, &number, &type) ||
          (type != kCurrentFile && type != kDescriptorFile &&
           type != kTableFile && type != kBlobFile && type != kWalFile)) {
        continue;
      }
      std::string bytes;
      ASSERT_OK(ReadFileToString(env_, dbname_ + "/" + name, &bytes));
      original.emplace_back(name, std::move(bytes));
    }
    ASSERT_FALSE(original.empty());
    if (scenario == 1) {
      // The MANIFEST read fails. RepairDB must still find the evidence in the
      // SST (its exact reference metadata) and refuse to run.
      SyncPoint::GetInstance()->SetCallBack(
          "Repairer::FeatureProbe", [](void* arg) {
            *static_cast<Status*>(arg) = Status::IOError("optional probe");
          });
    } else if (scenario == 2) {
      SyncPoint::GetInstance()->SetCallBack(
          "Repairer::BlobIndirectionDetected", [](void* arg) {
            *static_cast<Status*>(arg) =
                Status::IOError("after positive evidence");
          });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    // Only durable evidence decides if the DB uses the feature, never the
    // current option value. Here the option is off, but the files hold feature
    // data, so RepairDB must still refuse and change no file.
    options.enable_blob_indirection = false;
    const Status repaired = RepairDB(dbname_, options);
    if (scenario == 2) {
      ASSERT_TRUE(repaired.IsIOError());
    } else {
      ASSERT_TRUE(repaired.IsNotSupported()) << repaired.ToString();
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    for (const auto& file : original) {
      std::string bytes;
      ASSERT_OK(ReadFileToString(env_, dbname_ + "/" + file.first, &bytes));
      ASSERT_EQ(file.second, bytes) << file.first;
    }
  }
}

TEST_F(RepairTest, UnreadableManifestWithoutEvidenceRunsLegacyRepair) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  Close();
  SyncPoint::GetInstance()->SetCallBack(
      "Repairer::FeatureProbe", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("optional probe");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // The MANIFEST read fails, and no SST holds feature evidence, so RepairDB
  // runs the legacy repair and keeps the data. This is not a proof that the DB
  // never used the feature: RepairDB could not read the MANIFEST.
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  ASSERT_OK(RepairDB(dbname_, options));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Reopen(options);
  ASSERT_EQ("value", Get("key"));
}

TEST_F(RepairTest, NeverActivatedRepairWithPolicyEnabledKeepsLegacyOutput) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 128;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("legacy", "sst"));
  ASSERT_OK(Flush());
  // GetAllDataFiles() takes a std::unordered_map.
  std::unordered_map<std::string, uint64_t> legacy_ssts;
  ASSERT_OK(GetAllDataFiles(kTableFile, &legacy_ssts));
  ASSERT_EQ(legacy_ssts.size(), 1U);

  // The option is on, but no file holds feature evidence yet: the only SST was
  // built with the option off, and the new write stays in the WAL.
  options.enable_blob_indirection = true;
  Reopen(options);
  ASSERT_OK(Put("wal", "unflushed"));
  Close();

  ASSERT_OK(RepairDB(dbname_, options));
  // The first repair converts the WAL to an SST without exact reference
  // metadata, so a second repair finds no feature evidence and succeeds.
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  EXPECT_EQ(Get("legacy"), "sst");
  EXPECT_EQ(Get("wal"), "unflushed");
  for (const auto& sst : legacy_ssts) {
    ASSERT_OK(env_->FileExists(sst.first));
  }
  const VersionStorageInfo* const vstorage = dbfull()
                                                 ->GetVersionSet()
                                                 ->GetColumnFamilySet()
                                                 ->GetDefault()
                                                 ->current()
                                                 ->storage_info();
  size_t files = 0;
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    for (const FileMetaData* file : vstorage->LevelFiles(level)) {
      EXPECT_TRUE(file->blob_reference_descriptor.empty());
      ++files;
    }
  }
  EXPECT_EQ(files, 2U);
}

TEST_F(RepairTest, RefusesDbWithOnlyClassifiedSst) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 1 << 20;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  // A legacy L1 SST (written before blob indirection is on) with the same keys
  // makes the L0 to L0 compaction below not bottommost. A bottommost compaction
  // writes exact output.
  for (const char* key : {"a", "b"}) {
    ASSERT_OK(Put(key, "low"));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  for (const char* key : {"a", "b"}) {
    ASSERT_OK(Put(key, "value"));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  // The values are inline (min_blob_size is 1 MB). An L0 compaction output with
  // no exact input that is not bottommost gets only the classification
  // property, with frontier 0 (no indirect references; see "Coarse SST" in
  // db/blob/blob_reference_metadata.h), and no exact metadata descriptor.
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  std::vector<std::string> inputs;
  for (const SstFileMetaData& file : cf_meta.levels[0].files) {
    inputs.push_back(file.name);
  }
  ASSERT_EQ(inputs.size(), 2U);
  ASSERT_EQ(cf_meta.levels[1].files.size(), 1U);
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, 0));
  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 2U);
  // Only the L0 output has the classification; the legacy L1 SST has no
  // blob reference properties.
  std::string sst_path;
  for (const auto& entry : properties) {
    const UserCollectedProperties& collected =
        entry.second->user_collected_properties;
    ASSERT_EQ(collected.count(kBlobReferenceDescriptorPropertyName), 0U);
    if (collected.count(kBlobReferenceClassificationPropertyName) == 1U) {
      ASSERT_TRUE(sst_path.empty());
      sst_path = entry.first;
    }
  }
  ASSERT_FALSE(sst_path.empty());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  // Delete the MANIFEST, so the SST's classification is the only sign that the
  // DB used enable_blob_indirection.
  ASSERT_OK(env_->DeleteFile(manifest_path));

  // An earlier test in this binary can leave a lost/ directory, so check that
  // the failed repair moved nothing into it.
  const auto lost_files = [&](std::vector<std::string>* files) {
    const Status status = env_->GetChildren(dbname_ + "/lost", files);
    return status.IsNotFound() ? Status::OK() : status;
  };
  std::vector<std::string> lost_before;
  ASSERT_OK(lost_files(&lost_before));
  ASSERT_TRUE(RepairDB(dbname_, options).IsNotSupported());
  ASSERT_OK(env_->FileExists(sst_path));
  std::vector<std::string> lost_after;
  ASSERT_OK(lost_files(&lost_after));
  ASSERT_EQ(lost_after, lost_before);
}

TEST_F(RepairTest, SortRepairedDBL0ByEpochNumber) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);

  ASSERT_OK(Put("k1", "oldest"));
  ASSERT_OK(Put("k1", "older"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_OK(Put("k1", "old"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("k1", "new"));

  std::vector<FileMetaData*> level0_files = GetLevelFileMetadatas(0 /* level*/);
  ASSERT_EQ(level0_files.size(), 1);
  ASSERT_EQ(level0_files[0]->epoch_number, 2);
  std::vector<FileMetaData*> level1_files = GetLevelFileMetadatas(1 /* level*/);
  ASSERT_EQ(level1_files.size(), 1);
  ASSERT_EQ(level1_files[0]->epoch_number, 1);

  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  EXPECT_EQ(Get("k1"), "new");

  level0_files = GetLevelFileMetadatas(0 /* level*/);
  ASSERT_EQ(level0_files.size(), 3);
  EXPECT_EQ(level0_files[0]->epoch_number, 3);
  EXPECT_EQ(level0_files[1]->epoch_number, 2);
  EXPECT_EQ(level0_files[2]->epoch_number, 1);
  level1_files = GetLevelFileMetadatas(1 /* level*/);
  ASSERT_EQ(level1_files.size(), 0);
}

TEST_F(RepairTest, BlobIndirectionRepairLeavesDbDirUnchanged) {
  // RepairDB does not support a DB that uses blob indirection. When it finds
  // evidence of the feature, it must return NotSupported before it changes
  // any file. The test runs two cases on a DB written with
  // enable_blob_indirection:
  //   1. The MANIFEST and CURRENT stay. The MANIFEST holds the evidence.
  //   2. Every MANIFEST, CURRENT and every SST are deleted. Only the blob
  //      files are left, so their version 2 header is the only evidence.
  //      RepairDB often runs because the MANIFEST is lost, so it must find
  //      this evidence without one.
  // In both cases, the test compares a snapshot of the DB directory taken
  // before and after RepairDB: no file is changed, added or removed.
  for (const bool only_blob_files : {false, true}) {
    SCOPED_TRACE("only_blob_files=" + std::to_string(only_blob_files));
    Options options = CurrentOptions();
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", "value1"));
    ASSERT_OK(Put("key2", "value2"));
    ASSERT_OK(Flush());
    Close();

    std::vector<uint32_t> versions;
    ASSERT_NO_FATAL_FAILURE(GetBlobFileHeaderVersions(&versions));
    ASSERT_FALSE(versions.empty());
    for (const uint32_t version : versions) {
      ASSERT_EQ(version, kVersion2);
    }
    if (only_blob_files) {
      ASSERT_NO_FATAL_FAILURE(
          DeleteDbFiles({kDescriptorFile, kCurrentFile, kTableFile}));
    }

    std::map<std::string, std::string> before;
    ASSERT_NO_FATAL_FAILURE(SnapshotDbDir(&before));
    const Status repaired = RepairDB(dbname_, options);
    ASSERT_TRUE(repaired.IsNotSupported()) << repaired.ToString();
    std::map<std::string, std::string> after;
    ASSERT_NO_FATAL_FAILURE(SnapshotDbDir(&after));
    ASSERT_EQ(before, after);
  }
}

TEST_F(RepairTest, LegacyBlobFilesRepairWithoutIndirection) {
  // A DB that never used blob indirection writes blob files with a version 1
  // header. Such a header is not evidence of the feature, so RepairDB runs the
  // legacy repair on a lost MANIFEST: it succeeds, keeps the inline value and
  // leaves the blob files in place. The legacy repair does not add blob files
  // to the new MANIFEST, so the test does not read the value in the blob file.
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 16;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("inline", "small"));
  ASSERT_OK(Put("blob", "a value that goes to a blob file"));
  ASSERT_OK(Flush());
  Close();

  std::vector<uint32_t> versions;
  ASSERT_NO_FATAL_FAILURE(GetBlobFileHeaderVersions(&versions));
  ASSERT_FALSE(versions.empty());
  for (const uint32_t version : versions) {
    ASSERT_EQ(version, kVersion1);
  }
  std::unordered_map<std::string, uint64_t> blob_files;
  ASSERT_OK(GetAllDataFiles(kBlobFile, &blob_files));
  ASSERT_NO_FATAL_FAILURE(DeleteDbFiles({kDescriptorFile}));

  ASSERT_OK(RepairDB(dbname_, options));
  for (const auto& blob_file : blob_files) {
    ASSERT_OK(env_->FileExists(blob_file.first));
  }
  Reopen(options);
  ASSERT_EQ(Get("inline"), "small");
}

TEST_F(RepairTest, LostManifest) {
  // Add a couple SST files, delete the manifest, and verify RepairDB() saves
  // the day.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, LostManifestMoreDbFeatures) {
  // Add a couple SST files, delete the manifest, and verify RepairDB() saves
  // the day.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Put("key3", "val3"));
  ASSERT_OK(Put("key4", "val4"));
  ASSERT_OK(Flush());
  // Test an SST file containing only a range tombstone
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "key2",
                             "key3z"));
  ASSERT_OK(Flush());
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  // repair from sst should work with unique_id verification
  ReopenWithSstIdVerify();

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "NOT_FOUND");
  ASSERT_EQ(Get("key3"), "NOT_FOUND");
  ASSERT_EQ(Get("key4"), "val4");
}

TEST_F(RepairTest, CorruptManifest) {
  // Manifest is in an invalid format. Expect a full recovery.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  // Need to get path before Close() deletes db_, but overwrite it after Close()
  // to ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));

  ASSERT_OK(CreateFile(env_->GetFileSystem(), manifest_path, "blah",
                       false /* use_fsync */));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithSstIdVerify();

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, IncompleteManifest) {
  // In this case, the manifest is valid but does not reference all of the SST
  // files. Expect a full recovery.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  std::string orig_manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  CopyFile(orig_manifest_path, orig_manifest_path + ".tmp");
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  // Need to get path before Close() deletes db_, but overwrite it after Close()
  // to ensure Close() didn't change the manifest.
  std::string new_manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(new_manifest_path));
  // Replace the manifest with one that is only aware of the first SST file.
  CopyFile(orig_manifest_path + ".tmp", new_manifest_path);
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithSstIdVerify();

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, PostRepairSstFileNumbering) {
  // Verify after a DB is repaired, new files will be assigned higher numbers
  // than old files.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  uint64_t pre_repair_file_num = dbfull()->TEST_Current_Next_FileNo();
  Close();

  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithSstIdVerify();

  uint64_t post_repair_file_num = dbfull()->TEST_Current_Next_FileNo();
  ASSERT_GE(post_repair_file_num, pre_repair_file_num);
}

TEST_F(RepairTest, LostSst) {
  // Delete one of the SST files but preserve the manifest that refers to it,
  // then verify the DB is still usable for the intact SST.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  std::string sst_path;
  ASSERT_OK(GetFirstSstPath(&sst_path));
  ASSERT_FALSE(sst_path.empty());
  ASSERT_OK(env_->DeleteFile(sst_path));

  Close();
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  // Exactly one of the key-value pairs should be in the DB now.
  ASSERT_TRUE((Get("key") == "val") != (Get("key2") == "val2"));
}

TEST_F(RepairTest, CorruptSst) {
  // Corrupt one of the SST files but preserve the manifest that refers to it,
  // then verify the DB is still usable for the intact SST.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  std::string sst_path;
  ASSERT_OK(GetFirstSstPath(&sst_path));
  ASSERT_FALSE(sst_path.empty());

  ASSERT_OK(CreateFile(env_->GetFileSystem(), sst_path, "blah",
                       false /* use_fsync */));

  Close();
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  // Exactly one of the key-value pairs should be in the DB now.
  ASSERT_TRUE((Get("key") == "val") != (Get("key2") == "val2"));
}

TEST_F(RepairTest, UnflushedSst) {
  // This test case invokes repair while some data is unflushed, then verifies
  // that data is in the db.
  ASSERT_OK(Put("key", "val"));
  VectorLogPtr wal_files;
  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 1);
  {
    uint64_t total_ssts_size;
    std::unordered_map<std::string, uint64_t> sst_files;
    ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
    ASSERT_EQ(total_ssts_size, 0);
  }
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 0);
  {
    uint64_t total_ssts_size;
    std::unordered_map<std::string, uint64_t> sst_files;
    ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
    ASSERT_GT(total_ssts_size, 0);
  }
  ASSERT_EQ(Get("key"), "val");
}

// Test parameters:
// param 0): paranoid file check
// param 1): user-defined timestamp test mode
class RepairTestWithTimestamp
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<bool, test::UserDefinedTimestampTestMode>> {
 public:
  RepairTestWithTimestamp()
      : DBBasicTestWithTimestampBase("repair_test_with_timestamp") {}

  Status Put(const Slice& key, const Slice& ts, const Slice& value) {
    WriteOptions write_opts;
    return db_->Put(write_opts, handles_[0], key, ts, value);
  }

  void CheckGet(const ReadOptions& read_opts, const Slice& key,
                const std::string& expected_value,
                const std::string& expected_ts) {
    std::string actual_value;
    std::string actual_ts;
    ASSERT_OK(db_->Get(read_opts, handles_[0], key, &actual_value, &actual_ts));
    ASSERT_EQ(expected_value, actual_value);
    ASSERT_EQ(expected_ts, actual_ts);
  }

  void CheckFileBoundaries(const Slice& smallest_user_key,
                           const Slice& largest_user_key) {
    std::vector<std::vector<FileMetaData>> level_to_files;
    dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                    &level_to_files);
    ASSERT_GT(level_to_files.size(), 1);
    // L0 only has one SST file.
    ASSERT_EQ(level_to_files[0].size(), 1);
    auto file_meta = level_to_files[0][0];
    ASSERT_EQ(smallest_user_key, file_meta.smallest.user_key());
    ASSERT_EQ(largest_user_key, file_meta.largest.user_key());
  }
};

TEST_P(RepairTestWithTimestamp, UnflushedSst) {
  Destroy(last_options_);

  bool paranoid_file_checks = std::get<0>(GetParam());
  bool persist_udt = test::ShouldPersistUDT(std::get<1>(GetParam()));
  std::string smallest_ukey_without_ts = "bar";
  std::string largest_ukey_without_ts = "foo";
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  std::string min_ts;
  std::string write_ts;
  PutFixed64(&min_ts, 0);
  PutFixed64(&write_ts, 1);
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = persist_udt;
  if (!persist_udt) {
    options.allow_concurrent_memtable_write = false;
  }
  options.paranoid_file_checks = paranoid_file_checks;

  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);

  ASSERT_OK(DB::Open(options, dbname_, column_families, &handles_, &db_));

  ASSERT_OK(Put(smallest_ukey_without_ts, write_ts,
                smallest_ukey_without_ts + ":val"));
  ASSERT_OK(
      Put(largest_ukey_without_ts, write_ts, largest_ukey_without_ts + ":val"));
  VectorLogPtr wal_files;
  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 1);
  {
    uint64_t total_ssts_size;
    std::unordered_map<std::string, uint64_t> sst_files;
    ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
    ASSERT_EQ(total_ssts_size, 0);
  }
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  ASSERT_OK(DB::Open(options, dbname_, column_families, &handles_, &db_));

  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 0);
  {
    uint64_t total_ssts_size;
    std::unordered_map<std::string, uint64_t> sst_files;
    ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
    ASSERT_GT(total_ssts_size, 0);
  }

  // Check file boundaries are correct for different
  // `persist_user_defined_timestamps` option values.
  if (persist_udt) {
    CheckFileBoundaries(smallest_ukey_without_ts + write_ts,
                        largest_ukey_without_ts + write_ts);
  } else {
    CheckFileBoundaries(smallest_ukey_without_ts + min_ts,
                        largest_ukey_without_ts + min_ts);
  }

  ReadOptions read_opts;
  Slice read_ts_slice = write_ts;
  read_opts.timestamp = &read_ts_slice;
  if (persist_udt) {
    CheckGet(read_opts, smallest_ukey_without_ts,
             smallest_ukey_without_ts + ":val", write_ts);
    CheckGet(read_opts, largest_ukey_without_ts,
             largest_ukey_without_ts + ":val", write_ts);
  } else {
    // TODO(yuzhangyu): currently when `persist_user_defined_timestamps` is
    //  false, ts is unconditionally stripped during flush.
    //  When `full_history_ts_low` is set and respected during flush.
    //  We should prohibit reading below `full_history_ts_low` all together.
    CheckGet(read_opts, smallest_ukey_without_ts,
             smallest_ukey_without_ts + ":val", min_ts);
    CheckGet(read_opts, largest_ukey_without_ts,
             largest_ukey_without_ts + ":val", min_ts);
  }
}

// Param 0: paranoid file check
// Param 1: test mode for the user-defined timestamp feature
INSTANTIATE_TEST_CASE_P(
    UnflushedSst, RepairTestWithTimestamp,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Values(
            test::UserDefinedTimestampTestMode::kStripUserDefinedTimestamp,
            test::UserDefinedTimestampTestMode::kNormal)));

TEST_F(RepairTest, SeparateWalDir) {
  do {
    Options options = CurrentOptions();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", "val"));
    ASSERT_OK(Put("foo", "bar"));
    VectorLogPtr wal_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
    ASSERT_EQ(wal_files.size(), 1);
    {
      uint64_t total_ssts_size;
      std::unordered_map<std::string, uint64_t> sst_files;
      ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
      ASSERT_EQ(total_ssts_size, 0);
    }
    std::string manifest_path =
        DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

    Close();
    ASSERT_OK(env_->FileExists(manifest_path));
    ASSERT_OK(env_->DeleteFile(manifest_path));
    ASSERT_OK(RepairDB(dbname_, options));

    // make sure that all WALs are converted to SSTables.
    options.wal_dir = "";

    ReopenWithSstIdVerify();
    ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
    ASSERT_EQ(wal_files.size(), 0);
    {
      uint64_t total_ssts_size;
      std::unordered_map<std::string, uint64_t> sst_files;
      ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
      ASSERT_GT(total_ssts_size, 0);
    }
    ASSERT_EQ(Get("key"), "val");
    ASSERT_EQ(Get("foo"), "bar");

  } while (ChangeWalOptions());
}

TEST_F(RepairTest, RepairMultipleColumnFamilies) {
  // Verify repair logic associates SST files with their original column
  // families.
  const int kNumCfs = 3;
  const int kEntriesPerCf = 2;
  DestroyAndReopen(CurrentOptions());
  CreateAndReopenWithCF({"pikachu1", "pikachu2"}, CurrentOptions());
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_OK(Put(i, "key" + std::to_string(j), "val" + std::to_string(j)));
      if (j == kEntriesPerCf - 1 && i == kNumCfs - 1) {
        // Leave one unflushed so we can verify WAL entries are properly
        // associated with column families.
        continue;
      }
      ASSERT_OK(Flush(i));
    }
  }

  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() doesn't re-create the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithColumnFamilies({"default", "pikachu1", "pikachu2"},
                           CurrentOptions());
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + std::to_string(j)), "val" + std::to_string(j));
    }
  }
}

TEST_F(RepairTest, RepairColumnFamilyOptions) {
  // Verify repair logic uses correct ColumnFamilyOptions when repairing a
  // database with different options for column families.
  const int kNumCfs = 2;
  const int kEntriesPerCf = 2;

  Options opts(CurrentOptions());
  Options rev_opts(CurrentOptions());
  opts.comparator = BytewiseComparator();
  rev_opts.comparator = ReverseBytewiseComparator();

  DestroyAndReopen(opts);
  CreateColumnFamilies({"reverse"}, rev_opts);
  ReopenWithColumnFamilies({"default", "reverse"},
                           std::vector<Options>{opts, rev_opts});
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_OK(Put(i, "key" + std::to_string(j), "val" + std::to_string(j)));
      if (i == kNumCfs - 1 && j == kEntriesPerCf - 1) {
        // Leave one unflushed so we can verify RepairDB's flush logic
        continue;
      }
      ASSERT_OK(Flush(i));
    }
  }
  Close();

  // RepairDB() records the comparator in the manifest, and DB::Open would fail
  // if a different comparator were used.
  ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}, {"reverse", rev_opts}},
                     opts /* unknown_cf_opts */));
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "reverse"},
                                        std::vector<Options>{opts, rev_opts}));
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + std::to_string(j)), "val" + std::to_string(j));
    }
  }

  // Examine table properties to verify RepairDB() used the right options when
  // converting WAL->SST
  TablePropertiesCollection fname_to_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[1], &fname_to_props));
  ASSERT_EQ(fname_to_props.size(), 2U);
  for (const auto& fname_and_props : fname_to_props) {
    std::string comparator_name(rev_opts.comparator->Name());
    ASSERT_EQ(comparator_name, fname_and_props.second->comparator_name);
  }
  Close();

  // Also check comparator when it's provided via "unknown" CF options
  ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}},
                     rev_opts /* unknown_cf_opts */));
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "reverse"},
                                        std::vector<Options>{opts, rev_opts}));
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + std::to_string(j)), "val" + std::to_string(j));
    }
  }
}

TEST_F(RepairTest, DbNameContainsTrailingSlash) {
  {
    bool tmp;
    if (env_->AreFilesSame("", "", &tmp).IsNotSupported()) {
      fprintf(stderr,
              "skipping RepairTest.DbNameContainsTrailingSlash due to "
              "unsupported Env::AreFilesSame\n");
      return;
    }
  }

  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  Close();

  ASSERT_OK(RepairDB(dbname_ + "/", CurrentOptions()));
  ReopenWithSstIdVerify();
  ASSERT_EQ(Get("key"), "val");
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
