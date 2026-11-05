//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

#include "db/blob/blob_log_writer.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/wal_iterator.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void WriteFooterlessBlobFile(const ImmutableOptions& immutable_options,
                             uint64_t blob_file_number) {
  assert(!immutable_options.cf_paths.empty());

  const std::string blob_file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                            FileOptions()));

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, FileOptions(), immutable_options.clock));

  constexpr Statistics* statistics = nullptr;
  constexpr bool use_fsync = false;
  constexpr bool do_flush = false;
  BlobLogWriter blob_log_writer(std::move(file_writer), immutable_options.clock,
                                statistics, use_fsync, do_flush);

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  BlobLogHeader header(/*column_family_id=*/0, kNoCompression, has_ttl,
                       expiration_range);
  ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));

  uint64_t key_offset = 0;
  uint64_t blob_offset = 0;
  ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), "key", "blob",
                                      &key_offset, &blob_offset));
  ASSERT_OK(blob_log_writer.file()->Close(IOOptions()));
}

std::vector<uint64_t> ListBlobFileNumbers(Env* env, const std::string& path) {
  assert(env != nullptr);

  std::vector<std::string> children;
  EXPECT_OK(env->GetChildren(path, &children));

  std::vector<uint64_t> blob_file_numbers;
  for (const auto& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kBlobFile) {
      blob_file_numbers.push_back(number);
    }
  }

  std::sort(blob_file_numbers.begin(), blob_file_numbers.end());
  return blob_file_numbers;
}

}  // namespace

class ObsoleteFilesTest : public DBTestBase {
 public:
  ObsoleteFilesTest()
      : DBTestBase("obsolete_files_test", /*env_do_fsync=*/true),
        wal_dir_(dbname_ + "/wal_files") {}

  void AddKeys(int numkeys, int startkey) {
    WriteOptions options;
    options.sync = false;
    for (int i = startkey; i < (numkeys + startkey); i++) {
      std::string temp = std::to_string(i);
      Slice key(temp);
      Slice value(temp);
      ASSERT_OK(db_->Put(options, key, value));
    }
  }

  void createLevel0Files(int numFiles, int numKeysPerFile) {
    int startKey = 0;
    for (int i = 0; i < numFiles; i++) {
      AddKeys(numKeysPerFile, startKey);
      startKey += numKeysPerFile;
      ASSERT_OK(dbfull()->TEST_FlushMemTable());
      ASSERT_OK(
          dbfull()->TEST_WaitForCompact());  // wait for background flush (flush
                                             // is also a kind of compaction).
    }
  }

  void CheckFileTypeCounts(const std::string& dir, int required_log,
                           int required_sst, int required_manifest) {
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dir, &filenames));

    int log_cnt = 0;
    int sst_cnt = 0;
    int manifest_cnt = 0;
    for (const auto& file : filenames) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type)) {
        log_cnt += (type == kWalFile);
        sst_cnt += (type == kTableFile);
        manifest_cnt += (type == kDescriptorFile);
      }
    }
    ASSERT_EQ(required_log, log_cnt);
    ASSERT_EQ(required_sst, sst_cnt);
    ASSERT_EQ(required_manifest, manifest_cnt);
  }

  void ReopenDB() {
    Options options = CurrentOptions();
    // Trigger compaction when the number of level 0 files reaches 2.
    options.create_if_missing = true;
    options.level0_file_num_compaction_trigger = 2;
    options.disable_auto_compactions = false;
    options.delete_obsolete_files_period_micros = 0;  // always do full purge
    options.enable_thread_tracking = true;
    options.write_buffer_size = 1024 * 1024 * 1000;
    options.target_file_size_base = 1024 * 1024 * 1000;
    options.max_bytes_for_level_base = 1024 * 1024 * 1000;
    options.WAL_ttl_seconds = 300;     // Used to test log files
    options.WAL_size_limit_MB = 1024;  // Used to test log files
    options.wal_dir = wal_dir_;

    // Note: the following prevents an otherwise harmless data race between the
    // test setup code (AddBlobFile) in ObsoleteFilesTest.BlobFiles and the
    // periodic stat dumping thread.
    options.stats_dump_period_sec = 0;

    Destroy(options);
    Reopen(options);
  }

  const std::string wal_dir_;
};

TEST_F(ObsoleteFilesTest, RaceForObsoleteFileDeletion) {
  ReopenDB();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BackgroundCallCompaction:FoundObsoleteFiles",
       "ObsoleteFilesTest::RaceForObsoleteFileDeletion:1"},
      {"DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles",
       "ObsoleteFilesTest::RaceForObsoleteFileDeletion:2"},
  });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DeleteObsoleteFileImpl:AfterDeletion", [&](void* arg) {
        Status* p_status = static_cast<Status*>(arg);
        ASSERT_OK(*p_status);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CloseHelper:PendingPurgeFinished", [&](void* arg) {
        std::unordered_set<uint64_t>* files_grabbed_for_purge_ptr =
            reinterpret_cast<std::unordered_set<uint64_t>*>(arg);
        ASSERT_TRUE(files_grabbed_for_purge_ptr->empty());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  createLevel0Files(2, 50000);
  CheckFileTypeCounts(wal_dir_, 1, 0, 0);

  port::Thread user_thread([this]() {
    JobContext jobCxt(0);
    TEST_SYNC_POINT("ObsoleteFilesTest::RaceForObsoleteFileDeletion:1");
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&jobCxt, true /* force=true */,
                                false /* no_full_scan=false */);
    dbfull()->TEST_UnlockMutex();
    TEST_SYNC_POINT("ObsoleteFilesTest::RaceForObsoleteFileDeletion:2");
    dbfull()->PurgeObsoleteFiles(jobCxt);
    jobCxt.Clean();
  });

  user_thread.join();
}

TEST_F(ObsoleteFilesTest, DeleteObsoleteOptionsFile) {
  ReopenDB();

  createLevel0Files(2, 50000);
  CheckFileTypeCounts(wal_dir_, 1, 0, 0);

  ASSERT_OK(dbfull()->DisableFileDeletions());
  for (int i = 0; i != 4; ++i) {
    if (i % 2) {
      ASSERT_OK(dbfull()->SetOptions(dbfull()->DefaultColumnFamily(),
                                     {{"paranoid_file_checks", "false"}}));
    } else {
      ASSERT_OK(dbfull()->SetOptions(dbfull()->DefaultColumnFamily(),
                                     {{"paranoid_file_checks", "true"}}));
    }
  }
  ASSERT_OK(dbfull()->EnableFileDeletions());

  Close();

  std::vector<std::string> files;
  int opts_file_count = 0;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  for (const auto& file : files) {
    uint64_t file_num;
    Slice dummy_info_log_name_prefix;
    FileType type;
    WalFileType log_type;
    if (ParseFileName(file, &file_num, dummy_info_log_name_prefix, &type,
                      &log_type) &&
        type == kOptionsFile) {
      opts_file_count++;
    }
  }
  ASSERT_EQ(2, opts_file_count);
}

TEST_F(ObsoleteFilesTest, BlobFiles) {
  ReopenDB();

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);
  assert(versions->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  const auto& cf_paths = cfd->ioptions().cf_paths;
  assert(!cf_paths.empty());

  const std::string& path = cf_paths.front().path;

  // Add an obsolete blob file.
  constexpr uint64_t first_blob_file_number = 234;
  versions->AddObsoleteBlobFile(first_blob_file_number, path);

  // Add a live blob file.
  Version* const version = cfd->current();
  assert(version);

  VersionStorageInfo* const storage_info = version->storage_info();
  assert(storage_info);

  constexpr uint64_t second_blob_file_number = 456;
  constexpr uint64_t second_total_blob_count = 100;
  constexpr uint64_t second_total_blob_bytes = 2000000;
  constexpr char second_checksum_method[] = "CRC32B";
  constexpr char second_checksum_value[] = "\x6d\xbd\xf2\x3a";

  auto shared_meta = SharedBlobFileMetaData::Create(
      second_blob_file_number, second_total_blob_count, second_total_blob_bytes,
      second_checksum_method, second_checksum_value);

  constexpr uint64_t second_garbage_blob_count = 0;
  constexpr uint64_t second_garbage_blob_bytes = 0;

  auto meta = BlobFileMetaData::Create(
      std::move(shared_meta), BlobFileMetaData::LinkedSsts(),
      second_garbage_blob_count, second_garbage_blob_bytes);

  storage_info->AddBlobFile(std::move(meta));

  // Check for obsolete files and make sure the first blob file is picked up
  // and grabbed for purge. The second blob file should be on the live list.
  constexpr int job_id = 0;
  JobContext job_context{job_id};

  dbfull()->TEST_LockMutex();
  constexpr bool force_full_scan = false;
  dbfull()->FindObsoleteFiles(&job_context, force_full_scan);
  dbfull()->TEST_UnlockMutex();

  ASSERT_TRUE(job_context.HaveSomethingToDelete());
  ASSERT_EQ(job_context.blob_delete_files.size(), 1);
  ASSERT_EQ(job_context.blob_delete_files[0].GetBlobFileNumber(),
            first_blob_file_number);

  const auto& files_grabbed_for_purge =
      dbfull()->TEST_GetFilesGrabbedForPurge();
  ASSERT_NE(files_grabbed_for_purge.find(first_blob_file_number),
            files_grabbed_for_purge.end());

  ASSERT_EQ(job_context.blob_live.size(), 1);
  ASSERT_EQ(job_context.blob_live[0], second_blob_file_number);

  // Hack the job context a bit by adding a few files to the full scan
  // list and adjusting the pending file number. We add the two files
  // above as well as two additional ones, where one is old
  // and should be cleaned up, and the other is still pending.
  constexpr uint64_t old_blob_file_number = 123;
  constexpr uint64_t pending_blob_file_number = 567;

  job_context.full_scan_candidate_files.emplace_back(
      BlobFileName(old_blob_file_number), path);
  job_context.full_scan_candidate_files.emplace_back(
      BlobFileName(first_blob_file_number), path);
  job_context.full_scan_candidate_files.emplace_back(
      BlobFileName(second_blob_file_number), path);
  job_context.full_scan_candidate_files.emplace_back(
      BlobFileName(pending_blob_file_number), path);

  job_context.min_pending_output = pending_blob_file_number;
  job_context.min_blob_file_number_to_keep = pending_blob_file_number;

  // Purge obsolete files and make sure we purge the old file and the first file
  // (and keep the second file and the pending file).
  std::vector<std::string> deleted_files;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DeleteObsoleteFileImpl::BeforeDeletion", [&](void* arg) {
        const std::string* file = static_cast<std::string*>(arg);
        assert(file);

        constexpr char blob_extension[] = ".blob";

        if (file->find(blob_extension) != std::string::npos) {
          deleted_files.emplace_back(*file);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  dbfull()->PurgeObsoleteFiles(job_context);
  job_context.Clean();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(files_grabbed_for_purge.find(first_blob_file_number),
            files_grabbed_for_purge.end());

  std::sort(deleted_files.begin(), deleted_files.end());
  const std::vector<std::string> expected_deleted_files{
      BlobFileName(path, old_blob_file_number),
      BlobFileName(path, first_blob_file_number)};

  ASSERT_EQ(deleted_files, expected_deleted_files);
}

TEST_F(ObsoleteFilesTest, FooterlessBlobFileIsKeptDuringPurge) {
  ReopenDB();

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);
  assert(versions->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  const auto& cf_paths = cfd->ioptions().cf_paths;
  assert(!cf_paths.empty());
  const std::string& path = cf_paths.front().path;

  constexpr uint64_t blob_file_number = 777;
  const std::string blob_file_path = BlobFileName(path, blob_file_number);
  WriteFooterlessBlobFile(cfd->ioptions(), blob_file_number);
  ASSERT_OK(env_->FileExists(blob_file_path));

  constexpr int job_id = 0;
  JobContext job_context{job_id};
  dbfull()->TEST_LockMutex();
  constexpr bool force_full_scan = false;
  dbfull()->FindObsoleteFiles(&job_context, force_full_scan);
  dbfull()->TEST_UnlockMutex();

  job_context.full_scan_candidate_files.emplace_back(
      BlobFileName(blob_file_number), path);
  job_context.min_pending_output = blob_file_number + 1;
  job_context.min_blob_file_number_to_keep = blob_file_number + 1;

  bool deleted = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DeleteObsoleteFileImpl::BeforeDeletion", [&](void* arg) {
        const std::string* file = static_cast<std::string*>(arg);
        assert(file);
        if (*file == blob_file_path) {
          deleted = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  dbfull()->PurgeObsoleteFiles(job_context);
  job_context.Clean();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_FALSE(deleted);
  ASSERT_OK(env_->FileExists(blob_file_path));
}

TEST_F(ObsoleteFilesTest, SealedDirectWriteBlobFileIsKeptDuringPurge) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.delete_obsolete_files_period_micros = 0;
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 1;
  options.min_blob_size = 16;
  options.blob_file_size = 200;
  options.write_buffer_size = 1024 * 1024;
  options.target_file_size_base = 1024 * 1024;
  options.max_bytes_for_level_base = 1024 * 1024;

  Destroy(options);
  Reopen(options);

  constexpr int job_id = 0;
  JobContext job_context{job_id};
  dbfull()->TEST_LockMutex();
  constexpr bool force_full_scan = false;
  dbfull()->FindObsoleteFiles(&job_context, force_full_scan);
  dbfull()->TEST_UnlockMutex();

  const std::string key1 = "blob_key_1";
  const std::string key2 = "blob_key_2";
  const std::string value1(128, 'a');
  const std::string value2(128, 'b');

  ASSERT_OK(db_->Put(WriteOptions(), key1, value1));
  ASSERT_OK(db_->Put(WriteOptions(), key2, value2));

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);
  assert(versions->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);
  assert(!cfd->ioptions().cf_paths.empty());
  const std::string& path = cfd->ioptions().cf_paths.front().path;

  const std::vector<uint64_t> blob_file_numbers =
      ListBlobFileNumbers(env_, path);
  ASSERT_EQ(blob_file_numbers.size(), 2U);

  const uint64_t sealed_blob_file_number = blob_file_numbers.front();
  const std::string sealed_blob_file_path =
      BlobFileName(path, sealed_blob_file_number);

  job_context.full_scan_candidate_files.emplace_back(
      BlobFileName(sealed_blob_file_number), path);
  job_context.min_pending_output = sealed_blob_file_number + 1;
  job_context.min_blob_file_number_to_keep = sealed_blob_file_number + 1;

  bool deleted = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DeleteObsoleteFileImpl::BeforeDeletion", [&](void* arg) {
        const std::string* file = static_cast<std::string*>(arg);
        assert(file);
        if (*file == sealed_blob_file_path) {
          deleted = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  dbfull()->PurgeObsoleteFiles(job_context, /*schedule_only=*/true);
  job_context.Clean();
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), key1, &value));
  ASSERT_EQ(value, value1);
  ASSERT_FALSE(deleted);
  ASSERT_OK(env_->FileExists(sealed_blob_file_path));
}

TEST_F(ObsoleteFilesTest, GetSortedWalFilesHangsAfterNoopPurge) {
  // This test used to trigger a hang in `DB::GetSortedWalFiles()`, where it
  // would wait for a no-op purge that did not signal the CV upon completion.

  // Grab an iterator and flush to switch the super version. That way, when the
  // iterator is destroyed, it will go through the purge path.
  DB* db =
      db_.get();  // Only using `db` makes it clear we only use DB-level APIs.
  ASSERT_OK(db->Put(WriteOptions(), "key", "value"));
  std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
  ASSERT_OK(db->Flush(FlushOptions()));

  // Sync points ensure `GetSortedWalFiles()` waits for a purge after
  // `FindObsoleteFiles()` releases the mutex but before its corresponding purge
  // completes.
  SyncPoint::GetInstance()->SetCallBack(
      "FindObsoleteFiles::PostMutexUnlock", [&](void* /* arg */) {
        TEST_SYNC_POINT(
            "ObsoleteFilesTest::GetSortedWalFilesHangsAfterNoopPurge:"
            "InCallback:1");
        TEST_SYNC_POINT(
            "ObsoleteFilesTest::GetSortedWalFilesHangsAfterNoopPurge:"
            "InCallback:2");
      });
  SyncPoint::GetInstance()->LoadDependency({
      {"ObsoleteFilesTest::GetSortedWalFilesHangsAfterNoopPurge:InCallback:1",
       "ObsoleteFilesTest::GetSortedWalFilesHangsAfterNoopPurge:Thread:Begin"},
      {"DBImpl::GetSortedWalFilesImpl:WaitPurge",
       "ObsoleteFilesTest::GetSortedWalFilesHangsAfterNoopPurge:InCallback:2"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread get_sorted_wal_files_thread([db]() {
    TEST_SYNC_POINT(
        "ObsoleteFilesTest::GetSortedWalFilesHangsAfterNoopPurge:Thread:Begin");
    VectorWalPtr files;
    ASSERT_OK(db->GetSortedWalFiles(files));
  });
  iter.reset();
  get_sorted_wal_files_thread.join();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
