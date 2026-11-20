//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {
class OptionsFileTest : public testing::Test {
 public:
  OptionsFileTest() : dbname_(test::PerThreadDBPath("options_file_test")) {}

  std::string dbname_;
};

namespace {
std::string Basename(const std::string& fname) {
  const size_t basename_pos = fname.find_last_of("/\\");
  return basename_pos == std::string::npos ? fname
                                           : fname.substr(basename_pos + 1);
}

bool IsOptionsFile(const std::string& fname) {
  return Basename(fname).find(kOptionsFileNamePrefix) == 0;
}

class FailOptionsDirFsyncFileSystem : public FileSystemWrapper {
 public:
  explicit FailOptionsDirFsyncFileSystem(
      const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  static const char* kClassName() { return "FailOptionsDirFsyncFileSystem"; }
  const char* Name() const override { return kClassName(); }

  void FailOptionsDirFsyncAfterNextOptionsFile() {
    observe_options_file_.store(true, std::memory_order_release);
    fail_options_dir_fsync_.store(false, std::memory_order_release);
    options_dir_fsync_failures_.store(0, std::memory_order_release);
  }

  int options_dir_fsync_failures() const {
    return options_dir_fsync_failures_.load(std::memory_order_acquire);
  }

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    IOStatus s =
        FileSystemWrapper::NewWritableFile(fname, file_opts, result, dbg);
    if (!s.ok()) {
      return s;
    }
    if (observe_options_file_.load(std::memory_order_acquire) &&
        IsOptionsFile(fname)) {
      fail_options_dir_fsync_.store(true, std::memory_order_release);
      observe_options_file_.store(false, std::memory_order_release);
    }
    return s;
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    IOStatus s = FileSystemWrapper::NewDirectory(name, io_opts, result, dbg);
    if (s.ok()) {
      *result = std::make_unique<FailOptionsDirFsyncDirectory>(
          std::move(*result), *this);
    }
    return s;
  }

 private:
  class FailOptionsDirFsyncDirectory : public FSDirectoryWrapper {
   public:
    FailOptionsDirFsyncDirectory(std::unique_ptr<FSDirectory>&& dir,
                                 FailOptionsDirFsyncFileSystem& fs)
        : FSDirectoryWrapper(std::move(dir)), fs_(fs) {}

    IOStatus FsyncWithDirOptions(
        const IOOptions& options, IODebugContext* dbg,
        const DirFsyncOptions& dir_fsync_options) override {
      if (fs_.fail_options_dir_fsync_.load(std::memory_order_acquire) &&
          IsOptionsFile(dir_fsync_options.renamed_new_name)) {
        fs_.fail_options_dir_fsync_.store(false, std::memory_order_release);
        fs_.options_dir_fsync_failures_.fetch_add(1, std::memory_order_acq_rel);
        return IOStatus::IOError("Injected OPTIONS directory fsync failure");
      }
      return FSDirectoryWrapper::FsyncWithDirOptions(options, dbg,
                                                     dir_fsync_options);
    }

   private:
    FailOptionsDirFsyncFileSystem& fs_;
  };

  std::atomic<bool> observe_options_file_{false};
  std::atomic<bool> fail_options_dir_fsync_{false};
  std::atomic<int> options_dir_fsync_failures_{0};
};

void UpdateOptionsFiles(DB* db,
                        std::unordered_set<std::string>* filename_history,
                        int* options_files_count) {
  std::vector<std::string> filenames;
  EXPECT_OK(db->GetEnv()->GetChildren(db->GetName(), &filenames));
  uint64_t number;
  FileType type;
  *options_files_count = 0;
  for (const auto& filename : filenames) {
    if (ParseFileName(filename, &number, &type) && type == kOptionsFile) {
      filename_history->insert(filename);
      (*options_files_count)++;
    }
  }
}

// Verify whether the current Options Files are the latest ones.
void VerifyOptionsFileName(
    DB* db, const std::unordered_set<std::string>& past_filenames) {
  std::vector<std::string> filenames;
  std::unordered_set<std::string> current_filenames;
  EXPECT_OK(db->GetEnv()->GetChildren(db->GetName(), &filenames));
  uint64_t number;
  FileType type;
  for (const auto& filename : filenames) {
    if (ParseFileName(filename, &number, &type) && type == kOptionsFile) {
      current_filenames.insert(filename);
    }
  }
  for (const auto& past_filename : past_filenames) {
    if (current_filenames.find(past_filename) != current_filenames.end()) {
      continue;
    }
    for (const auto& filename : current_filenames) {
      ASSERT_GT(filename, past_filename);
    }
  }
}

int CountOptionsFiles(Env* env, const std::string& dbname) {
  std::vector<std::string> filenames;
  EXPECT_OK(env->GetChildren(dbname, &filenames));

  int options_files_count = 0;
  uint64_t number;
  FileType type;
  for (const auto& filename : filenames) {
    if (ParseFileName(filename, &number, &type) && type == kOptionsFile) {
      ++options_files_count;
    }
  }
  return options_files_count;
}

int CountOptionsFiles(DB* db) {
  return CountOptionsFiles(db->GetEnv(), db->GetName());
}

void GenerateStaleOptionsFiles(DB* db, int options_file_count) {
  ASSERT_OK(db->DisableFileDeletions());
  for (int i = 0; i < options_file_count; ++i) {
    ASSERT_OK(db->SetOptions(
        {{"level0_file_num_compaction_trigger", std::to_string(8 + i)}}));
  }
  ASSERT_GT(CountOptionsFiles(db), 2);
}
}  // anonymous namespace

TEST_F(OptionsFileTest, NumberOfOptionsFiles) {
  const int kReopenCount = 20;
  Options opt;
  opt.create_if_missing = true;
  ASSERT_OK(DestroyDB(dbname_, opt));
  std::unordered_set<std::string> filename_history;
  std::unique_ptr<DB> db;
  for (int i = 0; i < kReopenCount; ++i) {
    ASSERT_OK(DB::Open(opt, dbname_, &db));
    int num_options_files = 0;
    UpdateOptionsFiles(db.get(), &filename_history, &num_options_files);
    ASSERT_GT(num_options_files, 0);
    ASSERT_LE(num_options_files, 2);
    // Make sure we always keep the latest option files.
    VerifyOptionsFileName(db.get(), filename_history);
    db.reset();
  }
}

TEST_F(OptionsFileTest, ObsoleteOptionsFilesPurgedSynchronouslyOnOpen) {
  Options opt;
  opt.create_if_missing = true;
  ASSERT_OK(DestroyDB(dbname_, opt));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(opt, dbname_, &db));
  GenerateStaleOptionsFiles(db.get(), 5);
  db.reset();

  opt.create_if_missing = false;
  opt.avoid_unnecessary_blocking_io = false;
  ASSERT_OK(DB::Open(opt, dbname_, &db));
  ASSERT_LE(CountOptionsFiles(db.get()), 2);
}

TEST_F(OptionsFileTest, ObsoleteOptionsFilesPurgedInBackgroundOnOpen) {
  Options opt;
  opt.create_if_missing = true;
  ASSERT_OK(DestroyDB(dbname_, opt));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(opt, dbname_, &db));
  GenerateStaleOptionsFiles(db.get(), 5);
  db.reset();

  opt.create_if_missing = false;
  opt.avoid_unnecessary_blocking_io = true;

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"OptionsFileTest::ObsoleteOptionsFilesPurgedInBackgroundOnOpen:"
        "ReleasePurge",
        "DBImpl::BGWorkPurge:start"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(DB::Open(opt, dbname_, &db));
  ASSERT_GT(CountOptionsFiles(db.get()), 2);

  TEST_SYNC_POINT(
      "OptionsFileTest::ObsoleteOptionsFilesPurgedInBackgroundOnOpen:"
      "ReleasePurge");
  ASSERT_OK(static_cast_with_check<DBImpl>(db.get())->TEST_WaitForPurge());
  ASSERT_LE(CountOptionsFiles(db.get()), 2);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(OptionsFileTest, ObsoleteOptionsFilesPurgedSynchronouslyOnOpenFailure) {
  auto fail_fs =
      std::make_shared<FailOptionsDirFsyncFileSystem>(FileSystem::Default());
  std::unique_ptr<Env> env(NewCompositeEnv(fail_fs));

  Options opt;
  opt.env = env.get();
  opt.create_if_missing = true;
  ASSERT_OK(DestroyDB(dbname_, opt));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(opt, dbname_, &db));
  GenerateStaleOptionsFiles(db.get(), 5);
  db.reset();

  opt.create_if_missing = false;
  opt.avoid_unnecessary_blocking_io = true;

  fail_fs->FailOptionsDirFsyncAfterNextOptionsFile();
  ASSERT_NOK(DB::Open(opt, dbname_, &db));
  ASSERT_EQ(fail_fs->options_dir_fsync_failures(), 1);
  ASSERT_LE(CountOptionsFiles(opt.env, dbname_), 2);
}

TEST_F(OptionsFileTest, OptionsFileName) {
  const uint64_t kOptionsFileNum = 12345;
  uint64_t number;
  FileType type;

  auto options_file_name = OptionsFileName("", kOptionsFileNum);
  ASSERT_TRUE(ParseFileName(options_file_name, &number, &type, nullptr));
  ASSERT_EQ(type, kOptionsFile);
  ASSERT_EQ(number, kOptionsFileNum);

  const uint64_t kTempOptionsFileNum = 54352;
  auto temp_options_file_name = TempOptionsFileName("", kTempOptionsFileNum);
  ASSERT_TRUE(ParseFileName(temp_options_file_name, &number, &type, nullptr));
  ASSERT_NE(temp_options_file_name.find(kTempFileNameSuffix),
            std::string::npos);
  ASSERT_EQ(type, kTempFile);
  ASSERT_EQ(number, kTempOptionsFileNum);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
#if !(defined NDEBUG) || !defined(OS_WIN)
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  return 0;
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
}
