//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cinttypes>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/log_format.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/meta_blocks.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

static constexpr int kValueSize = 1000;
namespace {
// A wrapper that allows injection of errors.
class ErrorFS : public FileSystemWrapper {
 public:
  bool writable_file_error_;
  int num_writable_file_errors_;

  explicit ErrorFS(const std::shared_ptr<FileSystem>& _target)
      : FileSystemWrapper(_target),
        writable_file_error_(false),
        num_writable_file_errors_(0) {}
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
};
}  // anonymous namespace
class CorruptionTest : public testing::Test {
 public:
  std::shared_ptr<Env> env_guard_;
  std::shared_ptr<ErrorFS> fs_;
  std::unique_ptr<Env> env_;
  Env* base_env_;
  std::string dbname_;
  std::shared_ptr<Cache> tiny_cache_;
  Options options_;
  std::unique_ptr<DB> db_;

  CorruptionTest() {
    // If LRU cache shard bit is smaller than 2 (or -1 which will automatically
    // set it to 0), test SequenceNumberRecovery will fail, likely because of a
    // bug in recovery code. Keep it 4 for now to make the test passes.
    tiny_cache_ = NewLRUCache(100, 4);
    base_env_ = Env::Default();
    EXPECT_OK(
        test::CreateEnvFromSystem(ConfigOptions(), &base_env_, &env_guard_));
    EXPECT_NE(base_env_, nullptr);
    fs_.reset(new ErrorFS(base_env_->GetFileSystem()));
    env_ = NewCompositeEnv(fs_);
    options_.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
    options_.env = env_.get();
    dbname_ = test::PerThreadDBPath(env_.get(), "corruption_test");
    Status s = DestroyDB(dbname_, options_);
    EXPECT_OK(s);

    db_ = nullptr;
    options_.create_if_missing = true;
    BlockBasedTableOptions table_options;
    table_options.block_size_deviation = 0;  // make unit test pass for now
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen();
    options_.create_if_missing = false;
  }

  ~CorruptionTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency({});
    SyncPoint::GetInstance()->ClearAllCallBacks();
    db_.reset();
    if (getenv("KEEP_DB")) {
      fprintf(stdout, "db is still at %s\n", dbname_.c_str());
    } else {
      Options opts;
      opts.env = base_env_;
      EXPECT_OK(DestroyDB(dbname_, opts));
    }
  }

  void CloseDb() { db_.reset(); }

  DBImpl* dbfull() { return static_cast_with_check<DBImpl>(db_.get()); }

  Status TryReopen(Options* options = nullptr) {
    db_.reset();
    Options opt = (options ? *options : options_);
    if (opt.env == Options().env) {
      // If env is not overridden, replace it with ErrorEnv.
      // Otherwise, the test already uses a non-default Env.
      opt.env = env_.get();
    }
    opt.arena_block_size = 4096;
    BlockBasedTableOptions table_options;
    table_options.block_cache = tiny_cache_;
    table_options.block_size_deviation = 0;
    opt.table_factory.reset(NewBlockBasedTableFactory(table_options));
    return DB::Open(opt, dbname_, &db_);
  }

  void Reopen(Options* options = nullptr) { ASSERT_OK(TryReopen(options)); }

  void RepairDB() {
    db_.reset();
    ASSERT_OK(::ROCKSDB_NAMESPACE::RepairDB(dbname_, options_));
  }

  void Build(int n, int start, int flush_every) {
    std::string key_space;
    std::string value_space;
    WriteBatch batch;
    for (int i = 0; i < n; i++) {
      if (flush_every != 0 && i != 0 && i % flush_every == 0) {
        ASSERT_OK(dbfull()->TEST_FlushMemTable());
      }
      // if ((i % 100) == 0) fprintf(stderr, "@ %d of %d\n", i, n);
      Slice key = Key(i + start, &key_space);
      batch.Clear();
      ASSERT_OK(batch.Put(key, Value(i + start, &value_space)));
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }
  }

  void Build(int n, int flush_every = 0) { Build(n, 0, flush_every); }

  void Check(int min_expected, int max_expected) {
    Check(min_expected, max_expected, ReadOptions(false, true));
  }

  void Check(int min_expected, int max_expected, ReadOptions read_options) {
    uint64_t next_expected = 0;
    uint64_t missed = 0;
    int bad_keys = 0;
    int bad_values = 0;
    int correct = 0;
    std::string value_space;
    // Do not verify checksums. If we verify checksums then the
    // db itself will raise errors because data is corrupted.
    // Instead, we want the reads to be successful and this test
    // will detect whether the appropriate corruptions have
    // occurred.
    Iterator* iter = db_->NewIterator(read_options);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      uint64_t key;
      Slice in(iter->key());
      if (!ConsumeDecimalNumber(&in, &key) || !in.empty() ||
          key < next_expected) {
        bad_keys++;
        continue;
      }
      missed += (key - next_expected);
      next_expected = key + 1;
      if (iter->value() != Value(static_cast<int>(key), &value_space)) {
        bad_values++;
      } else {
        correct++;
      }
    }
    iter->status().PermitUncheckedError();
    delete iter;

    fprintf(
        stderr,
        "expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%llu\n",
        min_expected, max_expected, correct, bad_keys, bad_values,
        static_cast<unsigned long long>(missed));
    ASSERT_LE(min_expected, correct);
    ASSERT_GE(max_expected, correct);
  }

  void Corrupt(FileType filetype, int offset, int bytes_to_corrupt) {
    // Pick file to corrupt
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    uint64_t number;
    FileType type;
    std::string fname;
    int picked_number = -1;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) && type == filetype &&
          static_cast<int>(number) > picked_number) {  // Pick latest file
        fname = dbname_ + "/" + filenames[i];
        picked_number = static_cast<int>(number);
      }
    }
    ASSERT_TRUE(!fname.empty()) << filetype;

    ASSERT_OK(test::CorruptFile(env_.get(), fname, offset, bytes_to_corrupt,
                                /*verify_checksum*/ filetype == kTableFile));
  }

  // corrupts exactly one file at level `level`. if no file found at level,
  // asserts
  void CorruptTableFileAtLevel(int level, int offset, int bytes_to_corrupt) {
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    for (const auto& m : metadata) {
      if (m.level == level) {
        ASSERT_OK(test::CorruptFile(env_.get(), dbname_ + "/" + m.name, offset,
                                    bytes_to_corrupt));
        return;
      }
    }
    FAIL() << "no file found at level";
  }

  int Property(const std::string& name) {
    std::string property;
    int result;
    if (db_->GetProperty(name, &property) &&
        sscanf(property.c_str(), "%d", &result) == 1) {
      return result;
    } else {
      return -1;
    }
  }

  // Return the ith key
  Slice Key(int i, std::string* storage) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%016d", i);
    storage->assign(buf, strlen(buf));
    return Slice(*storage);
  }

  // Return the value to associate with the specified key
  Slice Value(int k, std::string* storage) {
    if (k == 0) {
      // Ugh.  Random seed of 0 used to produce no entropy.  This code
      // preserves the implementation that was in place when all of the
      // magic values in this file were picked.
      *storage = std::string(kValueSize, ' ');
    } else {
      Random r(k);
      *storage = r.RandomString(kValueSize);
    }
    return Slice(*storage);
  }

  void GetSortedWalFiles(std::vector<uint64_t>& file_nums) {
    std::vector<std::string> tmp_files;
    ASSERT_OK(env_->GetChildren(dbname_, &tmp_files));
    FileType type = kWalFile;
    for (const auto& file : tmp_files) {
      uint64_t number = 0;
      if (ParseFileName(file, &number, &type) && type == kWalFile) {
        file_nums.push_back(number);
      }
    }
    std::sort(file_nums.begin(), file_nums.end());
  }

  void CorruptFileWithTruncation(FileType file, uint64_t number,
                                 uint64_t bytes_to_truncate = 0) {
    std::string path;
    switch (file) {
      case FileType::kWalFile:
        path = LogFileName(dbname_, number);
        break;
      case FileType::kDBLockFile:
      case FileType::kTableFile:
      case FileType::kDescriptorFile:
      case FileType::kCurrentFile:
      case FileType::kTempFile:
      case FileType::kInfoLogFile:
      case FileType::kMetaDatabase:
      case FileType::kIdentityFile:
      case FileType::kOptionsFile:
      case FileType::kBlobFile:
      case FileType::kCompactionProgressFile:
      default:
        return;
    }
    uint64_t old_size = 0;
    ASSERT_OK(env_->GetFileSize(path, &old_size));
    assert(old_size > bytes_to_truncate);
    uint64_t new_size = old_size - bytes_to_truncate;
    // If bytes_to_truncate == 0, it will do full truncation.
    if (bytes_to_truncate == 0) {
      new_size = 0;
    }
    ASSERT_OK(test::TruncateFile(env_.get(), path, new_size));
  }
};

TEST_F(CorruptionTest, Recovery) {
  Build(100);
  Check(100, 100);
#ifdef OS_WIN
  // On Wndows OS Disk cache does not behave properly
  // We do not call FlushBuffers on every Flush. If we do not close
  // the log file prior to the corruption we end up with the first
  // block not corrupted but only the second. However, under the debugger
  // things work just fine but never pass when running normally
  // For that reason people may want to run with unbuffered I/O. That option
  // is not available for WAL though.
  CloseDb();
#endif
  Corrupt(kWalFile, 19, 1);  // WriteBatch tag for first record
  Corrupt(kWalFile, log::kBlockSize + 1000, 1);  // Somewhere in second block
  ASSERT_TRUE(!TryReopen().ok());
  options_.paranoid_checks = false;
  Reopen(&options_);

  // The 64 records in the first two log blocks are completely lost.
  Check(36, 36);
}

TEST_F(CorruptionTest, PostPITRCorruptionWALsRetained) {
  // Repro for bug where WALs following the point-in-time recovery were not
  // retained leading to the next recovery failing.
  CloseDb();

  options_.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  const std::string test_cf_name = "test_cf";
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  cf_descs.emplace_back(test_cf_name, ColumnFamilyOptions());

  uint64_t log_num;
  {
    options_.create_missing_column_families = true;
    std::vector<ColumnFamilyHandle*> cfhs;
    ASSERT_OK(DB::Open(options_, dbname_, cf_descs, &cfhs, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report

    ASSERT_OK(db_->Put(WriteOptions(), cfhs[0], "k", "v"));
    ASSERT_OK(db_->Put(WriteOptions(), cfhs[1], "k", "v"));
    ASSERT_OK(db_->Put(WriteOptions(), cfhs[0], "k2", "v2"));
    std::vector<uint64_t> file_nums;
    GetSortedWalFiles(file_nums);
    log_num = file_nums.back();
    for (auto* cfh : cfhs) {
      delete cfh;
    }
    CloseDb();
  }

  CorruptFileWithTruncation(FileType::kWalFile, log_num,
                            /*bytes_to_truncate=*/1);

  {
    // Recover "k" -> "v" for both CFs. "k2" -> "v2" is lost due to truncation.
    options_.avoid_flush_during_recovery = true;
    std::vector<ColumnFamilyHandle*> cfhs;
    ASSERT_OK(DB::Open(options_, dbname_, cf_descs, &cfhs, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report

    // Flush one but not both CFs and write some data so there's a seqno gap
    // between the PITR corruption and the next DB session's first WAL.
    ASSERT_OK(db_->Put(WriteOptions(), cfhs[1], "k2", "v2"));
    ASSERT_OK(db_->Flush(FlushOptions(), cfhs[1]));

    for (auto* cfh : cfhs) {
      delete cfh;
    }
    CloseDb();
  }

  // With the bug, this DB open would remove the WALs following the PITR
  // corruption. Then, the next recovery would fail.
  for (int i = 0; i < 2; ++i) {
    std::vector<ColumnFamilyHandle*> cfhs;
    ASSERT_OK(DB::Open(options_, dbname_, cf_descs, &cfhs, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report

    for (auto* cfh : cfhs) {
      delete cfh;
    }
    CloseDb();
  }
}

TEST_F(CorruptionTest, RecoverWriteError) {
  fs_->writable_file_error_ = true;
  Status s = TryReopen();
  ASSERT_TRUE(!s.ok());
}

TEST_F(CorruptionTest, NewFileErrorDuringWrite) {
  // Do enough writing to force minor compaction
  fs_->writable_file_error_ = true;
  const int num =
      static_cast<int>(3 + (Options().write_buffer_size / kValueSize));
  std::string value_storage;
  Status s;
  bool failed = false;
  for (int i = 0; i < num; i++) {
    WriteBatch batch;
    ASSERT_OK(batch.Put("a", Value(100, &value_storage)));
    s = db_->Write(WriteOptions(), &batch);
    if (!s.ok()) {
      failed = true;
    }
    ASSERT_TRUE(!failed || !s.ok());
  }
  ASSERT_TRUE(!s.ok());
  ASSERT_GE(fs_->num_writable_file_errors_, 1);
  fs_->writable_file_error_ = false;
  Reopen();
}

TEST_F(CorruptionTest, TableFile) {
  Build(100);
  DBImpl* dbi = dbfull();
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_OK(dbi->TEST_CompactRange(1, nullptr, nullptr));

  Corrupt(kTableFile, 100, 1);
  Check(99, 99);
  ASSERT_NOK(db_->VerifyChecksum());
}

TEST_F(CorruptionTest, VerifyChecksumReadahead) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  SpecialEnv senv(base_env_);
  options.env = &senv;
  // Disable block cache as we are going to check checksum for
  // the same file twice and measure number of reads.
  BlockBasedTableOptions table_options_no_bc;
  table_options_no_bc.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options_no_bc));

  Reopen(&options);

  Build(10000);
  DBImpl* dbi = dbfull();
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_OK(dbi->TEST_CompactRange(1, nullptr, nullptr));

  senv.count_random_reads_ = true;
  senv.random_read_counter_.Reset();
  ASSERT_OK(db_->VerifyChecksum());

  // Make sure the counter is enabled.
  ASSERT_GT(senv.random_read_counter_.Read(), 0);

  // The SST file is about 10MB. Default readahead size is 256KB.
  // Give a conservative 20 reads for metadata blocks, The number
  // of random reads should be within 10 MB / 256KB + 20 = 60.
  ASSERT_LT(senv.random_read_counter_.Read(), 60);

  senv.random_read_bytes_counter_ = 0;
  ReadOptions ro;
  ro.readahead_size = size_t{32 * 1024};
  ASSERT_OK(db_->VerifyChecksum(ro));
  // The SST file is about 10MB. We set readahead size to 32KB.
  // Give 0 to 20 reads for metadata blocks, and allow real read
  // to range from 24KB to 48KB. The lower bound would be:
  //   10MB / 48KB + 0 = 213
  // The higher bound is
  //   10MB / 24KB + 20 = 447.
  ASSERT_GE(senv.random_read_counter_.Read(), 213);
  ASSERT_LE(senv.random_read_counter_.Read(), 447);

  // Test readahead shouldn't break mmap mode (where it should be
  // disabled).
  options.allow_mmap_reads = true;
  Reopen(&options);
  ASSERT_OK(db_->VerifyChecksum(ro));

  CloseDb();
}

TEST_F(CorruptionTest, TableFileIndexData) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  // very big, we'll trigger flushes manually
  options.write_buffer_size = 100 * 1024 * 1024;
  Reopen(&options);
  // build 2 tables, flush at 5000
  Build(10000, 5000);
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  // corrupt an index block of an entire file
  Corrupt(kTableFile, -2000, 500);
  options.paranoid_checks = false;
  Reopen(&options);
  // one full file may be readable, since only one was corrupted
  // the other file should be fully non-readable, since index was corrupted
  Check(0, 5000, ReadOptions(true, true));
  ASSERT_NOK(db_->VerifyChecksum());

  // In paranoid mode, the db cannot be opened due to the corrupted file.
  ASSERT_TRUE(TryReopen().IsCorruption());
}

TEST_F(CorruptionTest, TableFileFooterMagic) {
  Build(100);
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  Check(100, 100);
  // Corrupt the whole footer
  Corrupt(kTableFile, -100, 100);
  Status s = TryReopen();
  ASSERT_TRUE(s.IsCorruption());
  // Contains useful message, and magic number should be the first thing
  // reported as corrupt.
  ASSERT_TRUE(s.ToString().find("magic number") != std::string::npos);
  // with file name
  ASSERT_TRUE(s.ToString().find(".sst") != std::string::npos);
}

TEST_F(CorruptionTest, TableFileFooterNotMagic) {
  Build(100);
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  Check(100, 100);
  // Corrupt footer except magic number
  Corrupt(kTableFile, -100, 92);
  Status s = TryReopen();
  ASSERT_TRUE(s.IsCorruption());
  // The next thing checked after magic number is format_version
  ASSERT_TRUE(s.ToString().find("format_version") != std::string::npos);
  // with file name
  ASSERT_TRUE(s.ToString().find(".sst") != std::string::npos);
}

TEST_F(CorruptionTest, DBOpenWithWrongFileSize) {
  // Validate that when paranoid flag is true, DB::Open() fails if one of the
  // file corrupted. Validate that when paranoid flag is false, DB::Open()
  // succeed if one of the file corrupted, and the healthy file is readable.
  CloseDb();

  const std::string test_cf_name = "test_cf";
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  cf_descs.emplace_back(test_cf_name, ColumnFamilyOptions());

  {
    options_.create_missing_column_families = true;
    std::vector<ColumnFamilyHandle*> cfhs;
    ASSERT_OK(DB::Open(options_, dbname_, cf_descs, &cfhs, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report

    ASSERT_OK(db_->Put(WriteOptions(), cfhs[0], "k", "v"));
    ASSERT_OK(db_->Put(WriteOptions(), cfhs[1], "k1", "v1"));
    ASSERT_OK(db_->Put(WriteOptions(), cfhs[0], "k2", "v2"));
    for (auto* cfh : cfhs) {
      delete cfh;
    }
    ASSERT_OK(dbfull()->TEST_FlushMemTable());

    // ********************************************
    // Corrupt the file by making the file bigger
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    std::string filename = dbname_ + metadata[0].name;
    const auto& fs = options_.env->GetFileSystem();
    {
      std::unique_ptr<FSWritableFile> f;
      ASSERT_OK(fs->ReopenWritableFile(filename, FileOptions(), &f, nullptr));
      ASSERT_OK(f->Append("blahblah", IOOptions(), nullptr));
      ASSERT_OK(f->Close(IOOptions(), nullptr));
    }
    CloseDb();
  }

  // DB failed to open due to one of the file is corrupted, as paranoid flag is
  // true
  options_.paranoid_checks = true;
  std::vector<ColumnFamilyHandle*> cfhs;
  Status s;
  s = DB::Open(options_, dbname_, cf_descs, &cfhs, &db_);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("file size mismatch") != std::string::npos);

  // DB opened successfully, as paranoid flag is false, validate the one that is
  // healthy is still accessible
  options_.paranoid_checks = false;
  ASSERT_OK(DB::Open(options_, dbname_, cf_descs, &cfhs, &db_));
  assert(db_ != nullptr);  // suppress false clang-analyze report

  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), cfhs[1], "k1", &v));
  ASSERT_EQ(v, "v1");

  // Validate the default column family is corrupted
  Check(0, 0);
  s = db_->Get(ReadOptions(), cfhs[0], "k1", &v);
  ASSERT_TRUE(s.IsCorruption());

  delete cfhs[1];
  delete cfhs[0];
}

TEST_F(CorruptionTest, TableFileWrongSize) {
  Build(100);
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  Check(100, 100);

  // ********************************************
  // Make the file bigger by appending to it
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(1U, metadata.size());
  std::string filename = dbname_ + metadata[0].name;
  const auto& fs = options_.env->GetFileSystem();
  {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs->ReopenWritableFile(filename, FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append("blahblah", IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  // DB actually accepts this without paranoid checks, relying on size
  // recorded in manifest to locate the SST footer.
  options_.paranoid_checks = false;
  Reopen();
  // As footer could not be extraced, file is completely unreadable
  Check(0, 0);
  std::string v;
  auto s = db_->Get(ReadOptions(), "k1", &v);
  ASSERT_TRUE(s.IsCorruption());

  // But reports the issue with paranoid checks
  options_.paranoid_checks = true;
  s = TryReopen();
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("file size mismatch") != std::string::npos);

  // ********************************************
  // Make the file smaller with truncation.
  // First leaving a partial footer, and then completely removing footer.
  for (size_t bytes_lost : {8, 100}) {
    ASSERT_OK(test::TruncateFile(env_.get(), filename,
                                 metadata[0].size - bytes_lost));

    // Reported well with paranoid checks
    options_.paranoid_checks = true;
    s = TryReopen();
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("file size mismatch") != std::string::npos);

    // Without paranoid checks, not reported until read
    options_.paranoid_checks = false;
    Reopen();
    Check(0, 0);  // Missing data
  }
}

TEST_F(CorruptionTest, MissingDescriptor) {
  Build(1000);
  RepairDB();
  Reopen();
  Check(1000, 1000);
}

TEST_F(CorruptionTest, SequenceNumberRecovery) {
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v3"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v4"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v5"));
  RepairDB();
  Reopen();
  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &v));
  ASSERT_EQ("v5", v);
  // Write something.  If sequence number was not recovered properly,
  // it will be hidden by an earlier write.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v6"));
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &v));
  ASSERT_EQ("v6", v);
  Reopen();
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &v));
  ASSERT_EQ("v6", v);
}

TEST_F(CorruptionTest, CorruptedDescriptor) {
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "hello"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(
      db_->CompactRange(cro, db_->DefaultColumnFamily(), nullptr, nullptr));

  Corrupt(kDescriptorFile, 0, 1000);
  Status s = TryReopen();
  ASSERT_TRUE(!s.ok());

  RepairDB();
  Reopen();
  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), "foo", &v));
  ASSERT_EQ("hello", v);
}

TEST_F(CorruptionTest, CompactionInputError) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  Reopen(&options);
  Build(10);
  DBImpl* dbi = dbfull();
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_OK(dbi->TEST_CompactRange(1, nullptr, nullptr));
  ASSERT_EQ(1, Property("rocksdb.num-files-at-level2"));

  Corrupt(kTableFile, 100, 1);
  Check(9, 9);
  ASSERT_NOK(db_->VerifyChecksum());

  // Force compactions by writing lots of values
  Build(10000);
  Check(10000, 10000);
  ASSERT_NOK(db_->VerifyChecksum());
}

TEST_F(CorruptionTest, CompactionInputErrorParanoid) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.paranoid_checks = true;
  options.write_buffer_size = 131072;
  options.max_write_buffer_number = 2;
  Reopen(&options);
  DBImpl* dbi = dbfull();

  // Fill levels >= 1
  for (int level = 1; level < db_->NumberLevels(); level++) {
    ASSERT_OK(db_->Put(WriteOptions(), "", "begin"));
    ASSERT_OK(db_->Put(WriteOptions(), "~", "end"));
    ASSERT_OK(dbi->TEST_FlushMemTable());
    for (int comp_level = 0; comp_level < db_->NumberLevels() - level;
         ++comp_level) {
      ASSERT_OK(dbi->TEST_CompactRange(comp_level, nullptr, nullptr));
    }
  }

  Reopen(&options);

  dbi = dbfull();
  Build(10);
  ASSERT_OK(dbi->TEST_FlushMemTable());
  ASSERT_OK(dbi->TEST_WaitForCompact());
  ASSERT_EQ(1, Property("rocksdb.num-files-at-level0"));

  CorruptTableFileAtLevel(0, 100, 1);
  Check(9, 9);
  ASSERT_NOK(db_->VerifyChecksum());

  // Write must eventually fail because of corrupted table
  Status s;
  std::string tmp1;
  std::string tmp2;
  bool failed = false;
  for (int i = 0; i < 10000; i++) {
    s = db_->Put(WriteOptions(), Key(i, &tmp1), Value(i, &tmp2));
    if (!s.ok()) {
      failed = true;
    }
    // if one write failed, every subsequent write must fail, too
    ASSERT_TRUE(!failed || !s.ok()) << "write did not fail in a corrupted db";
  }
  ASSERT_TRUE(!s.ok()) << "write did not fail in corrupted paranoid db";
}

TEST_F(CorruptionTest, UnrelatedKeys) {
  Build(10);
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  Corrupt(kTableFile, 100, 1);
  ASSERT_NOK(db_->VerifyChecksum());

  std::string tmp1;
  std::string tmp2;
  ASSERT_OK(db_->Put(WriteOptions(), Key(1000, &tmp1), Value(1000, &tmp2)));
  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), Key(1000, &tmp1), &v));
  ASSERT_EQ(Value(1000, &tmp2).ToString(), v);
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  ASSERT_OK(db_->Get(ReadOptions(), Key(1000, &tmp1), &v));
  ASSERT_EQ(Value(1000, &tmp2).ToString(), v);
}

TEST_F(CorruptionTest, RangeDeletionCorrupted) {
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(static_cast<size_t>(1), metadata.size());
  std::string filename = dbname_ + metadata[0].name;

  FileOptions file_opts;
  const auto& fs = options_.env->GetFileSystem();
  std::unique_ptr<RandomAccessFileReader> file_reader;
  ASSERT_OK(RandomAccessFileReader::Create(fs, filename, file_opts,
                                           &file_reader, nullptr));

  uint64_t file_size;
  ASSERT_OK(
      fs->GetFileSize(filename, file_opts.io_options, &file_size, nullptr));

  BlockHandle range_del_handle;
  const ReadOptions read_options;
  ASSERT_OK(FindMetaBlockInFile(file_reader.get(), file_size,
                                kBlockBasedTableMagicNumber,
                                ImmutableOptions(options_), read_options,
                                kRangeDelBlockName, &range_del_handle));

  ASSERT_OK(TryReopen());
  ASSERT_OK(test::CorruptFile(env_.get(), filename,
                              static_cast<int>(range_del_handle.offset()), 1));
  ASSERT_TRUE(TryReopen().IsCorruption());
}

TEST_F(CorruptionTest, FileSystemStateCorrupted) {
  for (int iter = 0; iter < 2; ++iter) {
    Options options;
    options.level_compaction_dynamic_level_bytes = false;
    options.env = env_.get();
    options.paranoid_checks = true;
    options.create_if_missing = true;
    Reopen(&options);
    Build(10);
    ASSERT_OK(db_->Flush(FlushOptions()));
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    ASSERT_GT(metadata.size(), 0);
    std::string filename = dbname_ + metadata[0].name;

    db_.reset();

    if (iter == 0) {  // corrupt file size
      std::unique_ptr<WritableFile> file;
      ASSERT_OK(env_->NewWritableFile(filename, &file, EnvOptions()));
      ASSERT_OK(file->Append(Slice("corrupted sst")));
      file.reset();
      Status x = TryReopen(&options);
      ASSERT_TRUE(x.IsCorruption());
    } else {  // delete the file
      ASSERT_OK(env_->DeleteFile(filename));
      Status x = TryReopen(&options);
      ASSERT_TRUE(x.IsCorruption());
    }

    ASSERT_OK(DestroyDB(dbname_, options_));
  }
}

static const auto& corruption_modes = {
    mock::MockTableFactory::kCorruptNone, mock::MockTableFactory::kCorruptKey,
    mock::MockTableFactory::kCorruptValue,
    mock::MockTableFactory::kCorruptReorderKey};

TEST_F(CorruptionTest, ParanoidFileChecksOnFlush) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  Status s;
  for (const auto& mode : corruption_modes) {
    db_.reset();
    s = DestroyDB(dbname_, options);
    ASSERT_OK(s);
    std::shared_ptr<mock::MockTableFactory> mock =
        std::make_shared<mock::MockTableFactory>();
    options.table_factory = mock;
    mock->SetCorruptionMode(mode);
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report
    Build(10);
    s = db_->Flush(FlushOptions());
    if (mode == mock::MockTableFactory::kCorruptNone) {
      ASSERT_OK(s);
    } else {
      ASSERT_NOK(s);
    }
  }
}

TEST_F(CorruptionTest, ParanoidFileChecksOnCompact) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  Status s;
  for (const auto& mode : corruption_modes) {
    db_.reset();
    s = DestroyDB(dbname_, options);
    ASSERT_OK(s);
    std::shared_ptr<mock::MockTableFactory> mock =
        std::make_shared<mock::MockTableFactory>();
    options.table_factory = mock;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report
    Build(100, 2);
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    mock->SetCorruptionMode(mode);
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    s = db_->CompactRange(cro, db_->DefaultColumnFamily(), nullptr, nullptr);
    if (mode == mock::MockTableFactory::kCorruptNone) {
      ASSERT_OK(s);
    } else {
      ASSERT_NOK(s);
    }
  }
}

TEST_F(CorruptionTest, ParanoidFileChecksWithDeleteRangeFirst) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  for (bool do_flush : {true, false}) {
    db_.reset();
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string start;
    std::string end;
    assert(db_ != nullptr);  // suppress false clang-analyze report
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(3, &start), Key(7, &end)));
    auto snap = db_->GetSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(8, &start), Key(9, &end)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(2, &start), Key(5, &end)));
    Build(10);
    if (do_flush) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else {
      ASSERT_OK(dbfull()->TEST_FlushMemTable());
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      ASSERT_OK(
          db_->CompactRange(cro, db_->DefaultColumnFamily(), nullptr, nullptr));
    }
    db_->ReleaseSnapshot(snap);
  }
}

TEST_F(CorruptionTest, ParanoidFileChecksWithDeleteRange) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  for (bool do_flush : {true, false}) {
    db_.reset();
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report
    Build(10, 0, 0);
    std::string start;
    std::string end;
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(5, &start), Key(15, &end)));
    auto snap = db_->GetSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(8, &start), Key(9, &end)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(12, &start), Key(17, &end)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(2, &start), Key(4, &end)));
    Build(10, 10, 0);
    if (do_flush) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else {
      ASSERT_OK(dbfull()->TEST_FlushMemTable());
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      ASSERT_OK(
          db_->CompactRange(cro, db_->DefaultColumnFamily(), nullptr, nullptr));
    }
    db_->ReleaseSnapshot(snap);
  }
}

TEST_F(CorruptionTest, ParanoidFileChecksWithDeleteRangeLast) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.paranoid_file_checks = true;
  options.create_if_missing = true;
  for (bool do_flush : {true, false}) {
    db_.reset();
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    assert(db_ != nullptr);  // suppress false clang-analyze report
    std::string start;
    std::string end;
    Build(10);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(3, &start), Key(7, &end)));
    auto snap = db_->GetSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(6, &start), Key(8, &end)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(2, &start), Key(5, &end)));
    if (do_flush) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else {
      ASSERT_OK(dbfull()->TEST_FlushMemTable());
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      ASSERT_OK(
          db_->CompactRange(cro, db_->DefaultColumnFamily(), nullptr, nullptr));
    }
    db_->ReleaseSnapshot(snap);
  }
}

TEST_F(CorruptionTest, LogCorruptionErrorsInCompactionIterator) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.create_if_missing = true;
  options.allow_data_in_errors = true;
  auto mode = mock::MockTableFactory::kCorruptKey;
  db_.reset();
  ASSERT_OK(DestroyDB(dbname_, options));

  std::shared_ptr<mock::MockTableFactory> mock =
      std::make_shared<mock::MockTableFactory>();
  mock->SetCorruptionMode(mode);
  options.table_factory = mock;

  ASSERT_OK(DB::Open(options, dbname_, &db_));
  assert(db_ != nullptr);  // suppress false clang-analyze report
  Build(100, 2);

  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  Status s =
      db_->CompactRange(cro, db_->DefaultColumnFamily(), nullptr, nullptr);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(CorruptionTest, CompactionKeyOrderCheck) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.paranoid_file_checks = false;
  options.create_if_missing = true;
  db_.reset();
  ASSERT_OK(DestroyDB(dbname_, options));
  std::shared_ptr<mock::MockTableFactory> mock =
      std::make_shared<mock::MockTableFactory>();
  options.table_factory = mock;
  ASSERT_OK(DB::Open(options, dbname_, &db_));
  assert(db_ != nullptr);  // suppress false clang-analyze report
  mock->SetCorruptionMode(mock::MockTableFactory::kCorruptReorderKey);
  Build(100, 2);
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  mock->SetCorruptionMode(mock::MockTableFactory::kCorruptNone);
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_NOK(
      db_->CompactRange(cro, db_->DefaultColumnFamily(), nullptr, nullptr));
}

TEST_F(CorruptionTest, FlushKeyOrderCheck) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  options.paranoid_file_checks = false;
  options.create_if_missing = true;

  ASSERT_OK(db_->Put(WriteOptions(), "foo1", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo2", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo3", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo4", "v1"));

  int cnt = 0;
  // Generate some out of order keys from the memtable
  SyncPoint::GetInstance()->SetCallBack(
      "MemTableIterator::Next:0", [&](void* arg) {
        MemTableRep::Iterator* mem_iter =
            static_cast<MemTableRep::Iterator*>(arg);
        if (++cnt == 3) {
          mem_iter->Prev();
          mem_iter->Prev();
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Status s = dbfull()->TEST_FlushMemTable();
  ASSERT_NOK(s);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CorruptionTest, VerifyWholeTableChecksum) {
  CloseDb();
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_.get();
  ASSERT_OK(DestroyDB(dbname_, options));
  options.create_if_missing = true;
  options.file_checksum_gen_factory =
      ROCKSDB_NAMESPACE::GetFileChecksumGenCrc32cFactory();
  Reopen(&options);

  Build(10, 5);

  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
  CloseDb();

  // Corrupt the first byte of each table file, this must be data block.
  Corrupt(kTableFile, 0, 1);

  ASSERT_OK(TryReopen(&options));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  int count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::VerifyFullFileChecksum:mismatch", [&](void* arg) {
        auto* s = static_cast<Status*>(arg);
        ASSERT_NE(s, nullptr);
        ++count;
        ASSERT_NOK(*s);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(db_->VerifyFileChecksums(ReadOptions()).IsCorruption());
  ASSERT_EQ(1, count);
}

class CrashDuringRecoveryWithCorruptionTest
    : public CorruptionTest,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  explicit CrashDuringRecoveryWithCorruptionTest()
      : CorruptionTest(),
        avoid_flush_during_recovery_(std::get<0>(GetParam())),
        track_and_verify_wals_in_manifest_(std::get<1>(GetParam())) {}

 protected:
  const bool avoid_flush_during_recovery_;
  const bool track_and_verify_wals_in_manifest_;
};

INSTANTIATE_TEST_CASE_P(CorruptionTest, CrashDuringRecoveryWithCorruptionTest,
                        ::testing::Values(std::make_tuple(true, false),
                                          std::make_tuple(false, false),
                                          std::make_tuple(true, true),
                                          std::make_tuple(false, true)));

// In case of non-TransactionDB with avoid_flush_during_recovery = true, RocksDB
// won't flush the data from WAL to L0 for all column families if possible. As a
// result, not all column families can increase their log_numbers, and
// min_log_number_to_keep won't change.
// It may prematurely persist a new MANIFEST even before we can declare the DB
// is in consistent state after recovery (this is when the new WAL is synced)
// and advances log_numbers for some column families.
//
// If there is power failure before we sync the new WAL, we will end up in
// a situation in which after persisting the MANIFEST, RocksDB will see some
// column families' log_numbers larger than the corrupted wal, and
// "Column family inconsistency: SST file contains data beyond the point of
// corruption" error will be hit, causing recovery to fail.
//
// After adding the fix, only after new WAL is synced, RocksDB persist a new
// MANIFEST with column families to ensure RocksDB is in consistent state.
// RocksDB writes an empty WriteBatch as a sentinel to the new WAL which is
// synced immediately afterwards. The sequence number of the sentinel
// WriteBatch will be the next sequence number immediately after the largest
// sequence number recovered from previous WALs and MANIFEST because of which DB
// will be in consistent state.
// If a future recovery starts from the new MANIFEST, then it means the new WAL
// is successfully synced. Due to the sentinel empty write batch at the
// beginning, kPointInTimeRecovery of WAL is guaranteed to go after this point.
// If future recovery starts from the old MANIFEST, it means the writing the new
// MANIFEST failed. It won't have the "SST ahead of WAL" error.
//
// The combination of corrupting a WAL and injecting an error during subsequent
// re-open exposes the bug of prematurely persisting a new MANIFEST with
// advanced ColumnFamilyData::log_number.
TEST_P(CrashDuringRecoveryWithCorruptionTest, CrashDuringRecovery) {
  CloseDb();
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.track_and_verify_wals_in_manifest =
      track_and_verify_wals_in_manifest_;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.avoid_flush_during_recovery = false;
  options.env = env_.get();
  ASSERT_OK(DestroyDB(dbname_, options));
  options.create_if_missing = true;
  options.max_write_buffer_number = 8;

  Reopen(&options);
  Status s;
  const std::string test_cf_name = "test_cf";
  ColumnFamilyHandle* cfh = nullptr;
  s = db_->CreateColumnFamily(options, test_cf_name, &cfh);
  ASSERT_OK(s);
  delete cfh;
  CloseDb();

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, options);
  std::vector<ColumnFamilyHandle*> handles;

  // 1. Open and populate the DB. Write and flush default_cf several times to
  // advance wal number so that some column families have advanced log_number
  // while other don't.
  {
    ASSERT_OK(DB::Open(options, dbname_, cf_descs, &handles, &db_));
    auto* dbimpl = dbfull();
    assert(dbimpl);

    // Write one key to test_cf.
    ASSERT_OK(db_->Put(WriteOptions(), handles[1], "old_key", "dontcare"));
    ASSERT_OK(db_->Flush(FlushOptions(), handles[1]));

    // Write to default_cf and flush this cf several times to advance wal
    // number. TEST_SwitchMemtable makes sure WALs are not synced and test can
    // corrupt un-sync WAL.
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), "key" + std::to_string(i),
                         "value" + std::to_string(i)));
      ASSERT_OK(dbimpl->TEST_SwitchMemtable());
    }

    for (auto* h : handles) {
      delete h;
    }
    handles.clear();
    CloseDb();
  }

  // 2. Corrupt second last un-syned wal file to emulate power reset which
  // caused the DB to lose the un-synced WAL.
  {
    std::vector<uint64_t> file_nums;
    GetSortedWalFiles(file_nums);
    size_t size = file_nums.size();
    assert(size >= 2);
    uint64_t log_num = file_nums[size - 2];
    CorruptFileWithTruncation(FileType::kWalFile, log_num,
                              /*bytes_to_truncate=*/8);
  }

  // 3. After first crash reopen the DB which contains corrupted WAL. Default
  // family has higher log number than corrupted wal number.
  //
  // Case1: If avoid_flush_during_recovery = true, RocksDB won't flush the data
  // from WAL to L0 for all column families (test_cf_name in this case). As a
  // result, not all column families can increase their log_numbers, and
  // min_log_number_to_keep won't change.
  //
  // Case2: If avoid_flush_during_recovery = false, all column families have
  // flushed their data from WAL to L0 during recovery, and none of them will
  // ever need to read the WALs again.

  // 4. Fault is injected to fail the recovery.
  {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::GetLogSizeAndMaybeTruncate:0", [&](void* arg) {
          auto* tmp_s = static_cast<Status*>(arg);
          assert(tmp_s);
          *tmp_s = Status::IOError("Injected");
        });
    SyncPoint::GetInstance()->EnableProcessing();

    handles.clear();
    options.avoid_flush_during_recovery = true;
    s = DB::Open(options, dbname_, cf_descs, &handles, &db_);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_EQ("IO error: Injected", s.ToString());
    for (auto* h : handles) {
      delete h;
    }
    CloseDb();

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  // 5. After second crash reopen the db with second corruption. Default family
  // has higher log number than corrupted wal number.
  //
  // Case1: If avoid_flush_during_recovery = true, we persist a new
  // MANIFEST with advanced log_numbers for some column families only after
  // syncing the WAL. So during second crash, RocksDB will skip the corrupted
  // WAL files as they have been moved to different folder. Since newly synced
  // WAL file's sequence number (sentinel WriteBatch) will be the next
  // sequence number immediately after the largest sequence number recovered
  // from previous WALs and MANIFEST, db will be in consistent state and opens
  // successfully.
  //
  // Case2: If avoid_flush_during_recovery = false, the corrupted WAL is below
  // this number. So during a second crash after persisting the new MANIFEST,
  // RocksDB will skip the corrupted WAL(s) because they are all below this
  // bound. Therefore, we won't hit the "column family inconsistency" error
  // message.
  {
    options.avoid_flush_during_recovery = avoid_flush_during_recovery_;
    ASSERT_OK(DB::Open(options, dbname_, cf_descs, &handles, &db_));

    // Verify that data is not lost.
    {
      std::string v;
      ASSERT_OK(db_->Get(ReadOptions(), handles[1], "old_key", &v));
      ASSERT_EQ("dontcare", v);

      v.clear();
      ASSERT_OK(db_->Get(ReadOptions(), "key" + std::to_string(0), &v));
      ASSERT_EQ("value" + std::to_string(0), v);

      // Since  it's corrupting second last wal, below key is not found.
      v.clear();
      ASSERT_EQ(db_->Get(ReadOptions(), "key" + std::to_string(1), &v),
                Status::NotFound());
    }

    for (auto* h : handles) {
      delete h;
    }
    handles.clear();
    CloseDb();
  }
}

// In case of TransactionDB, it enables two-phase-commit. The prepare section of
// an uncommitted transaction always need to be kept. Even if we perform flush
// during recovery, we may still need to hold an old WAL. The
// min_log_number_to_keep won't change, and "Column family inconsistency: SST
// file contains data beyond the point of corruption" error will be hit, causing
// recovery to fail.
//
// After adding the fix, only after new WAL is synced, RocksDB persist a new
// MANIFEST with column families to ensure RocksDB is in consistent state.
// RocksDB writes an empty WriteBatch as a sentinel to the new WAL which is
// synced immediately afterwards. The sequence number of the sentinel
// WriteBatch will be the next sequence number immediately after the largest
// sequence number recovered from previous WALs and MANIFEST because of which DB
// will be in consistent state.
// If a future recovery starts from the new MANIFEST, then it means the new WAL
// is successfully synced. Due to the sentinel empty write batch at the
// beginning, kPointInTimeRecovery of WAL is guaranteed to go after this point.
// If future recovery starts from the old MANIFEST, it means the writing the new
// MANIFEST failed. It won't have the "SST ahead of WAL" error.
//
// The combination of corrupting a WAL and injecting an error during subsequent
// re-open exposes the bug of prematurely persisting a new MANIFEST with
// advanced ColumnFamilyData::log_number.
TEST_P(CrashDuringRecoveryWithCorruptionTest, TxnDbCrashDuringRecovery) {
  CloseDb();
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.track_and_verify_wals_in_manifest =
      track_and_verify_wals_in_manifest_;
  options.avoid_flush_during_recovery = false;
  options.env = env_.get();
  ASSERT_OK(DestroyDB(dbname_, options));
  options.create_if_missing = true;
  options.max_write_buffer_number = 3;
  Reopen(&options);

  // Create cf test_cf_name.
  ColumnFamilyHandle* cfh = nullptr;
  const std::string test_cf_name = "test_cf";
  Status s = db_->CreateColumnFamily(options, test_cf_name, &cfh);
  ASSERT_OK(s);
  delete cfh;
  CloseDb();

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, options);
  std::vector<ColumnFamilyHandle*> handles;

  TransactionDB* txn_db = nullptr;
  TransactionDBOptions txn_db_opts;

  // 1. Open and populate the DB. Write and flush default_cf several times to
  // advance wal number so that some column families have advanced log_number
  // while other don't.
  {
    ASSERT_OK(TransactionDB::Open(options, txn_db_opts, dbname_, cf_descs,
                                  &handles, &txn_db));

    auto* txn = txn_db->BeginTransaction(WriteOptions(), TransactionOptions());
    // Put cf1
    ASSERT_OK(txn->Put(handles[1], "foo", "value"));
    ASSERT_OK(txn->SetName("txn0"));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn_db->Flush(FlushOptions()));

    delete txn;
    txn = nullptr;

    auto* dbimpl = static_cast_with_check<DBImpl>(txn_db->GetRootDB());
    assert(dbimpl);

    // Put and flush cf0
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(txn_db->Put(WriteOptions(), "key" + std::to_string(i),
                            "value" + std::to_string(i)));
      ASSERT_OK(dbimpl->TEST_SwitchMemtable());
    }

    // Put cf1
    txn = txn_db->BeginTransaction(WriteOptions(), TransactionOptions());
    ASSERT_OK(txn->Put(handles[1], "foo1", "value1"));
    ASSERT_OK(txn->Commit());

    delete txn;
    txn = nullptr;

    for (auto* h : handles) {
      delete h;
    }
    handles.clear();
    delete txn_db;
  }

  // 2. Corrupt second last wal to emulate power reset which caused the DB to
  // lose the un-synced WAL.
  {
    std::vector<uint64_t> file_nums;
    GetSortedWalFiles(file_nums);
    size_t size = file_nums.size();
    assert(size >= 2);
    uint64_t log_num = file_nums[size - 2];
    CorruptFileWithTruncation(FileType::kWalFile, log_num,
                              /*bytes_to_truncate=*/8);
  }

  // 3. After first crash reopen the DB which contains corrupted WAL. Default
  // family has higher log number than corrupted wal number. There may be old
  // WAL files that it must not delete because they can contain data of
  // uncommitted transactions. As a result, min_log_number_to_keep won't change.

  {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::Open::BeforeSyncWAL", [&](void* arg) {
          auto* tmp_s = static_cast<Status*>(arg);
          assert(tmp_s);
          *tmp_s = Status::IOError("Injected");
        });
    SyncPoint::GetInstance()->EnableProcessing();

    handles.clear();
    s = TransactionDB::Open(options, txn_db_opts, dbname_, cf_descs, &handles,
                            &txn_db);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_EQ("IO error: Injected", s.ToString());
    for (auto* h : handles) {
      delete h;
    }
    CloseDb();

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  // 4. Corrupt max_wal_num.
  {
    std::vector<uint64_t> file_nums;
    GetSortedWalFiles(file_nums);
    size_t size = file_nums.size();
    uint64_t log_num = file_nums[size - 1];
    CorruptFileWithTruncation(FileType::kWalFile, log_num);
  }

  // 5. After second crash reopen the db with second corruption. Default family
  // has higher log number than corrupted wal number.
  // We persist a new MANIFEST with advanced log_numbers for some column
  // families only after syncing the WAL. So during second crash, RocksDB will
  // skip the corrupted WAL files as they have been moved to different folder.
  // Since newly synced WAL file's sequence number (sentinel WriteBatch) will be
  // the next sequence number immediately after the largest sequence number
  // recovered from previous WALs and MANIFEST, db will be in consistent state
  // and opens successfully.
  {
    ASSERT_OK(TransactionDB::Open(options, txn_db_opts, dbname_, cf_descs,
                                  &handles, &txn_db));

    // Verify that data is not lost.
    {
      std::string v;
      // Key not visible since it's not committed.
      ASSERT_EQ(txn_db->Get(ReadOptions(), handles[1], "foo", &v),
                Status::NotFound());

      v.clear();
      ASSERT_OK(txn_db->Get(ReadOptions(), "key" + std::to_string(0), &v));
      ASSERT_EQ("value" + std::to_string(0), v);

      // Last WAL is corrupted which contains two keys below.
      v.clear();
      ASSERT_EQ(txn_db->Get(ReadOptions(), "key" + std::to_string(1), &v),
                Status::NotFound());
      v.clear();
      ASSERT_EQ(txn_db->Get(ReadOptions(), handles[1], "foo1", &v),
                Status::NotFound());
    }

    for (auto* h : handles) {
      delete h;
    }
    delete txn_db;
  }
}

// This test is similar to
// CrashDuringRecoveryWithCorruptionTest.CrashDuringRecovery except it calls
// flush and corrupts Last WAL. It calls flush to sync some of the WALs and
// remaining are unsyned one of which is then corrupted to simulate crash.
//
// In case of non-TransactionDB with avoid_flush_during_recovery = true, RocksDB
// won't flush the data from WAL to L0 for all column families if possible. As a
// result, not all column families can increase their log_numbers, and
// min_log_number_to_keep won't change.
// It may prematurely persist a new MANIFEST even before we can declare the DB
// is in consistent state after recovery (this is when the new WAL is synced)
// and advances log_numbers for some column families.
//
// If there is power failure before we sync the new WAL, we will end up in
// a situation in which after persisting the MANIFEST, RocksDB will see some
// column families' log_numbers larger than the corrupted wal, and
// "Column family inconsistency: SST file contains data beyond the point of
// corruption" error will be hit, causing recovery to fail.
//
// After adding the fix, only after new WAL is synced, RocksDB persist a new
// MANIFEST with column families to ensure RocksDB is in consistent state.
// RocksDB writes an empty WriteBatch as a sentinel to the new WAL which is
// synced immediately afterwards. The sequence number of the sentinel
// WriteBatch will be the next sequence number immediately after the largest
// sequence number recovered from previous WALs and MANIFEST because of which DB
// will be in consistent state.
// If a future recovery starts from the new MANIFEST, then it means the new WAL
// is successfully synced. Due to the sentinel empty write batch at the
// beginning, kPointInTimeRecovery of WAL is guaranteed to go after this point.
// If future recovery starts from the old MANIFEST, it means the writing the new
// MANIFEST failed. It won't have the "SST ahead of WAL" error.

// The combination of corrupting a WAL and injecting an error during subsequent
// re-open exposes the bug of prematurely persisting a new MANIFEST with
// advanced ColumnFamilyData::log_number.
TEST_P(CrashDuringRecoveryWithCorruptionTest, CrashDuringRecoveryWithFlush) {
  CloseDb();
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.avoid_flush_during_recovery = false;
  options.env = env_.get();
  options.create_if_missing = true;

  ASSERT_OK(DestroyDB(dbname_, options));
  Reopen(&options);

  ColumnFamilyHandle* cfh = nullptr;
  const std::string test_cf_name = "test_cf";
  Status s = db_->CreateColumnFamily(options, test_cf_name, &cfh);
  ASSERT_OK(s);
  delete cfh;

  CloseDb();

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  cf_descs.emplace_back(test_cf_name, options);
  std::vector<ColumnFamilyHandle*> handles;

  {
    ASSERT_OK(DB::Open(options, dbname_, cf_descs, &handles, &db_));

    // Write one key to test_cf.
    ASSERT_OK(db_->Put(WriteOptions(), handles[1], "old_key", "dontcare"));

    // Write to default_cf and flush this cf several times to advance wal
    // number.
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), "key" + std::to_string(i),
                         "value" + std::to_string(i)));
      ASSERT_OK(db_->Flush(FlushOptions()));
    }

    ASSERT_OK(db_->Put(WriteOptions(), handles[1], "dontcare", "dontcare"));
    for (auto* h : handles) {
      delete h;
    }
    handles.clear();
    CloseDb();
  }

  // Corrupt second last un-syned wal file to emulate power reset which
  // caused the DB to lose the un-synced WAL.
  {
    std::vector<uint64_t> file_nums;
    GetSortedWalFiles(file_nums);
    size_t size = file_nums.size();
    uint64_t log_num = file_nums[size - 1];
    CorruptFileWithTruncation(FileType::kWalFile, log_num,
                              /*bytes_to_truncate=*/8);
  }

  // Fault is injected to fail the recovery.
  {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::GetLogSizeAndMaybeTruncate:0", [&](void* arg) {
          auto* tmp_s = static_cast<Status*>(arg);
          assert(tmp_s);
          *tmp_s = Status::IOError("Injected");
        });
    SyncPoint::GetInstance()->EnableProcessing();

    handles.clear();
    options.avoid_flush_during_recovery = true;
    s = DB::Open(options, dbname_, cf_descs, &handles, &db_);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_EQ("IO error: Injected", s.ToString());
    for (auto* h : handles) {
      delete h;
    }
    CloseDb();

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  // Reopen db again
  {
    options.avoid_flush_during_recovery = avoid_flush_during_recovery_;
    ASSERT_OK(DB::Open(options, dbname_, cf_descs, &handles, &db_));

    // Verify that data is not lost.
    {
      std::string v;
      ASSERT_OK(db_->Get(ReadOptions(), handles[1], "old_key", &v));
      ASSERT_EQ("dontcare", v);

      for (int i = 0; i < 2; ++i) {
        v.clear();
        ASSERT_OK(db_->Get(ReadOptions(), "key" + std::to_string(i), &v));
        ASSERT_EQ("value" + std::to_string(i), v);
      }

      // Since it's corrupting last wal after Flush, below key is not found.
      v.clear();
      ASSERT_EQ(db_->Get(ReadOptions(), handles[1], "dontcare", &v),
                Status::NotFound());
    }

    for (auto* h : handles) {
      delete h;
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
