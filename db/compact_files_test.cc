//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class CompactFilesTest : public testing::Test {
 public:
  CompactFilesTest() {
    env_ = Env::Default();
    db_name_ = test::PerThreadDBPath("compact_files_test");
  }

  std::string db_name_;
  Env* env_;
};

// A class which remembers the name of each flushed file.
class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() = default;
  ~FlushedFileCollector() override = default;

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (const auto& fname : flushed_files_) {
      result.push_back(fname);
    }
    return result;
  }
  void ClearFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.clear();
  }

 private:
  std::vector<std::string> flushed_files_;
  std::mutex mutex_;
};

TEST_F(CompactFilesTest, L0ConflictsFiles) {
  Options options;
  // to trigger compaction more easily
  const int kWriteBufferSize = 10000;
  const int kLevel0Trigger = 2;
  options.create_if_missing = true;
  options.level_compaction_dynamic_level_bytes = false;
  options.compaction_style = kCompactionStyleLevel;
  // Small slowdown and stop trigger for experimental purpose.
  options.level0_slowdown_writes_trigger = 20;
  options.level0_stop_writes_trigger = 20;
  options.write_buffer_size = kWriteBufferSize;
  options.level0_file_num_compaction_trigger = kLevel0Trigger;
  options.compression = kNoCompression;

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  ASSERT_OK(DB::Open(options, db_name_, &db));
  assert(db);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"CompactFilesImpl:0", "BackgroundCallCompaction:0"},
      {"BackgroundCallCompaction:1", "CompactFilesImpl:1"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // create couple files
  // Background compaction starts and waits in BackgroundCallCompaction:0
  for (int i = 0; i < kLevel0Trigger * 4; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(i), ""));
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(100 - i), ""));
    ASSERT_OK(db->Flush(FlushOptions()));
  }

  ROCKSDB_NAMESPACE::ColumnFamilyMetaData meta;
  db->GetColumnFamilyMetaData(&meta);
  std::string file1;
  for (auto& file : meta.levels[0].files) {
    ASSERT_EQ(0, meta.levels[0].level);
    if (file1 == "") {
      file1 = file.db_path + "/" + file.name;
    } else {
      std::string file2 = file.db_path + "/" + file.name;
      // Another thread starts a compact files and creates an L0 compaction
      // The background compaction then notices that there is an L0 compaction
      // already in progress and doesn't do an L0 compaction
      // Once the background compaction finishes, the compact files finishes
      ASSERT_OK(db->CompactFiles(ROCKSDB_NAMESPACE::CompactionOptions(),
                                 {file1, file2}, 0));
      break;
    }
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(CompactFilesTest, MultipleLevel) {
  Options options;
  options.create_if_missing = true;
  // Otherwise background compaction can happen to
  // drain unnecessary level
  options.level_compaction_dynamic_level_bytes = false;
  options.num_levels = 6;
  // Add listener
  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  Status s = DB::Open(options, db_name_, &db);
  ASSERT_OK(s);
  ASSERT_NE(db.get(), nullptr);

  // create couple files in L0, L3, L4 and L5
  for (int i = 5; i > 2; --i) {
    collector->ClearFlushedFiles();
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(i), ""));
    ASSERT_OK(db->Flush(FlushOptions()));
    // Ensure background work is fully finished including listener callbacks
    // before accessing listener state.
    ASSERT_OK(
        static_cast_with_check<DBImpl>(db.get())->TEST_WaitForBackgroundWork());
    auto l0_files = collector->GetFlushedFiles();
    ASSERT_OK(db->CompactFiles(CompactionOptions(), l0_files, i));

    std::string prop;
    ASSERT_TRUE(db->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(i), &prop));
    ASSERT_EQ("1", prop);
  }
  ASSERT_OK(db->Put(WriteOptions(), std::to_string(0), ""));
  ASSERT_OK(db->Flush(FlushOptions()));

  ColumnFamilyMetaData meta;
  db->GetColumnFamilyMetaData(&meta);
  // Compact files except the file in L3
  std::vector<std::string> files;
  for (int i = 0; i < 6; ++i) {
    if (i == 3) {
      continue;
    }
    for (auto& file : meta.levels[i].files) {
      files.push_back(file.db_path + "/" + file.name);
    }
  }

  SyncPoint::GetInstance()->LoadDependency({
      {"CompactionJob::Run():Start", "CompactFilesTest.MultipleLevel:0"},
      {"CompactFilesTest.MultipleLevel:1", "CompactFilesImpl:3"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread thread([&] {
    TEST_SYNC_POINT("CompactFilesTest.MultipleLevel:0");
    ASSERT_OK(db->Put(WriteOptions(), "bar", "v2"));
    ASSERT_OK(db->Put(WriteOptions(), "foo", "v2"));
    ASSERT_OK(db->Flush(FlushOptions()));
    TEST_SYNC_POINT("CompactFilesTest.MultipleLevel:1");
  });

  // Compaction cannot move up the data to higher level
  // here we have input file from level 5, so the output level has to be >= 5
  for (int invalid_output_level = 0; invalid_output_level < 5;
       invalid_output_level++) {
    s = db->CompactFiles(CompactionOptions(), files, invalid_output_level);
    ASSERT_TRUE(s.IsInvalidArgument());
  }

  ASSERT_OK(db->CompactFiles(CompactionOptions(), files, 5));
  SyncPoint::GetInstance()->DisableProcessing();
  thread.join();
}

TEST_F(CompactFilesTest, ObsoleteFiles) {
  Options options;
  // to trigger compaction more easily
  const int kWriteBufferSize = 65536;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  options.compaction_style = kCompactionStyleNone;
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.write_buffer_size = kWriteBufferSize;
  options.max_write_buffer_number = 2;
  options.compression = kNoCompression;

  // Add listener
  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  Status s = DB::Open(options, db_name_, &db);
  ASSERT_OK(s);
  ASSERT_NE(db.get(), nullptr);

  // create couple files
  for (int i = 1000; i < 2000; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(i),
                      std::string(kWriteBufferSize / 10, 'a' + (i % 26))));
  }

  auto l0_files = collector->GetFlushedFiles();
  ASSERT_OK(db->CompactFiles(CompactionOptions(), l0_files, 1));
  ASSERT_OK(static_cast_with_check<DBImpl>(db.get())->TEST_WaitForCompact());

  // verify all compaction input files are deleted
  for (const auto& fname : l0_files) {
    ASSERT_EQ(Status::NotFound(), env_->FileExists(fname));
  }
}

TEST_F(CompactFilesTest, NotCutOutputOnLevel0) {
  Options options;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  options.compaction_style = kCompactionStyleNone;
  options.level0_slowdown_writes_trigger = 1000;
  options.level0_stop_writes_trigger = 1000;
  options.write_buffer_size = 65536;
  options.max_write_buffer_number = 2;
  options.compression = kNoCompression;
  options.max_compaction_bytes = 5000;

  // Add listener
  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  ASSERT_OK(DB::Open(options, db_name_, &db));
  assert(db);

  // create couple files
  for (int i = 0; i < 500; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(i),
                      std::string(1000, 'a' + (i % 26))));
  }
  ASSERT_OK(
      static_cast_with_check<DBImpl>(db.get())->TEST_WaitForFlushMemTable());
  auto l0_files_1 = collector->GetFlushedFiles();
  collector->ClearFlushedFiles();
  for (int i = 0; i < 500; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(i),
                      std::string(1000, 'a' + (i % 26))));
  }
  ASSERT_OK(
      static_cast_with_check<DBImpl>(db.get())->TEST_WaitForFlushMemTable());
  auto l0_files_2 = collector->GetFlushedFiles();
  ASSERT_OK(db->CompactFiles(CompactionOptions(), l0_files_1, 0));
  ASSERT_OK(db->CompactFiles(CompactionOptions(), l0_files_2, 0));
  // no assertion failure
}

TEST_F(CompactFilesTest, CapturingPendingFiles) {
  Options options;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  options.compaction_style = kCompactionStyleNone;
  // Always do full scans for obsolete files (needed to reproduce the issue).
  options.delete_obsolete_files_period_micros = 0;

  // Add listener.
  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  Status s = DB::Open(options, db_name_, &db);
  ASSERT_OK(s);
  assert(db);

  // Create 5 files.
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), "key" + std::to_string(i), "value"));
    ASSERT_OK(db->Flush(FlushOptions()));
  }

  // Ensure background work is fully finished including listener callbacks
  // before accessing listener state.
  ASSERT_OK(
      static_cast_with_check<DBImpl>(db.get())->TEST_WaitForBackgroundWork());
  auto l0_files = collector->GetFlushedFiles();
  EXPECT_EQ(5, l0_files.size());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"CompactFilesImpl:2", "CompactFilesTest.CapturingPendingFiles:0"},
      {"CompactFilesTest.CapturingPendingFiles:1", "CompactFilesImpl:3"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Start compacting files.
  ROCKSDB_NAMESPACE::port::Thread compaction_thread(
      [&] { EXPECT_OK(db->CompactFiles(CompactionOptions(), l0_files, 1)); });

  // In the meantime flush another file.
  TEST_SYNC_POINT("CompactFilesTest.CapturingPendingFiles:0");
  ASSERT_OK(db->Put(WriteOptions(), "key5", "value"));
  ASSERT_OK(db->Flush(FlushOptions()));
  TEST_SYNC_POINT("CompactFilesTest.CapturingPendingFiles:1");

  compaction_thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  db.reset();

  // Make sure we can reopen the DB.
  s = DB::Open(options, db_name_, &db);
  ASSERT_OK(s);
  assert(db);
}

TEST_F(CompactFilesTest, CompactionFilterWithGetSv) {
  class FilterWithGet : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      if (db_ == nullptr) {
        return true;
      }
      std::string res;
      EXPECT_TRUE(db_->Get(ReadOptions(), "", &res).IsNotFound());
      return true;
    }

    void SetDB(DB* db) { db_ = db; }

    const char* Name() const override { return "FilterWithGet"; }

   private:
    DB* db_;
  };

  std::shared_ptr<FilterWithGet> cf(new FilterWithGet());

  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.create_if_missing = true;
  options.compaction_filter = cf.get();

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  Status s = DB::Open(options, db_name_, &db);
  ASSERT_OK(s);

  cf->SetDB(db.get());

  // Write one L0 file
  ASSERT_OK(db->Put(WriteOptions(), "K1", "V1"));
  ASSERT_OK(db->Flush(FlushOptions()));

  // Compact all L0 files using CompactFiles
  ROCKSDB_NAMESPACE::ColumnFamilyMetaData meta;
  db->GetColumnFamilyMetaData(&meta);
  for (auto& file : meta.levels[0].files) {
    std::string fname = file.db_path + "/" + file.name;
    ASSERT_OK(
        db->CompactFiles(ROCKSDB_NAMESPACE::CompactionOptions(), {fname}, 0));
  }
}

TEST_F(CompactFilesTest, SentinelCompressionType) {
  if (!Zlib_Supported()) {
    fprintf(stderr, "zlib compression not supported, skip this test\n");
    return;
  }
  if (!Snappy_Supported()) {
    fprintf(stderr, "snappy compression not supported, skip this test\n");
    return;
  }
  // Check that passing `CompressionType::kDisableCompressionOption` to
  // `CompactFiles` causes it to use the column family compression options.
  for (auto compaction_style : {CompactionStyle::kCompactionStyleLevel,
                                CompactionStyle::kCompactionStyleUniversal,
                                CompactionStyle::kCompactionStyleNone}) {
    ASSERT_OK(DestroyDB(db_name_, Options()));
    Options options;
    options.level_compaction_dynamic_level_bytes = false;
    options.compaction_style = compaction_style;
    // L0: Snappy, L1: ZSTD, L2: Snappy
    options.compression_per_level = {CompressionType::kSnappyCompression,
                                     CompressionType::kZlibCompression,
                                     CompressionType::kSnappyCompression};
    options.create_if_missing = true;
    FlushedFileCollector* collector = new FlushedFileCollector();
    options.listeners.emplace_back(collector);
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(options, db_name_, &db));

    ASSERT_OK(db->Put(WriteOptions(), "key", "val"));
    ASSERT_OK(db->Flush(FlushOptions()));

    // Ensure background work is fully finished including listener callbacks
    // before accessing listener state.
    ASSERT_OK(
        static_cast_with_check<DBImpl>(db.get())->TEST_WaitForBackgroundWork());
    auto l0_files = collector->GetFlushedFiles();
    ASSERT_EQ(1, l0_files.size());

    // L0->L1 compaction, so output should be ZSTD-compressed
    CompactionOptions compaction_opts;
    compaction_opts.compression = CompressionType::kDisableCompressionOption;
    ASSERT_OK(db->CompactFiles(compaction_opts, l0_files, 1));

    ROCKSDB_NAMESPACE::TablePropertiesCollection all_tables_props;
    ASSERT_OK(db->GetPropertiesOfAllTables(&all_tables_props));
    for (const auto& name_and_table_props : all_tables_props) {
      // As of format_version 7, more elaborate information is encoded into the
      // compression_name property
      ASSERT_EQ("BuiltinV2;02;", name_and_table_props.second->compression_name);
    }
  }
}

TEST_F(CompactFilesTest, CompressionWithBlockAlign) {
  if (!Snappy_Supported()) {
    ROCKSDB_GTEST_SKIP("Test requires Snappy support");
    return;
  }
  Options options;
  options.compression = CompressionType::kNoCompression;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  std::shared_ptr<FlushedFileCollector> collector =
      std::make_shared<FlushedFileCollector>();
  options.listeners.push_back(collector);

  {
    BlockBasedTableOptions bbto;
    bbto.block_align = true;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  }

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, db_name_, &db));

  ASSERT_OK(db->Put(WriteOptions(), "key", "val"));
  ASSERT_OK(db->Flush(FlushOptions()));

  // Ensure background work is fully finished including listener callbacks
  // before accessing listener state.
  ASSERT_OK(
      static_cast_with_check<DBImpl>(db.get())->TEST_WaitForBackgroundWork());
  auto l0_files = collector->GetFlushedFiles();
  ASSERT_EQ(1, l0_files.size());

  // We can run this test even without Snappy support because we expect the
  // `CompactFiles()` to fail before actually invoking Snappy compression.
  CompactionOptions compaction_opts;
  compaction_opts.compression = CompressionType::kSnappyCompression;
  ASSERT_TRUE(db->CompactFiles(compaction_opts, l0_files, 1 /* output_level */)
                  .IsInvalidArgument());

  compaction_opts.compression = CompressionType::kDisableCompressionOption;
  ASSERT_OK(db->CompactFiles(compaction_opts, l0_files, 1 /* output_level */));
}

TEST_F(CompactFilesTest, GetCompactionJobInfo) {
  Options options;
  options.create_if_missing = true;
  // Disable RocksDB background compaction.
  options.compaction_style = kCompactionStyleNone;
  options.level0_slowdown_writes_trigger = 1000;
  options.level0_stop_writes_trigger = 1000;
  options.write_buffer_size = 65536;
  options.max_write_buffer_number = 2;
  options.compression = kNoCompression;
  options.max_compaction_bytes = 5000;

  // Add listener
  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  Status s = DB::Open(options, db_name_, &db);
  ASSERT_OK(s);
  assert(db);

  // create couple files
  for (int i = 0; i < 500; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(i),
                      std::string(1000, 'a' + (i % 26))));
  }
  ASSERT_OK(
      static_cast_with_check<DBImpl>(db.get())->TEST_WaitForFlushMemTable());
  auto l0_files_1 = collector->GetFlushedFiles();
  CompactionOptions co;
  co.compression = CompressionType::kLZ4Compression;
  CompactionJobInfo compaction_job_info{};
  ASSERT_OK(
      db->CompactFiles(co, l0_files_1, 0, -1, nullptr, &compaction_job_info));
  ASSERT_EQ(compaction_job_info.base_input_level, 0);
  ASSERT_EQ(compaction_job_info.cf_id, db->DefaultColumnFamily()->GetID());
  ASSERT_EQ(compaction_job_info.cf_name, db->DefaultColumnFamily()->GetName());
  ASSERT_EQ(compaction_job_info.compaction_reason,
            CompactionReason::kManualCompaction);
  ASSERT_EQ(compaction_job_info.compression, CompressionType::kLZ4Compression);
  ASSERT_EQ(compaction_job_info.output_level, 0);
  ASSERT_OK(compaction_job_info.status);
  // no assertion failure
}

// Helper function to generate zero-padded keys
// e.g., MakeKey("a", 5) -> "a05", MakeKey("b", 42) -> "b42"
static std::string MakeKey(const std::string& prefix, int index) {
  return prefix + (index < 10 ? "0" : "") + std::to_string(index);
}

TEST_F(CompactFilesTest, TrivialMoveNonOverlappingFiles) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.level_compaction_dynamic_level_bytes = false;

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  Status s = DB::Open(options, db_name_, &db);
  ASSERT_OK(s);
  ASSERT_NE(db.get(), nullptr);

  // Create 3 non-overlapping files in L0
  // File 1: keys [a00-a99]
  for (int i = 0; i < 100; i++) {
    std::string key = MakeKey("a", i);
    ASSERT_OK(db->Put(WriteOptions(), key, "value_" + key));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  // File 2: keys [b00-b99]
  for (int i = 0; i < 100; i++) {
    std::string key = MakeKey("b", i);
    ASSERT_OK(db->Put(WriteOptions(), key, "value_" + key));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  // File 3: keys [c00-c99]
  for (int i = 0; i < 100; i++) {
    std::string key = MakeKey("c", i);
    ASSERT_OK(db->Put(WriteOptions(), key, "value_" + key));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  // Verify files are in L0
  ColumnFamilyMetaData meta;
  db->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[0].files.size(), 3);
  ASSERT_EQ(meta.levels[1].files.size(), 0);

  // Get L0 files
  std::vector<std::string> l0_files;
  for (const auto& file : meta.levels[0].files) {
    l0_files.push_back(file.db_path + "/" + file.name);
  }

  CompactionOptions compact_option;
  compact_option.allow_trivial_move = true;
  // Compact all L0 files to L1 (non-overlapping in L1)
  ASSERT_OK(db->CompactFiles(compact_option, l0_files, 1));

  // Verify files are now in L1
  db->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[0].files.size(), 0);
  ASSERT_EQ(meta.levels[1].files.size(), 3);

  // Get the first file from L1 (should be the one with keys a00-a99)
  std::string l1_file_to_move;
  std::vector<std::string> l1_files_to_move_later;
  uint64_t l1_file_number = 0;
  for (const auto& file : meta.levels[1].files) {
    if (file.smallestkey[0] == 'a') {
      l1_file_to_move = file.db_path + "/" + file.name;
      l1_file_number = file.file_number;
    } else {
      l1_files_to_move_later.push_back(file.db_path + "/" + file.name);
    }
  }
  ASSERT_FALSE(l1_file_to_move.empty());

  // Set up sync point to verify trivial move path is taken
  bool trivial_move_executed = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CompactFilesImpl:TrivialMove",
      [&](void* /*arg*/) { trivial_move_executed = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Move the file from L1 to L6 - this should be a trivial move
  // because the file doesn't overlap with anything in L6
  std::vector<std::string> files_to_move = {l1_file_to_move};
  ASSERT_OK(db->CompactFiles(compact_option, files_to_move, 6));

  // Verify trivial move was executed
  ASSERT_TRUE(trivial_move_executed);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify the file is now in L6
  db->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[1].files.size(), 2);  // Two files remain in L1
  ASSERT_EQ(meta.levels[6].files.size(), 1);  // One file in L6

  // Verify it's the correct file in L6
  bool found_file_in_l6 = false;
  for (const auto& file : meta.levels[6].files) {
    if (file.file_number == l1_file_number) {
      found_file_in_l6 = true;
      // Verify key range hasn't changed
      ASSERT_EQ(file.smallestkey[0], 'a');
      ASSERT_EQ(file.largestkey[0], 'a');
      break;
    }
  }
  ASSERT_TRUE(found_file_in_l6);

  // Move the other 2 files from L1 to L6, with allow_trivial_move set to false.
  // This will trigger a normal compaction, so the 2 files will be compacted
  // into a single file in L6.
  ASSERT_OK(db->CompactFiles(CompactionOptions(), l1_files_to_move_later, 6));

  // Verify files in L6
  db->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[1].files.size(), 0);  // Zero files remain in L1
  ASSERT_EQ(meta.levels[6].files.size(), 2);  // Two file in L6

  // Verify data integrity - all keys should still be readable
  for (int i = 0; i < 100; i++) {
    std::string key = MakeKey("a", i);
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), key, &value));
    ASSERT_EQ(value, "value_" + key);
  }
}

TEST_F(CompactFilesTest, TrivialMoveBlockedByOverlap) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.level_compaction_dynamic_level_bytes = false;
  options.num_levels = 7;

  std::unique_ptr<DB> db;
  ASSERT_OK(DestroyDB(db_name_, options));
  Status s = DB::Open(options, db_name_, &db);
  ASSERT_OK(s);
  ASSERT_NE(db.get(), nullptr);

  // Create a file in L6 with keys [m00-m99] (wide range)
  for (int i = 0; i < 100; i++) {
    std::string key = MakeKey("m", i);
    ASSERT_OK(db->Put(WriteOptions(), key, "value_" + key));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  // Get L0 file
  ColumnFamilyMetaData meta;
  db->GetColumnFamilyMetaData(&meta);
  std::vector<std::string> l0_files;
  for (const auto& file : meta.levels[0].files) {
    l0_files.push_back(file.db_path + "/" + file.name);
  }

  CompactionOptions compact_option;
  compact_option.allow_trivial_move = true;

  // Move to L6
  ASSERT_OK(db->CompactFiles(compact_option, l0_files, 6));

  // Now create a file in L1 with overlapping keys [m50-m60]
  for (int i = 50; i <= 60; i++) {
    std::string key = "m" + std::to_string(i);
    ASSERT_OK(db->Put(WriteOptions(), key, "updated_value_" + key));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  // Get the L0 file
  db->GetColumnFamilyMetaData(&meta);
  std::vector<std::string> l0_files_2;
  for (const auto& file : meta.levels[0].files) {
    l0_files_2.push_back(file.db_path + "/" + file.name);
  }

  // Move to L1
  ASSERT_OK(db->CompactFiles(compact_option, l0_files_2, 1));

  // Get the L1 file
  db->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[1].files.size(), 1);
  std::string l1_file =
      meta.levels[1].files[0].db_path + "/" + meta.levels[1].files[0].name;

  // Set up sync point to verify full compaction path is taken
  bool trivial_move_executed = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CompactFilesImpl:TrivialMove",
      [&](void* /*arg*/) { trivial_move_executed = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Try to move from L1 to L6 - this should NOT be a trivial move
  // because the file overlaps with the existing file in L6
  ASSERT_OK(db->CompactFiles(compact_option, {l1_file}, 6));

  // Verify trivial move was NOT executed (full compaction happened)
  ASSERT_FALSE(trivial_move_executed);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify the result - should have merged data in L6
  db->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[1].files.size(), 0);  // L1 should be empty
  // L6 should have the merged file (may be 1 file if merged, or 2 if not)
  ASSERT_GE(meta.levels[6].files.size(), 1);

  // Verify updated values are present
  for (int i = 50; i <= 60; i++) {
    std::string key = "m" + std::to_string(i);
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), key, &value));
    ASSERT_EQ(value, "updated_value_" + key);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
