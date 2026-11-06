//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/utilities/ldb_cmd.h"

#include <cinttypes>
#include <iomanip>
#include <memory>

#include "db/db_test_util.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/file_checksum_helper.h"
#include "util/random.h"

using std::map;
using std::string;
using std::vector;

namespace ROCKSDB_NAMESPACE {

class LdbCmdTest : public testing::Test {
 public:
  LdbCmdTest() : testing::Test() {}

  Env* TryLoadCustomOrDefaultEnv() {
    Env* env = Env::Default();
    EXPECT_OK(test::CreateEnvFromSystem(ConfigOptions(), &env, &env_guard_));
    return env;
  }

 private:
  std::shared_ptr<Env> env_guard_;
};

TEST_F(LdbCmdTest, HelpAndVersion) {
  Options o;
  o.env = TryLoadCustomOrDefaultEnv();
  LDBOptions lo;
  static const char* help[] = {"./ldb", "--help"};
  ASSERT_EQ(0, LDBCommandRunner::RunCommand(2, help, o, lo, nullptr));
  static const char* version[] = {"./ldb", "--version"};
  ASSERT_EQ(0, LDBCommandRunner::RunCommand(2, version, o, lo, nullptr));
  static const char* bad[] = {"./ldb", "--not_an_option"};
  ASSERT_NE(0, LDBCommandRunner::RunCommand(2, bad, o, lo, nullptr));
}

TEST_F(LdbCmdTest, HexToString) {
  // map input to expected outputs.
  // odd number of "hex" half bytes doesn't make sense
  map<string, vector<int>> inputMap = {
      {"0x07", {7}},        {"0x5050", {80, 80}},          {"0xFF", {-1}},
      {"0x1234", {18, 52}}, {"0xaaAbAC", {-86, -85, -84}}, {"0x1203", {18, 3}},
  };

  for (const auto& inPair : inputMap) {
    auto actual = ROCKSDB_NAMESPACE::LDBCommand::HexToString(inPair.first);
    auto expected = inPair.second;
    for (unsigned int i = 0; i < actual.length(); i++) {
      EXPECT_EQ(expected[i], static_cast<int>((signed char)actual[i]));
    }
    auto reverse = ROCKSDB_NAMESPACE::LDBCommand::StringToHex(actual);
    EXPECT_STRCASEEQ(inPair.first.c_str(), reverse.c_str());
  }
}

TEST_F(LdbCmdTest, HexToStringBadInputs) {
  const vector<string> badInputs = {
      "0xZZ", "123", "0xx5", "0x111G", "0x123", "Ox12", "0xT", "0x1Q1",
  };
  for (const auto& badInput : badInputs) {
    try {
      ROCKSDB_NAMESPACE::LDBCommand::HexToString(badInput);
      std::cerr << "Should fail on bad hex value: " << badInput << "\n";
      FAIL();
    } catch (...) {
    }
  }
}

TEST_F(LdbCmdTest, MemEnv) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  for (int i = 0; i < 100; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    ASSERT_OK(db->Put(wopts, buf, buf));
  }
  FlushOptions fopts;
  fopts.wait = true;
  ASSERT_OK(db->Flush(fopts));

  db.reset();

  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
  char arg3[] = "dump_live_files";
  char* argv[] = {arg1, arg2, arg3};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));
}

class FileChecksumTestHelper {
 private:
  Options options_;
  DB* db_;
  std::string dbname_;

  Status VerifyChecksum(LiveFileMetaData& file_meta) {
    std::string cur_checksum;
    std::string checksum_func_name;

    Status s;
    EnvOptions soptions;
    std::unique_ptr<SequentialFile> file_reader;
    std::string file_path = dbname_ + "/" + file_meta.name;
    s = options_.env->NewSequentialFile(file_path, &file_reader, soptions);
    if (!s.ok()) {
      return s;
    }
    std::unique_ptr<char[]> scratch(new char[2048]);
    Slice result;
    FileChecksumGenFactory* file_checksum_gen_factory =
        options_.file_checksum_gen_factory.get();
    if (file_checksum_gen_factory == nullptr) {
      cur_checksum = kUnknownFileChecksum;
      checksum_func_name = kUnknownFileChecksumFuncName;
    } else {
      FileChecksumGenContext gen_context;
      gen_context.file_name = file_meta.name;
      std::unique_ptr<FileChecksumGenerator> file_checksum_gen =
          file_checksum_gen_factory->CreateFileChecksumGenerator(gen_context);
      checksum_func_name = file_checksum_gen->Name();
      s = file_reader->Read(2048, &result, scratch.get());
      if (!s.ok()) {
        return s;
      }
      while (result.size() != 0) {
        file_checksum_gen->Update(scratch.get(), result.size());
        s = file_reader->Read(2048, &result, scratch.get());
        if (!s.ok()) {
          return s;
        }
      }
      file_checksum_gen->Finalize();
      cur_checksum = file_checksum_gen->GetChecksum();
    }

    std::string stored_checksum = file_meta.file_checksum;
    std::string stored_checksum_func_name = file_meta.file_checksum_func_name;
    if ((cur_checksum != stored_checksum) ||
        (checksum_func_name != stored_checksum_func_name)) {
      return Status::Corruption(
          "Checksum does not match! The file: " + file_meta.name +
          ", checksum name: " + stored_checksum_func_name + " and checksum " +
          stored_checksum + ". However, expected checksum name: " +
          checksum_func_name + " and checksum " + cur_checksum);
    }
    return Status::OK();
  }

 public:
  FileChecksumTestHelper(Options& options, DB* db, std::string db_name)
      : options_(options), db_(db), dbname_(db_name) {}
  ~FileChecksumTestHelper() = default;

  // Verify the checksum information in Manifest.
  Status VerifyChecksumInManifest(
      const std::vector<LiveFileMetaData>& live_files) {
    // Step 1: verify if the dbname_ is correct
    if (dbname_.back() != '/') {
      dbname_.append("/");
    }

    // Step 2, get the the checksum information by recovering the VersionSet
    // from Manifest.
    std::unique_ptr<FileChecksumList> checksum_list(NewFileChecksumList());
    EnvOptions sopt;
    std::shared_ptr<Cache> tc(NewLRUCache(options_.max_open_files - 10,
                                          options_.table_cache_numshardbits));
    options_.db_paths.emplace_back(dbname_, 0);
    options_.num_levels = 64;
    WriteController wc(options_.delayed_write_rate);
    WriteBufferManager wb(options_.db_write_buffer_size);
    ImmutableDBOptions immutable_db_options(options_);
    VersionSet versions(dbname_, &immutable_db_options,
                        MutableDBOptions{options_}, sopt, tc.get(), &wb, &wc,
                        nullptr, nullptr, "", "",
                        options_.daily_offpeak_time_utc, nullptr,
                        /*read_only=*/false);
    std::vector<std::string> cf_name_list;
    Status s;
    s = versions.ListColumnFamilies(&cf_name_list, dbname_,
                                    immutable_db_options.fs.get());
    if (s.ok()) {
      std::vector<ColumnFamilyDescriptor> cf_list;
      for (const auto& name : cf_name_list) {
        fprintf(stdout, "cf_name: %s", name.c_str());
        cf_list.emplace_back(name, ColumnFamilyOptions(options_));
      }
      s = versions.Recover(cf_list, true);
    }
    if (s.ok()) {
      s = versions.GetLiveFilesChecksumInfo(checksum_list.get());
    }
    if (!s.ok()) {
      return s;
    }

    // Step 3 verify the checksum
    if (live_files.size() != checksum_list->size()) {
      return Status::Corruption("The number of files does not match!");
    }
    for (size_t i = 0; i < live_files.size(); i++) {
      std::string stored_checksum;
      std::string stored_func_name;
      s = checksum_list->SearchOneFileChecksum(
          live_files[i].file_number, &stored_checksum, &stored_func_name);
      if (s.IsNotFound()) {
        return s;
      }
      if (live_files[i].file_checksum != stored_checksum ||
          live_files[i].file_checksum_func_name != stored_func_name) {
        return Status::Corruption(
            "Checksum does not match! The file: " +
            std::to_string(live_files[i].file_number) +
            ". In Manifest, checksum name: " + stored_func_name +
            " and checksum " + stored_checksum +
            ". However, expected checksum name: " +
            live_files[i].file_checksum_func_name + " and checksum " +
            live_files[i].file_checksum);
      }
    }
    return Status::OK();
  }

  // Verify the checksum of each file by recalculting the checksum and
  // comparing it with the one being generated when a SST file is created.
  Status VerifyEachFileChecksum() {
    assert(db_ != nullptr);
    EXPECT_OK(db_->DisableFileDeletions());
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);
    Status cs;
    for (auto a_file : live_files) {
      cs = VerifyChecksum(a_file);
      if (!cs.ok()) {
        break;
      }
    }
    EXPECT_OK(db_->EnableFileDeletions());
    return cs;
  }
};

TEST_F(LdbCmdTest, DumpFileChecksumNoChecksum) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  FlushOptions fopts;
  fopts.wait = true;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 200; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 100; i < 300; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 200; i < 400; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 300; i < 400; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  ASSERT_OK(db->Close());
  db.reset();

  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
  char arg3[] = "file_checksum_dump";
  char arg4[] = "--hex";
  char* argv[] = {arg1, arg2, arg3, arg4};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  ASSERT_OK(DB::Open(opts, dbname, &db));

  // Verify each sst file checksum value and checksum name
  FileChecksumTestHelper fct_helper(opts, db.get(), dbname);
  ASSERT_OK(fct_helper.VerifyEachFileChecksum());

  // Manually trigger compaction
  char b_buf[16];
  snprintf(b_buf, sizeof(b_buf), "%08d", 0);
  char e_buf[16];
  snprintf(e_buf, sizeof(e_buf), "%08d", 399);
  Slice begin(b_buf);
  Slice end(e_buf);
  CompactRangeOptions options;
  ASSERT_OK(db->CompactRange(options, &begin, &end));
  // Verify each sst file checksum after compaction
  FileChecksumTestHelper fct_helper_ac(opts, db.get(), dbname);
  ASSERT_OK(fct_helper_ac.VerifyEachFileChecksum());

  ASSERT_OK(db->Close());
  db.reset();

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  ASSERT_OK(DB::Open(opts, dbname, &db));

  // Verify the checksum information in memory is the same as that in Manifest;
  std::vector<LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);
  db.reset();
  ASSERT_OK(fct_helper_ac.VerifyChecksumInManifest(live_files));
}

TEST_F(LdbCmdTest, BlobDBDumpFileChecksumNoChecksum) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;
  opts.enable_blob_files = true;

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  FlushOptions fopts;
  fopts.wait = true;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 200; i++) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << std::fixed << i;
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, oss.str(), v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 100; i < 300; i++) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << std::fixed << i;
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, oss.str(), v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 200; i < 400; i++) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << std::fixed << i;
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, oss.str(), v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 300; i < 400; i++) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << std::fixed << i;
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, oss.str(), v));
  }
  ASSERT_OK(db->Flush(fopts));
  ASSERT_OK(db->Close());
  db.reset();

  char arg1[] = "./ldb";
  std::string arg2_str = "--db=" + dbname;
  char arg3[] = "file_checksum_dump";
  char arg4[] = "--hex";
  char* argv[] = {arg1, const_cast<char*>(arg2_str.c_str()), arg3, arg4};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  ASSERT_OK(DB::Open(opts, dbname, &db));

  // Verify each sst and blob file checksum value and checksum name
  FileChecksumTestHelper fct_helper(opts, db.get(), dbname);
  ASSERT_OK(fct_helper.VerifyEachFileChecksum());

  // Manually trigger compaction
  std::ostringstream oss_b_buf;
  oss_b_buf << std::setfill('0') << std::setw(8) << std::fixed << 0;
  std::ostringstream oss_e_buf;
  oss_e_buf << std::setfill('0') << std::setw(8) << std::fixed << 399;
  std::string b_buf = oss_b_buf.str();
  std::string e_buf = oss_e_buf.str();
  Slice begin(b_buf);
  Slice end(e_buf);

  CompactRangeOptions options;
  ASSERT_OK(db->CompactRange(options, &begin, &end));
  // Verify each sst file checksum after compaction
  FileChecksumTestHelper fct_helper_ac(opts, db.get(), dbname);
  ASSERT_OK(fct_helper_ac.VerifyEachFileChecksum());

  ASSERT_OK(db->Close());
  db.reset();

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));
}

TEST_F(LdbCmdTest, DumpFileChecksumCRC32) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;
  opts.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  FlushOptions fopts;
  fopts.wait = true;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 100; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 50; i < 150; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 100; i < 200; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 150; i < 250; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%08d", i);
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, buf, v));
  }
  ASSERT_OK(db->Flush(fopts));
  ASSERT_OK(db->Close());
  db.reset();

  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
  char arg3[] = "file_checksum_dump";
  char arg4[] = "--hex";
  char* argv[] = {arg1, arg2, arg3, arg4};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  ASSERT_OK(DB::Open(opts, dbname, &db));

  // Verify each sst file checksum value and checksum name
  FileChecksumTestHelper fct_helper(opts, db.get(), dbname);
  ASSERT_OK(fct_helper.VerifyEachFileChecksum());

  // Manually trigger compaction
  char b_buf[16];
  snprintf(b_buf, sizeof(b_buf), "%08d", 0);
  char e_buf[16];
  snprintf(e_buf, sizeof(e_buf), "%08d", 249);
  Slice begin(b_buf);
  Slice end(e_buf);
  CompactRangeOptions options;
  ASSERT_OK(db->CompactRange(options, &begin, &end));
  // Verify each sst file checksum after compaction
  FileChecksumTestHelper fct_helper_ac(opts, db.get(), dbname);
  ASSERT_OK(fct_helper_ac.VerifyEachFileChecksum());

  ASSERT_OK(db->Close());
  db.reset();

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  ASSERT_OK(DB::Open(opts, dbname, &db));

  // Verify the checksum information in memory is the same as that in Manifest;
  std::vector<LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);
  ASSERT_OK(fct_helper_ac.VerifyChecksumInManifest(live_files));

  ASSERT_OK(db->Close());
  db.reset();
}

TEST_F(LdbCmdTest, BlobDBDumpFileChecksumCRC32) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;
  opts.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  opts.enable_blob_files = true;

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  FlushOptions fopts;
  fopts.wait = true;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 100; i++) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << std::fixed << i;
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, oss.str(), v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 50; i < 150; i++) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << std::fixed << i;
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, oss.str(), v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 100; i < 200; i++) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << std::fixed << i;
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, oss.str(), v));
  }
  ASSERT_OK(db->Flush(fopts));
  for (int i = 150; i < 250; i++) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << std::fixed << i;
    std::string v = rnd.RandomString(100);
    ASSERT_OK(db->Put(wopts, oss.str(), v));
  }
  ASSERT_OK(db->Flush(fopts));
  ASSERT_OK(db->Close());
  db.reset();

  char arg1[] = "./ldb";
  std::string arg2_str = "--db=" + dbname;
  char arg3[] = "file_checksum_dump";
  char arg4[] = "--hex";
  char* argv[] = {arg1, const_cast<char*>(arg2_str.c_str()), arg3, arg4};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  ASSERT_OK(DB::Open(opts, dbname, &db));

  // Verify each sst and blob file checksum value and checksum name
  FileChecksumTestHelper fct_helper(opts, db.get(), dbname);
  ASSERT_OK(fct_helper.VerifyEachFileChecksum());

  // Manually trigger compaction
  std::ostringstream oss_b_buf;
  oss_b_buf << std::setfill('0') << std::setw(8) << std::fixed << 0;
  std::ostringstream oss_e_buf;
  oss_e_buf << std::setfill('0') << std::setw(8) << std::fixed << 249;
  std::string b_buf = oss_b_buf.str();
  std::string e_buf = oss_e_buf.str();
  Slice begin(b_buf);
  Slice end(e_buf);

  CompactRangeOptions options;
  ASSERT_OK(db->CompactRange(options, &begin, &end));
  // Verify each sst file checksum after compaction
  FileChecksumTestHelper fct_helper_ac(opts, db.get(), dbname);
  ASSERT_OK(fct_helper_ac.VerifyEachFileChecksum());

  ASSERT_OK(db->Close());
  db.reset();

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));
}

TEST_F(LdbCmdTest, OptionParsing) {
  // test parsing flags
  Options opts;
  opts.env = TryLoadCustomOrDefaultEnv();
  {
    std::vector<std::string> args;
    args.emplace_back("scan");
    args.emplace_back("--ttl");
    args.emplace_back("--timestamp");
    LDBCommand* command = ROCKSDB_NAMESPACE::LDBCommand::InitFromCmdLineArgs(
        args, opts, LDBOptions(), nullptr);
    const std::vector<std::string> flags = command->TEST_GetFlags();
    EXPECT_EQ(flags.size(), 2);
    EXPECT_EQ(flags[0], "ttl");
    EXPECT_EQ(flags[1], "timestamp");
    delete command;
  }
  // test parsing options which contains equal sign in the option value
  {
    std::vector<std::string> args;
    args.emplace_back("scan");
    args.emplace_back("--db=/dev/shm/ldbtest/");
    args.emplace_back(
        "--from='abcd/efg/hijk/lmn/"
        "opq:__rst.uvw.xyz?a=3+4+bcd+efghi&jk=lm_no&pq=rst-0&uv=wx-8&yz=a&bcd_"
        "ef=gh.ijk'");
    LDBCommand* command = ROCKSDB_NAMESPACE::LDBCommand::InitFromCmdLineArgs(
        args, opts, LDBOptions(), nullptr);
    const std::map<std::string, std::string> option_map =
        command->TEST_GetOptionMap();
    EXPECT_EQ(option_map.at("db"), "/dev/shm/ldbtest/");
    EXPECT_EQ(option_map.at("from"),
              "'abcd/efg/hijk/lmn/"
              "opq:__rst.uvw.xyz?a=3+4+bcd+efghi&jk=lm_no&pq=rst-0&uv=wx-8&yz="
              "a&bcd_ef=gh.ijk'");
    delete command;
  }
}

TEST_F(LdbCmdTest, ListFileTombstone) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  ASSERT_OK(db->Put(wopts, "foo", "1"));
  ASSERT_OK(db->Put(wopts, "bar", "2"));

  FlushOptions fopts;
  fopts.wait = true;
  ASSERT_OK(db->Flush(fopts));

  ASSERT_OK(db->DeleteRange(wopts, db->DefaultColumnFamily(), "foo", "foo2"));
  ASSERT_OK(db->DeleteRange(wopts, db->DefaultColumnFamily(), "bar", "foo2"));
  ASSERT_OK(db->Flush(fopts));

  db.reset();

  {
    char arg1[] = "./ldb";
    char arg2[1024];
    snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
    char arg3[] = "list_file_range_deletes";
    char* argv[] = {arg1, arg2, arg3};

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "ListFileRangeDeletesCommand::DoCommand:BeforePrint", [&](void* arg) {
          std::string* out_str = static_cast<std::string*>(arg);

          // Count number of tombstones printed
          int num_tb = 0;
          const std::string kFingerprintStr = "start: ";
          auto offset = out_str->find(kFingerprintStr);
          while (offset != std::string::npos) {
            num_tb++;
            offset =
                out_str->find(kFingerprintStr, offset + kFingerprintStr.size());
          }
          EXPECT_EQ(2, num_tb);
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }

  // Test the case of limiting tombstones
  {
    char arg1[] = "./ldb";
    char arg2[1024];
    snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
    char arg3[] = "list_file_range_deletes";
    char arg4[] = "--max_keys=1";
    char* argv[] = {arg1, arg2, arg3, arg4};

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "ListFileRangeDeletesCommand::DoCommand:BeforePrint", [&](void* arg) {
          std::string* out_str = static_cast<std::string*>(arg);

          // Count number of tombstones printed
          int num_tb = 0;
          const std::string kFingerprintStr = "start: ";
          auto offset = out_str->find(kFingerprintStr);
          while (offset != std::string::npos) {
            num_tb++;
            offset =
                out_str->find(kFingerprintStr, offset + kFingerprintStr.size());
          }
          EXPECT_EQ(1, num_tb);
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(LdbCmdTest, DisableConsistencyChecks) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");

  {
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(opts, dbname, &db));

    WriteOptions wopts;
    FlushOptions fopts;
    fopts.wait = true;

    ASSERT_OK(db->Put(wopts, "foo1", "1"));
    ASSERT_OK(db->Put(wopts, "bar1", "2"));
    ASSERT_OK(db->Flush(fopts));

    ASSERT_OK(db->Put(wopts, "foo2", "3"));
    ASSERT_OK(db->Put(wopts, "bar2", "4"));
    ASSERT_OK(db->Flush(fopts));
  }

  {
    char arg1[] = "./ldb";
    char arg2[1024];
    snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
    char arg3[] = "checkconsistency";
    char* argv[] = {arg1, arg2, arg3};

    SyncPoint::GetInstance()->SetCallBack(
        "Version::PrepareAppend:forced_check", [&](void* arg) {
          bool* forced = static_cast<bool*>(arg);
          ASSERT_TRUE(*forced);
        });
    SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->DisableProcessing();
  }
  {
    char arg1[] = "./ldb";
    char arg2[1024];
    snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
    char arg3[] = "scan";
    char* argv[] = {arg1, arg2, arg3};

    SyncPoint::GetInstance()->SetCallBack(
        "Version::PrepareAppend:forced_check", [&](void* arg) {
          bool* forced = static_cast<bool*>(arg);
          ASSERT_TRUE(*forced);
        });
    SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(3, argv, opts, LDBOptions(), nullptr));

    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->DisableProcessing();
  }
  {
    char arg1[] = "./ldb";
    char arg2[1024];
    snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
    char arg3[] = "scan";
    char arg4[] = "--disable_consistency_checks";
    char* argv[] = {arg1, arg2, arg3, arg4};

    SyncPoint::GetInstance()->SetCallBack(
        "ColumnFamilyData::ColumnFamilyData", [&](void* arg) {
          ColumnFamilyOptions* cfo = static_cast<ColumnFamilyOptions*>(arg);
          ASSERT_FALSE(cfo->force_consistency_checks);
        });
    SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_EQ(
        0, LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(LdbCmdTest, TestBadDbPath) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s/.no_such_dir", dbname.c_str());
  char arg3[1024];
  snprintf(arg3, sizeof(arg3), "create_column_family");
  char arg4[] = "bad cf";
  char* argv[] = {arg1, arg2, arg3, arg4};

  ASSERT_EQ(1,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));
  snprintf(arg3, sizeof(arg3), "drop_column_family");
  ASSERT_EQ(1,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));
}
namespace {
class WrappedEnv : public EnvWrapper {
 public:
  explicit WrappedEnv(Env* t) : EnvWrapper(t) {}
  static const char* kClassName() { return "WrappedEnv"; }
  const char* Name() const override { return kClassName(); }
};
}  // namespace
TEST_F(LdbCmdTest, LoadCFOptionsAndOverride) {
  // Env* base_env = TryLoadCustomOrDefaultEnv();
  // std::unique_ptr<Env> env(NewMemEnv(base_env));
  std::unique_ptr<Env> env(new WrappedEnv(Env::Default()));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  ASSERT_OK(DestroyDB(dbname, opts));
  ASSERT_OK(DB::Open(opts, dbname, &db));

  ColumnFamilyHandle* cf_handle;
  ColumnFamilyOptions cf_opts;
  cf_opts.num_levels = 20;
  ASSERT_OK(db->CreateColumnFamily(cf_opts, "cf1", &cf_handle));

  delete cf_handle;
  db.reset();

  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
  char arg3[] = "put";
  char arg4[] = "key1";
  char arg5[] = "value1";
  char arg6[] = "--try_load_options";
  char arg7[] = "--column_family=cf1";
  char arg8[] = "--write_buffer_size=268435456";
  char* argv[] = {arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(8, argv, opts, LDBOptions(), nullptr));

  ConfigOptions config_opts;
  Options options;
  std::vector<ColumnFamilyDescriptor> column_families;
  config_opts.env = env.get();
  ASSERT_OK(LoadLatestOptions(config_opts, dbname, &options, &column_families));
  ASSERT_EQ(column_families.size(), 2);
  ASSERT_EQ(options.num_levels, opts.num_levels);
  ASSERT_EQ(column_families[1].options.num_levels, cf_opts.num_levels);
  ASSERT_EQ(column_families[1].options.write_buffer_size, 268435456);
}

TEST_F(LdbCmdTest, UnsafeRemoveSstFile) {
  Options opts;
  opts.level0_file_num_compaction_trigger = 10;
  opts.create_if_missing = true;

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(Env::Default(), "ldb_cmd_test");
  ASSERT_OK(DestroyDB(dbname, opts));
  ASSERT_OK(DB::Open(opts, dbname, &db));

  // Create three SST files
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(i), std::to_string(i)));
    ASSERT_OK(db->Flush(FlushOptions()));
  }

  // Determine which is the "middle" one
  std::vector<LiveFileMetaData> sst_files;
  db->GetLiveFilesMetaData(&sst_files);

  std::vector<uint64_t> numbers;
  for (auto& f : sst_files) {
    numbers.push_back(f.file_number);
  }
  ASSERT_EQ(numbers.size(), 3);
  std::sort(numbers.begin(), numbers.end());
  uint64_t to_remove = numbers[1];

  // Close for unsafe_remove_sst_file
  db.reset();

  char arg1[] = "./ldb";
  char arg2[1024];
  snprintf(arg2, sizeof(arg2), "--db=%s", dbname.c_str());
  char arg3[] = "unsafe_remove_sst_file";
  char arg4[20];
  snprintf(arg4, sizeof(arg4), "%" PRIu64, to_remove);
  char* argv[] = {arg1, arg2, arg3, arg4};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  // Re-open, and verify with Get that middle file is gone
  ASSERT_OK(DB::Open(opts, dbname, &db));

  std::string val;
  ASSERT_OK(db->Get(ReadOptions(), "0", &val));
  ASSERT_EQ(val, "0");

  ASSERT_OK(db->Get(ReadOptions(), "2", &val));
  ASSERT_EQ(val, "2");

  ASSERT_TRUE(db->Get(ReadOptions(), "1", &val).IsNotFound());

  // Now with extra CF, two more files
  ColumnFamilyHandle* cf_handle;
  ColumnFamilyOptions cf_opts;
  ASSERT_OK(db->CreateColumnFamily(cf_opts, "cf1", &cf_handle));
  for (size_t i = 3; i < 5; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), cf_handle, std::to_string(i),
                      std::to_string(i)));
    ASSERT_OK(db->Flush(FlushOptions(), cf_handle));
  }

  // Determine which is the "last" one
  sst_files.clear();
  db->GetLiveFilesMetaData(&sst_files);

  numbers.clear();
  for (auto& f : sst_files) {
    numbers.push_back(f.file_number);
  }
  ASSERT_EQ(numbers.size(), 4);
  std::sort(numbers.begin(), numbers.end());
  to_remove = numbers.back();

  // Close for unsafe_remove_sst_file
  delete cf_handle;
  db.reset();

  snprintf(arg4, sizeof(arg4), "%" PRIu64, to_remove);
  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  std::vector<ColumnFamilyDescriptor> cfds = {{kDefaultColumnFamilyName, opts},
                                              {"cf1", cf_opts}};
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(DB::Open(opts, dbname, cfds, &handles, &db));

  ASSERT_OK(db->Get(ReadOptions(), handles[1], "3", &val));
  ASSERT_EQ(val, "3");

  ASSERT_TRUE(db->Get(ReadOptions(), handles[1], "4", &val).IsNotFound());

  ASSERT_OK(db->Get(ReadOptions(), handles[0], "0", &val));
  ASSERT_EQ(val, "0");

  // Determine which is the "first" one (most likely to be opened in recovery)
  sst_files.clear();
  db->GetLiveFilesMetaData(&sst_files);

  numbers.clear();
  for (auto& f : sst_files) {
    numbers.push_back(f.file_number);
  }
  ASSERT_EQ(numbers.size(), 3);
  std::sort(numbers.begin(), numbers.end());
  to_remove = numbers.front();

  // This time physically delete the file before unsafe_remove
  {
    std::string f = dbname + "/" + MakeTableFileName(to_remove);
    ASSERT_OK(Env::Default()->DeleteFile(f));
  }

  // Close for unsafe_remove_sst_file
  for (auto& h : handles) {
    delete h;
  }
  db.reset();

  snprintf(arg4, sizeof(arg4), "%" PRIu64, to_remove);
  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), nullptr));

  ASSERT_OK(DB::Open(opts, dbname, cfds, &handles, &db));

  ASSERT_OK(db->Get(ReadOptions(), handles[1], "3", &val));
  ASSERT_EQ(val, "3");

  ASSERT_TRUE(db->Get(ReadOptions(), handles[0], "0", &val).IsNotFound());

  for (auto& h : handles) {
    delete h;
  }
  db.reset();
}

TEST_F(LdbCmdTest, FileTemperatureUpdateManifest) {
  auto test_fs = std::make_shared<FileTemperatureTestFS>(FileSystem::Default());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(Env::Default(), test_fs));
  Options opts;
  opts.last_level_temperature = Temperature::kWarm;
  opts.level0_file_num_compaction_trigger = 10;
  opts.create_if_missing = true;
  opts.env = env.get();

  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");
  ASSERT_OK(DestroyDB(dbname, opts));
  ASSERT_OK(DB::Open(opts, dbname, &db));

  std::array<Temperature, 5> kTestTemps = {
      Temperature::kCold, Temperature::kWarm, Temperature::kHot,
      Temperature::kWarm, Temperature::kCold};
  std::map<uint64_t, Temperature> number_to_temp;
  for (size_t i = 0; i < kTestTemps.size(); ++i) {
    ASSERT_OK(db->Put(WriteOptions(), std::to_string(i), std::to_string(i)));
    ASSERT_OK(db->Flush(FlushOptions()));

    std::map<uint64_t, Temperature> current_temps;
    test_fs->CopyCurrentSstFileTemperatures(&current_temps);
    for (auto e : current_temps) {
      if (e.second == Temperature::kUnknown) {
        test_fs->OverrideSstFileTemperature(e.first, kTestTemps[i]);
        number_to_temp[e.first] = kTestTemps[i];
      }
    }
  }

  // Close & reopen
  db.reset();
  test_fs->PopRequestedSstFileTemperatures();
  ASSERT_OK(DB::Open(opts, dbname, &db));

  for (size_t i = 0; i < kTestTemps.size(); ++i) {
    std::string val;
    ASSERT_OK(db->Get(ReadOptions(), std::to_string(i), &val));
    ASSERT_EQ(val, std::to_string(i));
  }

  // Still all unknown
  std::vector<std::pair<uint64_t, Temperature>> requests;
  test_fs->PopRequestedSstFileTemperatures(&requests);
  ASSERT_EQ(requests.size(), kTestTemps.size());
  for (auto& r : requests) {
    ASSERT_EQ(r.second, Temperature::kUnknown);
  }

  // Close for update_manifest
  db.reset();

  std::vector<std::string> args = {"update_manifest", "--db=" + dbname,
                                   "--update_temperatures"};
  std::unique_ptr<LDBCommand> command(LDBCommand::InitFromCmdLineArgs(
      args, opts, LDBOptions(), /*column_families=*/nullptr));
  ASSERT_NE(nullptr, command);
  command->Run();
  ASSERT_TRUE(command->GetExecuteState().IsSucceed());
  ASSERT_EQ("Manifest updates successful",
            command->GetExecuteState().ToString());

  // Re-open, get, and verify manifest temps (based on request)
  test_fs->PopRequestedSstFileTemperatures();
  ASSERT_OK(DB::Open(opts, dbname, &db));

  for (size_t i = 0; i < kTestTemps.size(); ++i) {
    std::string val;
    ASSERT_OK(db->Get(ReadOptions(), std::to_string(i), &val));
    ASSERT_EQ(val, std::to_string(i));
  }

  requests.clear();
  test_fs->PopRequestedSstFileTemperatures(&requests);
  ASSERT_EQ(requests.size(), kTestTemps.size());
  for (auto& r : requests) {
    ASSERT_EQ(r.second, number_to_temp[r.first]);
  }
  db.reset();
}

TEST_F(LdbCmdTest, RenameDbAndLoadOptions) {
  Env* env = TryLoadCustomOrDefaultEnv();
  Options opts;
  opts.env = env;
  opts.create_if_missing = false;

  std::string old_dbname = test::PerThreadDBPath(env, "ldb_cmd_test");
  std::string new_dbname = old_dbname + "_2";
  ASSERT_OK(DestroyDB(old_dbname, opts));
  ASSERT_OK(DestroyDB(new_dbname, opts));

  char old_arg[1024];
  snprintf(old_arg, sizeof(old_arg), "--db=%s", old_dbname.c_str());
  char new_arg[1024];
  snprintf(new_arg, sizeof(old_arg), "--db=%s", new_dbname.c_str());
  const char* argv1[] = {"./ldb",
                         old_arg,
                         "put",
                         "key1",
                         "value1",
                         "--try_load_options",
                         "--create_if_missing"};

  const char* argv2[] = {"./ldb", old_arg, "get", "key1", "--try_load_options"};
  const char* argv3[] = {"./ldb", new_arg,  "put",
                         "key2",  "value2", "--try_load_options"};

  const char* argv4[] = {"./ldb", new_arg, "get", "key1", "--try_load_options"};
  const char* argv5[] = {"./ldb", new_arg, "get", "key2", "--try_load_options"};

  ASSERT_EQ(
      0, LDBCommandRunner::RunCommand(7, argv1, opts, LDBOptions(), nullptr));
  ASSERT_EQ(
      0, LDBCommandRunner::RunCommand(5, argv2, opts, LDBOptions(), nullptr));
  ConfigOptions config_opts;
  Options options;
  std::vector<ColumnFamilyDescriptor> column_families;
  config_opts.env = env;
  ASSERT_OK(
      LoadLatestOptions(config_opts, old_dbname, &options, &column_families));
  ASSERT_EQ(options.wal_dir, "");

  ASSERT_OK(env->RenameFile(old_dbname, new_dbname));
  ASSERT_NE(
      0, LDBCommandRunner::RunCommand(6, argv1, opts, LDBOptions(), nullptr));
  ASSERT_NE(
      0, LDBCommandRunner::RunCommand(5, argv2, opts, LDBOptions(), nullptr));
  ASSERT_EQ(
      0, LDBCommandRunner::RunCommand(6, argv3, opts, LDBOptions(), nullptr));
  ASSERT_EQ(
      0, LDBCommandRunner::RunCommand(5, argv4, opts, LDBOptions(), nullptr));
  ASSERT_EQ(
      0, LDBCommandRunner::RunCommand(5, argv5, opts, LDBOptions(), nullptr));
  ASSERT_OK(DestroyDB(new_dbname, opts));
}

class MyComparator : public Comparator {
 public:
  int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }
  void FindShortSuccessor(std::string* /*key*/) const override {}
  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}
  const char* Name() const override { return "my_comparator"; }
};

TEST_F(LdbCmdTest, CustomComparator) {
  Env* env = TryLoadCustomOrDefaultEnv();
  MyComparator my_comparator;
  Options opts;
  opts.env = env;
  opts.create_if_missing = true;
  opts.create_missing_column_families = true;
  opts.comparator = &my_comparator;

  std::string dbname = test::PerThreadDBPath(env, "ldb_cmd_test");
  std::unique_ptr<DB> db;

  std::vector<ColumnFamilyDescriptor> cfds = {
      {kDefaultColumnFamilyName, opts}, {"cf1", opts}, {"cf2", opts}};
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(DestroyDB(dbname, opts));
  ASSERT_OK(DB::Open(opts, dbname, cfds, &handles, &db));
  ASSERT_OK(db->Put(WriteOptions(), "k1", "v1"));

  for (auto& h : handles) {
    ASSERT_OK(db->DestroyColumnFamilyHandle(h));
  }
  db.reset();

  char arg1[] = "./ldb";
  std::string arg2 = "--db=" + dbname;
  char arg3[] = "get";
  char arg4[] = "k1";
  char* argv[] = {arg1, const_cast<char*>(arg2.c_str()), arg3, arg4};

  ASSERT_EQ(0,
            LDBCommandRunner::RunCommand(4, argv, opts, LDBOptions(), &cfds));
}

TEST_F(LdbCmdTest, DumpWal) {
  Env* base_env = TryLoadCustomOrDefaultEnv();
  std::unique_ptr<Env> env(NewMemEnv(base_env));
  Options opts;
  opts.env = env.get();
  opts.create_if_missing = true;

  std::string dbname = test::PerThreadDBPath(env.get(), "ldb_cmd_test");

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(opts, dbname, &db));

  // PutLogData record
  WriteBatch batch;
  ASSERT_OK(batch.PutLogData("xxx"));
  WriteOptions wopts;
  ASSERT_OK(db->Write(wopts, &batch));
  batch.Clear();

  // PutBlobIndex record copied from db_blob_basic_test.cc
  std::string blob_index;
  constexpr uint64_t blob_file_number = 1000;
  constexpr uint64_t offset = 1234;
  constexpr uint64_t size = 5678;
  BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                        kNoCompression);
  constexpr uint64_t column_family_id = 0;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, column_family_id,
                                             "blob_index_key", blob_index));
  ASSERT_OK(db->Write(wopts, &batch));
  batch.Clear();

  // TimedPut record
  constexpr uint64_t write_unix_time = 1767123301;  // 2025-12-30T19:35:01Z
  ASSERT_OK(batch.TimedPut(nullptr, "timed_put_key", "v", write_unix_time));
  ASSERT_OK(db->Write(wopts, &batch));
  batch.Clear();

  ASSERT_OK(db->Close());
  db.reset();

  string walfile_arg = string("--walfile=") + dbname;
  const char* const argv[] = {"./ldb", "dump_wal", walfile_arg.c_str()};
  static const size_t argc = sizeof(argv) / sizeof(*argv);

  // capture cout while running the command
  std::stringstream captured_cout;
  std::streambuf* original_cout_buffer = std::cout.rdbuf(captured_cout.rdbuf());
  int result =
      LDBCommandRunner::RunCommand(argc, argv, opts, LDBOptions(), nullptr);
  std::cout.rdbuf(original_cout_buffer);

  // check the results of running dump_wal: log data must be included
  string captured_output = captured_cout.str();
  ASSERT_EQ(0, result) << "ldb output:\n\n" << captured_output;
  ASSERT_NE(std::string::npos, captured_output.find("LOG_DATA : 0x787878"))
      << "ldb output:\n\n"
      << captured_output;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
