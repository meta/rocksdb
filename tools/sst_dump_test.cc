//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstdint>
#include <memory>

#include "db/wide/wide_column_serialization.h"
#include "env/composite_env_wrapper.h"
#include "file/random_access_file_reader.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/sst_dump_tool.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/sst_file_dumper.h"
#include "table/table_builder.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

const uint32_t kOptLength = 1024;

namespace {
static std::string MakeKey(int i,
                           ValueType value_type = ValueType::kTypeValue) {
  char buf[100];
  snprintf(buf, sizeof(buf), "k_%04d", i);
  InternalKey key(std::string(buf), 0, value_type);
  return key.Encode().ToString();
}

static std::string MakeKeyWithTimeStamp(int i, uint64_t ts) {
  char buf[100];
  snprintf(buf, sizeof(buf), "k_%04d", i);
  return test::KeyStr(ts, std::string(buf), /*seq=*/0, kTypeValue);
}

static std::string MakeValue(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "v_%04d", i);
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

static std::string MakeWideColumn(int i) {
  std::string val = MakeValue(i);
  std::string val1 = "attr_1_val_" + val;
  std::string val2 = "attr_2_val_" + val;
  WideColumns columns{{"attr_1", val1}, {"attr_2", val2}};
  std::string entity;
  EXPECT_OK(WideColumnSerialization::Serialize(columns, entity));
  return entity;
}

void cleanup(const Options& opts, const std::string& file_name) {
  Env* env = opts.env;
  ASSERT_OK(env->DeleteFile(file_name));
  std::string outfile_name = file_name.substr(0, file_name.length() - 4);
  outfile_name.append("_dump.txt");
  env->DeleteFile(outfile_name).PermitUncheckedError();
}
}  // namespace

// Test for sst dump tool "raw" mode
class SSTDumpToolTest : public testing::Test {
  std::string test_dir_;
  Env* env_;
  std::shared_ptr<Env> env_guard_;

 public:
  SSTDumpToolTest() : env_(Env::Default()) {
    EXPECT_OK(test::CreateEnvFromSystem(ConfigOptions(), &env_, &env_guard_));
    test_dir_ = test::PerThreadDBPath(env_, "sst_dump_test_db");
    Status s = env_->CreateDirIfMissing(test_dir_);
    EXPECT_OK(s);
  }

  ~SSTDumpToolTest() override {
    if (getenv("KEEP_DB")) {  // NOLINT(concurrency-mt-unsafe)
      fprintf(stdout, "Data is still at %s\n", test_dir_.c_str());
    } else {
      EXPECT_OK(env_->DeleteDir(test_dir_));
    }
  }

  Env* env() { return env_; }

  std::string MakeFilePath(const std::string& file_name) const {
    std::string path(test_dir_);
    path.append("/").append(file_name);
    return path;
  }

  // RAII class to ensure cleanup of usage array
  template <std::size_t N>
  struct CleanupUsage {
    char* (&usage)[N];
    explicit CleanupUsage(char* (&_usage)[N]) : usage(_usage) {}
    // No copies/moves
    CleanupUsage(const CleanupUsage&) = delete;
    CleanupUsage& operator=(const CleanupUsage&) = delete;
    CleanupUsage(CleanupUsage&&) = delete;
    CleanupUsage& operator=(CleanupUsage&&) = delete;
    ~CleanupUsage() {
      for (std::size_t i = 0; i < N; ++i) {
        delete[] usage[i];
      }
    }
  };

#define ASSERT_TOOL_PASS(tool_expr) ASSERT_EQ(0, (tool_expr));
#define ASSERT_TOOL_FAIL(tool_expr) ASSERT_NE(0, (tool_expr));

  template <std::size_t N>
  void PopulateCommandArgs(const std::string& file_path, const char* command,
                           char* (&usage)[N]) const {
    for (int i = 0; i < static_cast<int>(N); ++i) {
      usage[i] = new char[kOptLength];
    }
    snprintf(usage[0], kOptLength, "./sst_dump");
    snprintf(usage[1], kOptLength, "%s", command);
    snprintf(usage[2], kOptLength, "--file=%s", file_path.c_str());
  }

  void createSST(const Options& opts, const std::string& file_name,
                 uint32_t wide_column_one_in = 0, bool range_del = false) {
    Env* test_env = opts.env;
    FileOptions file_options(opts);
    ReadOptions read_options;
    const ImmutableOptions imoptions(opts);
    const MutableCFOptions moptions(opts);
    ROCKSDB_NAMESPACE::InternalKeyComparator ikc(opts.comparator);
    std::unique_ptr<TableBuilder> tb;

    InternalTblPropCollFactories internal_tbl_prop_coll_factories;
    std::unique_ptr<WritableFileWriter> file_writer;
    ASSERT_OK(WritableFileWriter::Create(test_env->GetFileSystem(), file_name,
                                         file_options, &file_writer, nullptr));

    std::string column_family_name;
    int unknown_level = -1;
    const WriteOptions write_options;
    tb.reset(opts.table_factory->NewTableBuilder(
        TableBuilderOptions(
            imoptions, moptions, read_options, write_options, ikc,
            &internal_tbl_prop_coll_factories, CompressionType::kNoCompression,
            CompressionOptions(),
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
            column_family_name, unknown_level, kUnknownNewestKeyTime),
        file_writer.get()));

    // Populate slightly more than 1K keys
    uint32_t num_keys = kNumKey;
    const char* comparator_name = ikc.user_comparator()->Name();
    if (strcmp(comparator_name, ReverseBytewiseComparator()->Name()) == 0) {
      for (int32_t i = num_keys; i > 0; i--) {
        if (wide_column_one_in == 0 || i % wide_column_one_in != 0) {
          tb->Add(MakeKey(i), MakeValue(i));
        } else {
          tb->Add(MakeKey(i, ValueType::kTypeWideColumnEntity),
                  MakeWideColumn(i));
        }
      }
    } else if (strcmp(comparator_name,
                      test::BytewiseComparatorWithU64TsWrapper()->Name()) ==
               0) {
      for (uint32_t i = 0; i < num_keys; i++) {
        tb->Add(MakeKeyWithTimeStamp(i, 100 + i), MakeValue(i));
      }
    } else {
      uint32_t i = 0;
      if (range_del) {
        tb->Add(MakeKey(i, kTypeRangeDeletion), MakeValue(i + 1));
        i = 1;
      }
      for (; i < num_keys; i++) {
        if (wide_column_one_in == 0 || i % wide_column_one_in != 0) {
          tb->Add(MakeKey(i), MakeValue(i));
        } else {
          tb->Add(MakeKey(i, ValueType::kTypeWideColumnEntity),
                  MakeWideColumn(i));
        }
      }
    }
    ASSERT_OK(tb->Finish());
    ASSERT_OK(file_writer->Close(IOOptions()));
  }

 protected:
  constexpr static int kNumKey = 1024;

  void SSTDumpToolTestCase(Options& opts, bool filter, int wide_column_one_in,
                           const char* cmd_arg) {
    opts.env = env();
    BlockBasedTableOptions table_opts;
    if (filter) {
      table_opts.filter_policy.reset(
          ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));
    }
    opts.table_factory = std::make_shared<BlockBasedTableFactory>(table_opts);
    std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
    createSST(opts, file_path, wide_column_one_in);

    char* usage[3];
    auto cleanup_usage = CleanupUsage{usage};
    PopulateCommandArgs(file_path, cmd_arg, usage);

    ROCKSDB_NAMESPACE::SSTDumpTool tool;
    ASSERT_TOOL_PASS(tool.Run(3, usage, opts));

    cleanup(opts, file_path);
  }
};

TEST_F(SSTDumpToolTest, HelpAndVersion) {
  Options opts;
  opts.env = env();

  ROCKSDB_NAMESPACE::SSTDumpTool tool;

  static const char* help[] = {"./sst_dump", "--help"};
  ASSERT_TOOL_PASS(tool.Run(2, help, opts));
  static const char* bad_help[] = {"./sst_dump", "--", "--help"};
  ASSERT_TOOL_FAIL(tool.Run(3, bad_help, opts));
  static const char* version[] = {"./sst_dump", "--version"};
  ASSERT_TOOL_PASS(tool.Run(2, version, opts));
  static const char* bad[] = {"./sst_dump", "--not_an_option"};
  ASSERT_TOOL_FAIL(tool.Run(2, bad, opts));
}

TEST_F(SSTDumpToolTest, EmptyFilter) {
  Options opts;
  SSTDumpToolTestCase(opts, /*filter=*/false, /*wide_column_one_in=*/10,
                      "--command=raw");
}

TEST_F(SSTDumpToolTest, SstDumpReverseBytewiseComparator) {
  Options opts;
  opts.comparator = ReverseBytewiseComparator();
  SSTDumpToolTestCase(opts, /*filter=*/true, /*wide_column_one_in=*/10,
                      "--command=raw");
}

TEST_F(SSTDumpToolTest, SstDumpComparatorWithU64Ts) {
  Options opts;
  opts.comparator = test::BytewiseComparatorWithU64TsWrapper();
  SSTDumpToolTestCase(opts, /*filter=*/true, /*wide_column_one_in=*/10,
                      "--command=raw");
}

TEST_F(SSTDumpToolTest, FilterBlockWideColumn) {
  Options opts;
  SSTDumpToolTestCase(opts, /*filter=*/true, /*wide_column_one_in=*/10,
                      "--command=raw");
}

TEST_F(SSTDumpToolTest, FilterBlock) {
  Options opts;
  SSTDumpToolTestCase(opts, /*filter=*/true, /*wide_column_one_in=*/0,
                      "--command=raw");
}

TEST_F(SSTDumpToolTest, GetProperties) {
  Options opts;
  SSTDumpToolTestCase(opts, /*filter=*/true, /*wide_column_one_in=*/0,
                      "--show_properties");
}

TEST_F(SSTDumpToolTest, CompressedSizes) {
  Options opts;
  SSTDumpToolTestCase(opts, /*filter=*/true, /*wide_column_one_in=*/10,
                      "--command=recompress");
}

TEST_F(SSTDumpToolTest, VerifyCompression) {
  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(NewBloomFilterPolicy(10, false));
  opts.table_factory = std::make_shared<BlockBasedTableFactory>(table_opts);
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path, 10);

  char* usage[4];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "--command=recompress", usage);
  snprintf(usage[3], kOptLength, "--verify_compression=1");

  SSTDumpTool tool;
  ASSERT_TOOL_PASS(tool.Run(4, usage, opts));
  snprintf(usage[3], kOptLength, "--verify_compression=0");
  ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, BlockBasedTableOptions) {
  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(NewBloomFilterPolicy(10, false));
  opts.table_factory = std::make_shared<BlockBasedTableFactory>(table_opts);
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path, 10);

  char* usage[5];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "--command=recompress", usage);
  // Catch-all string sets arbitrary BlockBasedTableOptions fields, including
  // ones without their own dedicated command-line flag.
  snprintf(usage[3], kOptLength,
           "--block_based_table_options=index_type=kTwoLevelIndexSearch;data_"
           "block_index_type=kDataBlockBinaryAndHash");
  SSTDumpTool tool;
  ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

  // Applied in command-line order: the later argument wins over the earlier
  // one, whether it is a specific flag or the catch-all.
  snprintf(usage[3], kOptLength, "--block_based_table_options=block_size=1024");
  snprintf(usage[4], kOptLength, "--block_size=4096");
  ASSERT_TOOL_PASS(tool.Run(5, usage, opts));
  snprintf(usage[3], kOptLength, "--block_size=4096");
  snprintf(usage[4], kOptLength, "--block_based_table_options=block_size=1024");
  ASSERT_TOOL_PASS(tool.Run(5, usage, opts));

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, RecompressShowProperties) {
  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(NewBloomFilterPolicy(10, false));
  opts.table_factory = std::make_shared<BlockBasedTableFactory>(table_opts);
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path, 10);

  char* usage[4];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "--command=recompress", usage);
  snprintf(usage[3], kOptLength, "--show_properties");

  SSTDumpTool tool;
  ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, RecompressJson) {
  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(NewBloomFilterPolicy(10, false));
  opts.table_factory = std::make_shared<BlockBasedTableFactory>(table_opts);
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path, 10);

  char* usage[5];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "--command=recompress", usage);
  snprintf(usage[3], kOptLength, "--json");
  snprintf(usage[4], kOptLength, "--show_properties");

  SSTDumpTool tool;
  ASSERT_TOOL_PASS(tool.Run(5, usage, opts));

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, ListMetaBlocks) {
  Options opts;
  SSTDumpToolTestCase(opts, /*filter=*/true, /*wide_column_one_in=*/0,
                      "--list_meta_blocks");
}

namespace {
using Compressor8A = test::CompressorCustomAlg<kCustomCompression8A>;
class MyManager : public CompressionManager {
 public:
  static constexpr const char* kCompatibilityName = "SSTDumpToolTest:MyManager";
  const char* Name() const override { return kCompatibilityName; }
  const char* CompatibilityName() const override { return kCompatibilityName; }

  bool SupportsCompressionType(CompressionType type) const override {
    return type == kCustomCompression8A;
  }

  std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& /*opts*/,
                                            CompressionType type) override {
    switch (static_cast<unsigned char>(type)) {
      case kCustomCompression8A:
        return std::make_unique<Compressor8A>();
      default:
        return nullptr;
    }
  }

  std::shared_ptr<Decompressor> GetDecompressor() override {
    return std::make_shared<test::DecompressorCustomAlg>();
  }
};
}  // namespace

TEST_F(SSTDumpToolTest, CompressionManager) {
  if (!Compressor8A::Supported()) {
    fprintf(stderr,
            "Prerequisite compression library not supported. Skipping\n");
    return;
  }

  // Registery in ObjectLibrary to check that sst_dump can use named
  // CompressionManagers with dependency injection
  auto& library = *ObjectLibrary::Default();
  library.AddFactory<CompressionManager>(
      MyManager::kCompatibilityName,
      [](const std::string& /*uri*/, std::unique_ptr<CompressionManager>* guard,
         std::string* /*errmsg*/) {
        *guard = std::make_unique<MyManager>();
        return guard->get();
      });

  Options opts;
  opts.env = env();
  BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(
      ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10, false));
  opts.table_factory = std::make_shared<BlockBasedTableFactory>(table_opts);
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path, 10);

  char* usage[5];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "--command=recompress", usage);
  snprintf(usage[3], kOptLength, "--compression_manager=%s",
           MyManager::kCompatibilityName);
  snprintf(usage[4], kOptLength, "--compression_types=kCustomCompression8A");

  ROCKSDB_NAMESPACE::SSTDumpTool tool;
  ASSERT_TOOL_PASS(tool.Run(5, usage, opts));

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, MemEnv) {
  std::unique_ptr<Env> mem_env(NewMemEnv(env()));
  Options opts;
  opts.env = mem_env.get();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  char* usage[3];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "--command=verify_checksum", usage);

  ROCKSDB_NAMESPACE::SSTDumpTool tool;
  ASSERT_TOOL_PASS(tool.Run(3, usage, opts));

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, ReadaheadSize) {
  Options opts;
  opts.env = env();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  char* usage[4];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "--command=verify", usage);
  snprintf(usage[3], kOptLength, "--readahead_size=4000000");

  int num_reads = 0;
  SyncPoint::GetInstance()->SetCallBack("RandomAccessFileReader::Read",
                                        [&](void*) { num_reads++; });
  SyncPoint::GetInstance()->EnableProcessing();

  SSTDumpTool tool;
  ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

  // The file is approximately 10MB. Readahead is 4MB.
  // We usually need 3 reads + one metadata read.
  // Three extra read is needed before opening the file for metadata.
  ASSERT_EQ(7, num_reads);

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  cleanup(opts, file_path);
}

#ifndef __clang_analyzer__  // False positive memory leaks reported
TEST_F(SSTDumpToolTest, NoSstFile) {
  Options opts;
  opts.env = env();
  std::string file_path = MakeFilePath("no_such_file.sst");
  char* usage[3];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "", usage);
  ROCKSDB_NAMESPACE::SSTDumpTool tool;
  for (const auto& command :
       {"--command=check", "--command=dump", "--command=raw",
        "--command=verify", "--command=recompress", "--command=verify_checksum",
        "--show_properties"}) {
    snprintf(usage[1], kOptLength, "%s", command);
    ASSERT_TOOL_FAIL(tool.Run(3, usage, opts));
  }
}

TEST_F(SSTDumpToolTest, ValidSSTPath) {
  Options opts;
  opts.env = env();
  char* usage[5];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs("", "", usage);
  SSTDumpTool tool;
  std::string file_not_exists = MakeFilePath("file_not_exists.sst");
  std::string sst_file = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, sst_file);
  std::string text_file = MakeFilePath("text_file");
  ASSERT_OK(WriteStringToFile(opts.env, "Hello World!", text_file, false));
  std::string fake_sst = MakeFilePath("fake_sst.sst");
  ASSERT_OK(WriteStringToFile(opts.env, "Not an SST file!", fake_sst, false));
  std::string good_dir = MakeFilePath("");

  for (const auto& command_arg : {"--command=verify", "--command=identify"}) {
    snprintf(usage[1], kOptLength, "%s", command_arg);

    // Test both classic --file and standalone argument
    for (const auto& file_fmt : {"--file=%s", "%s"}) {
      snprintf(usage[2], kOptLength, file_fmt, file_not_exists.c_str());
      ASSERT_TOOL_FAIL(tool.Run(3, usage, opts));

      snprintf(usage[2], kOptLength, file_fmt, sst_file.c_str());
      ASSERT_TOOL_PASS(tool.Run(3, usage, opts));

      snprintf(usage[2], kOptLength, file_fmt, good_dir.c_str());
      ASSERT_TOOL_PASS(tool.Run(3, usage, opts));

      snprintf(usage[2], kOptLength, file_fmt, text_file.c_str());
      ASSERT_TOOL_FAIL(tool.Run(3, usage, opts));

      snprintf(usage[2], kOptLength, file_fmt, fake_sst.c_str());
      ASSERT_TOOL_FAIL(tool.Run(3, usage, opts));
    }

    // If one file is valid, that's enough to succeed as long as the others
    // exist
    for (const auto& good : {sst_file, good_dir}) {
      // Additional file-or-dir argument
      snprintf(usage[3], kOptLength, "%s", good.c_str());

      snprintf(usage[2], kOptLength, "%s", file_not_exists.c_str());
      ASSERT_TOOL_FAIL(tool.Run(4, usage, opts));

      snprintf(usage[2], kOptLength, "%s", sst_file.c_str());
      ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

      snprintf(usage[2], kOptLength, "%s", good_dir.c_str());
      ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

      snprintf(usage[2], kOptLength, "%s", text_file.c_str());
      // DIFFERENT
      ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

      snprintf(usage[2], kOptLength, "%s", fake_sst.c_str());
      // DIFFERENT
      ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

      // Some extra cases to test "--" handling
      snprintf(usage[2], kOptLength, "%s", "--");
      ASSERT_TOOL_PASS(tool.Run(4, usage, opts));

      snprintf(usage[4], kOptLength, "%s", file_not_exists.c_str());
      ASSERT_TOOL_FAIL(tool.Run(5, usage, opts));

      snprintf(usage[4], kOptLength, "%s", fake_sst.c_str());
      ASSERT_TOOL_PASS(tool.Run(5, usage, opts));
    }
  }
  ASSERT_OK(opts.env->DeleteFile(sst_file));
  ASSERT_OK(opts.env->DeleteFile(text_file));
  ASSERT_OK(opts.env->DeleteFile(fake_sst));
}
#endif  // __clang_analyzer__

TEST_F(SSTDumpToolTest, RawOutput) {
  Options opts;
  opts.env = env();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path, 10);

  char* usage[3];
  auto cleanup_usage = CleanupUsage{usage};
  PopulateCommandArgs(file_path, "--command=raw", usage);

  ROCKSDB_NAMESPACE::SSTDumpTool tool;
  ASSERT_TOOL_PASS(tool.Run(3, usage, opts));

  const std::string raw_path = MakeFilePath("rocksdb_sst_test_dump.txt");
  std::ifstream raw_file(raw_path);

  std::string tp;
  bool is_data_block = false;
  int key_count = 0;
  while (getline(raw_file, tp)) {
    if (tp.find("Data Block #") != std::string::npos) {
      is_data_block = true;
    }

    if (is_data_block && tp.find("HEX") != std::string::npos) {
      key_count++;
    }
  }

  ASSERT_EQ(kNumKey, key_count);

  raw_file.close();

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, SstFileDumperMmapReads) {
  Options opts;
  opts.env = env();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path, 10);

  EnvOptions env_opts;
  uint64_t data_size = 0;

  // Test all combinations of mmap read options
  for (int i = 0; i < 4; ++i) {
    SaveAndRestore<bool> sar_opts(&opts.allow_mmap_reads, (i & 1) != 0);
    SaveAndRestore<bool> sar_env_opts(&env_opts.use_mmap_reads, (i & 2) != 0);

    SstFileDumper dumper(opts, file_path, Temperature::kUnknown,
                         1024 /*readahead_size*/, true /*verify_checksum*/,
                         false /*output_hex*/, false /*decode_blob_index*/,
                         env_opts);
    ASSERT_OK(dumper.getStatus());
    std::shared_ptr<const TableProperties> tp;
    ASSERT_OK(dumper.ReadTableProperties(&tp));
    ASSERT_NE(tp.get(), nullptr);
    if (i == 0) {
      // Verify consistency of a populated field with some entropy
      data_size = tp->data_size;
      ASSERT_GT(data_size, 0);
    } else {
      ASSERT_EQ(data_size, tp->data_size);
    }
  }

  cleanup(opts, file_path);
}

namespace {
// SstFileDumper re-opens plain and cuckoo tables with mmap enabled. Fail only
// that second open; the first, non-mmap open still succeeds.
class FailMmapOpenFileSystem : public FileSystemWrapper {
 public:
  explicit FailMmapOpenFileSystem(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "FailMmapOpenFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    if (options.use_mmap_reads) {
      return IOStatus::IOError("injected mmap open failure");
    }
    return FileSystemWrapper::NewRandomAccessFile(fname, options, result, dbg);
  }
};
}  // namespace

namespace {
// Reads fail only once the test arms the flag, so the table opens normally and
// the failure lands during the data-block scan.
class ToggleFailReadsFile : public FSRandomAccessFileOwnerWrapper {
 public:
  ToggleFailReadsFile(std::unique_ptr<FSRandomAccessFile>&& target,
                      std::shared_ptr<std::atomic<bool>> fail)
      : FSRandomAccessFileOwnerWrapper(std::move(target)),
        fail_(std::move(fail)) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    if (fail_->load()) {
      return IOStatus::IOError("injected data block read failure");
    }
    return FSRandomAccessFileOwnerWrapper::Read(offset, n, options, result,
                                                scratch, dbg);
  }

 private:
  std::shared_ptr<std::atomic<bool>> fail_;
};

class ToggleFailReadsFileSystem : public FileSystemWrapper {
 public:
  ToggleFailReadsFileSystem(const std::shared_ptr<FileSystem>& base,
                            std::shared_ptr<std::atomic<bool>> fail)
      : FileSystemWrapper(base), fail_(std::move(fail)) {}

  static const char* kClassName() { return "ToggleFailReadsFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> target;
    IOStatus s =
        FileSystemWrapper::NewRandomAccessFile(fname, options, &target, dbg);
    if (!s.ok()) {
      return s;
    }
    result->reset(new ToggleFailReadsFile(std::move(target), fail_));
    return IOStatus::OK();
  }

 private:
  std::shared_ptr<std::atomic<bool>> fail_;
};
}  // namespace

TEST_F(SSTDumpToolTest, RecompressReturnsSourceReadError) {
  Options opts;
  opts.env = env();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  auto fail_reads = std::make_shared<std::atomic<bool>>(false);
  auto failing_fs = std::make_shared<ToggleFailReadsFileSystem>(
      env()->GetFileSystem(), fail_reads);
  std::unique_ptr<Env> failing_env = NewCompositeEnv(failing_fs);
  Options failing_opts = opts;
  failing_opts.env = failing_env.get();

  SstFileDumper dumper(failing_opts, file_path, Temperature::kUnknown,
                       1024 /*readahead_size*/, true /*verify_checksum*/,
                       false /*output_hex*/, false /*decode_blob_index*/,
                       EnvOptions(), /*silent=*/true);
  ASSERT_OK(dumper.getStatus());
  fail_reads->store(true);

  ASSERT_NOK(dumper.ShowAllCompressionSizes(
      {CompressionType::kNoCompression},
      /*compress_level_from=*/CompressionOptions().level,
      /*compress_level_to=*/CompressionOptions().level,
      /*compression_strategies=*/{},
      /*per_measurement=*/{}));

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, PlainTableMmapReopenFailure) {
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = kPlainTableVariableLength;
  plain_table_options.bloom_bits_per_key = 0;
  plain_table_options.hash_table_ratio = 0;

  Options opts;
  opts.env = env();
  opts.table_factory.reset(NewPlainTableFactory(plain_table_options));
  std::string file_path = MakeFilePath("rocksdb_sst_test_plain.sst");
  createSST(opts, file_path);

  // Same options, but every mmap open fails.
  auto failing_fs =
      std::make_shared<FailMmapOpenFileSystem>(env()->GetFileSystem());
  std::unique_ptr<Env> failing_env = NewCompositeEnv(failing_fs);
  Options failing_opts = opts;
  failing_opts.env = failing_env.get();

  SstFileDumper dumper(failing_opts, file_path, Temperature::kUnknown,
                       1024 /*readahead_size*/, true /*verify_checksum*/,
                       false /*output_hex*/, false /*decode_blob_index*/,
                       EnvOptions(), /*silent=*/true);
  ASSERT_NOK(dumper.getStatus());

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, ReadSequentialCountsOnlyTheKeysItRead) {
  Options opts;
  opts.env = env();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path);

  SstFileDumper dumper(opts, file_path, Temperature::kUnknown,
                       1024 /*readahead_size*/, true /*verify_checksum*/,
                       false /*output_hex*/, false /*decode_blob_index*/,
                       EnvOptions(), /*silent=*/true);
  ASSERT_OK(dumper.getStatus());

  constexpr uint64_t kLimit = 5;
  ASSERT_OK(dumper.ReadSequential(/*print_kv=*/false, kLimit,
                                  /*has_from=*/false, /*from_key=*/"",
                                  /*has_to=*/false, /*to_key=*/""));
  EXPECT_EQ(dumper.GetReadNumber(), kLimit);

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, DumpTableOutputFileCreationFails) {
  Options opts;
  opts.env = env();
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  createSST(opts, file_path, 10);

  SstFileDumper dumper(opts, file_path, Temperature::kUnknown,
                       1024 /*readahead_size*/, true /*verify_checksum*/,
                       false /*output_hex*/, false /*decode_blob_index*/,
                       EnvOptions(), /*silent=*/true);
  ASSERT_OK(dumper.getStatus());

  // Parent directory does not exist, so NewWritableFile fails and leaves the
  // output handle null.
  ASSERT_NOK(dumper.DumpTable(MakeFilePath("no_such_dir/dump.txt")));

  cleanup(opts, file_path);
}

TEST_F(SSTDumpToolTest, SstFileDumperVerifyNumRecords) {
  Options opts;
  opts.env = env();

  EnvOptions env_opts;
  std::string file_path = MakeFilePath("rocksdb_sst_test.sst");
  {
    createSST(opts, file_path, 10);
    SstFileDumper dumper(opts, file_path, Temperature::kUnknown,
                         1024 /*readahead_size*/, true /*verify_checksum*/,
                         false /*output_hex*/, false /*decode_blob_index*/,
                         env_opts, /*silent=*/true);
    ASSERT_OK(dumper.getStatus());
    ASSERT_OK(dumper.ReadSequential(
        /*print_kv=*/false,
        /*read_num_limit=*/std::numeric_limits<uint64_t>::max(),
        /*has_from=*/false, /*from_key=*/"",
        /*has_to=*/false, /*to_key=*/""));
    cleanup(opts, file_path);
  }

  {
    // Test with range del
    createSST(opts, file_path, 10, /*range_del=*/true);
    SstFileDumper dumper(opts, file_path, Temperature::kUnknown,
                         1024 /*readahead_size*/, true /*verify_checksum*/,
                         false /*output_hex*/, false /*decode_blob_index*/,
                         env_opts, /*silent=*/true);
    ASSERT_OK(dumper.getStatus());
    ASSERT_OK(dumper.ReadSequential(
        /*print_kv=*/false,
        /*read_num_limit=*/std::numeric_limits<uint64_t>::max(),
        /*has_from=*/false, /*from_key=*/"",
        /*has_to=*/false, /*to_key=*/""));
    cleanup(opts, file_path);
  }

  {
    SyncPoint::GetInstance()->SetCallBack(
        "PropertyBlockBuilder::AddTableProperty:Start", [&](void* arg) {
          TableProperties* props = reinterpret_cast<TableProperties*>(arg);
          props->num_entries = kNumKey + 2;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    createSST(opts, file_path, 10);
    SstFileDumper dumper(opts, file_path, Temperature::kUnknown,
                         1024 /*readahead_size*/, true /*verify_checksum*/,
                         false /*output_hex*/, false /*decode_blob_index*/,
                         env_opts, /*silent=*/true);
    ASSERT_OK(dumper.getStatus());
    Status s = dumper.ReadSequential(
        /*print_kv=*/false,
        /*read_num_limit==*/std::numeric_limits<uint64_t>::max(),
        /*has_from=*/false, /*from_key=*/"",
        /*has_to=*/false, /*to_key=*/"");
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(
        std::strstr("Table property expects 1026 entries when excluding range "
                    "deletions, but scanning the table returned 1024 entries",
                    s.getState()));

    // Validation is not performed when read_num, has_from, has_to are set
    ASSERT_OK(dumper.ReadSequential(
        /*print_kv=*/false, /*read_num_limit=*/10,
        /*has_from=*/false, /*from_key=*/"",
        /*has_to=*/false, /*to_key=*/""));

    ASSERT_OK(dumper.ReadSequential(
        /*print_kv=*/false,
        /*read_num_limit=*/std::numeric_limits<uint64_t>::max(),
        /*has_from=*/true, /*from_key=*/MakeKey(100),
        /*has_to=*/false, /*to_key=*/""));

    ASSERT_OK(dumper.ReadSequential(
        /*print_kv=*/false,
        /*read_num_limit=*/std::numeric_limits<uint64_t>::max(),
        /*has_from=*/false, /*from_key=*/"",
        /*has_to=*/true, /*to_key=*/MakeKey(100)));

    cleanup(opts, file_path);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
