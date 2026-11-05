//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/fault_injection_fs.h"

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

#include "port/port.h"
#include "test_util/testharness.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class InjectedErrorLogTest : public testing::Test {
 protected:
  static std::string DecodeCString(const char* data, size_t len) {
    size_t actual_len = 0;
    while (actual_len < len && data[actual_len] != '\0') {
      ++actual_len;
    }
    return std::string(data, actual_len);
  }

  std::string ReadRawLog(const std::string& path) {
    std::string raw;
    Status s = ReadFileToString(Env::Default(), path, &raw);
    EXPECT_OK(s);
    return raw;
  }

  InjectedErrorLog::Entry DecodeEntry(const std::string& raw, size_t index) {
    InjectedErrorLog::Entry entry{};
    size_t offset = index * sizeof(InjectedErrorLog::Entry);
    EXPECT_GE(raw.size(), offset + sizeof(entry));
    if (raw.size() >= offset + sizeof(entry)) {
      std::memcpy(&entry, raw.data() + offset, sizeof(entry));
    }
    return entry;
  }
};

class FaultInjectionTestFSTest : public testing::Test {};

namespace {

std::shared_ptr<FaultInjectionTestFS> NewFaultFsExcludingInfoLogs(
    Env* env, FaultInjectionIOType type) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env->GetFileSystem());
  fault_fs->SetFileTypesExcludedFromFaultInjection({FileType::kInfoLogFile});
  fault_fs->SetThreadLocalErrorContext(type, /*seed=*/0, /*one_in=*/1,
                                       /*retryable=*/false,
                                       /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(type);
  return fault_fs;
}

class CloseCountingWritableFile : public FSWritableFileOwnerWrapper {
 public:
  CloseCountingWritableFile(std::unique_ptr<FSWritableFile>&& target,
                            int* close_calls)
      : FSWritableFileOwnerWrapper(std::move(target)),
        close_calls_(close_calls) {}

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    ++(*close_calls_);
    return target()->Close(options, dbg);
  }

 private:
  int* close_calls_;
};

}  // namespace

// Goal: verify a single structured record is persisted to the binary log with
// the expected fixed-width fields. The test writes one entry, flushes the
// file, and decodes the raw bytes back into an entry struct.
TEST_F(InjectedErrorLogTest, BasicRecordAndFlush) {
  if (!port::kLittleEndian) {
    ROCKSDB_GTEST_BYPASS(
        "Persistent injected error log requires little-endian");
    return;
  }
  std::string path = test::PerThreadDBPath("injected_error_log_basic.bin");
  {
    InjectedErrorLog log(path);
    IOStatus status = IOStatus::IOError("injected write error");
    status.SetRetryable(false);
    status.SetDataLoss(true);
    log.Record("Append", "/tmp/000001.log",
               fault_injection_detail::OffsetSizeAndHead(7, Slice("abcd", 4)),
               status);
    log.Flush();
  }

  std::string raw = ReadRawLog(path);
  ASSERT_EQ(raw.size(), sizeof(InjectedErrorLog::Entry));
  auto entry = DecodeEntry(raw, 0);
  EXPECT_NE(entry.timestamp_us, 0U);
  EXPECT_EQ(entry.detail.offset, 7U);
  EXPECT_EQ(entry.detail.size, 4U);
  EXPECT_EQ(entry.detail_kind, InjectedErrorLog::kDetailOffsetSizeAndHead);
  EXPECT_EQ(entry.detail_payload_size, 4U);
  EXPECT_EQ(entry.retryable, 0U);
  EXPECT_EQ(entry.data_loss, 1U);
  EXPECT_EQ(DecodeCString(entry.op_name, sizeof(entry.op_name)), "Append");
  EXPECT_EQ(DecodeCString(entry.file_name, sizeof(entry.file_name)),
            "/tmp/000001.log");
  EXPECT_EQ(DecodeCString(entry.status_message, sizeof(entry.status_message)),
            "injected write error");
  EXPECT_EQ(std::string(entry.detail.payload, entry.detail.payload + 4),
            "abcd");
}

// Goal: verify the file-backed logger keeps all records instead of truncating
// to the old in-memory ring size. The test writes more than 1,000 entries and
// checks that both the first and last ones are still present in the file.
TEST_F(InjectedErrorLogTest, DirectLogKeepsAllEntries) {
  if (!port::kLittleEndian) {
    ROCKSDB_GTEST_BYPASS(
        "Persistent injected error log requires little-endian");
    return;
  }
  std::string path =
      test::PerThreadDBPath("injected_error_log_all_entries.bin");
  {
    InjectedErrorLog log(path);
    IOStatus status = IOStatus::IOError("injected write error");

    constexpr size_t kNumEntries = 1100;
    for (size_t i = 0; i < kNumEntries; ++i) {
      std::string file_name = "file" + std::to_string(i);
      log.Record("Append", file_name, fault_injection_detail::NoDetail(),
                 status);
    }
    log.Flush();
  }

  std::string raw = ReadRawLog(path);
  constexpr size_t kNumEntries = 1100;
  ASSERT_EQ(raw.size(), kNumEntries * sizeof(InjectedErrorLog::Entry));

  auto first = DecodeEntry(raw, 0);
  auto last = DecodeEntry(raw, kNumEntries - 1);
  EXPECT_EQ(DecodeCString(first.file_name, sizeof(first.file_name)), "file0");
  EXPECT_EQ(DecodeCString(last.file_name, sizeof(last.file_name)),
            "file" + std::to_string(kNumEntries - 1));
}

// Goal: verify concurrent Record() calls append independent entries to the
// shared file. The test has several threads emit records concurrently and then
// checks the raw file size matches the total number of writes.
TEST_F(InjectedErrorLogTest, ConcurrentRecord) {
  if (!port::kLittleEndian) {
    ROCKSDB_GTEST_BYPASS(
        "Persistent injected error log requires little-endian");
    return;
  }
  std::string path = test::PerThreadDBPath("injected_error_log_concurrent.bin");
  constexpr int kNumThreads = 4;
  constexpr int kRecordsPerThread = 200;
  {
    InjectedErrorLog log(path);
    IOStatus status = IOStatus::IOError("injected read error");

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int t = 0; t < kNumThreads; t++) {
      threads.emplace_back([&log, &status, t]() {
        for (int i = 0; i < kRecordsPerThread; i++) {
          std::string file_name =
              "thread" + std::to_string(t) + "_" + std::to_string(i);
          log.Record("Read", file_name, fault_injection_detail::NoDetail(),
                     status);
        }
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    log.Flush();
  }

  std::string raw = ReadRawLog(path);
  ASSERT_EQ(raw.size(), static_cast<size_t>(kNumThreads * kRecordsPerThread) *
                            sizeof(InjectedErrorLog::Entry));
}

// Goal: verify long file paths are suffix-truncated so the basename survives
// in the fixed-width record. The test logs a path longer than the file-name
// field and checks the stored sample matches the expected tail bytes.
TEST_F(InjectedErrorLogTest, LongFileNameKeepsSuffix) {
  if (!port::kLittleEndian) {
    ROCKSDB_GTEST_BYPASS(
        "Persistent injected error log requires little-endian");
    return;
  }
  std::string path =
      test::PerThreadDBPath("injected_error_log_suffix_truncation.bin");
  const std::string long_file_name =
      "/tmp/rocksdb_crashtest/artifacts/very/long/path/that/keeps/growing/"
      "db_crashtest/fault_injection/000123.sst";
  ASSERT_GT(long_file_name.size(), InjectedErrorLog::kMaxFileNameLen - 1);
  {
    InjectedErrorLog log(path);
    IOStatus status = IOStatus::IOError("injected write error");
    log.Record("Append", long_file_name, fault_injection_detail::NoDetail(),
               status);
    log.Flush();
  }

  std::string raw = ReadRawLog(path);
  auto entry = DecodeEntry(raw, 0);
  std::string stored = DecodeCString(entry.file_name, sizeof(entry.file_name));
  std::string expected = long_file_name.substr(
      long_file_name.size() - (InjectedErrorLog::kMaxFileNameLen - 1));
  EXPECT_EQ(stored, expected);
  EXPECT_EQ(stored.compare(stored.size() - strlen("000123.sst"),
                           strlen("000123.sst"), "000123.sst"),
            0);
}

// Goal: verify the human-readable hex helper still formats the payload samples
// exactly as expected, since the Python decoder depends on the same output.
TEST_F(InjectedErrorLogTest, HexHead) {
  const char data[] = "\x01\x02\xAB\xCD";
  std::string result = InjectedErrorLog::HexHead(data, 4);
  ASSERT_EQ(result, "01 02 ab cd");

  result = InjectedErrorLog::HexHead(data, 4, 2);
  ASSERT_EQ(result, "01 02 ...");
}

TEST_F(FaultInjectionTestFSTest, FaultInjectionExcludesInfoLogFiles) {
  Env* env = Env::Default();
  const std::string dbname =
      test::PerThreadDBPath("fault_injection_fs_test_metadata_read");
  const std::string log_dir = dbname + "_logs";
  ASSERT_OK(env->CreateDirIfMissing(dbname));
  ASSERT_OK(env->CreateDirIfMissing(log_dir));

  const std::string current_info_log = InfoLogFileName(dbname, dbname, log_dir);
  const std::string old_info_log =
      OldInfoLogFileName(dbname, 123, dbname, log_dir);
  const std::string manifest = DescriptorFileName(dbname, 1);
  const std::string manifest_for_write = DescriptorFileName(dbname, 2);
  const std::string manifest_for_delete = DescriptorFileName(dbname, 3);
  ASSERT_OK(
      WriteStringToFile(env, "old log", old_info_log, false /* should_sync */));
  ASSERT_OK(
      WriteStringToFile(env, "manifest", manifest, false /* should_sync */));
  ASSERT_OK(WriteStringToFile(env, "manifest delete", manifest_for_delete,
                              false /* should_sync */));

  {
    auto fault_fs =
        NewFaultFsExcludingInfoLogs(env, FaultInjectionIOType::kMetadataRead);

    ASSERT_OK(fault_fs->FileExists(old_info_log, IOOptions(), nullptr));
    ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kMetadataRead));

    IOStatus s = fault_fs->FileExists(manifest, IOOptions(), nullptr);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kMetadataRead));
  }

  {
    auto fault_fs =
        NewFaultFsExcludingInfoLogs(env, FaultInjectionIOType::kRead);
    std::unique_ptr<FSSequentialFile> seq_file;
    ASSERT_OK(fault_fs->NewSequentialFile(old_info_log, FileOptions(),
                                          &seq_file, nullptr /* dbg */));
    char scratch[16];
    Slice result;
    ASSERT_OK(seq_file->Read(sizeof(scratch), IOOptions(), &result, scratch,
                             nullptr /* dbg */));
    ASSERT_EQ("old log", result.ToString());
    ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kRead));

    std::unique_ptr<FSSequentialFile> manifest_seq_file;
    IOStatus s = fault_fs->NewSequentialFile(
        manifest, FileOptions(), &manifest_seq_file, nullptr /* dbg */);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kRead));
  }

  {
    auto fault_fs =
        NewFaultFsExcludingInfoLogs(env, FaultInjectionIOType::kWrite);
    std::unique_ptr<FSWritableFile> info_log_writer;
    ASSERT_OK(fault_fs->NewWritableFile(current_info_log, FileOptions(),
                                        &info_log_writer, nullptr /* dbg */));
    ASSERT_OK(
        info_log_writer->Append("current log", IOOptions(), nullptr /* dbg */));
    ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kWrite));

    std::unique_ptr<FSWritableFile> manifest_writer;
    IOStatus s = fault_fs->NewWritableFile(manifest_for_write, FileOptions(),
                                           &manifest_writer, nullptr /* dbg */);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kWrite));
  }

  {
    auto fault_fs =
        NewFaultFsExcludingInfoLogs(env, FaultInjectionIOType::kMetadataWrite);
    ASSERT_OK(fault_fs->DeleteFile(old_info_log, IOOptions(), nullptr));
    ASSERT_EQ(0, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kMetadataWrite));

    IOStatus s =
        fault_fs->DeleteFile(manifest_for_delete, IOOptions(), nullptr);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_EQ(1, fault_fs->GetAndResetInjectedThreadLocalErrorCount(
                     FaultInjectionIOType::kMetadataWrite));
  }
}

TEST_F(FaultInjectionTestFSTest, FileScopedWalWriteErrors) {
  auto fault_fs =
      NewFaultFsExcludingInfoLogs(Env::Default(), FaultInjectionIOType::kWrite);

  IOStatus status = fault_fs->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, IOOptions(), "Append", "/tmp/000001.log");
  ASSERT_TRUE(status.IsIOError());
  ASSERT_EQ(IOStatus::IOErrorScope::kIOErrorScopeFileSystem, status.GetScope());

  fault_fs->SetInjectFileScopeOnWALWriteError(true);
  status = fault_fs->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, IOOptions(), "Append", "/tmp/000002.log");
  ASSERT_TRUE(status.IsIOError());
  ASSERT_EQ(IOStatus::IOErrorScope::kIOErrorScopeFile, status.GetScope());

  status = fault_fs->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, IOOptions(), "Append", "/tmp/000003.sst");
  ASSERT_TRUE(status.IsIOError());
  ASSERT_EQ(IOStatus::IOErrorScope::kIOErrorScopeFileSystem, status.GetScope());

  fault_fs->SetInjectWriteFaultsOnlyOnWAL(true);
  status = fault_fs->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, IOOptions(), "Append", "/tmp/000004.sst");
  ASSERT_OK(status);
  status = fault_fs->MaybeInjectThreadLocalError(
      FaultInjectionIOType::kWrite, IOOptions(), "Append", "/tmp/000005.log");
  ASSERT_TRUE(status.IsIOError());
  ASSERT_EQ(IOStatus::IOErrorScope::kIOErrorScopeFile, status.GetScope());
}

TEST_F(FaultInjectionTestFSTest,
       InjectedMetadataCloseErrorDoesNotRetryInnerClose) {
  Env* env = Env::Default();
  const std::string dbname =
      test::PerThreadDBPath("fault_injection_fs_test_close_retry");
  ASSERT_OK(env->CreateDirIfMissing(dbname));

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env->GetFileSystem());
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataWrite, /*seed=*/0, /*one_in=*/1,
      /*retryable=*/false, /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataWrite);

  int inner_close_calls = 0;
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(fault_fs->NewWritableFile(dbname + "/CURRENT.tmp", FileOptions(),
                                      &file, nullptr /* dbg */));
  file.reset(new TestFSWritableFile(dbname + "/CURRENT.tmp", FileOptions(),
                                    std::make_unique<CloseCountingWritableFile>(
                                        std::move(file), &inner_close_calls),
                                    fault_fs.get()));

  ASSERT_OK(file->Append("test", IOOptions(), nullptr));

  IOStatus s = file->Close(IOOptions(), nullptr);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_EQ(0, inner_close_calls);

  // Mirror production layering: after Close() has already been attempted on
  // this wrapper, later Close() calls are wrapper-level no-ops rather than a
  // second raw Close() forwarded to the same owned file.
  ASSERT_OK(file->Close(IOOptions(), nullptr));
  ASSERT_EQ(0, inner_close_calls);

  // Destruction may still release the owned file object, but it must not
  // forward another raw Close().
  file.reset();
  ASSERT_EQ(0, inner_close_calls);
}

namespace {

// Opens a file for write through a FaultInjectionTestFS that holds unsynced
// appends in memory.
std::shared_ptr<FaultInjectionTestFS> NewUnsyncedDataLossFsAndFile(
    Env* env, const std::string& test_name, std::string* file_name,
    std::unique_ptr<FSWritableFile>* file) {
  const std::string dbname = test::PerThreadDBPath(test_name);
  *file_name = dbname + "/test-file";
  EXPECT_OK(env->CreateDirIfMissing(dbname));

  std::shared_ptr<FaultInjectionTestFS> fault_fs =
      std::make_shared<FaultInjectionTestFS>(env->GetFileSystem());
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(true);
  EXPECT_OK(fault_fs->NewWritableFile(*file_name, FileOptions(), file,
                                      nullptr /* dbg */));
  return fault_fs;
}

class FailTruncateWritableFile : public FSWritableFileOwnerWrapper {
 public:
  explicit FailTruncateWritableFile(std::unique_ptr<FSWritableFile>&& target)
      : FSWritableFileOwnerWrapper(std::move(target)) {}

  IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::IOError("injected truncate failure");
  }
};

class FailTruncateFS : public FileSystemWrapper {
 public:
  explicit FailTruncateFS(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}
  const char* Name() const override { return "FailTruncateFS"; }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    IOStatus s = target()->NewWritableFile(fname, opts, result, dbg);
    if (s.ok()) {
      result->reset(new FailTruncateWritableFile(std::move(*result)));
    }
    return s;
  }
};

}  // namespace

TEST_F(FaultInjectionTestFSTest, TruncatePreservesAndShrinksUnsyncedData) {
  Env* env = Env::Default();
  std::string file_name;
  std::unique_ptr<FSWritableFile> file;
  std::shared_ptr<FaultInjectionTestFS> fault_fs = NewUnsyncedDataLossFsAndFile(
      env, "fault_injection_fs_test_truncate_unsynced", &file_name, &file);
  ASSERT_NE(nullptr, file);

  ASSERT_OK(file->Append("synced", IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));

  ASSERT_OK(file->Append("unsynced", IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Truncate(/*size=*/14, IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));

  ASSERT_OK(file->Append("keepdiscard", IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Truncate(/*size=*/18, IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Close(IOOptions(), nullptr /* dbg */));

  std::string contents;
  ASSERT_OK(ReadFileToString(env, file_name, &contents));
  ASSERT_EQ("syncedunsyncedkeep", contents);
  ASSERT_OK(env->DeleteFile(file_name));
}

// Truncating below the last sync point discards everything still buffered.
TEST_F(FaultInjectionTestFSTest, TruncateBelowSyncPointDropsUnsyncedData) {
  Env* env = Env::Default();
  std::string file_name;
  std::unique_ptr<FSWritableFile> file;
  std::shared_ptr<FaultInjectionTestFS> fault_fs = NewUnsyncedDataLossFsAndFile(
      env, "fault_injection_fs_test_truncate_below_sync", &file_name, &file);
  ASSERT_NE(nullptr, file);

  ASSERT_OK(file->Append("synced", IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));

  ASSERT_OK(file->Append("unsynced", IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Truncate(/*size=*/3, IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));

  // Writing again after the truncation must resume from the new end of file.
  ASSERT_OK(file->Append("tail", IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Close(IOOptions(), nullptr /* dbg */));

  std::string contents;
  ASSERT_OK(ReadFileToString(env, file_name, &contents));
  ASSERT_EQ("syntail", contents);
  ASSERT_OK(env->DeleteFile(file_name));
}

// A reopened handle models offsets from zero while the file already holds
// data, so its truncation must still reach the file.
TEST_F(FaultInjectionTestFSTest, TruncateReopenedFileReachesTarget) {
  Env* env = Env::Default();
  std::string file_name;
  std::unique_ptr<FSWritableFile> file;
  std::shared_ptr<FaultInjectionTestFS> fault_fs = NewUnsyncedDataLossFsAndFile(
      env, "fault_injection_fs_test_truncate_reopened", &file_name, &file);
  ASSERT_NE(nullptr, file);
  ASSERT_OK(file->Append("synced", IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Close(IOOptions(), nullptr /* dbg */));

  ASSERT_OK(fault_fs->ReopenWritableFile(file_name, FileOptions(), &file,
                                         nullptr /* dbg */));
  ASSERT_OK(file->Truncate(/*size=*/0, IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Close(IOOptions(), nullptr /* dbg */));

  std::string contents;
  ASSERT_OK(ReadFileToString(env, file_name, &contents));
  ASSERT_EQ("", contents);
  ASSERT_OK(env->DeleteFile(file_name));
}

// A failed below-sync truncation must not discard buffered data: the file
// must still hold everything the caller wrote.
TEST_F(FaultInjectionTestFSTest, TruncateBelowSyncPointFailureKeepsBuffer) {
  Env* env = Env::Default();
  const std::string dbname =
      test::PerThreadDBPath("fault_injection_fs_test_truncate_fail");
  const std::string file_name = dbname + "/test-file";
  ASSERT_OK(env->CreateDirIfMissing(dbname));
  env->DeleteFile(file_name).PermitUncheckedError();

  auto base_fs = std::make_shared<FailTruncateFS>(env->GetFileSystem());
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(base_fs);
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(true);

  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(
      fault_fs->NewWritableFile(file_name, FileOptions(), &file, nullptr));
  ASSERT_NE(nullptr, file);

  ASSERT_OK(file->Append("synced", IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));

  ASSERT_OK(file->Append("unsynced", IOOptions(), nullptr /* dbg */));
  ASSERT_NOK(file->Truncate(/*size=*/3, IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));
  ASSERT_OK(file->Close(IOOptions(), nullptr /* dbg */));

  std::string contents;
  ASSERT_OK(ReadFileToString(env, file_name, &contents));
  ASSERT_EQ("syncedunsynced", contents);
  ASSERT_OK(env->DeleteFile(file_name));
}

// Absent a crash, FaultInjectionTestFS must produce byte-for-byte what the
// real filesystem would. Unsynced-data-loss injection defers appends into
// buffer_ instead of making them non-durable, so every operation that depends
// on the file's length needs a matching compensation; this replays randomized
// Append/Truncate/Sync sequences against both and compares the result. It is
// the guard that the injection machinery models POSIX rather than diverging
// from it.
TEST_F(FaultInjectionTestFSTest, UnsyncedInjectionMatchesRealFsWithoutCrash) {
  Env* env = Env::Default();
  const std::string dbname =
      test::PerThreadDBPath("fault_injection_fs_test_differential");
  ASSERT_OK(env->CreateDirIfMissing(dbname));
  const std::string real_name = dbname + "/real";
  const std::string fault_name = dbname + "/fault";

  const uint32_t seed = static_cast<uint32_t>(env->NowMicros());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  for (int trial = 0; trial < 200; ++trial) {
    env->DeleteFile(real_name).PermitUncheckedError();
    env->DeleteFile(fault_name).PermitUncheckedError();

    std::shared_ptr<FaultInjectionTestFS> fault_fs =
        std::make_shared<FaultInjectionTestFS>(env->GetFileSystem());
    fault_fs->SetFilesystemDirectWritable(false);
    fault_fs->SetInjectUnsyncedDataLoss(true);

    std::unique_ptr<FSWritableFile> real_file;
    std::unique_ptr<FSWritableFile> fault_file;
    ASSERT_OK(env->GetFileSystem()->NewWritableFile(real_name, FileOptions(),
                                                    &real_file, nullptr));
    ASSERT_OK(fault_fs->NewWritableFile(fault_name, FileOptions(), &fault_file,
                                        nullptr));

    uint64_t logical_size = 0;
    const int num_ops = 1 + static_cast<int>(rnd.Uniform(12));
    for (int op = 0; op < num_ops; ++op) {
      switch (rnd.Uniform(3)) {
        case 0: {  // Append
          const std::string data =
              rnd.RandomString(1 + static_cast<int>(rnd.Uniform(64)));
          ASSERT_OK(real_file->Append(data, IOOptions(), nullptr));
          ASSERT_OK(fault_file->Append(data, IOOptions(), nullptr));
          logical_size += data.size();
          break;
        }
        case 1: {  // Truncate somewhere at or below the logical end
          const uint64_t size =
              logical_size == 0
                  ? 0
                  : rnd.Uniform(static_cast<int>(logical_size + 1));
          ASSERT_OK(real_file->Truncate(size, IOOptions(), nullptr));
          ASSERT_OK(fault_file->Truncate(size, IOOptions(), nullptr));
          logical_size = size;
          break;
        }
        default: {  // Sync
          ASSERT_OK(real_file->Sync(IOOptions(), nullptr));
          ASSERT_OK(fault_file->Sync(IOOptions(), nullptr));
          break;
        }
      }
    }
    // A final Sync is what makes the two comparable: it is the point at which
    // the injected filesystem has promised the same durability as the real one.
    ASSERT_OK(real_file->Sync(IOOptions(), nullptr));
    ASSERT_OK(fault_file->Sync(IOOptions(), nullptr));
    ASSERT_OK(real_file->Close(IOOptions(), nullptr));
    ASSERT_OK(fault_file->Close(IOOptions(), nullptr));

    std::string real_contents;
    std::string fault_contents;
    ASSERT_OK(ReadFileToString(env, real_name, &real_contents));
    ASSERT_OK(ReadFileToString(env, fault_name, &fault_contents));
    ASSERT_EQ(real_contents.size(), logical_size) << "trial=" << trial;
    ASSERT_EQ(real_contents, fault_contents) << "trial=" << trial;
  }
  ASSERT_OK(env->DeleteFile(real_name));
  ASSERT_OK(env->DeleteFile(fault_name));
}

// Truncate must stay a fault injection point: the logical fast path above
// runs only after error injection has had its chance.
TEST_F(FaultInjectionTestFSTest, TruncateStillHonorsInjectedWriteError) {
  Env* env = Env::Default();
  std::string file_name;
  std::unique_ptr<FSWritableFile> file;
  std::shared_ptr<FaultInjectionTestFS> fault_fs = NewUnsyncedDataLossFsAndFile(
      env, "fault_injection_fs_test_truncate_injection", &file_name, &file);
  ASSERT_NE(nullptr, file);

  ASSERT_OK(file->Append("0123456789", IOOptions(), nullptr));
  fault_fs->SetThreadLocalErrorContext(FaultInjectionIOType::kWrite, /*seed=*/0,
                                       /*one_in=*/1, /*retryable=*/false,
                                       /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
  // Size 10 is the logical no-op that the fast path would otherwise short
  // circuit; injection still has to win.
  ASSERT_NOK(file->Truncate(/*size=*/10, IOOptions(), nullptr));
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  ASSERT_OK(file->Close(IOOptions(), nullptr));
  ASSERT_OK(env->DeleteFile(file_name));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
