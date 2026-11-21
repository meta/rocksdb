//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/fault_injection_fs.h"

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

#include "file/file_util.h"
#include "file/filename.h"
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

// Tests of the atomic-replace mode (FaultInjectionTestFS::
// SetAtomicReplaceOnClose()). Each test writes into its own empty DB
// directory on the default file system and reads the result through the
// target file system, which shows what a new process would see after a kill.
class FaultInjectionTestFSAtomicReplaceTest : public testing::Test {
 protected:
  FaultInjectionTestFSAtomicReplaceTest()
      : env_(Env::Default()),
        dbname_(test::PerThreadDBPath("fault_injection_fs_atomic_replace")),
        fault_fs_(
            std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem())) {
    EXPECT_OK(DestroyDir(env_, dbname_));
    EXPECT_OK(env_->CreateDirIfMissing(dbname_));
    fault_fs_->SetAtomicReplaceOnClose(true);
  }

  ~FaultInjectionTestFSAtomicReplaceTest() override {
    EXPECT_OK(DestroyDir(env_, dbname_));
  }

  // Returns the content of `fname` on the target file system, or "<none>" if
  // the file does not exist.
  std::string TargetContent(const std::string& fname) {
    if (env_->FileExists(fname).IsNotFound()) {
      return "<none>";
    }
    std::string data;
    EXPECT_OK(ReadFileToString(env_, fname, &data));
    return data;
  }

  // Opens `fname` through fault_fs_ and appends `data`.
  std::unique_ptr<FSWritableFile> OpenAndAppend(const std::string& fname,
                                                const std::string& data) {
    std::unique_ptr<FSWritableFile> file;
    EXPECT_OK(fault_fs_->NewWritableFile(fname, FileOptions(), &file,
                                         nullptr /* dbg */));
    if (file != nullptr) {
      EXPECT_OK(file->Append(data, IOOptions(), nullptr /* dbg */));
    }
    return file;
  }

  // Writes `data` to `fname` through fault_fs_ and closes the file.
  void WriteAndClose(const std::string& fname, const std::string& data) {
    std::unique_ptr<FSWritableFile> file = OpenAndAppend(fname, data);
    ASSERT_NE(nullptr, file);
    ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));
    ASSERT_OK(file->Close(IOOptions(), nullptr /* dbg */));
  }

  // Returns the names in dbname_ that start with the hidden temp prefix.
  std::vector<std::string> HiddenTempFiles() {
    std::vector<std::string> children;
    EXPECT_OK(env_->GetChildren(dbname_, &children));
    std::vector<std::string> hidden;
    for (const std::string& child : children) {
      if (Slice(child).starts_with(
              FaultInjectionTestFS::kAtomicTempFilePrefix)) {
        hidden.push_back(child);
      }
    }
    return hidden;
  }

  Env* const env_;
  const std::string dbname_;
  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
};

// The mode sets the bit. CURRENT, IDENTITY and OPTIONS files are not visible
// on the target until Close(), also after Sync(), and Close() replaces the old
// file. Without the mode, the bit is cleared.
TEST_F(FaultInjectionTestFSAtomicReplaceTest, HiddenUntilClose) {
  const int64_t bit = int64_t{1} << FSSupportedOps::kAtomicReplaceOnClose;
  int64_t ops = 0;
  fault_fs_->SupportedOps(ops);
  ASSERT_EQ(bit, ops & bit);

  const std::string current = CurrentFileName(dbname_);
  ASSERT_OK(WriteStringToFile(env_, "MANIFEST-000004\n", current,
                              true /* should_sync */));
  for (const std::string& fname :
       {current, IdentityFileName(dbname_), OptionsFileName(dbname_, 5)}) {
    SCOPED_TRACE(fname);
    const std::string old_content = TargetContent(fname);
    std::unique_ptr<FSWritableFile> file = OpenAndAppend(fname, "new");
    ASSERT_NE(nullptr, file);
    ASSERT_OK(file->Sync(IOOptions(), nullptr /* dbg */));
    ASSERT_OK(file->Fsync(IOOptions(), nullptr /* dbg */));
    ASSERT_EQ(old_content, TargetContent(fname));
    ASSERT_EQ(uint64_t{3}, file->GetFileSize(IOOptions(), nullptr /* dbg */));
    ASSERT_OK(file->Close(IOOptions(), nullptr /* dbg */));
    ASSERT_EQ("new", TargetContent(fname));
    ASSERT_TRUE(HiddenTempFiles().empty());
  }

  fault_fs_->SetAtomicReplaceOnClose(false);
  ops = bit;
  fault_fs_->SupportedOps(ops);
  ASSERT_EQ(0, ops & bit);
}

// Destroying the file without Close() and a Close() while the file system is
// inactive publish nothing and delete the hidden temp file.
TEST_F(FaultInjectionTestFSAtomicReplaceTest, NothingPublishedWithoutClose) {
  const std::string current = CurrentFileName(dbname_);
  ASSERT_OK(WriteStringToFile(env_, "old", current, true /* should_sync */));
  for (const char* how : {"destroy", "inactive"}) {
    SCOPED_TRACE(how);
    std::unique_ptr<FSWritableFile> file = OpenAndAppend(current, "new");
    ASSERT_NE(nullptr, file);
    ASSERT_EQ(size_t{1}, HiddenTempFiles().size());
    const std::string mode = how;
    if (mode == "inactive") {
      fault_fs_->SetFilesystemActive(false);
      ASSERT_NOK(file->Close(IOOptions(), nullptr /* dbg */));
      fault_fs_->SetFilesystemActive(true);
    }
    file.reset();
    ASSERT_EQ("old", TargetContent(current));
    ASSERT_TRUE(HiddenTempFiles().empty());
  }
}

// An injected Close() error fails the Close(). About half of the time it
// comes before the publish, and the old file stays. Otherwise it comes after
// the publish, and the new file is in place. Either way no hidden temp file
// stays. The test repeats the write until it sees both cases; with a chance
// of one half each time, 64 tries miss a case with a chance of about 2^-63.
TEST_F(FaultInjectionTestFSAtomicReplaceTest, InjectedCloseError) {
  const std::string current = CurrentFileName(dbname_);
  fault_fs_->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataWrite, /*seed=*/0, /*one_in=*/1,
      /*retryable=*/false, /*has_data_loss=*/false);
  int published = 0;
  int not_published = 0;
  for (int i = 0; i < 64 && (published == 0 || not_published == 0); ++i) {
    SCOPED_TRACE("try " + std::to_string(i));
    ASSERT_OK(WriteStringToFile(env_, "old", current, true /* should_sync */));
    std::unique_ptr<FSWritableFile> file = OpenAndAppend(current, "new");
    ASSERT_NE(nullptr, file);
    fault_fs_->EnableThreadLocalErrorInjection(
        FaultInjectionIOType::kMetadataWrite);
    IOStatus s = file->Close(IOOptions(), nullptr /* dbg */);
    fault_fs_->DisableThreadLocalErrorInjection(
        FaultInjectionIOType::kMetadataWrite);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    file.reset();
    const std::string content = TargetContent(current);
    if (content == "new") {
      ++published;
    } else {
      ASSERT_EQ("old", content);
      ++not_published;
    }
    ASSERT_TRUE(HiddenTempFiles().empty());
  }
  ASSERT_GT(published, 0);
  ASSERT_GT(not_published, 0);
}

// A write that is not published keeps the crash state of an older file with
// the same name. Here the older CURRENT was written without the mode after the
// last directory sync, so the simulated crash still deletes it.
TEST_F(FaultInjectionTestFSAtomicReplaceTest, UnpublishedWriteKeepsOldState) {
  const std::string current = CurrentFileName(dbname_);
  fault_fs_->SetAtomicReplaceOnClose(false);
  ASSERT_NO_FATAL_FAILURE(WriteAndClose(current, "old"));
  fault_fs_->SetAtomicReplaceOnClose(true);

  std::unique_ptr<FSWritableFile> file = OpenAndAppend(current, "new");
  ASSERT_NE(nullptr, file);
  file.reset();
  ASSERT_EQ("old", TargetContent(current));
  ASSERT_OK(fault_fs_->DeleteFilesCreatedAfterLastDirSync(IOOptions(),
                                                          nullptr /* dbg */));
  ASSERT_EQ("<none>", TargetContent(current));
}

// A published file is durable: the three functions that simulate a crash do
// not delete, revert or truncate it. A MANIFEST keeps the normal behavior: it
// was created after the last directory sync, so the simulated crash deletes
// it.
TEST_F(FaultInjectionTestFSAtomicReplaceTest, PublishedFileSurvivesCrash) {
  fault_fs_->SetInjectUnsyncedDataLoss(true);
  const std::string current = CurrentFileName(dbname_);
  const std::string identity = IdentityFileName(dbname_);
  const std::string options_file = OptionsFileName(dbname_, 5);
  const std::string manifest = DescriptorFileName(dbname_, 4);
  // An old CURRENT that FaultInjectionTestFS tracks as created after the last
  // directory sync, written without the mode.
  fault_fs_->SetAtomicReplaceOnClose(false);
  ASSERT_NO_FATAL_FAILURE(WriteAndClose(current, "old"));
  fault_fs_->SetAtomicReplaceOnClose(true);

  ASSERT_NO_FATAL_FAILURE(WriteAndClose(manifest, "manifest"));
  ASSERT_NO_FATAL_FAILURE(WriteAndClose(current, "MANIFEST-000004\n"));
  ASSERT_NO_FATAL_FAILURE(WriteAndClose(identity, "id"));
  ASSERT_NO_FATAL_FAILURE(WriteAndClose(options_file, "options"));

  Random rnd(301);
  ASSERT_OK(fault_fs_->DropRandomUnsyncedFileData(&rnd));
  ASSERT_OK(fault_fs_->DropUnsyncedFileData());
  ASSERT_OK(fault_fs_->DeleteFilesCreatedAfterLastDirSync(IOOptions(),
                                                          nullptr /* dbg */));
  ASSERT_EQ("MANIFEST-000004\n", TargetContent(current));
  ASSERT_EQ("id", TargetContent(identity));
  ASSERT_EQ("options", TargetContent(options_file));
  ASSERT_EQ("<none>", TargetContent(manifest));
}

// After a kill during Close(), the hidden temp file can stay. RocksDB cannot
// parse its name, so it ignores it. A next write of the same name truncates
// it, publishes the new content and leaves no hidden temp file. RocksDB does
// not reuse OPTIONS file numbers, so the test writes the OPTIONS name again
// only to check the file system.
TEST_F(FaultInjectionTestFSAtomicReplaceTest, LeftoverTempFileAfterRestart) {
  for (const std::string& fname :
       {CurrentFileName(dbname_), OptionsFileName(dbname_, 5)}) {
    SCOPED_TRACE(fname);
    const std::string base = fname.substr(dbname_.size() + 1);
    const std::string hidden =
        FaultInjectionTestFS::kAtomicTempFilePrefix + base;
    uint64_t number = 0;
    FileType type;
    ASSERT_FALSE(ParseFileName(hidden, &number, &type));
    ASSERT_OK(WriteStringToFile(env_, "partial content of a killed write",
                                dbname_ + "/" + hidden,
                                true /* should_sync */));

    // A new process gets a new file system.
    fault_fs_ = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
    fault_fs_->SetAtomicReplaceOnClose(true);
    ASSERT_NO_FATAL_FAILURE(WriteAndClose(fname, "new"));
    ASSERT_EQ("new", TargetContent(fname));
    ASSERT_TRUE(HiddenTempFiles().empty());
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
