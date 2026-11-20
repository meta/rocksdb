//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/copy_engine/copy_engine.h"

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "file/file_util.h"
#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class BlockingRenameFileSystem : public FileSystemWrapper {
 public:
  explicit BlockingRenameFileSystem(const std::shared_ptr<FileSystem>& fs)
      : FileSystemWrapper(fs) {}

  const char* Name() const override { return "BlockingRenameFileSystem"; }

  std::future<void> GetRenameStartedFuture() {
    return rename_started_.get_future();
  }
  void ReleaseRename() { release_rename_.set_value(); }

  IOStatus RenameFile(const std::string& src, const std::string& dst,
                      const IOOptions& options, IODebugContext* dbg) override {
    rename_started_.set_value();
    release_rename_future_.wait();
    return FileSystemWrapper::RenameFile(src, dst, options, dbg);
  }

 private:
  std::promise<void> rename_started_;
  std::promise<void> release_rename_;
  std::future<void> release_rename_future_ = release_rename_.get_future();
};

}  // namespace

class CopyEngineTest : public testing::Test {
 protected:
  CopyEngineTest() {
    env_ = Env::Default();
    dir_ = test::PerThreadDBPath(env_, "copy_engine_test");
  }

  // PerThreadDBPath is deterministic per (name, pid, thread), so every case in
  // this fixture maps to the same directory. Destroy it around each case so
  // files from one case (or a prior/repeated run) cannot leak into the next.
  void SetUp() override {
    ASSERT_OK(DestroyDir(env_, dir_));
    ASSERT_OK(env_->CreateDirIfMissing(dir_));
  }

  void TearDown() override { ASSERT_OK(DestroyDir(env_, dir_)); }

  std::string SrcPath(int i) const {
    return dir_ + "/src_" + std::to_string(i);
  }
  std::string DstPath(int i) const {
    return dir_ + "/dst_" + std::to_string(i);
  }

  void CreateSrc(int i, const std::string& content) {
    ASSERT_OK(WriteStringToFile(env_, content, SrcPath(i),
                                /*should_sync=*/false));
  }

  // Varied sizes exercise the dynamic pull-based pool (not a static split).
  std::string ContentFor(int i) const {
    return "file-" + std::to_string(i) + "-" + std::string((i + 1) * 1024, 'x');
  }

  WorkItem MakeCopyItem(int i) {
    return WorkItem(SrcPath(i), DstPath(i), Temperature::kUnknown,
                    Temperature::kUnknown, /*contents=*/"", env_, env_,
                    EnvOptions(), /*sync=*/false, /*use_fsync=*/false,
                    /*rate_limiter=*/nullptr,
                    /*size_limit=*/0, /*stats=*/nullptr);
  }

  WorkItem MakeLinkItem(int i) {
    WorkItem w = MakeCopyItem(i);
    w.type = WorkItemType::Link;
    return w;
  }

  void VerifyDstMatches(int i, const std::string& expected) {
    std::string got;
    ASSERT_OK(ReadFileToString(env_, DstPath(i), &got));
    ASSERT_EQ(expected, got);
  }

  Env* env_;
  std::string dir_;
};

TEST_F(CopyEngineTest, ParallelCopy) {
  constexpr int kNumFiles = 16;
  std::vector<std::string> contents;
  for (int i = 0; i < kNumFiles; ++i) {
    contents.push_back(ContentFor(i));
    CreateSrc(i, contents[i]);
  }

  CopyEngineOptions options;
  options.max_background_operations = 4;
  CopyEngine engine(std::move(options));

  std::vector<std::future<WorkItemResult>> results;
  for (int i = 0; i < kNumFiles; ++i) {
    WorkItem w = MakeCopyItem(i);
    results.push_back(w.result.get_future());
    engine.Submit(std::move(w));
  }

  for (int i = 0; i < kNumFiles; ++i) {
    WorkItemResult r = results[i].get();
    ASSERT_OK(r.io_status);
    ASSERT_EQ(contents[i].size(), r.size);
    VerifyDstMatches(i, contents[i]);
  }
}

TEST_F(CopyEngineTest, ParallelLink) {
  constexpr int kNumFiles = 16;
  std::vector<std::string> contents;
  for (int i = 0; i < kNumFiles; ++i) {
    contents.push_back(ContentFor(i));
    CreateSrc(i, contents[i]);
  }

  CopyEngineOptions options;
  options.max_background_operations = 4;
  CopyEngine engine(std::move(options));

  std::vector<std::future<WorkItemResult>> results;
  for (int i = 0; i < kNumFiles; ++i) {
    WorkItem w = MakeLinkItem(i);
    results.push_back(w.result.get_future());
    engine.Submit(std::move(w));
  }

  bool link_supported = true;
  for (int i = 0; i < kNumFiles; ++i) {
    WorkItemResult r = results[i].get();
    if (r.io_status.IsNotSupported()) {
      link_supported = false;
      continue;
    }
    ASSERT_OK(r.io_status);
    VerifyDstMatches(i, contents[i]);
  }
  if (!link_supported) {
    ROCKSDB_GTEST_SKIP("FileSystem does not support hard links");
  }
}

TEST_F(CopyEngineTest, SerialCopyMatchesSingleThread) {
  constexpr int kNumFiles = 5;
  std::vector<std::string> contents;
  for (int i = 0; i < kNumFiles; ++i) {
    contents.push_back(ContentFor(i));
    CreateSrc(i, contents[i]);
  }

  CopyEngineOptions options;
  options.max_background_operations = 1;
  CopyEngine engine(std::move(options));

  std::vector<std::future<WorkItemResult>> results;
  for (int i = 0; i < kNumFiles; ++i) {
    WorkItem w = MakeCopyItem(i);
    results.push_back(w.result.get_future());
    engine.Submit(std::move(w));
  }

  for (int i = 0; i < kNumFiles; ++i) {
    WorkItemResult r = results[i].get();
    ASSERT_OK(r.io_status);
    VerifyDstMatches(i, contents[i]);
  }
}

TEST_F(CopyEngineTest, ResultWaitsForRename) {
  const std::string content = ContentFor(0);
  CreateSrc(0, content);
  auto fs = std::make_shared<BlockingRenameFileSystem>(env_->GetFileSystem());
  std::unique_ptr<Env> dst_env = NewCompositeEnv(fs);
  std::future<void> rename_started = fs->GetRenameStartedFuture();
  CopyEngine engine{CopyEngineOptions{}};
  WorkItem item = MakeCopyItem(0);
  item.dst_env = dst_env.get();
  item.final_dst_path = DstPath(1);
  std::future<WorkItemResult> result = item.result.get_future();
  engine.Submit(std::move(item));

  const std::future_status rename_status =
      rename_started.wait_for(std::chrono::minutes(1));
  const std::future_status result_status =
      result.wait_for(std::chrono::seconds(0));
  fs->ReleaseRename();

  ASSERT_EQ(std::future_status::ready, rename_status);
  EXPECT_EQ(std::future_status::timeout, result_status);
  WorkItemResult copied = result.get();
  ASSERT_OK(copied.io_status);
  VerifyDstMatches(1, content);
}

TEST_F(CopyEngineTest, MissingSourceReportsErrorPerItem) {
  CreateSrc(0, ContentFor(0));

  CopyEngineOptions options;
  options.max_background_operations = 2;
  CopyEngine engine(std::move(options));

  WorkItem ok_item = MakeCopyItem(0);
  auto ok_future = ok_item.result.get_future();
  engine.Submit(std::move(ok_item));

  WorkItem bad_item = MakeCopyItem(1);  // no source file on disk
  auto bad_future = bad_item.result.get_future();
  engine.Submit(std::move(bad_item));

  WorkItemResult ok_result = ok_future.get();
  ASSERT_OK(ok_result.io_status);
  VerifyDstMatches(0, ContentFor(0));

  WorkItemResult bad_result = bad_future.get();
  ASSERT_NOK(bad_result.io_status);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
