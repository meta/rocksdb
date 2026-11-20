// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "env/mock_env.h"
#include "file/file_util.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/env_encryption.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
namespace {
using CreateEnvFunc = Env*();

// These functions are used to create the various environments under which this
// test can execute. These functions are used to allow the test cases to be
// created without the Env being initialized, thereby eliminating a potential
// static initialization fiasco/race condition when attempting to get a
// custom/configured env prior to main being invoked.

static Env* GetDefaultEnv() { return Env::Default(); }

static Env* GetMockEnv() {
  static std::unique_ptr<Env> mock_env(MockEnv::Create(Env::Default()));
  return mock_env.get();
}
static Env* NewTestEncryptedEnv(Env* base, const std::string& provider_id) {
  ConfigOptions config_opts;
  config_opts.invoke_prepare_options = false;

  std::shared_ptr<EncryptionProvider> provider;
  EXPECT_OK(EncryptionProvider::CreateFromString(config_opts, provider_id,
                                                 &provider));
  return NewEncryptedEnv(base, provider);
}

static Env* GetCtrEncryptedEnv() {
  static std::unique_ptr<Env> ctr_encrypt_env(
      NewTestEncryptedEnv(Env::Default(), "CTR://test"));
  return ctr_encrypt_env.get();
}

static Env* GetMemoryEnv() {
  static std::unique_ptr<Env> mem_env(NewMemEnv(Env::Default()));
  return mem_env.get();
}

static Env* GetTestEnv() {
  static std::shared_ptr<Env> env_guard;
  static Env* custom_env = nullptr;
  if (custom_env == nullptr) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char* env_uri = getenv("TEST_ENV_URI");
    if (env_uri != nullptr) {
      EXPECT_OK(Env::CreateFromUri(ConfigOptions(), env_uri, /*fs_uri=*/"",
                                   &custom_env, &env_guard));
    }
  }
  EXPECT_NE(custom_env, nullptr);
  return custom_env;
}

static Env* GetTestFS() {
  static std::shared_ptr<Env> fs_env_guard;
  static Env* fs_env = nullptr;
  if (fs_env == nullptr) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char* fs_uri = getenv("TEST_FS_URI");
    if (fs_uri != nullptr) {
      EXPECT_OK(Env::CreateFromUri(ConfigOptions(), /*env_uri=*/"", fs_uri,
                                   &fs_env, &fs_env_guard));
    }
  }
  EXPECT_NE(fs_env, nullptr);
  return fs_env;
}

}  // namespace
class EnvBasicTestWithParam
    : public testing::Test,
      public ::testing::WithParamInterface<CreateEnvFunc*> {
 public:
  Env* env_;
  const EnvOptions soptions_;
  std::string test_dir_;

  EnvBasicTestWithParam() : env_(GetParam()()) {
    test_dir_ = test::PerThreadDBPath(env_, "env_basic_test");
  }

  void SetUp() override { ASSERT_OK(env_->CreateDirIfMissing(test_dir_)); }

  void TearDown() override { ASSERT_OK(DestroyDir(env_, test_dir_)); }
};

class EnvMoreTestWithParam : public EnvBasicTestWithParam {};

INSTANTIATE_TEST_CASE_P(EnvDefault, EnvBasicTestWithParam,
                        ::testing::Values(&GetDefaultEnv));
INSTANTIATE_TEST_CASE_P(EnvDefault, EnvMoreTestWithParam,
                        ::testing::Values(&GetDefaultEnv));

INSTANTIATE_TEST_CASE_P(MockEnv, EnvBasicTestWithParam,
                        ::testing::Values(&GetMockEnv));

// next statements run env test against default encryption code.
INSTANTIATE_TEST_CASE_P(EncryptedEnv, EnvBasicTestWithParam,
                        ::testing::Values(&GetCtrEncryptedEnv));
INSTANTIATE_TEST_CASE_P(EncryptedEnv, EnvMoreTestWithParam,
                        ::testing::Values(&GetCtrEncryptedEnv));

INSTANTIATE_TEST_CASE_P(MemEnv, EnvBasicTestWithParam,
                        ::testing::Values(&GetMemoryEnv));

namespace {

// Returns a vector of 0 or 1 Env*, depending whether an Env is registered for
// TEST_ENV_URI.
//
// The purpose of returning an empty vector (instead of nullptr) is that gtest
// ValuesIn() will skip running tests when given an empty collection.
std::vector<CreateEnvFunc*> GetCustomEnvs() {
  std::vector<CreateEnvFunc*> res;
  const char* uri = getenv("TEST_ENV_URI");  // NOLINT(concurrency-mt-unsafe)
  if (uri != nullptr) {
    res.push_back(&GetTestEnv);
  }
  uri = getenv("TEST_FS_URI");  // NOLINT(concurrency-mt-unsafe)
  if (uri != nullptr) {
    res.push_back(&GetTestFS);
  }
  return res;
}

}  // anonymous namespace

INSTANTIATE_TEST_CASE_P(CustomEnv, EnvBasicTestWithParam,
                        ::testing::ValuesIn(GetCustomEnvs()));

INSTANTIATE_TEST_CASE_P(CustomEnv, EnvMoreTestWithParam,
                        ::testing::ValuesIn(GetCustomEnvs()));

TEST_P(EnvBasicTestWithParam, Basics) {
  uint64_t file_size;
  std::unique_ptr<WritableFile> writable_file;
  std::vector<std::string> children;

  // Check that the directory is empty.
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/non_existent"));
  ASSERT_TRUE(!env_->GetFileSize(test_dir_ + "/non_existent", &file_size).ok());
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(0U, children.size());

  // Create a file.
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Check that the file exists.
  ASSERT_OK(env_->FileExists(test_dir_ + "/f"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/f", &file_size));
  ASSERT_EQ(0U, file_size);
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(1U, children.size());
  ASSERT_EQ("f", children[0]);
  ASSERT_OK(env_->DeleteFile(test_dir_ + "/f"));

  // Write to the file.
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/f1", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("abc"));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/f2", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Check for expected size.
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/f1", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that renaming works.
  ASSERT_TRUE(
      !env_->RenameFile(test_dir_ + "/non_existent", test_dir_ + "/g").ok());
  ASSERT_OK(env_->RenameFile(test_dir_ + "/f1", test_dir_ + "/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/f1"));
  ASSERT_OK(env_->FileExists(test_dir_ + "/g"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/g", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that renaming overwriting works
  ASSERT_OK(env_->RenameFile(test_dir_ + "/f2", test_dir_ + "/g"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/g", &file_size));
  ASSERT_EQ(0U, file_size);

  // Check that opening non-existent file fails.
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  ASSERT_TRUE(!env_->NewSequentialFile(test_dir_ + "/non_existent", &seq_file,
                                       soptions_)
                   .ok());
  ASSERT_TRUE(!seq_file);
  ASSERT_NOK(env_->NewRandomAccessFile(test_dir_ + "/non_existent", &rand_file,
                                       soptions_));
  ASSERT_TRUE(!rand_file);

  // Check that deleting works.
  ASSERT_NOK(env_->DeleteFile(test_dir_ + "/non_existent"));
  ASSERT_OK(env_->DeleteFile(test_dir_ + "/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/g"));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(0U, children.size());
  Status s = env_->GetChildren(test_dir_ + "/non_existent", &children);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_P(EnvBasicTestWithParam, ReadWrite) {
  std::unique_ptr<WritableFile> writable_file;
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  Slice result;
  char scratch[100];

  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("hello "));
  ASSERT_OK(writable_file->Append("world"));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Read sequentially.
  ASSERT_OK(env_->NewSequentialFile(test_dir_ + "/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(seq_file->Skip(1));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Try reading past EOF.
  ASSERT_EQ(0U, result.size());
  ASSERT_OK(seq_file->Skip(100));  // Try to skip past end of file.
  ASSERT_OK(seq_file->Read(1000, &result, scratch));
  ASSERT_EQ(0U, result.size());

  // Random reads.
  ASSERT_OK(env_->NewRandomAccessFile(test_dir_ + "/f", &rand_file, soptions_));
  ASSERT_OK(rand_file->Read(6, 5, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(rand_file->Read(0, 5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(rand_file->Read(10, 100, &result, scratch));  // Read "d".
  ASSERT_EQ(0, result.compare("d"));

  // Too high offset.
  ASSERT_TRUE(rand_file->Read(1000, 5, &result, scratch).ok());
}

TEST_P(EnvBasicTestWithParam, Misc) {
  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/b", &writable_file, soptions_));

  // These are no-ops, but we test they return success.
  ASSERT_OK(writable_file->Sync());
  ASSERT_OK(writable_file->Flush());
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
}

TEST_P(EnvBasicTestWithParam, LargeWrite) {
  const size_t kWriteSize = 300 * 1024;
  char* scratch = new char[kWriteSize * 2];

  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, static_cast<char>(i));
  }

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("foo"));
  ASSERT_OK(writable_file->Append(write_data));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  std::unique_ptr<SequentialFile> seq_file;
  Slice result;
  ASSERT_OK(env_->NewSequentialFile(test_dir_ + "/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(3, &result, scratch));  // Read "foo".
  ASSERT_EQ(0, result.compare("foo"));

  size_t read = 0;
  std::string read_data;
  while (read < kWriteSize) {
    ASSERT_OK(seq_file->Read(kWriteSize - read, &result, scratch));
    read_data.append(result.data(), result.size());
    read += result.size();
  }
  ASSERT_TRUE(write_data == read_data);
  delete[] scratch;
}

TEST_P(EnvMoreTestWithParam, GetModTime) {
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/dir1"));
  uint64_t mtime1 = 0x0;
  ASSERT_OK(env_->GetFileModificationTime(test_dir_ + "/dir1", &mtime1));
}

TEST_P(EnvMoreTestWithParam, MakeDir) {
  ASSERT_OK(env_->CreateDir(test_dir_ + "/j"));
  ASSERT_OK(env_->FileExists(test_dir_ + "/j"));
  std::vector<std::string> children;
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(1U, children.size());
  // fail because file already exists
  ASSERT_TRUE(!env_->CreateDir(test_dir_ + "/j").ok());
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/j"));
  ASSERT_OK(env_->DeleteDir(test_dir_ + "/j"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/j"));
}

TEST_P(EnvMoreTestWithParam, GetChildren) {
  // empty folder returns empty vector
  std::vector<std::string> children;
  std::vector<Env::FileAttributes> childAttr;
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_OK(env_->FileExists(test_dir_));
  ASSERT_OK(env_->GetChildrenFileAttributes(test_dir_, &childAttr));
  ASSERT_EQ(0U, children.size());
  ASSERT_EQ(0U, childAttr.size());

  // folder with contents returns relative path to test dir
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/niu"));
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/you"));
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/guo"));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_OK(env_->GetChildrenFileAttributes(test_dir_, &childAttr));
  ASSERT_EQ(3U, children.size());
  ASSERT_EQ(3U, childAttr.size());
  for (const auto& each : children) {
    env_->DeleteDir(test_dir_ + "/" + each).PermitUncheckedError();
  }  // necessary for default POSIX env

  // non-exist directory returns IOError
  ASSERT_OK(env_->DeleteDir(test_dir_));
  ASSERT_NOK(env_->FileExists(test_dir_));
  ASSERT_NOK(env_->GetChildren(test_dir_, &children));
  ASSERT_NOK(env_->GetChildrenFileAttributes(test_dir_, &childAttr));

  // if dir is a file, returns IOError
  ASSERT_OK(env_->CreateDir(test_dir_));
  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/file", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
  ASSERT_NOK(env_->GetChildren(test_dir_ + "/file", &children));
  ASSERT_EQ(0U, children.size());
}

TEST_P(EnvMoreTestWithParam, GetChildrenIgnoresDotAndDotDot) {
  auto* env = Env::Default();
  ASSERT_OK(env->CreateDirIfMissing(test_dir_));

  // Create a single file
  std::string path = test_dir_;
  const EnvOptions soptions;
#ifdef OS_WIN
  path.append("\\test_file");
#else
  path.append("/test_file");
#endif
  std::string data("test data");
  std::unique_ptr<WritableFile> file;
  ASSERT_OK(env->NewWritableFile(path, &file, soptions));
  ASSERT_OK(file->Append("test data"));

  // get the children
  std::vector<std::string> result;
  ASSERT_OK(env->GetChildren(test_dir_, &result));

  // expect only one file named `test_data`, i.e. no `.` or `..` names
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result.at(0), "test_file");
}

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
