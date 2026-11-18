//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/random_access_file_reader.h"

#include <algorithm>
#include <stdexcept>

#include "file/file_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/file_system.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

#if defined(USE_COROUTINES)
#include "folly/Executor.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Task.h"
#include "folly/executors/IOThreadPoolExecutor.h"
#include "folly/synchronization/Baton.h"
#include "rocksdb/statistics.h"
#endif

namespace ROCKSDB_NAMESPACE {

class RandomAccessFileReaderTest : public testing::Test {
 public:
  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    test_dir_ = test::PerThreadDBPath("random_access_file_reader_test");
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
  }

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  void Write(const std::string& fname, const std::string& content) {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs_->NewWritableFile(Path(fname), FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append(content, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  void Read(const std::string& fname, const FileOptions& opts,
            std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string fpath = Path(fname);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(fpath, opts, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), fpath,
                                             env_->GetSystemClock().get()));
  }

  void AssertResult(const std::string& content,
                    const std::vector<FSReadRequest>& reqs) {
    for (const auto& r : reqs) {
      ASSERT_OK(r.status);
      ASSERT_EQ(r.len, r.result.size());
      ASSERT_EQ(content.substr(r.offset, r.len), r.result.ToString());
    }
  }

 protected:
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string test_dir_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }
};

namespace {
// Wraps an FSRandomAccessFile to observe the FSReadRequest that
// RandomAccessFileReader::ReadAsync forwards to the FileSystem layer. It
// records whether a request with a non-empty length but a null scratch buffer
// was submitted. A null scratch means no backing buffer was provided for the
// read; in production this surfaces as an EFAULT from io_uring (a null iovec
// base), reported as "Req failed: Unknown error -14".
class ScratchObservingRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  ScratchObservingRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& target,
                                   bool* saw_null_scratch)
      : FSRandomAccessFileOwnerWrapper(std::move(target)),
        saw_null_scratch_(saw_null_scratch) {}

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** /*io_handle*/,
                     IOHandleDeleter* /*del_fn*/,
                     IODebugContext* dbg) override {
    if (req.len > 0 && req.scratch == nullptr) {
      *saw_null_scratch_ = true;
      // A real direct-IO read with a null scratch would fault; synthesize an
      // error completion instead so the test can observe the submission.
      req.result = Slice();
      req.status = IOStatus::IOError("null scratch buffer submitted");
    } else {
      // Complete synchronously so the test does not depend on io_uring runtime
      // availability. Mirrors FSRandomAccessFile::ReadAsync's default fallback.
      req.status =
          Read(req.offset, req.len, opts, &req.result, req.scratch, dbg);
    }
    cb(req, cb_arg);
    return IOStatus::OK();
  }

 private:
  bool* saw_null_scratch_;
};

}  // namespace

#if defined(USE_COROUTINES)
TEST_F(RandomAccessFileReaderTest, CountsCoroutineReadSyncFallback) {
  const std::string fname = "async-read-fallback";
  const std::string content = "async read fallback";
  Write(fname, content);

  const std::string fpath = Path(fname);
  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(fs_->NewRandomAccessFile(fpath, FileOptions(), &file, nullptr));

  auto statistics = CreateDBStatistics();
  RandomAccessFileReader reader(std::move(file), fpath,
                                env_->GetSystemClock().get(), nullptr,
                                statistics.get());
  std::string scratch(content.size(), '\0');
  FSReadRequest req;
  req.offset = 0;
  req.len = content.size();
  req.scratch = scratch.data();

  folly::IOThreadPoolExecutor executor(1);
  folly::EventBase* event_base = executor.getEventBase();
  ASSERT_NE(event_base, nullptr);
  folly::coro::blockingWait(folly::coro::co_withExecutor(
      folly::Executor::getKeepAliveToken(event_base),
      reader.ReadCoroutine(IOOptions(), &req, nullptr, nullptr)));

  ASSERT_OK(req.status);
  ASSERT_EQ(content, req.result.ToString());
  EXPECT_EQ(1, statistics->getTickerCount(FILE_SUBMIT_ASYNC_READ_FALLBACK));
}

TEST_F(RandomAccessFileReaderTest, SubmitReadAsyncDirectIOAligned) {
  fs_->SetReadIOExecutorThreads(1);
  auto* read_executor = fs_->GetReadExecutor();
  if (read_executor == nullptr) {
    ROCKSDB_GTEST_SKIP("FileSystem has no read executor");
    return;
  }
  auto* read_event_base = read_executor->getEventBase();
  ASSERT_NE(read_event_base, nullptr);

  const std::string fname = "submit-read-async";
  Random rand(0);
  const std::string content = rand.RandomString(2 * kDefaultPageSize);
  Write(fname, content);

  FileOptions file_opts;
  file_opts.use_direct_reads = true;
  const std::string fpath = Path(fname);
  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(fs_->NewRandomAccessFile(fpath, file_opts, &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> reader(new RandomAccessFileReader(
      std::move(file), fpath, /*clock=*/nullptr,
      /*io_tracer=*/nullptr, /*stats=*/nullptr, Histograms::HISTOGRAM_ENUM_MAX,
      /*file_read_hist=*/nullptr, /*rate_limiter=*/nullptr,
      /*listeners=*/{}, Temperature::kUnknown, /*is_last_level=*/false,
      fs_.get()));

  const size_t page_size = reader->GetRequiredBufferAlignment();
  AlignedBuffer scratch;
  scratch.Alignment(page_size);
  scratch.AllocateNewBuffer(page_size);
  FSReadRequest req;
  req.offset = 0;
  req.len = page_size;
  req.scratch = scratch.BufferStart();

  bool underlying_submit_called = false;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadCoroutine:SubmitReadAsync", [&](void* arg) {
        underlying_submit_called = true;
        const auto* submitted_req = static_cast<FSReadRequest*>(arg);
        EXPECT_EQ(submitted_req->offset, req.offset);
        EXPECT_EQ(submitted_req->len, req.len);
        EXPECT_EQ(submitted_req->scratch, req.scratch);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  folly::Baton<> completion;
  size_t callback_count = 0;
  FSReadRequest* completed_req = nullptr;
  bool submitted_async = false;
  const IOOptions io_options;
  read_event_base->runInEventBaseThreadAndWait([&] {
    submitted_async = reader->SubmitReadAsync(
        req, io_options,
        [&](FSReadRequest& completed) {
          ++callback_count;
          completed_req = &completed;
          completion.post();
        },
        /*dbg=*/nullptr);
  });
  completion.wait();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(submitted_async);
  ASSERT_TRUE(underlying_submit_called);
  ASSERT_EQ(callback_count, 1);
  ASSERT_EQ(completed_req, &req);
  ASSERT_OK(req.status);
  ASSERT_EQ(req.result.data(), req.scratch);
  ASSERT_EQ(req.result, Slice(content.data(), req.len));
}

namespace {
// Accepts the first SubmitReadAsync and holds its callback, then throws on the
// second, like a file system whose client throws while issuing a read.
class ThrowOnSecondSubmitFile : public FSRandomAccessFileOwnerWrapper {
 public:
  explicit ThrowOnSecondSubmitFile(std::unique_ptr<FSRandomAccessFile>&& target)
      : FSRandomAccessFileOwnerWrapper(std::move(target)) {}

  bool SubmitReadAsync(FSReadRequest& req, const IOOptions& /*opts*/,
                       std::function<void(FSReadRequest&)> cb,
                       IODebugContext* /*dbg*/) override {
    if (!held_cb_) {
      held_req_ = &req;
      held_cb_ = std::move(cb);
      return true;
    }
    second_submit_threw_.post();
    throw std::runtime_error("submit failed");
  }

  void WaitForSecondSubmitToThrow() { second_submit_threw_.wait(); }

  void CompleteHeldRead() {
    held_req_->status = IOStatus::OK();
    held_req_->result = Slice();
    held_cb_(*held_req_);
  }

 private:
  FSReadRequest* held_req_ = nullptr;
  std::function<void(FSReadRequest&)> held_cb_;
  folly::Baton<> second_submit_threw_;
};
}  // namespace

TEST_F(RandomAccessFileReaderTest,
       MultiReadCoroutineWaitsForAcceptedReadsWhenASubmitThrows) {
  const std::string fname = "submit-throws";
  Write(fname, "0123456789");
  const std::string fpath = Path(fname);
  std::unique_ptr<FSRandomAccessFile> target;
  ASSERT_OK(fs_->NewRandomAccessFile(fpath, FileOptions(), &target, nullptr));
  auto file = std::make_unique<ThrowOnSecondSubmitFile>(std::move(target));
  ThrowOnSecondSubmitFile* throwing_file = file.get();
  RandomAccessFileReader reader(std::move(file), fpath,
                                env_->GetSystemClock().get());

  std::string scratch(4, '\0');
  std::vector<FSReadRequest> reqs(2);
  reqs[0].offset = 0;
  reqs[0].len = 2;
  reqs[0].scratch = scratch.data();
  reqs[1].offset = 5;
  reqs[1].len = 2;
  reqs[1].scratch = scratch.data() + 2;

  const IOOptions io_options;
  folly::IOThreadPoolExecutor executor(1);
  folly::EventBase* event_base = executor.getEventBase();
  ASSERT_NE(event_base, nullptr);
  auto multi_read =
      folly::coro::co_withExecutor(
          folly::Executor::getKeepAliveToken(event_base),
          reader.MultiReadCoroutine(io_options, reqs.data(), reqs.size(),
                                    nullptr, nullptr))
          .start();

  // The read must still be waiting for the accepted first request.
  throwing_file->WaitForSecondSubmitToThrow();
  event_base->runInEventBaseThreadAndWait([] {});
  const bool ended_with_read_in_flight = multi_read.isReady();
  EXPECT_FALSE(ended_with_read_in_flight);

  // Once that request completes, the read ends with the submit's exception.
  if (!ended_with_read_in_flight) {
    throwing_file->CompleteHeldRead();
  }
  EXPECT_THROW(std::move(multi_read).get(), std::runtime_error);
}

#endif  // USE_COROUTINES

// Skip the following tests in lite mode since direct I/O is unsupported.

TEST_F(RandomAccessFileReaderTest, ReadDirectIO) {
  std::string fname = "read-direct-io";
  Random rand(0);
  std::string content = rand.RandomString(kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();
  size_t offset = page_size / 2;
  size_t len = page_size / 3;
  Slice result;
  for (Env::IOPriority rate_limiter_priority : {Env::IO_LOW, Env::IO_TOTAL}) {
    IOOptions io_opts;
    io_opts.rate_limiter_priority = rate_limiter_priority;
    AlignedBuffer direct_io_buffer;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    FSReadRequest read_req;
    read_req.offset = offset;
    read_req.len = len;
    read_req.scratch = nullptr;
    r->Read(io_opts, &read_req, &direct_io_context, /*dbg=*/nullptr);
    ASSERT_OK(read_req.status);
    result = read_req.result;
    ASSERT_EQ(result.ToString(), content.substr(offset, len));
  }
}

TEST_F(RandomAccessFileReaderTest, ReadDirectIOCopiesToScratch) {
  std::string fname = "read-direct-io-copies-to-scratch";
  Random rand(0);
  std::string content = rand.RandomString(kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();
  size_t offset = page_size / 2;
  size_t len = page_size / 3;
  std::string scratch(len, '\0');
  Slice result;
  ASSERT_OK(r->Read(offset, len, IOOptions(), &result, scratch.data(),
                    /*dbg=*/nullptr));
  ASSERT_EQ(result.data(), scratch.data());
  ASSERT_EQ(result.ToString(), content.substr(offset, len));
}

TEST_F(RandomAccessFileReaderTest, ReadDirectIOUsesExternalBuffer) {
  std::string fname = "read-direct-io-external-buffer";
  Random rand(0);
  std::string content = rand.RandomString(kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();
  const size_t offset = page_size / 4;
  const size_t len = page_size / 2;

  AlignedBuffer external_storage;
  int allocations = 0;
  size_t requested_size = 0;
  size_t requested_alignment = 0;
  AlignedBuffer::Allocator allocator =
      [&](size_t size, size_t alignment,
          AlignedBuffer::ExternalAllocation* out) {
        ++allocations;
        requested_size = size;
        requested_alignment = alignment;
        external_storage.Alignment(alignment);
        external_storage.AllocateNewBuffer(size);
        out->data = external_storage.BufferStart();
        out->size = external_storage.Capacity();
        out->owner =
            FSAllocationPtr(external_storage.BufferStart(), [](void*) {});
        return Status::OK();
      };

  AlignedBuffer direct_io_buffer;
  AlignedBufferAllocationContext direct_io_context{&direct_io_buffer,
                                                   &allocator};
  FSReadRequest read_req;
  read_req.offset = offset;
  read_req.len = len;
  read_req.scratch = nullptr;
  Slice result;
  r->Read(IOOptions(), &read_req, &direct_io_context, /*dbg=*/nullptr);
  ASSERT_OK(read_req.status);
  result = read_req.result;
  ASSERT_EQ(result.ToString(), content.substr(offset, len));

  ASSERT_EQ(allocations, 1);
  ASSERT_EQ(requested_alignment, page_size);
  ASSERT_EQ(requested_size, page_size);
  ASSERT_EQ(direct_io_buffer.BufferStart(), external_storage.BufferStart());
  ASSERT_EQ(result.data(), external_storage.BufferStart() + offset);
}

TEST_F(RandomAccessFileReaderTest, MultiReadDirectIO) {
  std::vector<FSReadRequest> aligned_reqs;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::MultiRead:AlignedReqs", [&](void* reqs) {
        // Copy reqs, since it's allocated on stack inside MultiRead, which will
        // be deallocated after MultiRead returns.
        size_t i = 0;
        aligned_reqs.resize(
            (*reinterpret_cast<std::vector<FSReadRequest>*>(reqs)).size());
        for (auto& req :
             (*reinterpret_cast<std::vector<FSReadRequest>*>(reqs))) {
          aligned_reqs[i].offset = req.offset;
          aligned_reqs[i].len = req.len;
          aligned_reqs[i].result = req.result;
          aligned_reqs[i].status = req.status;
          aligned_reqs[i].scratch = req.scratch;
          i++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Creates a file with 3 pages.
  std::string fname = "multi-read-direct-io";
  Random rand(0);
  std::string content = rand.RandomString(3 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();

  {
    // Reads 2 blocks in the 1st page.
    // The results should be SharedSlices of the same underlying buffer.
    //
    // Illustration (each x is a 1/4 page)
    // First page: xxxx
    // 1st block:  x
    // 2nd block:    xx
    FSReadRequest r0;
    r0.offset = 0;
    r0.len = page_size / 4;
    r0.scratch = nullptr;

    FSReadRequest r1;
    r1.offset = page_size / 2;
    r1.len = page_size / 2;
    r1.scratch = nullptr;

    std::vector<FSReadRequest> reqs;
    reqs.push_back(std::move(r0));
    reqs.push_back(std::move(r1));
    AlignedBuffer direct_io_buffer;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    IODebugContext dbg;
    ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(),
                           &direct_io_context, &dbg));

    AssertResult(content, reqs);

    // Reads the first page internally.
    ASSERT_EQ(aligned_reqs.size(), 1);
    const FSReadRequest& aligned_r = aligned_reqs[0];
    ASSERT_OK(aligned_r.status);
    ASSERT_EQ(aligned_r.offset, 0);
    ASSERT_EQ(aligned_r.len, page_size);
  }

  {
    // Reads 3 blocks:
    // 1st block in the 1st page;
    // 2nd block from the middle of the 1st page to the middle of the 2nd page;
    // 3rd block in the 2nd page.
    // The results should be SharedSlices of the same underlying buffer.
    //
    // Illustration (each x is a 1/4 page)
    // 2 pages:   xxxxxxxx
    // 1st block: x
    // 2nd block:   xxxx
    // 3rd block:        x
    FSReadRequest r0;
    r0.offset = 0;
    r0.len = page_size / 4;
    r0.scratch = nullptr;

    FSReadRequest r1;
    r1.offset = page_size / 2;
    r1.len = page_size;
    r1.scratch = nullptr;

    FSReadRequest r2;
    r2.offset = 2 * page_size - page_size / 4;
    r2.len = page_size / 4;
    r2.scratch = nullptr;

    std::vector<FSReadRequest> reqs;
    reqs.push_back(std::move(r0));
    reqs.push_back(std::move(r1));
    reqs.push_back(std::move(r2));
    AlignedBuffer direct_io_buffer;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    IODebugContext dbg;
    ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(),
                           &direct_io_context, &dbg));

    AssertResult(content, reqs);

    // Reads the first two pages in one request internally.
    ASSERT_EQ(aligned_reqs.size(), 1);
    const FSReadRequest& aligned_r = aligned_reqs[0];
    ASSERT_OK(aligned_r.status);
    ASSERT_EQ(aligned_r.offset, 0);
    ASSERT_EQ(aligned_r.len, 2 * page_size);
  }

  {
    // Reads 3 blocks:
    // 1st block in the middle of the 1st page;
    // 2nd block in the middle of the 2nd page;
    // 3rd block in the middle of the 3rd page.
    // The results should be SharedSlices of the same underlying buffer.
    //
    // Illustration (each x is a 1/4 page)
    // 3 pages:   xxxxxxxxxxxx
    // 1st block:  xx
    // 2nd block:      xx
    // 3rd block:          xx
    FSReadRequest r0;
    r0.offset = page_size / 4;
    r0.len = page_size / 2;
    r0.scratch = nullptr;

    FSReadRequest r1;
    r1.offset = page_size + page_size / 4;
    r1.len = page_size / 2;
    r1.scratch = nullptr;

    FSReadRequest r2;
    r2.offset = 2 * page_size + page_size / 4;
    r2.len = page_size / 2;
    r2.scratch = nullptr;

    std::vector<FSReadRequest> reqs;
    reqs.push_back(std::move(r0));
    reqs.push_back(std::move(r1));
    reqs.push_back(std::move(r2));
    AlignedBuffer direct_io_buffer;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    IODebugContext dbg;
    ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(),
                           &direct_io_context, &dbg));

    AssertResult(content, reqs);

    // Reads the first 3 pages in one request internally.
    ASSERT_EQ(aligned_reqs.size(), 1);
    const FSReadRequest& aligned_r = aligned_reqs[0];
    ASSERT_OK(aligned_r.status);
    ASSERT_EQ(aligned_r.offset, 0);
    ASSERT_EQ(aligned_r.len, 3 * page_size);
  }

  {
    // Reads 2 blocks:
    // 1st block in the middle of the 1st page;
    // 2nd block in the middle of the 3rd page.
    // The results are two different buffers.
    //
    // Illustration (each x is a 1/4 page)
    // 3 pages:   xxxxxxxxxxxx
    // 1st block:  xx
    // 2nd block:          xx
    FSReadRequest r0;
    r0.offset = page_size / 4;
    r0.len = page_size / 2;
    r0.scratch = nullptr;

    FSReadRequest r1;
    r1.offset = 2 * page_size + page_size / 4;
    r1.len = page_size / 2;
    r1.scratch = nullptr;

    std::vector<FSReadRequest> reqs;
    reqs.push_back(std::move(r0));
    reqs.push_back(std::move(r1));
    AlignedBuffer direct_io_buffer;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    IODebugContext dbg;
    ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(),
                           &direct_io_context, &dbg));

    AssertResult(content, reqs);

    // Reads the 1st and 3rd pages in two requests internally.
    ASSERT_EQ(aligned_reqs.size(), 2);
    const FSReadRequest& aligned_r0 = aligned_reqs[0];
    const FSReadRequest& aligned_r1 = aligned_reqs[1];
    ASSERT_OK(aligned_r0.status);
    ASSERT_EQ(aligned_r0.offset, 0);
    ASSERT_EQ(aligned_r0.len, page_size);
    ASSERT_OK(aligned_r1.status);
    ASSERT_EQ(aligned_r1.offset, 2 * page_size);
    ASSERT_EQ(aligned_r1.len, page_size);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(RandomAccessFileReaderTest, MultiReadDirectIOUsesExternalBuffer) {
  std::string fname = "multi-read-direct-io-external-buffer";
  Random rand(0);
  std::string content = rand.RandomString(3 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();
  FSReadRequest r0;
  r0.offset = page_size / 4;
  r0.len = page_size / 2;
  r0.scratch = nullptr;

  FSReadRequest r1;
  r1.offset = 2 * page_size + page_size / 4;
  r1.len = page_size / 2;
  r1.scratch = nullptr;

  std::vector<FSReadRequest> reqs;
  reqs.push_back(std::move(r0));
  reqs.push_back(std::move(r1));

  AlignedBuffer external_storage;
  int allocations = 0;
  size_t requested_size = 0;
  size_t requested_alignment = 0;
  AlignedBuffer::Allocator allocator =
      [&](size_t size, size_t alignment,
          AlignedBuffer::ExternalAllocation* out) {
        ++allocations;
        requested_size = size;
        requested_alignment = alignment;
        external_storage.Alignment(alignment);
        external_storage.AllocateNewBuffer(size);
        out->data = external_storage.BufferStart();
        out->size = external_storage.Capacity();
        out->owner =
            FSAllocationPtr(external_storage.BufferStart(), [](void*) {});
        return Status::OK();
      };

  AlignedBuffer direct_io_buffer;
  AlignedBufferAllocationContext direct_io_context{&direct_io_buffer,
                                                   &allocator};
  ASSERT_OK(r->MultiRead(IOOptions(), reqs.data(), reqs.size(),
                         &direct_io_context, /*dbg=*/nullptr));
  AssertResult(content, reqs);

  ASSERT_EQ(allocations, 1);
  ASSERT_EQ(requested_alignment, page_size);
  ASSERT_EQ(requested_size, 2 * page_size);
  ASSERT_EQ(direct_io_buffer.BufferStart(), external_storage.BufferStart());
  const char* storage_begin = external_storage.BufferStart();
  const char* storage_end = storage_begin + external_storage.Capacity();
  for (const auto& req : reqs) {
    ASSERT_GE(req.result.data(), storage_begin);
    ASSERT_LE(req.result.data() + req.result.size(), storage_end);
  }
}

TEST_F(RandomAccessFileReaderTest, MultiReadDirectIOWithoutExternalBuffer) {
  const std::string fname = "multi-read-direct-io-fs-interface";
  Random rand(0);
  const std::string content = rand.RandomString(3 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> reader;
  Read(fname, opts, &reader);
  ASSERT_TRUE(reader->use_direct_io());

  const size_t page_size = reader->GetRequiredBufferAlignment();
  std::string scratch0(page_size / 2, '\0');
  std::string scratch1(page_size / 2, '\0');
  std::vector<FSReadRequest> reqs(2);
  reqs[0].offset = page_size / 4;
  reqs[0].len = scratch0.size();
  reqs[0].scratch = scratch0.data();
  reqs[1].offset = 2 * page_size + page_size / 4;
  reqs[1].len = scratch1.size();
  reqs[1].scratch = scratch1.data();

  FSRandomAccessFile* fs_reader = reader.get();
  ASSERT_OK(fs_reader->MultiRead(reqs.data(), reqs.size(), IOOptions(),
                                 /*dbg=*/nullptr));
  AssertResult(content, reqs);
  ASSERT_EQ(reqs[0].result.data(), reqs[0].scratch);
  ASSERT_EQ(reqs[1].result.data(), reqs[1].scratch);
}

TEST_F(RandomAccessFileReaderTest,
       MultiReadDirectIOSortsUnorderedUnalignedRequests) {
  const std::string fname = "multi-read-direct-io-unordered-unaligned";
  Random rand(0);
  const std::string content = rand.RandomString(3 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> reader;
  Read(fname, opts, &reader);
  ASSERT_TRUE(reader->use_direct_io());

  const size_t page_size = reader->GetRequiredBufferAlignment();
  std::string high_scratch(page_size / 2, '\0');
  std::string low_scratch(page_size / 2, '\0');
  std::vector<FSReadRequest> reqs(2);
  reqs[0].offset = 2 * page_size + page_size / 4;
  reqs[0].len = high_scratch.size();
  reqs[0].scratch = high_scratch.data();
  reqs[1].offset = page_size / 4;
  reqs[1].len = low_scratch.size();
  reqs[1].scratch = low_scratch.data();

  FSRandomAccessFile* fs_reader = reader.get();
  ASSERT_OK(fs_reader->MultiRead(reqs.data(), reqs.size(), IOOptions(),
                                 /*dbg=*/nullptr));
  AssertResult(content, reqs);
  ASSERT_EQ(reqs[0].result.data(), reqs[0].scratch);
  ASSERT_EQ(reqs[1].result.data(), reqs[1].scratch);
}

TEST_F(RandomAccessFileReaderTest,
       MultiReadDirectIOUsesUnorderedAlignedCallerBuffers) {
  const std::string fname = "multi-read-direct-io-aligned-caller-buffers";
  Random rand(0);
  const std::string content = rand.RandomString(3 * kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::unique_ptr<RandomAccessFileReader> reader;
  Read(fname, opts, &reader);
  ASSERT_TRUE(reader->use_direct_io());

  const size_t page_size = reader->GetRequiredBufferAlignment();

  AlignedBuffer scratch0;
  scratch0.Alignment(page_size);
  scratch0.AllocateNewBuffer(page_size);
  AlignedBuffer scratch1;
  scratch1.Alignment(page_size);
  scratch1.AllocateNewBuffer(page_size);

  std::vector<FSReadRequest> reqs(2);
  reqs[0].offset = 2 * page_size;
  reqs[0].len = page_size;
  reqs[0].scratch = scratch0.BufferStart();
  reqs[1].offset = 0;
  reqs[1].len = page_size;
  reqs[1].scratch = scratch1.BufferStart();

  FSRandomAccessFile* fs_reader = reader.get();
  ASSERT_OK(fs_reader->MultiRead(reqs.data(), reqs.size(), IOOptions(),
                                 /*dbg=*/nullptr));
  AssertResult(content, reqs);
  ASSERT_EQ(reqs[0].result.data(), reqs[0].scratch);
  ASSERT_EQ(reqs[1].result.data(), reqs[1].scratch);
}

// Regression test for a direct-IO async-read buffer bug. When a caller submits
// an already-aligned FSReadRequest with a null scratch and provides an
// `aligned_buf` out-parameter for the reader to allocate the backing buffer
// into (exactly how IODispatcher submits MultiScan async reads for plain direct
// IO), RandomAccessFileReader::ReadAsync must NOT take its "already aligned"
// fast path and forward the null scratch to the FileSystem. Doing so hands
// io_uring a null iovec base, which fails with EFAULT ("Req failed: Unknown
// error -14").
TEST_F(RandomAccessFileReaderTest, ReadAsyncDirectIOAlignedNullScratch) {
  std::string fname = "read-async-direct-io-aligned-null-scratch";
  Random rand(0);
  std::string content = rand.RandomString(kDefaultPageSize);
  Write(fname, content);

  FileOptions opts;
  opts.use_direct_reads = true;
  std::string fpath = Path(fname);
  std::unique_ptr<FSRandomAccessFile> f;
  ASSERT_OK(fs_->NewRandomAccessFile(fpath, opts, &f, nullptr));

  bool saw_null_scratch = false;
  std::unique_ptr<FSRandomAccessFile> wrapped(
      new ScratchObservingRandomAccessFile(std::move(f), &saw_null_scratch));
  std::unique_ptr<RandomAccessFileReader> r(new RandomAccessFileReader(
      std::move(wrapped), fpath, env_->GetSystemClock().get()));
  ASSERT_TRUE(r->use_direct_io());

  const size_t page_size = r->file()->GetRequiredBufferAlignment();

  // Offset and length are both alignment-aligned, so the reader would take its
  // "already aligned" fast path. scratch is null and an aligned_buf is
  // provided, so the reader is expected to allocate the backing buffer itself.
  FSReadRequest req;
  req.offset = 0;
  req.len = page_size;
  req.scratch = nullptr;

  AlignedBuf aligned_buf;
  bool completed = false;
  IOStatus completed_status;
  Slice completed_result;
  auto cb = [&](FSReadRequest& done, void* /*arg*/) {
    completed = true;
    completed_status = done.status;
    completed_result = done.result;
  };

  void* io_handle = nullptr;
  IOHandleDeleter del_fn = nullptr;
  ASSERT_OK(r->ReadAsync(req, IOOptions(), cb, /*cb_arg=*/nullptr, &io_handle,
                         &del_fn, &aligned_buf, /*dbg=*/nullptr,
                         /*direct_io_buffer_context=*/nullptr));
  // The read result is delivered via the callback (checked below); the reader
  // works on an internal copy and leaves this request's status untouched.
  req.status.PermitUncheckedError();

  if (io_handle != nullptr && del_fn != nullptr) {
    std::vector<void*> handles{io_handle};
    ASSERT_OK(fs_->Poll(handles, handles.size()));
    del_fn(io_handle);
  }

  ASSERT_TRUE(completed);
  // Core assertion: the reader must allocate a backing buffer for the aligned
  // direct-IO async read rather than forwarding a null scratch to the FS.
  EXPECT_FALSE(saw_null_scratch)
      << "ReadAsync forwarded a null scratch on the aligned direct-IO fast "
         "path; io_uring would fail this read with EFAULT";
  ASSERT_OK(completed_status);
  ASSERT_EQ(completed_result.size(), req.len);
  ASSERT_EQ(completed_result.ToString(), content.substr(0, req.len));
}

TEST(FSReadRequest, Align) {
  FSReadRequest r;
  r.offset = 2000;
  r.len = 2000;
  r.scratch = nullptr;
  ASSERT_OK(r.status);

  FSReadRequest aligned_r = Align(r, 1024);
  ASSERT_OK(r.status);
  ASSERT_OK(aligned_r.status);
  ASSERT_EQ(aligned_r.offset, 1024);
  ASSERT_EQ(aligned_r.len, 3072);
}

TEST(FSReadRequest, TryMerge) {
  // reverse means merging dest into src.
  for (bool reverse : {true, false}) {
    {
      // dest: [ ]
      //  src:      [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 15;
      src.len = 10;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_FALSE(TryMerge(&dest, src));
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [ ]
      //  src:   [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 10;
      src.len = 10;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 20);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [    ]
      //  src:   [    ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 5;
      src.len = 10;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 15);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [    ]
      //  src:   [  ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 5;
      src.len = 5;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 10);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [     ]
      //  src:   [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 5;
      src.len = 1;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 10);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [ ]
      //  src: [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 0;
      src.len = 10;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 10);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }

    {
      // dest: [   ]
      //  src: [ ]
      FSReadRequest dest;
      dest.offset = 0;
      dest.len = 10;
      dest.scratch = nullptr;
      ASSERT_OK(dest.status);

      FSReadRequest src;
      src.offset = 0;
      src.len = 5;
      src.scratch = nullptr;
      ASSERT_OK(src.status);

      if (reverse) {
        std::swap(dest, src);
      }
      ASSERT_TRUE(TryMerge(&dest, src));
      ASSERT_EQ(dest.offset, 0);
      ASSERT_EQ(dest.len, 10);
      ASSERT_OK(dest.status);
      ASSERT_OK(src.status);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
