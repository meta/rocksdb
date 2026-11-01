//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/io_dispatcher.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "options/options_helper.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/aligned_buffer.h"

// Enable io_uring support for this test
extern "C" bool RocksDbIOUringEnable() { return true; }

// Check if io_uring is available at compile time
#ifdef ROCKSDB_IOURING_PRESENT
static constexpr bool kIOUringPresent = true;
#else
static constexpr bool kIOUringPresent = false;
#endif

namespace ROCKSDB_NAMESPACE {

// Represents a single read operation recorded by the tracking file system
struct ReadOp {
  enum Type { kMultiRead, kReadAsync };
  Type type;
  // For MultiRead: contains all (offset, len) pairs in the request
  // For ReadAsync: contains a single (offset, len) pair
  std::vector<std::pair<uint64_t, size_t>> requests;
};

// Forward declaration
class ReadTrackingFS;

// Wrapper around FSRandomAccessFile that tracks read operations
class ReadTrackingRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  ReadTrackingRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& file,
                               ReadTrackingFS* fs)
      : FSRandomAccessFileOwnerWrapper(std::move(file)), fs_(fs) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;

 private:
  ReadTrackingFS* fs_;
};

// FileSystem wrapper that tracks all read operations for verification
class ReadTrackingFS : public FileSystemWrapper {
 public:
  explicit ReadTrackingFS(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  static const char* kClassName() { return "ReadTrackingFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    if (s.ok()) {
      result->reset(new ReadTrackingRandomAccessFile(std::move(file), this));
    }
    return s;
  }

  // Record a MultiRead operation
  void RecordMultiRead(const std::vector<std::pair<uint64_t, size_t>>& reqs) {
    std::lock_guard<std::mutex> lock(mutex_);
    ReadOp op;
    op.type = ReadOp::kMultiRead;
    op.requests = reqs;
    read_ops_.push_back(std::move(op));
  }

  // Record a ReadAsync operation
  void RecordReadAsync(uint64_t offset, size_t len) {
    std::lock_guard<std::mutex> lock(mutex_);
    ReadOp op;
    op.type = ReadOp::kReadAsync;
    op.requests.push_back({offset, len});
    read_ops_.push_back(std::move(op));
  }

  // Get all recorded read operations
  std::vector<ReadOp> GetReadOps() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return read_ops_;
  }

  // Clear recorded read operations
  void ClearReadOps() {
    std::lock_guard<std::mutex> lock(mutex_);
    read_ops_.clear();
  }

  // Get count of MultiRead operations
  size_t GetMultiReadCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (const auto& op : read_ops_) {
      if (op.type == ReadOp::kMultiRead) {
        count++;
      }
    }
    return count;
  }

  // Get count of ReadAsync operations
  size_t GetReadAsyncCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (const auto& op : read_ops_) {
      if (op.type == ReadOp::kReadAsync) {
        count++;
      }
    }
    return count;
  }

  // Simulates on-disk bit rot at `offset` by flipping a byte there once the
  // underlying read has already reported success. Off by default so the table
  // open inside CreateAndOpenSST still sees clean bytes.
  void CorruptReadsAtOffset(uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    corrupt_offset_ = offset;
    corrupt_reads_ = true;
  }

  // Makes successful reads covering `offset` report a short result ending at
  // that absolute file offset.
  void TruncateReadsAtOffset(uint64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    truncate_offset_ = offset;
    truncate_reads_ = true;
  }

  void MaybeCorruptRead(uint64_t offset, Slice* result) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (result != nullptr) {
      CorruptSpanLocked(offset, *result);
    }
  }

  void MaybeCorruptReads(FSReadRequest* reqs, size_t num_reqs) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t i = 0; i < num_reqs; i++) {
      if (reqs[i].status.ok()) {
        CorruptSpanLocked(reqs[i].offset, reqs[i].result);
      }
    }
  }

  void MaybeTruncateRead(uint64_t offset, Slice* result) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (result != nullptr) {
      TruncateSpanLocked(offset, result);
    }
  }

  void MaybeTruncateReads(FSReadRequest* reqs, size_t num_reqs) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t i = 0; i < num_reqs; i++) {
      if (reqs[i].status.ok()) {
        TruncateSpanLocked(reqs[i].offset, &reqs[i].result);
      }
    }
  }

  // Lets a test confirm the injection actually fired rather than silently
  // becoming a no-op if the read path stops matching.
  size_t GetCorruptHits() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return corrupt_hits_;
  }

  size_t GetTruncateHits() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return truncate_hits_;
  }

 private:
  // Neighbouring blocks get coalesced into one request, so locate the target
  // byte inside whichever span happens to cover it.
  void CorruptSpanLocked(uint64_t offset, const Slice& data) {
    if (!corrupt_reads_ || corrupt_offset_ < offset ||
        corrupt_offset_ >= offset + data.size()) {
      return;
    }
    const_cast<char*>(data.data())[corrupt_offset_ - offset] ^= 0xff;
    ++corrupt_hits_;
  }

  void TruncateSpanLocked(uint64_t offset, Slice* data) {
    if (!truncate_reads_ || truncate_offset_ < offset ||
        truncate_offset_ >= offset + data->size()) {
      return;
    }
    *data = Slice(data->data(), static_cast<size_t>(truncate_offset_ - offset));
    ++truncate_hits_;
  }

  mutable std::mutex mutex_;
  std::vector<ReadOp> read_ops_;
  bool corrupt_reads_ = false;
  uint64_t corrupt_offset_ = 0;
  size_t corrupt_hits_ = 0;
  bool truncate_reads_ = false;
  uint64_t truncate_offset_ = 0;
  size_t truncate_hits_ = 0;
};

// Deliberately does not record the operation the way MultiRead and ReadAsync
// do. This override exists only so fault injection covers a re-read through the
// plain Read API; `ReadOp` has no kRead type and every GetReadOps() consumer
// filters on kMultiRead or kReadAsync, so nothing would observe the entry.
// Recording would also put the tracking mutex on every read in a fixture shared
// by the whole file. Add a kRead type here if a test ever needs to see them.
IOStatus ReadTrackingRandomAccessFile::Read(uint64_t offset, size_t n,
                                            const IOOptions& options,
                                            Slice* result, char* scratch,
                                            IODebugContext* dbg) const {
  IOStatus s = target()->Read(offset, n, options, result, scratch, dbg);
  if (s.ok()) {
    fs_->MaybeCorruptRead(offset, result);
    fs_->MaybeTruncateRead(offset, result);
  }
  return s;
}

IOStatus ReadTrackingRandomAccessFile::MultiRead(FSReadRequest* reqs,
                                                 size_t num_reqs,
                                                 const IOOptions& options,
                                                 IODebugContext* dbg) {
  // Record the read operation before executing it
  std::vector<std::pair<uint64_t, size_t>> recorded_reqs;
  recorded_reqs.reserve(num_reqs);
  for (size_t i = 0; i < num_reqs; i++) {
    recorded_reqs.push_back({reqs[i].offset, reqs[i].len});
  }
  fs_->RecordMultiRead(recorded_reqs);

  // Delegate to underlying file
  IOStatus s = target()->MultiRead(reqs, num_reqs, options, dbg);
  if (s.ok()) {
    fs_->MaybeCorruptReads(reqs, num_reqs);
    fs_->MaybeTruncateReads(reqs, num_reqs);
  }
  return s;
}

IOStatus ReadTrackingRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* dbg) {
  // Record the read operation before executing it
  fs_->RecordReadAsync(req.offset, req.len);

  // Delegate to underlying file
  return target()->ReadAsync(req, opts, cb, cb_arg, io_handle, del_fn, dbg);
}

class ControlledAsyncFS;

// Test-only async handle used by ControlledAsyncFS. It stores the callback and
// original read parameters so the test filesystem can decide exactly when the
// async read completes.
struct ControlledAsyncHandle {
  FSRandomAccessFile* target = nullptr;
  IOOptions opts;
  uint64_t offset = 0;
  size_t len = 0;
  char* scratch = nullptr;
  std::function<void(FSReadRequest&, void*)> cb;
  void* cb_arg = nullptr;
};

// Wraps a real random-access file but defers ReadAsync() completion to
// ControlledAsyncFS. This keeps the read data realistic while making completion
// ordering deterministic.
class ControlledAsyncRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  ControlledAsyncRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& file,
                                  ControlledAsyncFS* fs)
      : FSRandomAccessFileOwnerWrapper(std::move(file)), fs_(fs) {}

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;

 private:
  ControlledAsyncFS* fs_;
};

// A scheduling wrapper around the real filesystem. ReadAsync() requests stay
// pending until Poll() or AbortIO() is called, letting tests model io_uring
// completing an unrelated request while waiting for a requested handle.
class ControlledAsyncFS : public ReadTrackingFS {
 public:
  explicit ControlledAsyncFS(const std::shared_ptr<FileSystem>& target)
      : ReadTrackingFS(target) {}

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    if (s.ok()) {
      result->reset(new ControlledAsyncRandomAccessFile(std::move(file), this));
    }
    return s;
  }

  IOStatus Poll(std::vector<void*>& io_handles,
                size_t /*min_completions*/) override {
    assert(!io_handles.empty());
    if (poll_delay_us_ > 0) {
      Env::Default()->SleepForMicroseconds(poll_delay_us_);
    }
    auto* requested_handle =
        static_cast<ControlledAsyncHandle*>(io_handles.front());

    // io_uring can reap completions for handles other than the one the caller
    // is waiting on. Complete a different pending handle first to exercise that
    // ordering deterministically.
    if (complete_stray_first_) {
      for (auto it = pending_handles_.begin(); it != pending_handles_.end();
           ++it) {
        if (*it != requested_handle) {
          ControlledAsyncHandle* stray_handle = *it;
          pending_handles_.erase(it);
          stray_completion_count_++;
          CompleteHandle(stray_handle);
          break;
        }
      }
    }

    auto requested_it = std::find(pending_handles_.begin(),
                                  pending_handles_.end(), requested_handle);
    if (requested_it != pending_handles_.end()) {
      pending_handles_.erase(requested_it);
      CompleteHandle(requested_handle);
    }

    return IOStatus::OK();
  }

  IOStatus AbortIO(std::vector<void*>& io_handles) override {
    for (void* io_handle : io_handles) {
      auto* handle = static_cast<ControlledAsyncHandle*>(io_handle);
      auto it =
          std::find(pending_handles_.begin(), pending_handles_.end(), handle);
      if (it == pending_handles_.end()) {
        continue;
      }
      pending_handles_.erase(it);
      abort_count_++;

      // Match the filesystem contract: aborting an in-flight read completes its
      // callback with an aborted status before the handle is deleted.
      FSReadRequest req;
      req.offset = handle->offset;
      req.len = handle->len;
      req.scratch = handle->scratch;
      req.status = IOStatus::Aborted();
      handle->cb(req, handle->cb_arg);
    }
    return IOStatus::OK();
  }

  void AddHandle(ControlledAsyncHandle* handle) {
    pending_handles_.push_back(handle);
  }

  void SetCompleteStrayFirst(bool complete_stray_first) {
    complete_stray_first_ = complete_stray_first;
  }

  void SetPollDelayMicros(int delay_us) { poll_delay_us_ = delay_us; }

  void CompletePendingHandles() {
    while (!pending_handles_.empty()) {
      ControlledAsyncHandle* handle = pending_handles_.back();
      pending_handles_.pop_back();
      CompleteHandle(handle);
    }
  }

  size_t abort_count() const { return abort_count_; }
  size_t stray_completion_count() const { return stray_completion_count_; }

 private:
  void CompleteHandle(ControlledAsyncHandle* handle) {
    FSReadRequest req;
    req.offset = handle->offset;
    req.len = handle->len;
    req.scratch = handle->scratch;
    // Use the wrapped file for the actual bytes; only scheduling is mocked.
    req.status =
        handle->target->Read(handle->offset, handle->len, handle->opts,
                             &req.result, handle->scratch, nullptr /*dbg*/);
    if (req.status.ok()) {
      MaybeCorruptRead(handle->offset, &req.result);
      MaybeTruncateRead(handle->offset, &req.result);
    }
    handle->cb(req, handle->cb_arg);
  }

  std::vector<ControlledAsyncHandle*> pending_handles_;
  bool complete_stray_first_ = true;
  int poll_delay_us_ = 0;
  size_t abort_count_ = 0;
  size_t stray_completion_count_ = 0;
};

IOStatus ControlledAsyncRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* /*dbg*/) {
  fs_->RecordReadAsync(req.offset, req.len);

  // Do not submit to the real filesystem here. The handle is queued until the
  // test explicitly completes it through ControlledAsyncFS::Poll() or
  // AbortIO().
  auto* handle = new ControlledAsyncHandle();
  handle->target = target();
  handle->opts = opts;
  handle->offset = req.offset;
  handle->len = req.len;
  handle->scratch = req.scratch;
  handle->cb = std::move(cb);
  handle->cb_arg = cb_arg;

  *io_handle = handle;
  *del_fn = [](void* arg) { delete static_cast<ControlledAsyncHandle*>(arg); };
  fs_->AddHandle(handle);
  return IOStatus::OK();
}

class IODispatcherTest : public DBTestBase {
 public:
  IODispatcherTest()
      : DBTestBase("io_dispatcher_test", /*env_do_fsync=*/false) {}

  ~IODispatcherTest() override {
    // Close any open tables
    for (auto& table : tables_) {
      table.reset();
    }
    tables_.clear();
  }

  // Helper to collect block handles from a table
  // We use TEST_GetDataBlockHandle to get handles for specific keys
  // Since we know the keys we inserted, we can collect their block handles
  Status CollectBlockHandles(BlockBasedTable* table, size_t num_keys,
                             std::vector<BlockHandle>* block_handles_out) {
    block_handles_out->clear();

    ReadOptions read_options;
    std::unordered_set<uint64_t> seen_offsets;

    // Iterate through all keys and get their block handles
    // We collect unique block handles (same block might contain multiple keys)
    IndexBlockIter iiter_on_stack;
    BlockCacheLookupContext context{TableReaderCaller::kUserVerifyChecksum};
    auto iiter = table->NewIndexIterator(read_options, false, &iiter_on_stack,
                                         nullptr, &context);
    std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    // Position the iterator at the first entry
    iiter->SeekToFirst();

    while (iiter->Valid()) {
      auto handle = iiter->value().handle;
      if (seen_offsets.find(handle.offset()) == seen_offsets.end()) {
        block_handles_out->push_back(handle);
        seen_offsets.insert(handle.offset());
        if (block_handles_out->size() >= num_keys) {
          break;
        }
      }
      iiter->Next();
    }

    return Status::OK();
  }

  std::string test_dir_{};
  Env* env_{};
  std::shared_ptr<FileSystem> base_fs_;
  std::shared_ptr<ReadTrackingFS> tracking_fs_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    test_dir_ = test::PerThreadDBPath("block_based_table_reader_test");
    env_ = Env::Default();
    base_fs_ = FileSystem::Default();
    tracking_fs_ = std::make_shared<ReadTrackingFS>(base_fs_);
    ASSERT_OK(base_fs_->CreateDir(test_dir_, IOOptions(), nullptr));
  }

  void TearDown() override {
    SetPerfLevel(kDisable);
    get_perf_context()->Reset();
    EXPECT_OK(DestroyDir(env_, test_dir_));
  }

  void NewFileWriter(const std::string& filename,
                     std::unique_ptr<WritableFileWriter>* writer) {
    std::string path = Path(filename);
    EnvOptions env_options;
    FileOptions foptions;
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(base_fs_->NewWritableFile(path, foptions, &file, nullptr));
    writer->reset(new WritableFileWriter(std::move(file), path, env_options));
  }

  void NewFileReader(const std::string& filename, const FileOptions& opt,
                     std::unique_ptr<RandomAccessFileReader>* reader,
                     Statistics* stats = nullptr) {
    std::string path = Path(filename);
    std::unique_ptr<FSRandomAccessFile> f;
    // Use tracking_fs_ to record read operations
    ASSERT_OK(tracking_fs_->NewRandomAccessFile(path, opt, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), path,
                                             env_->GetSystemClock().get(),
                                             /*io_tracer=*/nullptr,
                                             /*stats=*/stats));
  }

  std::vector<std::shared_ptr<Statistics>> all_stats_;
  std::vector<std::unique_ptr<BlockBasedTable>> tables_;

  // Options must be stored as member variables to avoid use-after-scope
  // The BlockBasedTable keeps references to these options
  std::vector<std::unique_ptr<ImmutableOptions>> all_ioptions_;
  std::vector<std::unique_ptr<EnvOptions>> all_env_options_;
  std::vector<std::unique_ptr<InternalKeyComparator>> all_comparators_;
  std::vector<std::shared_ptr<Statistics>> all_statistics_;

  class TestReadScopedBlockBufferProvider
      : public ReadScopedBlockBufferProvider {
   public:
    struct Allocation {
      TestReadScopedBlockBufferProvider* owner = nullptr;
      char* data = nullptr;
      size_t size = 0;
      size_t alignment = 1;
      AlignedBuffer storage;
    };

    struct AllocationRange {
      const char* data = nullptr;
      size_t size = 0;
    };

    Status Allocate(size_t size, size_t alignment, Lease* out) override {
      if (out == nullptr) {
        return Status::InvalidArgument("lease output is nullptr");
      }
      if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        return Status::InvalidArgument("alignment must be a power of two");
      }
      auto* allocation = new Allocation();
      allocation->owner = this;
      allocation->size = size;
      allocation->alignment = alignment;
      allocation->storage.Alignment(alignment);
      allocation->storage.AllocateNewBuffer(size);
      allocation->data = allocation->storage.BufferStart();
      bytes_outstanding_.fetch_add(size, std::memory_order_relaxed);
      allocations_.fetch_add(1, std::memory_order_relaxed);
      size_t current_alignment =
          max_requested_alignment_.load(std::memory_order_relaxed);
      while (current_alignment < alignment &&
             !max_requested_alignment_.compare_exchange_weak(
                 current_alignment, alignment, std::memory_order_relaxed,
                 std::memory_order_relaxed)) {
      }
      {
        std::lock_guard<std::mutex> lock(live_allocations_mutex_);
        live_allocations_.push_back(AllocationRange{allocation->data, size});
      }

      out->data = allocation->data;
      out->size = size;
      out->cleanup.Allocate();
      out->cleanup->RegisterCleanup(&ReleaseAllocation, allocation, nullptr);
      return Status::OK();
    }

    size_t allocations() const {
      return allocations_.load(std::memory_order_relaxed);
    }

    size_t bytes_outstanding() const {
      return bytes_outstanding_.load(std::memory_order_relaxed);
    }

    size_t max_requested_alignment() const {
      return max_requested_alignment_.load(std::memory_order_relaxed);
    }

    bool OwnsLiveAddressRange(const char* data, size_t size) const {
      if (data == nullptr) {
        return false;
      }
      const uintptr_t slice_begin = reinterpret_cast<uintptr_t>(data);
      const uintptr_t slice_end = slice_begin + size;

      std::lock_guard<std::mutex> lock(live_allocations_mutex_);
      for (const auto& allocation : live_allocations_) {
        const uintptr_t allocation_begin =
            reinterpret_cast<uintptr_t>(allocation.data);
        const uintptr_t allocation_end = allocation_begin + allocation.size;
        if (slice_begin >= allocation_begin && slice_end <= allocation_end) {
          return true;
        }
      }
      return false;
    }

   private:
    static void ReleaseAllocation(void* arg1, void* /*arg2*/) {
      auto* allocation = static_cast<Allocation*>(arg1);
      allocation->owner->bytes_outstanding_.fetch_sub(
          allocation->size, std::memory_order_relaxed);
      {
        std::lock_guard<std::mutex> lock(
            allocation->owner->live_allocations_mutex_);
        auto& live_allocations = allocation->owner->live_allocations_;
        for (auto it = live_allocations.begin(); it != live_allocations.end();
             ++it) {
          if (it->data == allocation->data) {
            live_allocations.erase(it);
            break;
          }
        }
      }
      delete allocation;
    }

    std::atomic<size_t> allocations_{0};
    std::atomic<size_t> bytes_outstanding_{0};
    std::atomic<size_t> max_requested_alignment_{1};
    mutable std::mutex live_allocations_mutex_;
    std::vector<AllocationRange> live_allocations_;
  };

  class InvalidReadScopedBlockBufferProvider
      : public ReadScopedBlockBufferProvider {
   public:
    enum class Mode {
      kReturnError,
      kNullData,
      kShortSize,
      kMissingCleanup,
    };

    explicit InvalidReadScopedBlockBufferProvider(Mode mode) : mode_(mode) {}
    ~InvalidReadScopedBlockBufferProvider() override {
      for (char* ptr : owned_buffers_) {
        delete[] ptr;
      }
    }

    Status Allocate(size_t size, size_t /*alignment*/, Lease* out) override {
      if (out == nullptr) {
        return Status::InvalidArgument("lease output is nullptr");
      }
      switch (mode_) {
        case Mode::kReturnError:
          return Status::MemoryLimit("injected allocate failure");
        case Mode::kNullData:
          out->size = size;
          out->cleanup.Allocate();
          return Status::OK();
        case Mode::kShortSize:
          out->data = new char[size];
          owned_buffers_.push_back(out->data);
          out->size = size - 1;
          out->cleanup.Allocate();
          return Status::OK();
        case Mode::kMissingCleanup:
          out->data = new char[size];
          owned_buffers_.push_back(out->data);
          out->size = size;
          return Status::OK();
      }
      return Status::InvalidArgument("unknown invalid provider mode");
    }

   private:
    Mode mode_;
    std::vector<char*> owned_buffers_;
  };

  // Helper to create an SST file and open it as a table
  // Following pattern from table_test.cc TableConstructor
  Status CreateAndOpenSST(
      int num_blocks, std::unique_ptr<BlockBasedTable>* table,
      std::vector<BlockHandle>* block_handles_out,
      const BlockBasedTableOptions* table_options_override = nullptr,
      bool use_direct_reads = false,
      CompressionType compression_type = kNoCompression,
      bool allow_mmap_reads = false, Env* env_override = nullptr,
      std::shared_ptr<Statistics> statistics = nullptr) {
    // Create options - store in member variables to avoid use-after-scope
    // The BlockBasedTable will keep references to these options
    Options options{};
    options.statistics = statistics;
    options.allow_mmap_reads = allow_mmap_reads;
    if (env_override != nullptr) {
      options.env = env_override;
    }
    BlockBasedTableOptions table_options;
    if (table_options_override != nullptr) {
      table_options = *table_options_override;
    } else {
      table_options.block_cache = NewLRUCache(8 * 1024 * 1024);
      table_options.block_size = 16 * 1024;
      table_options.no_block_cache = false;
    }
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // These are stored in member variables after the table reader opens.
    auto ioptions = std::make_unique<ImmutableOptions>(options);
    auto moptions = MutableCFOptions{options};
    auto internal_comparator =
        std::make_unique<InternalKeyComparator>(options.comparator);

    // Create in-memory file using StringSink (like table_test.cc)
    auto table_name = "test_table";
    std::unique_ptr<WritableFileWriter> file_writer;
    NewFileWriter(table_name, &file_writer);

    // Create table builder
    std::string column_family_name;
    const ReadOptions read_options;
    const WriteOptions write_options;
    std::vector<std::unique_ptr<InternalTblPropCollFactory>>
        int_tbl_prop_coll_factories;
    TableBuilderOptions builder_options(
        *ioptions, moptions, read_options, write_options, *internal_comparator,
        &int_tbl_prop_coll_factories, compression_type,
        options.compression_opts, 0 /* column_family_id */, column_family_name,
        -1 /* level */, kUnknownNewestKeyTime);

    std::unique_ptr<TableBuilder> builder(
        options.table_factory->NewTableBuilder(builder_options,
                                               file_writer.get()));

    Status s;
    auto rnd = Random::GetTLSInstance();
    // Add keys to the table
    // 10k * 1Kib = ~10MiB
    for (int i = 0; i < 10000; i++) {
      std::string value = rnd->RandomString(2 << 10);
      InternalKey ikey(Key(i), i, kTypeValue);
      builder->Add(ikey.Encode(), value);
    }
    s = builder->Finish();
    if (!s.ok()) {
      return s;
    }

    uint64_t file_size = builder->FileSize();

    IOOptions io_options;
    s = file_writer->Flush(io_options);
    if (!s.ok()) {
      return s;
    }

    // Now open the file for reading using StringSource (like table_test.cc)
    std::unique_ptr<RandomAccessFileReader> file;
    FileOptions foptions;
    foptions.use_direct_reads = use_direct_reads;
    foptions.use_mmap_reads = allow_mmap_reads;

    NewFileReader(table_name, foptions, &file, statistics.get());

    auto soptions = std::make_unique<EnvOptions>();
    soptions->use_mmap_reads = allow_mmap_reads;
    BlockCacheTracer block_cache_tracer;
    std::unique_ptr<TableReader> table_reader;

    TableReaderOptions reader_options(*ioptions, moptions.prefix_extractor,
                                      moptions.compression_manager.get(),
                                      *soptions, *internal_comparator,
                                      0 /* block_protection_bytes_per_key */);

    s = options.table_factory->NewTableReader(reader_options, std::move(file),
                                              file_size, &table_reader);

    if (!s.ok()) {
      return s;
    }

    // Keep table reader dependencies alive for the returned BlockBasedTable.
    all_ioptions_.push_back(std::move(ioptions));
    all_env_options_.push_back(std::move(soptions));
    all_comparators_.push_back(std::move(internal_comparator));
    all_statistics_.push_back(std::move(statistics));

    table->reset(static_cast<BlockBasedTable*>(table_reader.release()));

    // Collect actual block handles from the table's index
    // This is similar to how block_based_table_iterator.cc CollectBlockHandles
    // works
    s = CollectBlockHandles(table->get(), num_blocks, block_handles_out);
    if (!s.ok()) {
      return s;
    }

    return Status::OK();
  }

  static std::shared_ptr<IOJob> MakeRollingPrefetchJob(
      BlockBasedTable* table, const std::vector<BlockHandle>& block_handles,
      uint64_t window_size, uint64_t max_batch_size = 0) {
    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table;
    job->job_options.read_options.async_io = true;
    job->job_options.read_options.fill_cache = false;
    job->job_options.block_handles_are_sorted = true;
    job->job_options.rolling_prefetch_window_size = window_size;
    job->job_options.rolling_prefetch_max_batch_size = max_batch_size;
    return job;
  }

  static uint64_t cur_file_num_;

  static uint64_t GetCoalescedReadBytes(
      const std::vector<BlockHandle>& block_handles) {
    assert(!block_handles.empty());
    return block_handles.back().offset() +
           BlockBasedTable::BlockSizeWithTrailer(block_handles.back()) -
           block_handles.front().offset();
  }
};

uint64_t IODispatcherTest::cur_file_num_ = 1;

TEST_F(IODispatcherTest, BasicSSTRead) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  ReadOptions read_options;
  // Only use async IO when io_uring is available
  job->job_options.read_options.async_io = kIOUringPresent;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Read blocks using the new ReadSet API and verify they are valid
  // ReadIndex will poll for async IO completion internally, no need to sleep
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);

    // Verify the block has reasonable content
    const Block* block_ptr = block.GetValue();
    ASSERT_GT(block_ptr->size(), 0);
  }

  // Verify statistics - some blocks should have been read asynchronously
  // Note: actual counts depend on cache behavior and IO completion
  uint64_t total_reads = read_set->GetNumSyncReads() +
                         read_set->GetNumAsyncReads() +
                         read_set->GetNumCacheHits();
  ASSERT_EQ(total_reads, block_handles.size());
}

// A prefetched data block that suffers bit rot has to fail the read rather
// than reach the caller as valid keys, and must stay out of the shared block
// cache, where readers that never prefetched would pick it up on a hit.
TEST_F(IODispatcherTest, CorruptPrefetchedBlockFailsChecksumVerification) {
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(8 * 1024 * 1024);
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = false;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                             false /* use_direct_reads */, kNoCompression,
                             false /* allow_mmap_reads */,
                             nullptr /* env_override */, statistics));
  ASSERT_FALSE(block_handles.empty());
  block_handles.resize(1);

  const uint64_t data_blocks_cached_before =
      statistics->getTickerCount(BLOCK_CACHE_DATA_ADD);
  tracking_fs_->CorruptReadsAtOffset(block_handles[0].offset());

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;
  // The sync and async dispatcher paths share the block-construction helper
  // this exercises, so pinning the sync path keeps the test independent of
  // io_uring availability.
  ASSERT_TRUE(job->job_options.read_options.verify_checksums);

  IODispatcherOptions dispatcher_options;
  dispatcher_options.max_prefetch_memory_bytes = 1024 * 1024;
  dispatcher_options.statistics = statistics.get();
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(dispatcher_options));
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);
  EXPECT_EQ(statistics->getTickerCount(BLOCK_CHECKSUM_MISMATCH_COUNT), 1);
  EXPECT_EQ(statistics->getTickerCount(PREFETCH_MEMORY_BYTES_GRANTED),
            statistics->getTickerCount(PREFETCH_MEMORY_BYTES_RELEASED));

  CachableEntry<Block> block;
  const Status s = read_set->ReadIndex(0, &block);
  ASSERT_GT(tracking_fs_->GetCorruptHits(), 0u);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();

  // A rejected prefetch falls back to the ordinary block-fetch path, which
  // re-reads and rejects the block again, so more than one mismatch is
  // expected here.
  EXPECT_GE(statistics->getTickerCount(BLOCK_CHECKSUM_MISMATCH_COUNT), 2);
  EXPECT_EQ(data_blocks_cached_before,
            statistics->getTickerCount(BLOCK_CACHE_DATA_ADD));
}

TEST_F(IODispatcherTest, TruncatedPrefetchedBlockFailsBeforeConstruction) {
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(8 * 1024 * 1024);
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = false;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                             false /* use_direct_reads */, kNoCompression,
                             false /* allow_mmap_reads */,
                             nullptr /* env_override */, statistics));
  ASSERT_FALSE(block_handles.empty());
  block_handles.resize(1);

  const uint64_t data_blocks_cached_before =
      statistics->getTickerCount(BLOCK_CACHE_DATA_ADD);
  tracking_fs_->TruncateReadsAtOffset(block_handles[0].offset() +
                                      block_handles[0].size());

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);

  CachableEntry<Block> block;
  const Status s = read_set->ReadIndex(0, &block);
  ASSERT_GE(tracking_fs_->GetTruncateHits(), 2u);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  EXPECT_EQ(data_blocks_cached_before,
            statistics->getTickerCount(BLOCK_CACHE_DATA_ADD));
}

TEST_F(IODispatcherTest,
       CorruptAsyncPrefetchKeepsValidSiblingAndRetriesFailedBlock) {
  auto controlled_fs = std::make_shared<ControlledAsyncFS>(base_fs_);
  controlled_fs->SetCompleteStrayFirst(false);
  tracking_fs_ = controlled_fs;
  std::unique_ptr<Env> controlled_env = NewCompositeEnv(controlled_fs);
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(8 * 1024 * 1024);
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = false;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                             false /* use_direct_reads */, kNoCompression,
                             false /* allow_mmap_reads */, controlled_env.get(),
                             statistics));
  ASSERT_GE(block_handles.size(), 2);
  block_handles.resize(2);
  controlled_fs->CorruptReadsAtOffset(block_handles[1].offset());

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;
  job->job_options.io_coalesce_threshold = 1024 * 1024;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);
  ASSERT_EQ(controlled_fs->GetReadAsyncCount(), 1);

  CachableEntry<Block> first_block;
  ASSERT_OK(read_set->ReadIndex(0, &first_block));
  ASSERT_NE(first_block.GetValue(), nullptr);

  CachableEntry<Block> second_block;
  ASSERT_OK(read_set->ReadIndex(1, &second_block));
  ASSERT_NE(second_block.GetValue(), nullptr);
  EXPECT_EQ(controlled_fs->GetCorruptHits(), 1u);
  EXPECT_EQ(statistics->getTickerCount(BLOCK_CHECKSUM_MISMATCH_COUNT), 1);
  EXPECT_EQ(read_set->GetNumAsyncReads(), 2);
  EXPECT_EQ(read_set->GetNumSyncReads(), 1);
}

// Verifies that setting a read-scoped provider makes IODispatcher data-block
// reads bypass the block cache even when one exists. The test checks there are
// no cache hits and the returned block memory belongs to the provider.
TEST_F(IODispatcherTest, ReadScopedProviderBypassesBlockCache) {
  TestReadScopedBlockBufferProvider provider;
  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(8 * 1024 * 1024);
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = false;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options));
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.read_scoped_block_buffer_provider = &provider;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);
  EXPECT_GT(provider.allocations(), 0);
  EXPECT_EQ(read_set->GetNumCacheHits(), 0);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_TRUE(provider.OwnsLiveAddressRange(block.GetValue()->data(),
                                            block.GetValue()->size()));

  block.Reset();
  read_set.reset();
  EXPECT_EQ(provider.bytes_outstanding(), 0);
}

// Verifies the provider path also works for tables configured without a block
// cache. The test reads a block and asserts the BlockContents points into the
// provider allocation and that releasing the block releases the provider lease.
TEST_F(IODispatcherTest, ReadScopedProviderWorksWithoutBlockCache) {
  TestReadScopedBlockBufferProvider provider;
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options));
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.read_scoped_block_buffer_provider = &provider;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);
  EXPECT_GT(provider.allocations(), 0);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_TRUE(provider.OwnsLiveAddressRange(block.GetValue()->data(),
                                            block.GetValue()->size()));

  block.Reset();
  read_set.reset();
  EXPECT_EQ(provider.bytes_outstanding(), 0);
}

// Verifies mmap reads disable the read-scoped provider path. The provider is
// ignored because mmap file reads can return pointers into the mapped file
// instead of writing into RocksDB-provided scratch buffers.
TEST_F(IODispatcherTest, ReadScopedProviderIgnoredWithMmapReads) {
  TestReadScopedBlockBufferProvider provider;
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                             false /* use_direct_reads */, kNoCompression,
                             true /* allow_mmap_reads */));
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.read_scoped_block_buffer_provider = &provider;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_EQ(provider.allocations(), 0);

  block.Reset();
  read_set.reset();
  EXPECT_EQ(provider.bytes_outstanding(), 0);
}

// Verifies compressed blocks are decompressed directly into provider-backed
// storage. The test builds a compressed table, reads a block through
// IODispatcher, and checks the final uncompressed block bytes are provider
// owned without using provider-backed serialized read scratch.
TEST_F(IODispatcherTest, ReadScopedBuffersOwnDecompressedBlocks) {
  CompressionType compression_type = kNoCompression;
  for (CompressionType supported : GetSupportedCompressions()) {
    if (supported != kNoCompression) {
      compression_type = supported;
      break;
    }
  }
  if (compression_type == kNoCompression) {
    ROCKSDB_GTEST_SKIP("Test requires a supported compression type");
    return;
  }

  for (bool use_direct_reads : {false, true}) {
    SCOPED_TRACE("use_direct_reads=" + std::to_string(use_direct_reads));
    TestReadScopedBlockBufferProvider provider;
    BlockBasedTableOptions table_options;
    table_options.block_cache = nullptr;
    table_options.block_size = 16 * 1024;
    table_options.no_block_cache = true;

    std::unique_ptr<BlockBasedTable> table;
    std::vector<BlockHandle> block_handles;
    ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                               use_direct_reads, compression_type));
    ASSERT_GT(block_handles.size(), 0);

    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    job->job_options.read_options.read_scoped_block_buffer_provider = &provider;

    std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
    std::shared_ptr<ReadSet> read_set;
    ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
    ASSERT_NE(read_set, nullptr);
    EXPECT_EQ(provider.allocations(), block_handles.size());
    EXPECT_EQ(provider.max_requested_alignment(), 1);

    CachableEntry<Block> block;
    ASSERT_OK(read_set->ReadIndex(0, &block));
    ASSERT_NE(block.GetValue(), nullptr);
    EXPECT_TRUE(provider.OwnsLiveAddressRange(block.GetValue()->data(),
                                              block.GetValue()->size()));

    block.Reset();
    read_set.reset();
    EXPECT_EQ(provider.bytes_outstanding(), 0);
  }
}

// Verifies RocksDB rejects invalid provider leases instead of pinning unsafe
// memory. The test uses a provider that omits cleanup and expects ReadIndex()
// to fail with InvalidArgument.
TEST_F(IODispatcherTest, InvalidReadScopedLeaseFailsRead) {
  InvalidReadScopedBlockBufferProvider provider(
      InvalidReadScopedBlockBufferProvider::Mode::kMissingCleanup);
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options));
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.read_scoped_block_buffer_provider = &provider;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);

  CachableEntry<Block> block;
  Status s = read_set->ReadIndex(0, &block);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

// Verifies uncompressed provider-backed blocks require a cleanup object on the
// read buffer before RocksDB pins the block. The test clears the cleanup via a
// sync point and checks the block is not made available.
TEST_F(IODispatcherTest,
       ReadScopedProviderRequiresReadBufferCleanupForUncompressedBlock) {
  TestReadScopedBlockBufferProvider provider;
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options));
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;
  job->job_options.read_options.read_scoped_block_buffer_provider = &provider;

  auto* sync_point = SyncPoint::GetInstance();
  struct SyncPointGuard {
    explicit SyncPointGuard(SyncPoint* sp) : sync_point(sp) {}
    ~SyncPointGuard() {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    }

    SyncPoint* sync_point;
  } sync_point_guard(sync_point);

  int cleanup_callbacks = 0;
  sync_point->SetCallBack(
      "CreateAndPinBlockFromBuffer:ReadBufferCleanup", [&](void* arg) {
        ++cleanup_callbacks;
        auto* cleanup = static_cast<SharedCleanablePtr*>(arg);
        cleanup->Reset();
      });
  sync_point->EnableProcessing();

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  Status s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);
  EXPECT_GT(cleanup_callbacks, 0);
  EXPECT_FALSE(read_set->IsBlockAvailable(0));
  EXPECT_EQ(provider.bytes_outstanding(), 0);
}

// Verifies cache lookup is a no-op when the table has no block cache. The test
// calls LookupAndPinBlocksInCache() directly and checks it returns no pinned
// block without crashing.
TEST_F(IODispatcherTest, LookupAndPinBlocksInCacheNoOpsWithoutBlockCache) {
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(5, &table, &block_handles, &table_options));
  ASSERT_NE(table, nullptr);
  ASSERT_FALSE(table->get_rep()->table_options.block_cache);
  ASSERT_FALSE(block_handles.empty());

  int lookup_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::LookupAndPinBlocksInCache:Start",
      [&](void*) { ++lookup_calls; });
  SyncPoint::GetInstance()->EnableProcessing();

  CachableEntry<Block_kData> block;
  ASSERT_OK(table->LookupAndPinBlocksInCache<Block_kData>(
      ReadOptions(), block_handles.front(), &block));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_EQ(lookup_calls, 1);
  EXPECT_EQ(block.GetValue(), nullptr);
}

// Verifies SubmitJob() does not perform cache lookups for tables without a
// block cache. The test counts the lookup sync point while reading blocks and
// expects disk reads but zero lookup calls.
TEST_F(IODispatcherTest, SubmitJobWithoutBlockCacheSkipsCacheLookup) {
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(10, &table, &block_handles, &table_options));
  ASSERT_NE(table, nullptr);
  ASSERT_FALSE(table->get_rep()->table_options.block_cache);
  ASSERT_GE(block_handles.size(), 5);
  block_handles.resize(5);

  int lookup_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::LookupAndPinBlocksInCache:Start",
      [&](void*) { ++lookup_calls; });
  SyncPoint::GetInstance()->EnableProcessing();

  tracking_fs_->ClearReadOps();
  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;
  job->job_options.io_coalesce_threshold = 1024 * 1024;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);
  EXPECT_EQ(read_set->GetNumCacheHits(), 0);
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    ASSERT_OK(read_set->ReadIndex(i, &block));
    ASSERT_NE(block.GetValue(), nullptr);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_EQ(lookup_calls, 0);
  EXPECT_GT(tracking_fs_->GetMultiReadCount(), 0);
}

// Verifies fill_cache=false still permits cache hits while avoiding insertion
// for newly read blocks. The test warms one block, reads a hit and miss with
// fill_cache=false, then verifies the miss was not cached.
TEST_F(IODispatcherTest, FillCacheFalseStillLooksUpExistingCacheEntries) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(10, &table, &block_handles));
  ASSERT_NE(table, nullptr);
  ASSERT_TRUE(table->get_rep()->table_options.block_cache);
  ASSERT_GE(block_handles.size(), 2);

  auto warm_job = std::make_shared<IOJob>();
  warm_job->block_handles = {block_handles[0]};
  warm_job->table = table.get();
  warm_job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> warm_read_set;
  ASSERT_OK(dispatcher->SubmitJob(warm_job, &warm_read_set));
  ASSERT_NE(warm_read_set, nullptr);
  {
    CachableEntry<Block> block;
    ASSERT_OK(warm_read_set->ReadIndex(0, &block));
    ASSERT_NE(block.GetValue(), nullptr);
  }
  warm_read_set.reset();

  int lookup_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::LookupAndPinBlocksInCache:Start",
      [&](void*) { ++lookup_calls; });
  SyncPoint::GetInstance()->EnableProcessing();

  tracking_fs_->ClearReadOps();
  auto fill_cache_false_job = std::make_shared<IOJob>();
  fill_cache_false_job->block_handles = {block_handles[0], block_handles[1]};
  fill_cache_false_job->table = table.get();
  fill_cache_false_job->job_options.read_options.fill_cache = false;
  fill_cache_false_job->job_options.read_options.async_io = false;
  fill_cache_false_job->job_options.io_coalesce_threshold = 1024 * 1024;

  std::shared_ptr<ReadSet> fill_cache_false_read_set;
  ASSERT_OK(
      dispatcher->SubmitJob(fill_cache_false_job, &fill_cache_false_read_set));
  ASSERT_NE(fill_cache_false_read_set, nullptr);
  EXPECT_EQ(lookup_calls, 2);
  EXPECT_EQ(fill_cache_false_read_set->GetNumCacheHits(), 1);
  EXPECT_GT(tracking_fs_->GetMultiReadCount(), 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  for (size_t i = 0; i < fill_cache_false_job->block_handles.size(); ++i) {
    CachableEntry<Block> block;
    ASSERT_OK(fill_cache_false_read_set->ReadIndex(i, &block));
    ASSERT_NE(block.GetValue(), nullptr);
  }
  fill_cache_false_read_set.reset();

  tracking_fs_->ClearReadOps();
  auto verify_miss_not_cached_job = std::make_shared<IOJob>();
  verify_miss_not_cached_job->block_handles = {block_handles[1]};
  verify_miss_not_cached_job->table = table.get();
  verify_miss_not_cached_job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> verify_miss_not_cached_read_set;
  ASSERT_OK(dispatcher->SubmitJob(verify_miss_not_cached_job,
                                  &verify_miss_not_cached_read_set));
  ASSERT_NE(verify_miss_not_cached_read_set, nullptr);
  EXPECT_EQ(verify_miss_not_cached_read_set->GetNumCacheHits(), 0);
  EXPECT_GT(tracking_fs_->GetMultiReadCount(), 0);
  CachableEntry<Block> block;
  ASSERT_OK(verify_miss_not_cached_read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);
}

// Verifies a read-scoped provider skips both data-block cache lookup and
// data-block cache insertion. The test warms one block, reads a warmed block
// plus a miss with the provider enabled, and then confirms the missed block was
// still not cached.
TEST_F(IODispatcherTest, ReadScopedProviderSkipsCacheLookupAndInsert) {
  TestReadScopedBlockBufferProvider provider;
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(10, &table, &block_handles));
  ASSERT_NE(table, nullptr);
  ASSERT_TRUE(table->get_rep()->table_options.block_cache);
  ASSERT_GE(block_handles.size(), 2);

  auto warm_job = std::make_shared<IOJob>();
  warm_job->block_handles = {block_handles[0]};
  warm_job->table = table.get();
  warm_job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> warm_read_set;
  ASSERT_OK(dispatcher->SubmitJob(warm_job, &warm_read_set));
  ASSERT_NE(warm_read_set, nullptr);
  {
    CachableEntry<Block> block;
    ASSERT_OK(warm_read_set->ReadIndex(0, &block));
    ASSERT_NE(block.GetValue(), nullptr);
  }
  warm_read_set.reset();

  int lookup_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::LookupAndPinBlocksInCache:Start",
      [&](void*) { ++lookup_calls; });
  SyncPoint::GetInstance()->EnableProcessing();

  tracking_fs_->ClearReadOps();
  auto provider_job = std::make_shared<IOJob>();
  provider_job->block_handles = {block_handles[0], block_handles[1]};
  provider_job->table = table.get();
  provider_job->job_options.read_options.async_io = false;
  provider_job->job_options.read_options.read_scoped_block_buffer_provider =
      &provider;
  provider_job->job_options.io_coalesce_threshold = 1024 * 1024;

  std::shared_ptr<ReadSet> provider_read_set;
  ASSERT_OK(dispatcher->SubmitJob(provider_job, &provider_read_set));
  ASSERT_NE(provider_read_set, nullptr);
  EXPECT_EQ(provider_read_set->GetNumCacheHits(), 0);
  EXPECT_GT(tracking_fs_->GetMultiReadCount(), 0);
  for (size_t i = 0; i < provider_job->block_handles.size(); ++i) {
    CachableEntry<Block> block;
    ASSERT_OK(provider_read_set->ReadIndex(i, &block));
    ASSERT_NE(block.GetValue(), nullptr);
  }
  EXPECT_EQ(lookup_calls, 0);
  provider_read_set.reset();
  EXPECT_EQ(provider.bytes_outstanding(), 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  tracking_fs_->ClearReadOps();
  auto verify_miss_not_cached_job = std::make_shared<IOJob>();
  verify_miss_not_cached_job->block_handles = {block_handles[1]};
  verify_miss_not_cached_job->table = table.get();
  verify_miss_not_cached_job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> verify_miss_not_cached_read_set;
  ASSERT_OK(dispatcher->SubmitJob(verify_miss_not_cached_job,
                                  &verify_miss_not_cached_read_set));
  ASSERT_NE(verify_miss_not_cached_read_set, nullptr);
  EXPECT_EQ(verify_miss_not_cached_read_set->GetNumCacheHits(), 0);
  EXPECT_GT(tracking_fs_->GetMultiReadCount(), 0);
  CachableEntry<Block> block;
  ASSERT_OK(verify_miss_not_cached_read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);
}

// Verifies read-scoped provider cache bypass is also honored when prefetch
// cannot acquire memory and ReadIndex() falls back to synchronous reading. The
// test forces the fallback with a one-byte memory budget and checks no
// data-block cache lookup occurred.
TEST_F(IODispatcherTest,
       ReadScopedProviderSkipsCacheLookupForSyncReadFallback) {
  TestReadScopedBlockBufferProvider provider;
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 1;
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(10, &table, &block_handles));
  ASSERT_NE(table, nullptr);
  ASSERT_TRUE(table->get_rep()->table_options.block_cache);
  ASSERT_FALSE(block_handles.empty());

  int lookup_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::LookupAndPinBlocksInCache:Start",
      [&](void*) { ++lookup_calls; });
  SyncPoint::GetInstance()->EnableProcessing();

  auto provider_job = std::make_shared<IOJob>();
  provider_job->block_handles = {block_handles[0]};
  provider_job->table = table.get();
  provider_job->job_options.read_options.async_io = false;
  provider_job->job_options.read_options.read_scoped_block_buffer_provider =
      &provider;

  std::shared_ptr<ReadSet> provider_read_set;
  ASSERT_OK(dispatcher->SubmitJob(provider_job, &provider_read_set));
  ASSERT_NE(provider_read_set, nullptr);
  EXPECT_EQ(provider_read_set->GetNumCacheHits(), 0);

  CachableEntry<Block> block;
  ASSERT_OK(provider_read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_EQ(provider_read_set->GetNumSyncReads(), 1);
  EXPECT_EQ(lookup_calls, 0);
  block.Reset();
  provider_read_set.reset();
  EXPECT_EQ(provider.bytes_outstanding(), 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Verifies MultiScan passes the read-scoped provider through to IODispatcher.
// The provider makes supported data-block reads bypass the data-block cache.
TEST_F(IODispatcherTest, MultiScanReadScopedProviderSkipsLookup) {
  TestReadScopedBlockBufferProvider provider;
  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(10, &table, &block_handles));
  ASSERT_NE(table, nullptr);
  ASSERT_TRUE(table->get_rep()->table_options.block_cache);

  int lookup_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::LookupAndPinBlocksInCache:Start",
      [&](void*) { ++lookup_calls; });
  SyncPoint::GetInstance()->EnableProcessing();

  tracking_fs_->ClearReadOps();
  ReadOptions read_options;
  read_options.read_scoped_block_buffer_provider = &provider;
  std::unique_ptr<InternalIterator> iter(table->NewIterator(
      read_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  std::string start = Key(0);
  std::string limit = Key(64);
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(Slice(start), Slice(limit));

  iter->Prepare(&scan_options);
  ASSERT_OK(iter->status());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_EQ(lookup_calls, 0);
  EXPECT_GT(tracking_fs_->GetMultiReadCount(), 0);
  iter.reset();
  EXPECT_EQ(provider.bytes_outstanding(), 0);
}

// Verifies direct I/O MultiRead uses one aligned provider allocation as the
// scratch buffer. The test opens the table with direct reads and checks the
// provider saw one aligned allocation and owns the returned block memory.
TEST_F(IODispatcherTest, ReadScopedDirectIOUsesAlignedProviderScratch) {
  TestReadScopedBlockBufferProvider provider;
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                             true /* use_direct_reads */));
  ASSERT_GT(block_handles.size(), 0);

  tracking_fs_->ClearReadOps();

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.read_scoped_block_buffer_provider = &provider;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);
  EXPECT_EQ(tracking_fs_->GetMultiReadCount(), 1);
  EXPECT_EQ(provider.allocations(), 1);
  EXPECT_GT(provider.max_requested_alignment(), 1);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_TRUE(provider.OwnsLiveAddressRange(block.GetValue()->data(),
                                            block.GetValue()->size()));

  block.Reset();
  read_set.reset();
  EXPECT_EQ(provider.bytes_outstanding(), 0);
}

// Verifies the regular block-based table iterator path, outside MultiScan,
// backs data blocks with the read-scoped provider. The test covers buffered and
// direct-I/O reads, and compressed and uncompressed blocks; each case seeks a
// normal iterator and checks its returned value slice points into a live
// provider allocation.
TEST_F(IODispatcherTest, RegularIteratorUsesReadScopedProvider) {
  std::vector<CompressionType> compression_types = {kNoCompression};
  for (CompressionType supported : GetSupportedCompressions()) {
    if (supported != kNoCompression) {
      compression_types.push_back(supported);
      break;
    }
  }

  for (bool use_direct_reads : {false, true}) {
    for (CompressionType compression_type : compression_types) {
      SCOPED_TRACE("use_direct_reads=" + std::to_string(use_direct_reads) +
                   " compression_type=" +
                   std::to_string(static_cast<int>(compression_type)));

      TestReadScopedBlockBufferProvider provider;
      BlockBasedTableOptions table_options;
      table_options.block_cache = NewLRUCache(8 * 1024 * 1024);
      table_options.block_size = 16 * 1024;
      table_options.no_block_cache = false;

      std::unique_ptr<BlockBasedTable> table;
      std::vector<BlockHandle> block_handles;
      ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                                 use_direct_reads, compression_type));
      ASSERT_GT(block_handles.size(), 0);

      ReadOptions read_options;
      read_options.read_scoped_block_buffer_provider = &provider;
      std::unique_ptr<InternalIterator> iter(table->NewIterator(
          read_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
          /*skip_filters=*/false, TableReaderCaller::kUncategorized));

      iter->SeekToFirst();
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_GT(provider.allocations(), 0);
      EXPECT_TRUE(provider.OwnsLiveAddressRange(iter->value().data(),
                                                iter->value().size()));
      if (use_direct_reads && compression_type == kNoCompression) {
        EXPECT_GT(provider.max_requested_alignment(), 1);
      } else {
        EXPECT_EQ(provider.max_requested_alignment(), 1);
      }

      iter.reset();
      EXPECT_EQ(provider.bytes_outstanding(), 0);
    }
  }
}

TEST_F(IODispatcherTest, MultipleSSTFiles) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::vector<std::shared_ptr<ReadSet>> read_sets;
  std::vector<std::vector<BlockHandle>> all_block_handles;

  // Create and submit jobs for multiple SST files
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<BlockBasedTable> table;
    std::vector<BlockHandle> block_handles;

    Status s = CreateAndOpenSST(30 + i * 10, &table, &block_handles);
    ASSERT_OK(s);

    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    tables_.push_back(std::move(table));

    all_block_handles.push_back(block_handles);
    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);
    read_sets.push_back(read_set);
  }

  // Verify all ReadSets can read their blocks successfully
  // ReadIndex will poll for async IO completion internally, no need to sleep
  for (size_t i = 0; i < read_sets.size(); ++i) {
    for (size_t j = 0; j < all_block_handles[i].size(); ++j) {
      CachableEntry<Block> block;
      Status read_status = read_sets[i]->ReadIndex(j, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);
    }
  }
}

TEST_F(IODispatcherTest, StatisticsTracking) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(30, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  // Only use async IO when io_uring is available
  job->job_options.read_options.async_io = kIOUringPresent;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Read all blocks - ReadIndex handles polling for async IO completion
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }

  // Read the same blocks again - should all be cache hits now
  std::shared_ptr<ReadSet> read_set2;
  s = dispatcher->SubmitJob(job, &read_set2);
  ASSERT_OK(s);
  EXPECT_EQ(read_set2->GetNumCacheHits(), block_handles.size());

  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set2->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
    read_set2->ReleaseBlock(i);
  }

  // After reading all blocks, verify statistics
  uint64_t num_sync = read_set->GetNumSyncReads();
  uint64_t num_async = read_set->GetNumAsyncReads();
  uint64_t num_cache = read_set->GetNumCacheHits();

  // Total reads should equal number of blocks
  uint64_t total_reads = num_sync + num_async + num_cache;
  ASSERT_EQ(total_reads, block_handles.size());
}

TEST_F(IODispatcherTest, SyncPrefetchRecordsDataBlockPerfCounters) {
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(10, &table, &block_handles, &table_options));
  ASSERT_GE(block_handles.size(), 7);

  const std::vector<BlockHandle> test_handles = {
      block_handles[0], block_handles[2], block_handles[4], block_handles[6]};
  const uint64_t expected_block_read_bytes =
      GetCoalescedReadBytes(test_handles);
  uint64_t selected_block_bytes = 0;
  for (const auto& block_handle : test_handles) {
    selected_block_bytes += BlockBasedTable::BlockSizeWithTrailer(block_handle);
  }
  ASSERT_GT(expected_block_read_bytes, selected_block_bytes);

  auto job = std::make_shared<IOJob>();
  job->block_handles = test_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;
  job->job_options.io_coalesce_threshold = 1024 * 1024;

  tracking_fs_->ClearReadOps();
  SetPerfLevel(kEnableCount);
  get_perf_context()->Reset();

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);

  EXPECT_EQ(get_perf_context()->block_read_count, test_handles.size());
  EXPECT_EQ(get_perf_context()->block_read_byte, expected_block_read_bytes);
  EXPECT_EQ(get_perf_context()->data_block_read_byte,
            expected_block_read_bytes);

  const auto read_ops = tracking_fs_->GetReadOps();
  ASSERT_EQ(read_ops.size(), 1);
  ASSERT_EQ(read_ops[0].type, ReadOp::kMultiRead);
  ASSERT_EQ(read_ops[0].requests.size(), 1);
  EXPECT_EQ(read_ops[0].requests[0].second, expected_block_read_bytes);
}

TEST_F(IODispatcherTest, AsyncPrefetchRecordsDataBlockPerfCounters) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Test requires io_uring support");
    return;
  }

  auto controlled_fs = std::make_shared<ControlledAsyncFS>(base_fs_);
  controlled_fs->SetCompleteStrayFirst(false);
  tracking_fs_ = controlled_fs;
  std::unique_ptr<Env> controlled_env = NewCompositeEnv(controlled_fs);

  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(
      8, &table, &block_handles, &table_options, true /* use_direct_reads */,
      kNoCompression, false /* allow_mmap_reads */, controlled_env.get()));
  ASSERT_GE(block_handles.size(), 3);

  block_handles.resize(3);
  const uint64_t expected_block_read_bytes =
      GetCoalescedReadBytes(block_handles);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;
  job->job_options.io_coalesce_threshold = 1024 * 1024;

  tracking_fs_->ClearReadOps();
  SetPerfLevel(kEnableCount);
  get_perf_context()->Reset();

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);
  ASSERT_EQ(tracking_fs_->GetReadAsyncCount(), 1);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);

  EXPECT_EQ(get_perf_context()->block_read_count, block_handles.size());
  EXPECT_EQ(get_perf_context()->block_read_byte, expected_block_read_bytes);
  EXPECT_EQ(get_perf_context()->data_block_read_byte,
            expected_block_read_bytes);
}

TEST_F(IODispatcherTest, AsyncReadLatencyHistograms) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Test requires io_uring support");
    return;
  }

  auto controlled_fs = std::make_shared<ControlledAsyncFS>(base_fs_);
  controlled_fs->SetCompleteStrayFirst(false);
  tracking_fs_ = controlled_fs;
  std::unique_ptr<Env> controlled_env = NewCompositeEnv(controlled_fs);
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                             true /* use_direct_reads */, kNoCompression,
                             false /* allow_mmap_reads */, controlled_env.get(),
                             statistics));
  ASSERT_FALSE(block_handles.empty());
  block_handles.resize(1);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_NE(read_set, nullptr);
  EXPECT_EQ(tracking_fs_->GetReadAsyncCount(), 1);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);

  HistogramData observed_completion;
  HistogramData poll_wait;
  HistogramData prefetch_lead;
  statistics->histogramData(IO_DISPATCHER_ASYNC_READ_OBSERVED_COMPLETION_MICROS,
                            &observed_completion);
  statistics->histogramData(IO_DISPATCHER_ASYNC_READ_POLL_WAIT_MICROS,
                            &poll_wait);
  statistics->histogramData(IO_DISPATCHER_ASYNC_READ_PREFETCH_LEAD_MICROS,
                            &prefetch_lead);

  EXPECT_EQ(observed_completion.count, 1);
  EXPECT_EQ(poll_wait.count, 1);
  EXPECT_EQ(prefetch_lead.count, 1);
  EXPECT_GE(observed_completion.sum, prefetch_lead.sum);
}

TEST_F(IODispatcherTest, AsyncAndSyncRead) {
  // This test verifies the difference between async_io=true and async_io=false
  // by checking the statistics after reading all blocks.
  // Only test async_io=true when io_uring is available.
  std::vector<bool> async_modes = {false};
  if (kIOUringPresent) {
    async_modes.push_back(true);
  }

  for (auto async : async_modes) {
    std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

    std::unique_ptr<BlockBasedTable> table;
    std::vector<BlockHandle> block_handles;
    Status s = CreateAndOpenSST(40, &table, &block_handles);
    ASSERT_OK(s);
    ASSERT_NE(table, nullptr);
    ASSERT_GT(block_handles.size(), 0);

    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    ReadOptions read_options;
    // Ensure we don't use cache for this test - we want fresh reads
    read_options.fill_cache = false;
    job->job_options.read_options.async_io = async;

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);
    ASSERT_NE(read_set, nullptr);

    // Read all blocks - ReadIndex handles polling for async IO internally
    for (size_t i = 0; i < block_handles.size(); ++i) {
      CachableEntry<Block> block;
      Status read_status = read_set->ReadIndex(i, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);

      // Verify the block has reasonable content
      const Block* block_ptr = block.GetValue();
      ASSERT_GT(block_ptr->size(), 0);
    }

    // Verify statistics
    uint64_t num_sync = read_set->GetNumSyncReads();
    uint64_t num_async = read_set->GetNumAsyncReads();
    uint64_t num_cache = read_set->GetNumCacheHits();

    // Total reads should equal number of blocks
    uint64_t total_reads = num_sync + num_async + num_cache;
    EXPECT_EQ(total_reads, block_handles.size());

    // When async_io is false, we always expect sync reads
    if (!async) {
      EXPECT_GT(num_sync, 0) << "Expected sync reads when async_io=false";
      EXPECT_EQ(num_async, 0) << "Expected no async reads when async_io=false";
    }
    // When async_io is true:
    // - If io_uring is available, we expect async reads
    // - If io_uring is NOT available, ReadAsync returns NotSupported and
    //   we fall back to sync reads. This is valid behavior.
    // So we only verify that ALL blocks were read (checked above).
  }
}

TEST_F(IODispatcherTest, VerifyBlockContent) {
  // Test that blocks retrieved through ReadSet contain the correct data
  // that was written to the SST file
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GT(block_handles.size(), 0);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  ReadOptions read_options;
  job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Read each block and verify its content
  int t = 0;
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block_entry;
    Status read_status = read_set->ReadIndex(i, &block_entry);
    ASSERT_OK(read_status);
    ASSERT_NE(block_entry.GetValue(), nullptr);

    Block* block = block_entry.GetValue();
    ASSERT_GT(block->size(), 0);

    // Create an iterator to walk through the block's keys
    // We use InternalKeyComparator for data blocks
    InternalKeyComparator internal_comparator(BytewiseComparator());
    std::unique_ptr<DataBlockIter> iter(block->NewDataIterator(
        internal_comparator.user_comparator(), kDisableGlobalSequenceNumber));

    // Iterate through all keys in this block
    size_t num_keys_in_block = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      num_keys_in_block++;

      // Verify key is not empty
      ASSERT_GT(iter->key().size(), 0)
          << "Block " << i << " contains empty key";

      // Verify value is not empty (we wrote 1KB values)
      ASSERT_GT(iter->value().size(), 2 ^ 10)
          << "Block " << i << " contains empty value";

      // Parse the internal key
      ParsedInternalKey parsed_key;
      Status parse_status =
          ParseInternalKey(iter->key(), &parsed_key, true /* log_err */);
      ASSERT_OK(parse_status) << "Failed to parse internal key in block " << i;

      // Verify the key matches the expected format from CreateAndOpenSST
      // Keys are created with Key(i) which generates keys like "key000000"
      std::string user_key = parsed_key.user_key.ToString();
      auto check = Key(t);
      t++;
      ASSERT_TRUE(user_key.find("key") == 0)
          << "Unexpected key format in block " << i << ": " << user_key;

      ASSERT_EQ(check.c_str(), user_key);

      // Verify value type is correct (should be kTypeValue)
      ASSERT_EQ(parsed_key.type, kTypeValue)
          << "Unexpected value type in block " << i;
    }

    // Verify iterator status after iteration
    ASSERT_OK(iter->status()) << "Iterator error in block " << i;

    // Each block should contain at least one key
    ASSERT_GT(num_keys_in_block, 0) << "Block " << i << " contains no keys";
  }
}

// We want to test here that even when we DONT read from the readset that all
// pinned blocks will be unpinned.
TEST_F(IODispatcherTest, ReadSetDestroysUnpinsBlocks) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(30, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(block_handles.size(), 30);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  ReadOptions read_options;
  job->job_options.read_options.async_io =
      false;  // Use sync IO so blocks are pinned immediately

  auto* rep = table->get_rep();
  auto cache = rep->table_options.block_cache.get();
  ASSERT_NE(cache, nullptr);

  auto initial_pinned_usage = cache->GetPinnedUsage();
  ASSERT_EQ(initial_pinned_usage, 0);

  {
    std::shared_ptr<ReadSet> read_set;
    Status t = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(t);
    ASSERT_NE(read_set, nullptr);

    // With sync IO, blocks are already pinned in read_set->pinned_blocks_
    // We do NOT call read_set->Read() - blocks should remain in pinned_blocks_

    // At this point, blocks should be pinned in the ReadSet
    auto pinned_usage_with_blocks = cache->GetPinnedUsage();
    ASSERT_GT(pinned_usage_with_blocks, initial_pinned_usage)
        << "Expected pinned usage to increase after SubmitJob, but "
        << "initial=" << initial_pinned_usage
        << " current=" << pinned_usage_with_blocks;

    // ReadSet goes out of scope here, its destructor should unpin all blocks
  }

  // ReadSet destroyed - all blocks should be unpinned
  auto final_pinned_usage = cache->GetPinnedUsage();
  ASSERT_EQ(final_pinned_usage, initial_pinned_usage)
      << "Expected pinned usage to return to initial value after ReadSet "
      << "destruction, but initial=" << initial_pinned_usage
      << " final=" << final_pinned_usage;
}

// Test that verifies the coalescing logic: adjacent blocks within the
// coalesce threshold should be combined into a single read request.
TEST_F(IODispatcherTest, VerifyCoalescing) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  // Get many blocks so we can test coalescing behavior
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GE(block_handles.size(), 20);

  tracking_fs_->ClearReadOps();

  // Test coalescing with sync reads (uses MultiRead)
  {
    auto job = std::make_shared<IOJob>();
    // Use a subset of adjacent blocks
    std::vector<BlockHandle> adjacent_blocks;
    for (size_t i = 0; i < 10 && i < block_handles.size(); ++i) {
      adjacent_blocks.push_back(block_handles[i]);
    }
    job->block_handles = adjacent_blocks;
    job->table = table.get();
    job->job_options.read_options.async_io = false;
    // Set a large coalesce threshold so all adjacent blocks are combined
    job->job_options.io_coalesce_threshold = 1024 * 1024;  // 1MB

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);

    for (size_t i = 0; i < adjacent_blocks.size(); ++i) {
      CachableEntry<Block> block;
      Status read_status = read_set->ReadIndex(i, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);
    }

    // With a large coalesce threshold and adjacent blocks, we expect
    // all blocks to be coalesced into a single MultiRead request
    auto read_ops = tracking_fs_->GetReadOps();
    size_t multiread_count = 0;
    size_t total_requests_in_multireads = 0;
    for (const auto& op : read_ops) {
      if (op.type == ReadOp::kMultiRead) {
        multiread_count++;
        total_requests_in_multireads += op.requests.size();
      }
    }

    // Adjacent blocks should be coalesced into a single read request
    // (assuming they're within the coalesce threshold)
    EXPECT_EQ(multiread_count, 1)
        << "Expected 1 MultiRead call with coalesced blocks";
    EXPECT_EQ(total_requests_in_multireads, 1)
        << "Expected all adjacent blocks to be coalesced into 1 request";
  }

  tracking_fs_->ClearReadOps();

  // Test with zero coalesce threshold and non-adjacent blocks
  // Non-adjacent blocks (with gaps) should NOT be coalesced with threshold=0
  {
    // Create new table to avoid cache hits
    std::unique_ptr<BlockBasedTable> table2;
    std::vector<BlockHandle> block_handles2;
    s = CreateAndOpenSST(50, &table2, &block_handles2);
    ASSERT_OK(s);
    ASSERT_GE(block_handles2.size(), 20);

    tracking_fs_->ClearReadOps();

    auto job = std::make_shared<IOJob>();
    // Skip every other block to create gaps between requested blocks
    // This ensures there are gaps that won't be bridged with threshold=0
    std::vector<BlockHandle> non_adjacent_blocks;
    for (size_t i = 0;
         i < block_handles2.size() && non_adjacent_blocks.size() < 5; i += 2) {
      non_adjacent_blocks.push_back(block_handles2[i]);
    }
    job->block_handles = non_adjacent_blocks;
    job->table = table2.get();
    job->job_options.read_options.async_io = false;
    // Set zero coalesce threshold - blocks with gaps should not be coalesced
    job->job_options.io_coalesce_threshold = 0;

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);

    for (size_t i = 0; i < non_adjacent_blocks.size(); ++i) {
      CachableEntry<Block> block;
      Status read_status = read_set->ReadIndex(i, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);
    }

    // With zero coalesce threshold and non-adjacent blocks (with gaps),
    // each block should be a separate request
    auto read_ops = tracking_fs_->GetReadOps();
    size_t total_requests_in_multireads = 0;
    for (const auto& op : read_ops) {
      if (op.type == ReadOp::kMultiRead) {
        total_requests_in_multireads += op.requests.size();
      }
    }

    // Each non-adjacent block should be a separate request since there are
    // gaps between them and threshold=0 means no gap tolerance
    EXPECT_EQ(total_requests_in_multireads, non_adjacent_blocks.size())
        << "Expected each non-adjacent block to be a separate request with "
           "zero coalesce threshold";
  }
}

TEST_F(IODispatcherTest, SortedBlockHandlesSkipDispatcherSorts) {
  struct SortCounters {
    int submit_job = 0;
  };

  auto install_callbacks = [](SortCounters* counters) {
    SyncPoint::GetInstance()->SetCallBack(
        "IODispatcherImpl::SubmitJob:SortBlockHandles",
        [counters](void*) { ++counters->submit_job; });
    SyncPoint::GetInstance()->EnableProcessing();
  };

  auto submit_job = [](IODispatcher* dispatcher, BlockBasedTable* table,
                       const std::vector<BlockHandle>& block_handles,
                       bool sorted) {
    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table;
    job->job_options.block_handles_are_sorted = sorted;
    job->job_options.read_options.async_io = false;
    job->job_options.io_coalesce_threshold = 1024 * 1024;

    std::shared_ptr<ReadSet> read_set;
    ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
    ASSERT_NE(read_set, nullptr);
    for (size_t i = 0; i < block_handles.size(); ++i) {
      CachableEntry<Block> block;
      ASSERT_OK(read_set->ReadIndex(i, &block));
      ASSERT_NE(block.GetValue(), nullptr);
      ASSERT_GT(block.GetValue()->size(), 0);

      InternalKeyComparator internal_comparator(BytewiseComparator());
      std::unique_ptr<DataBlockIter> iter(block.GetValue()->NewDataIterator(
          internal_comparator.user_comparator(), kDisableGlobalSequenceNumber));
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());

      ParsedInternalKey parsed_key;
      ASSERT_OK(
          ParseInternalKey(iter->key(), &parsed_key, true /* log_err_key */));
      ASSERT_TRUE(parsed_key.user_key.starts_with("key"));
      ASSERT_OK(iter->status());
    }
  };

  std::unique_ptr<BlockBasedTable> sorted_table;
  std::vector<BlockHandle> sorted_handles;
  ASSERT_OK(CreateAndOpenSST(10, &sorted_table, &sorted_handles));
  ASSERT_GE(sorted_handles.size(), 5);
  sorted_handles.resize(5);

  SortCounters sorted_counters;
  install_callbacks(&sorted_counters);
  std::unique_ptr<IODispatcher> sorted_dispatcher(NewIODispatcher());
  submit_job(sorted_dispatcher.get(), sorted_table.get(), sorted_handles,
             true /* sorted */);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_EQ(sorted_counters.submit_job, 0);

  std::unique_ptr<BlockBasedTable> unsorted_table;
  std::vector<BlockHandle> unsorted_handles;
  ASSERT_OK(CreateAndOpenSST(10, &unsorted_table, &unsorted_handles));
  ASSERT_GE(unsorted_handles.size(), 5);
  unsorted_handles.resize(5);
  std::reverse(unsorted_handles.begin(), unsorted_handles.end());

  SortCounters unsorted_counters;
  install_callbacks(&unsorted_counters);
  std::unique_ptr<IODispatcher> unsorted_dispatcher(NewIODispatcher());
  submit_job(unsorted_dispatcher.get(), unsorted_table.get(), unsorted_handles,
             false /* sorted */);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_EQ(unsorted_counters.submit_job, 1);
}

// Test that verifies the read request offsets and lengths match the
// expected block handles.
TEST_F(IODispatcherTest, VerifyReadRequestDetails) {
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(10, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_NE(table, nullptr);
  ASSERT_GE(block_handles.size(), 5);

  tracking_fs_->ClearReadOps();

  // Use just a few non-adjacent blocks to avoid coalescing
  std::vector<BlockHandle> test_blocks;
  // Pick every other block to ensure they're not adjacent
  for (size_t i = 0; i < block_handles.size(); i += 2) {
    test_blocks.push_back(block_handles[i]);
  }

  auto job = std::make_shared<IOJob>();
  job->block_handles = test_blocks;
  job->table = table.get();
  job->job_options.read_options.async_io = false;
  // Small coalesce threshold to minimize coalescing for this test
  job->job_options.io_coalesce_threshold = 0;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);

  for (size_t i = 0; i < test_blocks.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
  }

  // Verify the read requests match the block handles
  auto read_ops = tracking_fs_->GetReadOps();
  std::unordered_set<uint64_t> expected_offsets;
  for (const auto& handle : test_blocks) {
    expected_offsets.insert(handle.offset());
  }

  std::unordered_set<uint64_t> actual_offsets;
  for (const auto& op : read_ops) {
    if (op.type == ReadOp::kMultiRead) {
      for (const auto& req : op.requests) {
        actual_offsets.insert(req.first);
      }
    }
  }

  // Verify all expected offsets were read
  for (const auto& expected : expected_offsets) {
    EXPECT_TRUE(actual_offsets.count(expected) > 0)
        << "Expected read at offset " << expected << " but it was not found";
  }
}

// Test that memory limiting blocks when the limit is exceeded
TEST_F(IODispatcherTest, MemoryLimitBlocksWhenExceeded) {
  // Create dispatcher with a small memory limit (1MB)
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 1 * 1024 * 1024;  // 1MB
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GT(block_handles.size(), 0);

  // Submit a job - should succeed immediately (non-blocking)
  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Read all blocks - they may be read synchronously if prefetch was deferred
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }
}

// Test that SubmitJob never blocks even when memory is exhausted
TEST_F(IODispatcherTest, SubmitJobNeverBlocks) {
  // Create dispatcher with a tiny memory limit
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 1024;  // 1KB - very small
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GT(block_handles.size(), 0);

  // Submit first job - uses up all memory
  auto job1 = std::make_shared<IOJob>();
  job1->block_handles = block_handles;
  job1->table = table.get();
  job1->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set1;
  s = dispatcher->SubmitJob(job1, &read_set1);
  ASSERT_OK(s);  // Should succeed immediately

  // Submit second job - should also succeed immediately (not block)
  std::unique_ptr<BlockBasedTable> table2;
  std::vector<BlockHandle> block_handles2;
  s = CreateAndOpenSST(30, &table2, &block_handles2);
  ASSERT_OK(s);

  auto job2 = std::make_shared<IOJob>();
  job2->block_handles = block_handles2;
  job2->table = table2.get();
  job2->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set2;
  s = dispatcher->SubmitJob(job2, &read_set2);
  ASSERT_OK(s);  // Should succeed immediately - prefetch is just deferred

  // Reads work - blocks are fetched synchronously on demand
  for (size_t i = 0; i < block_handles2.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set2->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }
}

// Test that releasing blocks triggers pending prefetches
TEST_F(IODispatcherTest, BlockReleaseTriggersWaitingJob) {
  // Create dispatcher with a small memory limit
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 100 * 1024;  // 100KB
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(30, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GT(block_handles.size(), 0);

  // Submit first job
  auto job1 = std::make_shared<IOJob>();
  job1->block_handles = block_handles;
  job1->table = table.get();
  job1->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set1;
  s = dispatcher->SubmitJob(job1, &read_set1);
  ASSERT_OK(s);
  ASSERT_NE(read_set1, nullptr);

  // Read all blocks from first job
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set1->ReadIndex(i, &block);
    ASSERT_OK(read_status);
  }

  // Submit second job - prefetch will be deferred due to memory limit
  std::unique_ptr<BlockBasedTable> table2;
  std::vector<BlockHandle> block_handles2;
  s = CreateAndOpenSST(20, &table2, &block_handles2);
  ASSERT_OK(s);

  auto job2 = std::make_shared<IOJob>();
  job2->block_handles = block_handles2;
  job2->table = table2.get();
  job2->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set2;
  s = dispatcher->SubmitJob(job2, &read_set2);
  ASSERT_OK(s);  // Should succeed immediately
  ASSERT_NE(read_set2, nullptr);

  // Release blocks from first job - this should trigger pending prefetches
  for (size_t i = 0; i < block_handles.size(); ++i) {
    read_set1->ReleaseBlock(i);
  }

  // Read all blocks from second job - should work
  for (size_t i = 0; i < block_handles2.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set2->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }
}

TEST_F(IODispatcherTest, DrainedSchedulerDoesNotStrandWaitingJob) {
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(20, &table, &block_handles, &table_options));
  ASSERT_GE(block_handles.size(), 2);

  size_t second_block = 1;
  const size_t budget =
      BlockBasedTable::BlockSizeWithTrailer(block_handles.front());
  while (second_block < block_handles.size() &&
         BlockBasedTable::BlockSizeWithTrailer(block_handles[second_block]) >
             budget) {
    ++second_block;
  }
  ASSERT_LT(second_block, block_handles.size());

  IODispatcherOptions options;
  options.max_prefetch_memory_bytes = budget;
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(options));

  auto first_job = std::make_shared<IOJob>();
  first_job->block_handles = {block_handles.front(),
                              block_handles[second_block]};
  first_job->table = table.get();
  first_job->job_options.read_options.async_io = false;
  first_job->job_options.block_handles_are_sorted = true;

  std::shared_ptr<ReadSet> first_read_set;
  ASSERT_OK(dispatcher->SubmitJob(first_job, &first_read_set));
  ASSERT_EQ(first_read_set->GetNumBlocksPrefetched(), 1);

  auto waiting_job = std::make_shared<IOJob>();
  waiting_job->block_handles = {block_handles.front()};
  waiting_job->table = table.get();
  waiting_job->job_options.read_options.async_io = false;
  waiting_job->job_options.block_handles_are_sorted = true;

  std::shared_ptr<ReadSet> waiting_read_set;
  ASSERT_OK(dispatcher->SubmitJob(waiting_job, &waiting_read_set));
  ASSERT_EQ(waiting_read_set->GetNumBlocksPrefetched(), 0);

  first_read_set->ReleaseBlock(1);
  first_read_set->ReleaseBlock(0);
  EXPECT_EQ(waiting_read_set->GetNumBlocksPrefetched(), 1);
}

// Test that multiple ReadSets share the memory budget
TEST_F(IODispatcherTest, MultipleReadSetsShareMemoryBudget) {
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 10 * 1024 * 1024;  // 10MB
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::vector<std::shared_ptr<ReadSet>> read_sets;
  std::vector<std::vector<BlockHandle>> all_block_handles;

  // Create and submit multiple jobs
  for (int i = 0; i < 3; i++) {
    std::unique_ptr<BlockBasedTable> table;
    std::vector<BlockHandle> block_handles;

    Status s = CreateAndOpenSST(20 + i * 5, &table, &block_handles);
    ASSERT_OK(s);

    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    job->job_options.read_options.async_io = false;
    tables_.push_back(std::move(table));

    all_block_handles.push_back(block_handles);
    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);
    read_sets.push_back(read_set);
  }

  // Verify all ReadSets can read their blocks
  for (size_t i = 0; i < read_sets.size(); ++i) {
    for (size_t j = 0; j < all_block_handles[i].size(); ++j) {
      CachableEntry<Block> block;
      Status read_status = read_sets[i]->ReadIndex(j, &block);
      ASSERT_OK(read_status);
      ASSERT_NE(block.GetValue(), nullptr);
    }
  }

  // Release all blocks from first ReadSet
  for (size_t i = 0; i < all_block_handles[0].size(); ++i) {
    read_sets[0]->ReleaseBlock(i);
  }

  // Create another job - should work because first ReadSet released memory
  std::unique_ptr<BlockBasedTable> table_new;
  std::vector<BlockHandle> block_handles_new;
  Status s = CreateAndOpenSST(25, &table_new, &block_handles_new);
  ASSERT_OK(s);

  auto job_new = std::make_shared<IOJob>();
  job_new->block_handles = block_handles_new;
  job_new->table = table_new.get();
  job_new->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set_new;
  s = dispatcher->SubmitJob(job_new, &read_set_new);
  ASSERT_OK(s);
  ASSERT_NE(read_set_new, nullptr);

  for (size_t i = 0; i < block_handles_new.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set_new->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }
}

// Test that no memory limiting is applied when max_prefetch_memory_bytes is 0
TEST_F(IODispatcherTest, NoMemoryLimitWhenZero) {
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 0;  // No limit
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(50, &table, &block_handles);
  ASSERT_OK(s);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }
}

// Test memory release on ReadSet destruction triggers pending prefetches
TEST_F(IODispatcherTest, MemoryReleasedOnReadSetDestruction) {
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 100 * 1024;  // 100KB
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  // Create table outside the scope so it outlives the ReadSet
  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(30, &table, &block_handles);
  ASSERT_OK(s);

  // Second table - created now so it's available after first ReadSet is
  // destroyed
  std::unique_ptr<BlockBasedTable> table2;
  std::vector<BlockHandle> block_handles2;
  s = CreateAndOpenSST(30, &table2, &block_handles2);
  ASSERT_OK(s);

  std::shared_ptr<ReadSet> read_set2;

  {
    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    job->job_options.read_options.async_io = false;

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);
    ASSERT_NE(read_set, nullptr);

    // Submit second job while first is still alive - prefetch will be deferred
    auto job2 = std::make_shared<IOJob>();
    job2->block_handles = block_handles2;
    job2->table = table2.get();
    job2->job_options.read_options.async_io = false;

    s = dispatcher->SubmitJob(job2, &read_set2);
    ASSERT_OK(s);  // Should succeed immediately
    ASSERT_NE(read_set2, nullptr);

    // First ReadSet goes out of scope here and should release all memory,
    // which triggers pending prefetches for second ReadSet
  }

  // Read all blocks from second job - should work because first ReadSet
  // released its memory on destruction
  for (size_t i = 0; i < block_handles2.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set2->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }
}

TEST_F(IODispatcherTest, ReadSetOutlivesDispatcher) {
  auto statistics = CreateDBStatistics();
  IODispatcherOptions options;
  options.max_prefetch_memory_bytes = 10 * 1024 * 1024;
  options.statistics = statistics.get();

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(20, &table, &block_handles));

  std::shared_ptr<ReadSet> read_set;
  {
    std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(options));
    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    job->job_options.read_options.async_io = false;
    ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  }

  const uint64_t released_before =
      statistics->getTickerCount(PREFETCH_MEMORY_BYTES_RELEASED);
  for (size_t block_index = 0; block_index < block_handles.size();
       ++block_index) {
    read_set->ReleaseBlock(block_index);
  }
  EXPECT_EQ(released_before,
            statistics->getTickerCount(PREFETCH_MEMORY_BYTES_RELEASED));

  for (size_t block_index = 0; block_index < block_handles.size();
       ++block_index) {
    CachableEntry<Block> block;
    ASSERT_OK(read_set->ReadIndex(block_index, &block));
    ASSERT_NE(block.GetValue(), nullptr);
  }
}

TEST_F(IODispatcherTest, RollingPrefetchChecksPrerequisites) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(12, &table, &block_handles));
  ASSERT_GE(block_handles.size(), 2);

  size_t rolling_dispatches = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RollingPrefetchScheduler::DispatchNext:WindowCount",
      [&](void* /*arg*/) { ++rolling_dispatches; });
  SyncPoint::GetInstance()->EnableProcessing();

  const uint64_t window_size =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]) +
      BlockBasedTable::BlockSizeWithTrailer(block_handles[1]);
  std::shared_ptr<ReadSet> read_set;

  auto job = MakeRollingPrefetchJob(table.get(), block_handles, window_size);
  job->job_options.read_options.async_io = false;
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  read_set.reset();

  job = MakeRollingPrefetchJob(table.get(), block_handles, window_size);
  job->job_options.block_handles_are_sorted = false;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  read_set.reset();

  IODispatcherOptions options;
  options.max_prefetch_memory_bytes = 1 << 30;
  dispatcher.reset(NewIODispatcher(options));
  job = MakeRollingPrefetchJob(table.get(), block_handles, window_size);
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  read_set.reset();

  EXPECT_EQ(rolling_dispatches, 0);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(IODispatcherTest, RollingPrefetchBatchesWithinByteBudget) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> all_block_handles;
  ASSERT_OK(CreateAndOpenSST(32, &table, &all_block_handles));
  ASSERT_GE(all_block_handles.size(), 16);

  std::vector<BlockHandle> block_handles;
  for (size_t i = 0; i < 16; i += 2) {
    block_handles.push_back(all_block_handles[i]);
  }
  const uint64_t byte_budget =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]) +
      BlockBasedTable::BlockSizeWithTrailer(block_handles[1]) +
      BlockBasedTable::BlockSizeWithTrailer(block_handles[2]);

  std::vector<size_t> dispatched_window_counts;
  SyncPoint::GetInstance()->SetCallBack(
      "RollingPrefetchScheduler::DispatchNext:WindowCount", [&](void* arg) {
        dispatched_window_counts.push_back(*static_cast<size_t*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto job = MakeRollingPrefetchJob(table.get(), block_handles, byte_budget);
  job->job_options.io_coalesce_threshold = 0;
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_EQ(dispatched_window_counts, std::vector<size_t>({3}));

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_EQ(dispatched_window_counts.size(), 2);
  EXPECT_GT(dispatched_window_counts.back(), 1);
  ASSERT_OK(read_set->ReadIndex(1, &block));
  EXPECT_EQ(dispatched_window_counts.size(), 2);

  for (size_t i = 2; i < block_handles.size(); ++i) {
    ASSERT_OK(read_set->ReadIndex(i, &block));
    ASSERT_NE(block.GetValue(), nullptr);
  }
  EXPECT_EQ(read_set->GetNumSyncReads(), 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(IODispatcherTest, RollingPrefetchDispatchesOversizedWindow) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(12, &table, &block_handles));
  ASSERT_GE(block_handles.size(), 2);

  std::vector<size_t> dispatched_window_counts;
  SyncPoint::GetInstance()->SetCallBack(
      "RollingPrefetchScheduler::DispatchNext:WindowCount", [&](void* arg) {
        dispatched_window_counts.push_back(*static_cast<size_t*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const uint64_t byte_budget =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]) - 1;
  auto job = MakeRollingPrefetchJob(table.get(), block_handles, byte_budget);
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  ASSERT_EQ(dispatched_window_counts, std::vector<size_t>({1}));

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  EXPECT_EQ(dispatched_window_counts, std::vector<size_t>({1, 1}));
  EXPECT_EQ(read_set->GetNumSyncReads(), 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(IODispatcherTest, RollingPrefetchGrowsAfterStall) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  auto controlled_fs = std::make_shared<ControlledAsyncFS>(base_fs_);
  controlled_fs->SetCompleteStrayFirst(false);
  controlled_fs->SetPollDelayMicros(1000);
  tracking_fs_ = controlled_fs;
  std::unique_ptr<Env> controlled_env = NewCompositeEnv(controlled_fs);

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(
      32, &table, &block_handles, nullptr /* table_options_override */,
      false /* use_direct_reads */, kNoCompression,
      false /* allow_mmap_reads */, controlled_env.get()));
  ASSERT_GE(block_handles.size(), 8);

  uint64_t window_size = 0;
  for (const auto& block_handle : block_handles) {
    window_size = std::max<uint64_t>(
        window_size, BlockBasedTable::BlockSizeWithTrailer(block_handle));
  }
  auto job = MakeRollingPrefetchJob(table.get(), block_handles, window_size,
                                    window_size * 4);
  job->job_options.io_coalesce_threshold = 0;

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  EXPECT_EQ(controlled_fs->GetReadAsyncCount(), 1);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  EXPECT_EQ(controlled_fs->GetReadAsyncCount(), 3);
  controlled_fs->CompletePendingHandles();
  ASSERT_OK(read_set->ReadIndex(1, &block));
  EXPECT_EQ(controlled_fs->GetReadAsyncCount(), 5);

  for (const auto& op : controlled_fs->GetReadOps()) {
    for (const auto& request : op.requests) {
      EXPECT_LE(request.second, window_size);
    }
  }
}

TEST_F(IODispatcherTest, RollingPrefetchStopsAfterBackwardRead) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  auto controlled_fs = std::make_shared<ControlledAsyncFS>(base_fs_);
  controlled_fs->SetCompleteStrayFirst(false);
  tracking_fs_ = controlled_fs;
  std::unique_ptr<Env> controlled_env = NewCompositeEnv(controlled_fs);

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(
      12, &table, &block_handles, nullptr /* table_options_override */,
      false /* use_direct_reads */, kNoCompression,
      false /* allow_mmap_reads */, controlled_env.get()));
  ASSERT_GE(block_handles.size(), 6);

  const uint64_t window_size =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]) +
      BlockBasedTable::BlockSizeWithTrailer(block_handles[1]);
  auto job = MakeRollingPrefetchJob(table.get(), block_handles, window_size);
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_OK(read_set->ReadIndex(2, &block));
  EXPECT_EQ(read_set->GetNumSyncReads(), 0);
  ASSERT_OK(read_set->ReadIndex(1, &block));
  ASSERT_OK(read_set->ReadIndex(block_handles.size() - 1, &block));
  EXPECT_EQ(read_set->GetNumSyncReads(), 1);
}

TEST_F(IODispatcherTest, RollingPrefetchRetriesPartialDispatch) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  auto controlled_fs = std::make_shared<ControlledAsyncFS>(base_fs_);
  controlled_fs->SetCompleteStrayFirst(false);
  tracking_fs_ = controlled_fs;
  std::unique_ptr<Env> controlled_env = NewCompositeEnv(controlled_fs);

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(
      12, &table, &block_handles, nullptr /* table_options_override */,
      false /* use_direct_reads */, kNoCompression,
      false /* allow_mmap_reads */, controlled_env.get()));
  ASSERT_GE(block_handles.size(), 6);

  size_t submissions = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadAsync:InjectStatus", [&](void* arg) {
        if (++submissions == 3) {
          *static_cast<IOStatus*>(arg) =
              IOStatus::Busy("injected partial dispatch failure");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const uint64_t window_size =
      block_handles[2].offset() +
      BlockBasedTable::BlockSizeWithTrailer(block_handles[2]) -
      block_handles[0].offset();
  auto job = MakeRollingPrefetchJob(table.get(), block_handles, window_size);
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
  std::shared_ptr<ReadSet> read_set;
  ASSERT_OK(dispatcher->SubmitJob(job, &read_set));
  read_set->ReleaseBlock(4);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_EQ(submissions, 3);
  ASSERT_OK(read_set->ReadIndex(3, &block));
  EXPECT_EQ(submissions, 5);
  ASSERT_OK(read_set->ReadIndex(5, &block));
  EXPECT_EQ(submissions, 6);
  EXPECT_EQ(read_set->GetNumSyncReads(), 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Test that partial prefetch dispatches as many blocks as memory allows
// and queues the rest for later dispatch
TEST_F(IODispatcherTest, PartialPrefetchDispatchesWhatFits) {
  // Skip this test if io_uring is not available since partial prefetch
  // only applies to async IO
  if (!kIOUringPresent) {
    return;  // io_uring not available, skip async IO test
  }

  // Create dispatcher with memory limit that allows only some blocks
  // Each block is ~16KB, so 50KB allows roughly 3 blocks
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 50 * 1024;  // 50KB
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  // Create 10 blocks - only ~3 should fit in memory
  Status s = CreateAndOpenSST(10, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 5);

  // Use sync point to count blocks dispatched during SubmitJob
  size_t blocks_dispatched_on_submit = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "IODispatcherImpl::DispatchPrefetch:BlockCount", [&](void* arg) {
        auto* indices = static_cast<std::vector<size_t>*>(arg);
        blocks_dispatched_on_submit += indices->size();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;  // Use async IO

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // With partial prefetch, we expect SOME blocks to have been dispatched
  // (the ones that fit in memory), but not ALL blocks
  // This is the key assertion: partial prefetch means > 0 blocks dispatched
  // even though total memory needed exceeds the limit
  EXPECT_GT(blocks_dispatched_on_submit, 0)
      << "Expected some blocks to be dispatched with partial prefetch";
  EXPECT_LT(blocks_dispatched_on_submit, block_handles.size())
      << "Expected not all blocks to be dispatched (memory limit should apply)";

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Now read all blocks - remaining blocks will be fetched on demand
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }

  // Verify all blocks were ultimately read
  uint64_t total_reads = read_set->GetNumSyncReads() +
                         read_set->GetNumAsyncReads() +
                         read_set->GetNumCacheHits();
  EXPECT_EQ(total_reads, block_handles.size());
}

// Test that earlier block indices are prioritized in partial prefetch
TEST_F(IODispatcherTest, PartialPrefetchPrioritizesEarlierIndices) {
  // Skip this test if io_uring is not available
  if (!kIOUringPresent) {
    return;  // io_uring not available, skip async IO test
  }

  // Create dispatcher with memory limit that allows only 1-2 blocks
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 20 * 1024;  // 20KB - room for ~1 block
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(10, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 5);

  tracking_fs_->ClearReadOps();

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);

  // Get the async reads that were dispatched
  auto read_ops = tracking_fs_->GetReadOps();

  // Find the offset of the first async read
  uint64_t first_async_offset = UINT64_MAX;
  for (const auto& op : read_ops) {
    if (op.type == ReadOp::kReadAsync && !op.requests.empty()) {
      first_async_offset = std::min(first_async_offset, op.requests[0].first);
    }
  }

  // The first async read should be for the first block (lowest offset)
  // This verifies that earlier indices are prioritized
  if (first_async_offset != UINT64_MAX) {
    EXPECT_EQ(first_async_offset, block_handles[0].offset())
        << "Expected first async read to be for the first block (earliest "
           "index)";
  }

  // Read all blocks to complete the test
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }
}

// Test that blocks larger than the memory budget are excluded from prefetch
// and fall back to synchronous read
TEST_F(IODispatcherTest, OversizedBlocksFallbackToSyncRead) {
  // Skip this test if io_uring is not available since we need async IO
  if (!kIOUringPresent) {
    return;
  }

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(10, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 3);

  // Calculate the size of a single block
  size_t single_block_size =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]);

  // Create dispatcher with memory limit smaller than a single block
  // This means ALL blocks are "oversized" and should fall back to sync read
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = single_block_size / 2;  // Half a block
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  // Track dispatches - with oversized blocks, nothing should be dispatched
  size_t blocks_dispatched = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "IODispatcherImpl::DispatchPrefetch:BlockCount", [&](void* arg) {
        auto* indices = static_cast<std::vector<size_t>*>(arg);
        blocks_dispatched += indices->size();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // No blocks should have been dispatched since they're all oversized
  EXPECT_EQ(blocks_dispatched, 0)
      << "Expected no blocks to be dispatched when all blocks are oversized";

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // All blocks should still be readable via sync fallback
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }

  // All reads should be sync since blocks couldn't be prefetched
  EXPECT_GT(read_set->GetNumSyncReads(), 0)
      << "Expected sync reads for oversized blocks";
}

// Test that reading blocks before prefetch dispatch correctly updates
// memory accounting for coalesced groups
TEST_F(IODispatcherTest, PartialReadsUpdateCoalescedGroups) {
  // Skip this test if io_uring is not available
  if (!kIOUringPresent) {
    return;
  }

  // Create dispatcher with memory limit that allows only some blocks
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 50 * 1024;  // 50KB
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(20, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 10);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Read some blocks directly (simulating on-demand access before prefetch)
  // This removes them from pending and should update coalesced group accounting
  for (size_t i = 0; i < 5 && i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
  }

  // Release the blocks we read - this frees memory
  for (size_t i = 0; i < 5 && i < block_handles.size(); ++i) {
    read_set->ReleaseBlock(i);
  }

  // Now read the remaining blocks - these should work correctly
  // The key test: memory accounting should be correct even though some blocks
  // were removed from pending groups before dispatch
  for (size_t i = 5; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status) << "Failed to read block " << i;
    ASSERT_NE(block.GetValue(), nullptr) << "Block " << i << " is null";
  }

  // Verify all remaining blocks were read successfully
  uint64_t total_reads = read_set->GetNumSyncReads() +
                         read_set->GetNumAsyncReads() +
                         read_set->GetNumCacheHits();
  // We read 5 blocks initially, then the remaining blocks
  EXPECT_GE(total_reads, block_handles.size() - 5)
      << "Expected at least the remaining blocks to be counted";
}

// Test that a mix of oversized and normal blocks works correctly
TEST_F(IODispatcherTest, MixedOversizedAndNormalBlocks) {
  // Skip this test if io_uring is not available
  if (!kIOUringPresent) {
    return;
  }

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(10, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 5);

  // Calculate the size of a typical block
  size_t typical_block_size =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]);

  // Create dispatcher with memory limit that allows exactly 2 typical blocks
  // This means groups of 3+ blocks become "oversized" as a group
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = typical_block_size * 2;
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // All blocks should be readable regardless of prefetch status
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status) << "Failed to read block " << i;
    ASSERT_NE(block.GetValue(), nullptr) << "Block " << i << " is null";
  }

  // Verify total reads match
  uint64_t total_reads = read_set->GetNumSyncReads() +
                         read_set->GetNumAsyncReads() +
                         read_set->GetNumCacheHits();
  EXPECT_EQ(total_reads, block_handles.size());
}

// Test that memory is properly accounted when groups are partially consumed
TEST_F(IODispatcherTest, MemoryAccountingWithPartialGroupConsumption) {
  // Skip this test if io_uring is not available
  if (!kIOUringPresent) {
    return;
  }

  // Create dispatcher with a specific memory limit
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 100 * 1024;  // 100KB
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(30, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 10);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // Verify that discarding blocks updates queued scheduling state before the
  // memory scheduler selects its next dispatch group.
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status) << "Failed to read block " << i;
    ASSERT_NE(block.GetValue(), nullptr) << "Block " << i << " is null";

    // Release the block immediately after reading
    read_set->ReleaseBlock(i);
  }

  // Verify total reads match
  uint64_t total_reads = read_set->GetNumSyncReads() +
                         read_set->GetNumAsyncReads() +
                         read_set->GetNumCacheHits();
  EXPECT_EQ(total_reads, block_handles.size());
}

// Test that sync prefetching respects memory limits
TEST_F(IODispatcherTest, SyncPrefetchWithMemoryLimit) {
  // Create dispatcher with a small memory limit
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 50 * 1024;  // 50KB
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(20, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 10);

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;  // Sync IO

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // All blocks should be readable even with memory limits
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status) << "Failed to read block " << i;
    ASSERT_NE(block.GetValue(), nullptr) << "Block " << i << " is null";
  }

  // Verify all were sync reads
  EXPECT_GT(read_set->GetNumSyncReads(), 0)
      << "Expected sync reads with async_io=false";
  EXPECT_EQ(read_set->GetNumAsyncReads(), 0)
      << "Expected no async reads with async_io=false";
}

// Test that oversized blocks work correctly with sync IO
TEST_F(IODispatcherTest, OversizedBlocksWithSyncIO) {
  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(10, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 3);

  // Calculate the size of a single block
  size_t single_block_size =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]);

  // Create dispatcher with memory limit smaller than a single block
  // This means ALL blocks are "oversized"
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = single_block_size / 2;  // Half a block
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;  // Sync IO

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // All blocks should still be readable via sync fallback
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status) << "Failed to read block " << i;
    ASSERT_NE(block.GetValue(), nullptr) << "Block " << i << " is null";
  }

  // All reads should be sync
  EXPECT_GT(read_set->GetNumSyncReads(), 0)
      << "Expected sync reads for oversized blocks";
}

// Test that a single block larger than total memory budget still works
TEST_F(IODispatcherTest, SingleBlockLargerThanTotalMemory) {
  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(5, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 1);

  // Set memory limit to 1 byte - smaller than any block
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = 1;
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  // Test with both sync and async modes
  for (bool async : {false, true}) {
    // Skip async if io_uring not available
    if (async && !kIOUringPresent) {
      continue;
    }

    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    job->job_options.read_options.async_io = async;

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s) << "SubmitJob failed with async=" << async;
    ASSERT_NE(read_set, nullptr);

    // All blocks should be readable
    for (size_t i = 0; i < block_handles.size(); ++i) {
      CachableEntry<Block> block;
      Status read_status = read_set->ReadIndex(i, &block);
      ASSERT_OK(read_status)
          << "Failed to read block " << i << " with async=" << async;
      ASSERT_NE(block.GetValue(), nullptr)
          << "Block " << i << " is null with async=" << async;
    }
  }
}

// Test that sync prefetching defers later groups and dispatches them
// when memory is released
TEST_F(IODispatcherTest, SyncPrefetchDefersAndDispatchesLaterGroups) {
  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  // Create 10+ blocks so we have enough to test deferred dispatch
  Status s = CreateAndOpenSST(20, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 10);

  // Calculate typical block size
  size_t typical_block_size =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]);

  // Set memory limit to fit approximately 3 blocks
  // This should cause groups to be split and some deferred
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = typical_block_size * 3;
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  // Track dispatch calls
  std::vector<size_t> dispatch_counts;
  SyncPoint::GetInstance()->SetCallBack(
      "IODispatcherImpl::DispatchPrefetch:BlockCount", [&](void* arg) {
        auto* indices = static_cast<std::vector<size_t>*>(arg);
        dispatch_counts.push_back(indices->size());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;  // Sync IO

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // After SubmitJob, some blocks should have been dispatched (first group)
  // and remaining groups should be queued
  size_t initial_dispatch_count = dispatch_counts.size();
  EXPECT_GT(initial_dispatch_count, 0)
      << "Expected at least one dispatch during SubmitJob";

  // Read and release first few blocks - this should trigger deferred dispatch
  for (size_t i = 0; i < 3 && i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    ASSERT_NE(block.GetValue(), nullptr);
    // Release to free memory
    read_set->ReleaseBlock(i);
  }

  // After releasing blocks, more dispatches should have occurred
  // as the pending queue gets processed
  size_t dispatch_count_after_release = dispatch_counts.size();
  EXPECT_GE(dispatch_count_after_release, initial_dispatch_count)
      << "Expected more dispatches after releasing blocks";

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // All remaining blocks should still be readable
  for (size_t i = 3; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status) << "Failed to read block " << i;
    ASSERT_NE(block.GetValue(), nullptr) << "Block " << i << " is null";
  }
}

// Test that coalesced groups are properly split based on memory budget
TEST_F(IODispatcherTest, CoalescedGroupsSplitByMemoryBudget) {
  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(15, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GE(block_handles.size(), 10);

  // Calculate typical block size
  size_t typical_block_size =
      BlockBasedTable::BlockSizeWithTrailer(block_handles[0]);

  // Set memory limit to fit exactly 5 blocks
  // With 10+ blocks, we should get at least 2 groups
  IODispatcherOptions opts;
  opts.max_prefetch_memory_bytes = typical_block_size * 5;
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

  // Track how many blocks are in each dispatch call
  std::vector<size_t> blocks_per_dispatch;
  SyncPoint::GetInstance()->SetCallBack(
      "IODispatcherImpl::DispatchPrefetch:BlockCount", [&](void* arg) {
        auto* indices = static_cast<std::vector<size_t>*>(arg);
        blocks_per_dispatch.push_back(indices->size());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto job = std::make_shared<IOJob>();
  job->block_handles = block_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = false;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);

  // First dispatch should have at most 5 blocks (memory limit)
  ASSERT_GT(blocks_per_dispatch.size(), 0);
  EXPECT_LE(blocks_per_dispatch[0], 5)
      << "First dispatch should be limited by memory budget";

  // Read and release all blocks to trigger remaining dispatches
  for (size_t i = 0; i < block_handles.size(); ++i) {
    CachableEntry<Block> block;
    Status read_status = read_set->ReadIndex(i, &block);
    ASSERT_OK(read_status);
    read_set->ReleaseBlock(i);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify each dispatch was limited by memory budget
  for (size_t i = 0; i < blocks_per_dispatch.size(); ++i) {
    EXPECT_LE(blocks_per_dispatch[i], 5)
        << "Dispatch " << i << " exceeded memory budget";
  }
}

// Regression tests for a bug where ReadIndex moved values out of
// pinned_blocks_ via std::move, but neither ReleaseBlock() nor the destructor
// released memory accounting because they checked pinned_blocks_.GetValue()
// which was null after the move.
// Tests run with both sync and async IO modes to cover Case 1 and Case 2
// in ReadIndex().
TEST_F(IODispatcherTest, MemoryReleasedAfterReadIndexThenReleaseBlock) {
  for (bool async : {false, true}) {
    // Skip async if io_uring not available
    if (async && !kIOUringPresent) {
      continue;
    }
    SCOPED_TRACE("async_io=" + std::to_string(async));

    auto stats = CreateDBStatistics();
    IODispatcherOptions opts;
    opts.max_prefetch_memory_bytes = 100 * 1024;  // 100KB
    opts.statistics = stats.get();
    std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

    std::unique_ptr<BlockBasedTable> table;
    std::vector<BlockHandle> block_handles;
    Status s = CreateAndOpenSST(20, &table, &block_handles);
    ASSERT_OK(s);
    ASSERT_GT(block_handles.size(), 0);

    auto job = std::make_shared<IOJob>();
    job->block_handles = block_handles;
    job->table = table.get();
    job->job_options.read_options.async_io = async;

    std::shared_ptr<ReadSet> read_set;
    s = dispatcher->SubmitJob(job, &read_set);
    ASSERT_OK(s);
    ASSERT_NE(read_set, nullptr);

    // Some memory should have been granted for prefetch
    ASSERT_GT(stats->getTickerCount(PREFETCH_MEMORY_BYTES_GRANTED), 0);

    // Read all blocks -- ReadIndex moves values out of pinned_blocks_.
    // This also lets the memory scheduler acquire and dispatch pending groups,
    // so granted grows during this loop.
    for (size_t i = 0; i < block_handles.size(); ++i) {
      CachableEntry<Block> block;
      ASSERT_OK(read_set->ReadIndex(i, &block));
      ASSERT_NE(block.GetValue(), nullptr);
    }

    // Release all blocks -- should be a no-op for memory accounting since
    // ReadIndex already released memory when moving values out
    for (size_t i = 0; i < block_handles.size(); ++i) {
      read_set->ReleaseBlock(i);
    }

    // Read both counters after all operations complete because ReadIndex may
    // have released memory that was then granted to pending groups.
    uint64_t granted = stats->getTickerCount(PREFETCH_MEMORY_BYTES_GRANTED);
    uint64_t released = stats->getTickerCount(PREFETCH_MEMORY_BYTES_RELEASED);
    // With the bug, released < granted because ReleaseBlock skips
    // ReleaseMemory when pinned_blocks_ value was already moved out
    EXPECT_EQ(released, granted);
  }
}

// Test that ReadSet destructor releases memory for blocks that were read
// via ReadIndex but never explicitly released via ReleaseBlock.
TEST_F(IODispatcherTest, DestructorReleasesMemoryAfterReadIndex) {
  for (bool async : {false, true}) {
    // Skip async if io_uring not available
    if (async && !kIOUringPresent) {
      continue;
    }
    SCOPED_TRACE("async_io=" + std::to_string(async));

    auto stats = CreateDBStatistics();
    IODispatcherOptions opts;
    opts.max_prefetch_memory_bytes = 100 * 1024;  // 100KB
    opts.statistics = stats.get();
    std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));

    std::unique_ptr<BlockBasedTable> table;
    std::vector<BlockHandle> block_handles;
    Status s = CreateAndOpenSST(20, &table, &block_handles);
    ASSERT_OK(s);
    ASSERT_GT(block_handles.size(), 0);

    {
      auto job = std::make_shared<IOJob>();
      job->block_handles = block_handles;
      job->table = table.get();
      job->job_options.read_options.async_io = async;

      std::shared_ptr<ReadSet> read_set;
      s = dispatcher->SubmitJob(job, &read_set);
      ASSERT_OK(s);
      ASSERT_NE(read_set, nullptr);

      uint64_t granted = stats->getTickerCount(PREFETCH_MEMORY_BYTES_GRANTED);
      ASSERT_GT(granted, 0);

      // Read all blocks via ReadIndex (moves values out of pinned_blocks_)
      // but do NOT call ReleaseBlock -- let the destructor handle cleanup
      for (size_t i = 0; i < block_handles.size(); ++i) {
        CachableEntry<Block> block;
        ASSERT_OK(read_set->ReadIndex(i, &block));
      }
      // read_set goes out of scope -- destructor should release all memory
    }

    uint64_t granted = stats->getTickerCount(PREFETCH_MEMORY_BYTES_GRANTED);
    uint64_t released = stats->getTickerCount(PREFETCH_MEMORY_BYTES_RELEASED);
    // Destructor should release memory for all prefetched blocks,
    // even those whose values were moved out by ReadIndex
    EXPECT_EQ(released, granted);
  }
}

// Verifies releasing one block from a coalesced async read removes only that
// block from the shared async state. The test releases block 0, reads block 1
// from the same in-flight request, and checks block 0 is not pinned later and
// the underlying request is not aborted.
TEST_F(IODispatcherTest, ReleaseBlockRemovesBlockFromCoalescedAsyncRead) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(20, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GT(block_handles.size(), 4);

  std::vector<BlockHandle> test_handles(block_handles.begin(),
                                        block_handles.begin() + 4);

  tracking_fs_->ClearReadOps();
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  auto job = std::make_shared<IOJob>();
  job->block_handles = test_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;
  job->job_options.io_coalesce_threshold = 1024 * 1024;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  if (tracking_fs_->GetReadAsyncCount() != 1 ||
      tracking_fs_->GetMultiReadCount() != 0) {
    ROCKSDB_GTEST_SKIP("Async IO not available (sync fallback used)");
    return;
  }

  auto* sync_point = SyncPoint::GetInstance();
  size_t abort_count = 0;
  sync_point->SetCallBack("ReadSet::ReleaseAsyncIOForBlock:AbortIO",
                          [&](void*) { abort_count++; });
  sync_point->EnableProcessing();

  read_set->ReleaseBlock(0);
  EXPECT_FALSE(read_set->IsBlockAvailable(0));
  EXPECT_EQ(abort_count, 0);

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(1, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_FALSE(read_set->IsBlockAvailable(0));
  EXPECT_EQ(abort_count, 0);

  sync_point->DisableProcessing();
  sync_point->ClearAllCallBacks();
}

// Verifies releasing the final block covered by a coalesced async read aborts
// and deletes the pending IO handle before the async state is released. The
// test releases all blocks in the request and counts the abort sync point.
TEST_F(IODispatcherTest, ReleaseLastBlockFromCoalescedAsyncReadAbortsIO) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(20, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GT(block_handles.size(), 4);

  std::vector<BlockHandle> test_handles(block_handles.begin(),
                                        block_handles.begin() + 4);

  tracking_fs_->ClearReadOps();
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  auto job = std::make_shared<IOJob>();
  job->block_handles = test_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;
  job->job_options.io_coalesce_threshold = 1024 * 1024;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  if (tracking_fs_->GetReadAsyncCount() != 1 ||
      tracking_fs_->GetMultiReadCount() != 0) {
    ROCKSDB_GTEST_SKIP("Async IO not available (sync fallback used)");
    return;
  }

  auto* sync_point = SyncPoint::GetInstance();
  size_t abort_count = 0;
  sync_point->SetCallBack("ReadSet::ReleaseAsyncIOForBlock:AbortIO",
                          [&](void*) { abort_count++; });
  sync_point->EnableProcessing();

  for (size_t i = 0; i + 1 < test_handles.size(); ++i) {
    read_set->ReleaseBlock(i);
    EXPECT_EQ(abort_count, 0);
  }

  read_set->ReleaseBlock(test_handles.size() - 1);
  EXPECT_EQ(abort_count, 1);

  sync_point->DisableProcessing();
  sync_point->ClearAllCallBacks();
}

// Uses a controlled async filesystem to force the old failure mode: after block
// 0 is released, a later Poll() for block 1 completes block 0's raw filesystem
// callback first. With the old ReleaseBlock() behavior, that callback reaches
// RandomAccessFileReader::ReadAsyncCallback after AsyncIOState is gone. The fix
// aborts block 0's handle during ReleaseBlock(), so Poll() must never see it.
TEST_F(IODispatcherTest, ReleasedDirectIOAsyncReadIsNotCompletedByLaterPoll) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  auto controlled_fs = std::make_shared<ControlledAsyncFS>(base_fs_);
  tracking_fs_ = controlled_fs;
  std::unique_ptr<Env> controlled_env = NewCompositeEnv(controlled_fs);

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(
      20, &table, &block_handles, nullptr /* table_options_override */,
      true /* use_direct_reads */, kNoCompression, false /* allow_mmap_reads */,
      controlled_env.get());
  ASSERT_OK(s);
  ASSERT_GT(block_handles.size(), 2);

  std::vector<BlockHandle> test_handles = {block_handles[0], block_handles[2]};

  tracking_fs_->ClearReadOps();
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  auto job = std::make_shared<IOJob>();
  job->block_handles = test_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;
  job->job_options.io_coalesce_threshold = 0;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);
  ASSERT_EQ(tracking_fs_->GetReadAsyncCount(), test_handles.size());
  ASSERT_EQ(tracking_fs_->GetMultiReadCount(), 0);

  read_set->ReleaseBlock(0);
  EXPECT_FALSE(read_set->IsBlockAvailable(0));

  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(1, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_EQ(controlled_fs->abort_count(), 1);
  EXPECT_EQ(controlled_fs->stray_completion_count(), 0);
}

// Simulates a MultiScan that reads one block from a first range, skips the
// remaining pending block in that range, and then seeks into a later range. The
// skipped block must be aborted before polling the later range so Poll() only
// drains completions for blocks still owned by the ReadSet.
TEST_F(IODispatcherTest, ReleasedRangeRemainderDoesNotBlockLaterAsyncRead) {
  if (!kIOUringPresent) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }

  auto controlled_fs = std::make_shared<ControlledAsyncFS>(base_fs_);
  tracking_fs_ = controlled_fs;
  std::unique_ptr<Env> controlled_env = NewCompositeEnv(controlled_fs);

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(
      30, &table, &block_handles, nullptr /* table_options_override */,
      true /* use_direct_reads */, kNoCompression, false /* allow_mmap_reads */,
      controlled_env.get());
  ASSERT_OK(s);
  ASSERT_GT(block_handles.size(), 6);

  // Logical ranges: [0, 1] and [2, 3]. Each block uses a separate async IO.
  std::vector<BlockHandle> test_handles = {block_handles[0], block_handles[2],
                                           block_handles[4], block_handles[6]};

  tracking_fs_->ClearReadOps();
  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  auto job = std::make_shared<IOJob>();
  job->block_handles = test_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;
  job->job_options.io_coalesce_threshold = 0;

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);
  ASSERT_EQ(tracking_fs_->GetReadAsyncCount(), test_handles.size());
  ASSERT_EQ(tracking_fs_->GetMultiReadCount(), 0);

  controlled_fs->SetCompleteStrayFirst(false);
  CachableEntry<Block> block;
  ASSERT_OK(read_set->ReadIndex(0, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_FALSE(read_set->IsBlockAvailable(0));
  EXPECT_EQ(controlled_fs->abort_count(), 0);
  EXPECT_EQ(controlled_fs->stray_completion_count(), 0);

  controlled_fs->SetCompleteStrayFirst(true);
  read_set->ReleaseBlock(1);
  EXPECT_FALSE(read_set->IsBlockAvailable(1));
  EXPECT_EQ(controlled_fs->abort_count(), 1);

  ASSERT_OK(read_set->ReadIndex(2, &block));
  ASSERT_NE(block.GetValue(), nullptr);
  EXPECT_EQ(controlled_fs->abort_count(), 1);
  EXPECT_EQ(controlled_fs->stray_completion_count(), 1);

  ASSERT_OK(read_set->ReadIndex(3, &block));
  ASSERT_NE(block.GetValue(), nullptr);
}

// Regression test: when ReadAsync returns NotSupported (e.g., io_uring
// unavailable at runtime due to seccomp), ExecuteAsyncIO must fall back to
// sync IO during SubmitJob. Without the fix, no MultiRead is issued during
// SubmitJob and ReadIndex later fetches each block individually via SyncRead.
TEST_F(IODispatcherTest, AsyncNotSupportedFallsBackToSync) {
  auto* sync_point = SyncPoint::GetInstance();
  struct SyncPointGuard {
    explicit SyncPointGuard(SyncPoint* sp) : sync_point(sp) {}
    ~SyncPointGuard() {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    }

    SyncPoint* sync_point;
  } sync_point_guard(sync_point);

  // Inject NotSupported into ReadAsync via SyncPoint
  sync_point->SetCallBack(
      "RandomAccessFileReader::ReadAsync:InjectStatus", [](void* arg) {
        auto* s = static_cast<IOStatus*>(arg);
        *s = IOStatus::NotSupported("simulated io_uring unavailability");
      });
  sync_point->EnableProcessing();

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  Status s = CreateAndOpenSST(20, &table, &block_handles);
  ASSERT_OK(s);
  ASSERT_GT(block_handles.size(), 4);

  tracking_fs_->ClearReadOps();

  // Use only the first 4 adjacent blocks
  std::vector<BlockHandle> test_handles(block_handles.begin(),
                                        block_handles.begin() + 4);
  const uint64_t expected_block_read_bytes =
      GetCoalescedReadBytes(test_handles);

  std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());

  auto job = std::make_shared<IOJob>();
  job->block_handles = test_handles;
  job->table = table.get();
  job->job_options.read_options.async_io = true;         // Request async IO
  job->job_options.io_coalesce_threshold = 1024 * 1024;  // Force coalescing

  SetPerfLevel(kEnableCount);
  get_perf_context()->Reset();

  std::shared_ptr<ReadSet> read_set;
  s = dispatcher->SubmitJob(job, &read_set);
  ASSERT_OK(s);
  ASSERT_NE(read_set, nullptr);

  // The sync fallback should happen during SubmitJob, using one coalesced
  // MultiRead for the adjacent blocks. Without the fix, no MultiRead happens
  // here and num_sync_reads remains zero until ReadIndex() issues one SyncRead
  // per block later.
  auto read_ops = tracking_fs_->GetReadOps();
  size_t multiread_count = 0;
  size_t total_requests_in_multireads = 0;
  for (const auto& op : read_ops) {
    if (op.type == ReadOp::kMultiRead) {
      multiread_count++;
      total_requests_in_multireads += op.requests.size();
    } else if (op.type == ReadOp::kReadAsync) {
      FAIL() << "ReadAsync should not reach the filesystem when NotSupported "
                "is injected";
    }
  }
  ASSERT_EQ(read_set->GetNumSyncReads(), test_handles.size());
  ASSERT_EQ(read_set->GetNumAsyncReads(), 0);
  ASSERT_EQ(multiread_count, 1);
  ASSERT_EQ(total_requests_in_multireads, 1);
  EXPECT_EQ(get_perf_context()->block_read_count, test_handles.size());
  EXPECT_EQ(get_perf_context()->block_read_byte, expected_block_read_bytes);
  EXPECT_EQ(get_perf_context()->data_block_read_byte,
            expected_block_read_bytes);

  // All blocks should then be readable from the prefetched sync fallback.
  for (size_t i = 0; i < test_handles.size(); ++i) {
    CachableEntry<Block> block;
    ASSERT_OK(read_set->ReadIndex(i, &block));
    ASSERT_NE(block.GetValue(), nullptr)
        << "Block " << i << " should be readable after sync fallback";
  }
}

TEST_F(IODispatcherTest,
       RegularIteratorIgnoresReadScopedProviderWithMmapReads) {
  TestReadScopedBlockBufferProvider provider;
  BlockBasedTableOptions table_options;
  table_options.block_cache = nullptr;
  table_options.block_size = 16 * 1024;
  table_options.no_block_cache = true;

  std::unique_ptr<BlockBasedTable> table;
  std::vector<BlockHandle> block_handles;
  ASSERT_OK(CreateAndOpenSST(8, &table, &block_handles, &table_options,
                             false /* use_direct_reads */, kNoCompression,
                             true /* allow_mmap_reads */));
  ASSERT_GT(block_handles.size(), 0);

  ReadOptions read_options;
  read_options.read_scoped_block_buffer_provider = &provider;
  std::unique_ptr<InternalIterator> iter(table->NewIterator(
      read_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(provider.allocations(), 0);

  iter.reset();
  EXPECT_EQ(provider.bytes_outstanding(), 0);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
