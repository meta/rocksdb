//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).#pragma once
#pragma once

#if USE_COROUTINES
#include "file/random_access_file_reader.h"
#include "folly/coro/ViaIfAsync.h"
#include "port/port.h"
#include "rocksdb/file_system.h"
#include "rocksdb/statistics.h"
#include "util/autovector.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
class SingleThreadExecutor;

// AsyncFileReader implements the Awaitable concept, which allows calling
// coroutines to co_await it. When the AsyncFileReader Awaitable is
// resumed, it initiates the fie reads requested by the awaiting caller
// by calling RandomAccessFileReader's ReadAsync. It then suspends the
// awaiting coroutine. The suspended awaiter is later resumed by Wait().
class AsyncFileReader {
  class ReadAwaiter;
  template <typename Awaiter>
  class ReadOperation;

 public:
  AsyncFileReader(FileSystem* fs, Statistics* stats) : fs_(fs), stats_(stats) {}

  ~AsyncFileReader() = default;

  ReadOperation<ReadAwaiter> MultiReadAsync(RandomAccessFileReader* file,
                                            const IOOptions& opts,
                                            FSReadRequest* read_reqs,
                                            size_t num_reqs,
                                            IODebugContext* dbg) noexcept {
    return ReadOperation<ReadAwaiter>{*this,     file,     opts,
                                      read_reqs, num_reqs, dbg};
  }

 private:
  friend SingleThreadExecutor;

  // Implementation of the Awaitable concept
  class ReadAwaiter {
   public:
    explicit ReadAwaiter(AsyncFileReader& reader, RandomAccessFileReader* file,
                         const IOOptions& opts, FSReadRequest* read_reqs,
                         size_t num_reqs, IODebugContext* dbg) noexcept
        : reader_(reader),
          file_(file),
          opts_(opts),
          read_reqs_(read_reqs),
          num_reqs_(num_reqs),
          dbg_(dbg),
          next_(nullptr) {}

    bool await_ready() noexcept { return false; }

    // A return value of true means suspend the awaiter (calling coroutine). The
    // awaiting_coro parameter is the handle of the awaiter. The handle can be
    // resumed later, so we cache it here.
    bool await_suspend(folly::coro::coroutine_handle<> awaiting_coro) noexcept {
      awaiting_coro_ = awaiting_coro;
      // MultiReadAsyncImpl always returns true, so caller will be suspended
      return reader_.MultiReadAsyncImpl(this);
    }

    void await_resume() noexcept {}

   private:
    friend AsyncFileReader;

    // The parameters passed to MultiReadAsync are cached here when the caller
    // calls MultiReadAsync. Later, when the execution of this awaitable is
    // started, these are used to do the actual IO
    AsyncFileReader& reader_;
    RandomAccessFileReader* file_;
    const IOOptions& opts_;
    FSReadRequest* read_reqs_;
    size_t num_reqs_;
    IODebugContext* dbg_;
    autovector<void*, 32> io_handle_;
    autovector<IOHandleDeleter, 32> del_fn_;
    folly::coro::coroutine_handle<> awaiting_coro_;
    // Use this to link to the next ReadAwaiter in the suspended coroutine
    // list. The head and tail of the list are tracked by AsyncFileReader.
    // We use this approach rather than an STL container in order to avoid
    // extra memory allocations. The coroutine call already allocates a
    // ReadAwaiter object.
    ReadAwaiter* next_;
  };

  // An instance of ReadOperation is returned to the caller of MultiGetAsync.
  // This represents an awaitable that can be started later.
  template <typename Awaiter>
  class ReadOperation {
   public:
    explicit ReadOperation(AsyncFileReader& reader,
                           RandomAccessFileReader* file, const IOOptions& opts,
                           FSReadRequest* read_reqs, size_t num_reqs,
                           IODebugContext* dbg) noexcept
        : reader_(reader),
          file_(file),
          opts_(opts),
          read_reqs_(read_reqs),
          num_reqs_(num_reqs),
          dbg_(dbg) {}

    auto viaIfAsync(folly::Executor::KeepAlive<> executor) const {
      return folly::coro::co_viaIfAsync(
          std::move(executor),
          Awaiter{reader_, file_, opts_, read_reqs_, num_reqs_, dbg_});
    }

   private:
    AsyncFileReader& reader_;
    RandomAccessFileReader* file_;
    const IOOptions& opts_;
    FSReadRequest* read_reqs_;
    size_t num_reqs_;
    IODebugContext* dbg_;
  };

  // This function does the actual work when this awaitable starts execution
  bool MultiReadAsyncImpl(ReadAwaiter* awaiter);

  // Called by the SingleThreadExecutor to poll for async IO completion.
  // This also resumes the awaiting coroutines.
  void Wait();

  // Head of the queue of awaiters waiting for async IO completion
  ReadAwaiter* head_ = nullptr;
  // Tail of the awaiter queue
  ReadAwaiter* tail_ = nullptr;
  // Total number of pending async IOs
  size_t num_reqs_ = 0;
  FileSystem* fs_;
  Statistics* stats_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // USE_COROUTINES
