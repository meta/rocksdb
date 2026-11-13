//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_thread.h"

#include <chrono>
#include <thread>

#include "db/column_family.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

WriteThread::WriteThread(const ImmutableDBOptions& db_options)
    : max_yield_usec_(db_options.enable_write_thread_adaptive_yield
                          ? db_options.write_thread_max_yield_usec
                          : 0),
      slow_yield_usec_(db_options.write_thread_slow_yield_usec),
      allow_concurrent_memtable_write_(
          db_options.allow_concurrent_memtable_write),
      enable_pipelined_write_(db_options.enable_pipelined_write),
      max_write_batch_group_size_bytes(
          db_options.max_write_batch_group_size_bytes),
      newest_writer_(nullptr),
      newest_memtable_writer_(nullptr),
      last_sequence_(0),
      write_stall_dummy_(),
      stall_mu_(),
      stall_cv_(&stall_mu_) {}

uint8_t WriteThread::BlockingAwaitState(Writer* w, uint8_t goal_mask) {
  // We're going to block.  Lazily create the mutex.  We guarantee
  // propagation of this construction to the waker via the
  // STATE_LOCKED_WAITING state.  The waker won't try to touch the mutex
  // or the condvar unless they CAS away the STATE_LOCKED_WAITING that
  // we install below.
  w->CreateMutex();

  auto state = w->state.load(std::memory_order_acquire);
  assert(state != STATE_LOCKED_WAITING);
  if ((state & goal_mask) == 0 &&
      w->state.compare_exchange_strong(state, STATE_LOCKED_WAITING)) {
    // we have permission (and an obligation) to use StateMutex
    std::unique_lock<std::mutex> guard(w->StateMutex());
    w->StateCV().wait(guard, [w] {
      return w->state.load(std::memory_order_relaxed) != STATE_LOCKED_WAITING;
    });
    state = w->state.load(std::memory_order_relaxed);
  }
  // else tricky.  Goal is met or CAS failed.  In the latter case the waker
  // must have changed the state, and compare_exchange_strong has updated
  // our local variable with the new one.  At the moment WriteThread never
  // waits for a transition across intermediate states, so we know that
  // since a state change has occurred the goal must have been met.
  assert((state & goal_mask) != 0);
  return state;
}

uint8_t WriteThread::AwaitState(Writer* w, uint8_t goal_mask,
                                AdaptationContext* ctx) {
  uint8_t state = 0;

  // 1. Busy loop using "pause" for 1 micro sec
  // 2. Else SOMETIMES busy loop using "yield" for 100 micro sec (default)
  // 3. Else blocking wait

  // On a modern Xeon each loop takes about 7 nanoseconds (most of which
  // is the effect of the pause instruction), so 200 iterations is a bit
  // more than a microsecond.  This is long enough that waits longer than
  // this can amortize the cost of accessing the clock and yielding.
  for (uint32_t tries = 0; tries < 200; ++tries) {
    state = w->state.load(std::memory_order_acquire);
    if ((state & goal_mask) != 0) {
      return state;
    }
    port::AsmVolatilePause();
  }

  // This is below the fast path, so that the stat is zero when all writes are
  // from the same thread.
  PERF_TIMER_FOR_WAIT_GUARD(write_thread_wait_nanos);

  // If we're only going to end up waiting a short period of time,
  // it can be a lot more efficient to call std::this_thread::yield()
  // in a loop than to block in StateMutex().  For reference, on my 4.0
  // SELinux test server with support for syscall auditing enabled, the
  // minimum latency between FUTEX_WAKE to returning from FUTEX_WAIT is
  // 2.7 usec, and the average is more like 10 usec.  That can be a big
  // drag on RockDB's single-writer design.  Of course, spinning is a
  // bad idea if other threads are waiting to run or if we're going to
  // wait for a long time.  How do we decide?
  //
  // We break waiting into 3 categories: short-uncontended,
  // short-contended, and long.  If we had an oracle, then we would always
  // spin for short-uncontended, always block for long, and our choice for
  // short-contended might depend on whether we were trying to optimize
  // RocksDB throughput or avoid being greedy with system resources.
  //
  // Bucketing into short or long is easy by measuring elapsed time.
  // Differentiating short-uncontended from short-contended is a bit
  // trickier, but not too bad.  We could look for involuntary context
  // switches using getrusage(RUSAGE_THREAD, ..), but it's less work
  // (portability code and CPU) to just look for yield calls that take
  // longer than we expect.  sched_yield() doesn't actually result in any
  // context switch overhead if there are no other runnable processes
  // on the current core, in which case it usually takes less than
  // a microsecond.
  //
  // There are two primary tunables here: the threshold between "short"
  // and "long" waits, and the threshold at which we suspect that a yield
  // is slow enough to indicate we should probably block.  If these
  // thresholds are chosen well then CPU-bound workloads that don't
  // have more threads than cores will experience few context switches
  // (voluntary or involuntary), and the total number of context switches
  // (voluntary and involuntary) will not be dramatically larger (maybe
  // 2x) than the number of voluntary context switches that occur when
  // --max_yield_wait_micros=0.
  //
  // There's another constant, which is the number of slow yields we will
  // tolerate before reversing our previous decision.  Solitary slow
  // yields are pretty common (low-priority small jobs ready to run),
  // so this should be at least 2.  We set this conservatively to 3 so
  // that we can also immediately schedule a ctx adaptation, rather than
  // waiting for the next update_ctx.

  const size_t kMaxSlowYieldsWhileSpinning = 3;

  // Whether the yield approach has any credit in this context. The credit is
  // added by yield being succesfull before timing out, and decreased otherwise.
  auto& yield_credit = ctx->value;
  // Update the yield_credit based on sample runs or right after a hard failure
  bool update_ctx = false;
  // Should we reinforce the yield credit
  bool would_spin_again = false;
  // The samling base for updating the yeild credit. The sampling rate would be
  // 1/sampling_base.
  const int sampling_base = 256;

  if (max_yield_usec_ > 0) {
    update_ctx = Random::GetTLSInstance()->OneIn(sampling_base);

    if (update_ctx || yield_credit.load(std::memory_order_relaxed) >= 0) {
      // we're updating the adaptation statistics, or spinning has >
      // 50% chance of being shorter than max_yield_usec_ and causing no
      // involuntary context switches
      auto spin_begin = std::chrono::steady_clock::now();

      // this variable doesn't include the final yield (if any) that
      // causes the goal to be met
      size_t slow_yield_count = 0;

      auto iter_begin = spin_begin;
      while ((iter_begin - spin_begin) <=
             std::chrono::microseconds(max_yield_usec_)) {
        std::this_thread::yield();

        state = w->state.load(std::memory_order_acquire);
        if ((state & goal_mask) != 0) {
          // success
          would_spin_again = true;
          break;
        }

        auto now = std::chrono::steady_clock::now();
        if (now == iter_begin ||
            now - iter_begin >= std::chrono::microseconds(slow_yield_usec_)) {
          // conservatively count it as a slow yield if our clock isn't
          // accurate enough to measure the yield duration
          ++slow_yield_count;
          if (slow_yield_count >= kMaxSlowYieldsWhileSpinning) {
            // Not just one ivcsw, but several.  Immediately update yield_credit
            // and fall back to blocking
            update_ctx = true;
            break;
          }
        }
        iter_begin = now;
      }
    }
  }

  if ((state & goal_mask) == 0) {
    TEST_SYNC_POINT_CALLBACK("WriteThread::AwaitState:BlockingWaiting", w);
    state = BlockingAwaitState(w, goal_mask);
  }

  if (update_ctx) {
    // Since our update is sample based, it is ok if a thread overwrites the
    // updates by other threads. Thus the update does not have to be atomic.
    auto v = yield_credit.load(std::memory_order_relaxed);
    // fixed point exponential decay with decay constant 1/1024, with +1
    // and -1 scaled to avoid overflow for int32_t
    //
    // On each update the positive credit is decayed by a facor of 1/1024 (i.e.,
    // 0.1%). If the sampled yield was successful, the credit is also increased
    // by X. Setting X=2^17 ensures that the credit never exceeds
    // 2^17*2^10=2^27, which is lower than 2^31 the upperbound of int32_t. Same
    // logic applies to negative credits.
    v = v - (v / 1024) + (would_spin_again ? 1 : -1) * 131072;
    yield_credit.store(v, std::memory_order_relaxed);
  }

  assert((state & goal_mask) != 0);
  return state;
}

bool WriteThread::IsValidStateTransition(uint8_t old_state,
                                         uint8_t new_state) {
  if (new_state == STATE_INIT || new_state == STATE_LOCKED_WAITING) {
    return false;
  }

  if (old_state == new_state) {
    return false;
  }

  switch (old_state) {
    case STATE_INIT:
      return new_state == STATE_GROUP_LEADER ||
             new_state == STATE_MEMTABLE_WRITER_LEADER ||
             new_state == STATE_PARALLEL_MEMTABLE_CALLER ||
             new_state == STATE_PARALLEL_MEMTABLE_WRITER ||
             new_state == STATE_COMPLETED;
    case STATE_GROUP_LEADER:
      return new_state == STATE_MEMTABLE_WRITER_LEADER ||
             new_state == STATE_PARALLEL_MEMTABLE_WRITER ||
             new_state == STATE_COMPLETED;
    case STATE_MEMTABLE_WRITER_LEADER:
      return new_state == STATE_PARALLEL_MEMTABLE_CALLER ||
             new_state == STATE_PARALLEL_MEMTABLE_WRITER ||
             new_state == STATE_COMPLETED;
    case STATE_PARALLEL_MEMTABLE_CALLER:
      return new_state == STATE_PARALLEL_MEMTABLE_WRITER ||
             new_state == STATE_COMPLETED;
    case STATE_PARALLEL_MEMTABLE_WRITER:
      return new_state == STATE_COMPLETED;
    case STATE_LOCKED_WAITING:
      return new_state == STATE_GROUP_LEADER ||
             new_state == STATE_MEMTABLE_WRITER_LEADER ||
             new_state == STATE_PARALLEL_MEMTABLE_CALLER ||
             new_state == STATE_PARALLEL_MEMTABLE_WRITER ||
             new_state == STATE_COMPLETED;
    default:
      return false;
  }
}

void WriteThread::SetState(Writer* w, uint8_t new_state) {
  assert(w);
  auto state = w->state.load(std::memory_order_acquire);
  assert(IsValidStateTransition(state, new_state));
  if (state == STATE_LOCKED_WAITING ||
      !w->state.compare_exchange_strong(state, new_state)) {
    assert(state == STATE_LOCKED_WAITING);
    assert(IsValidStateTransition(state, new_state));

    std::lock_guard<std::mutex> guard(w->StateMutex());
    assert(w->state.load(std::memory_order_relaxed) != new_state);
    w->state.store(new_state, std::memory_order_relaxed);
    w->StateCV().notify_one();
  }
}

bool WriteThread::LinkOne(Writer* w, std::atomic<Writer*>* newest_writer) {
  assert(newest_writer != nullptr);
  assert(w->state == STATE_INIT);
  Writer* writers = newest_writer->load(std::memory_order_relaxed);
  while (true) {
    assert(writers != w);
    // If a write stall is in effect, fail fast when requested. Otherwise,
    // block here until the stall is cleared.
    if (writers == &write_stall_dummy_) {
      if (w->no_slowdown || w->non_blocking_join) {
        w->status = Status::Incomplete("Write stall");
        SetState(w, STATE_COMPLETED);
        return false;
      }
      // Since no_slowdown is false, wait here to be notified of the write
      // stall clearing
      {
        MutexLock lock(&stall_mu_);
        writers = newest_writer->load(std::memory_order_relaxed);
        if (writers == &write_stall_dummy_) {
          TEST_SYNC_POINT_CALLBACK("WriteThread::WriteStall::Wait", w);
          stall_cv_.Wait();
          // Load newest_writers_ again since it may have changed
          writers = newest_writer->load(std::memory_order_relaxed);
          continue;
        }
      }
    }
    w->link_older = writers;
    if (newest_writer->compare_exchange_weak(writers, w)) {
      return (writers == nullptr);
    }
  }
}

bool WriteThread::LinkGroup(WriteGroup& write_group,
                            std::atomic<Writer*>* newest_writer) {
  assert(newest_writer != nullptr);
  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;
  Writer* w = last_writer;
  while (true) {
    // Unset link_newer pointers to make sure when we call
    // CreateMissingNewerLinks later it create all missing links.
    w->link_newer = nullptr;
    w->write_group = nullptr;
    if (w == leader) {
      break;
    }
    w = w->link_older;
  }
  Writer* newest = newest_writer->load(std::memory_order_relaxed);
  while (true) {
    leader->link_older = newest;
    if (newest_writer->compare_exchange_weak(newest, last_writer)) {
      return (newest == nullptr);
    }
  }
}

void WriteThread::CreateMissingNewerLinks(Writer* head) {
  while (true) {
    Writer* next = head->link_older;
    if (next == nullptr || next->link_newer != nullptr) {
      assert(next == nullptr || next->link_newer == head);
      break;
    }
    next->link_newer = head;
    head = next;
  }
}

void WriteThread::CompleteLeader(WriteGroup& write_group) {
  assert(write_group.size > 0);
  Writer* leader = write_group.leader;
  if (write_group.size == 1) {
    write_group.leader = nullptr;
    write_group.last_writer = nullptr;
  } else {
    assert(leader->link_newer != nullptr);
    leader->link_newer->link_older = nullptr;
    write_group.leader = leader->link_newer;
  }
  write_group.size -= 1;
  SetState(leader, STATE_COMPLETED);
}

void WriteThread::CompleteFollower(Writer* w, WriteGroup& write_group) {
  assert(write_group.size > 1);
  assert(w != write_group.leader);
  if (w == write_group.last_writer) {
    w->link_older->link_newer = nullptr;
    write_group.last_writer = w->link_older;
  } else {
    w->link_older->link_newer = w->link_newer;
    w->link_newer->link_older = w->link_older;
  }
  write_group.size -= 1;
  SetState(w, STATE_COMPLETED);
}

void WriteThread::BeginWriteStall() {
  ++stall_begun_count_;
  LinkOne(&write_stall_dummy_, &newest_writer_);

  // Walk writer list until w->write_group != nullptr. The current write group
  // will not have a mix of slowdown/no_slowdown, so its ok to stop at that
  // point
  Writer* w = write_stall_dummy_.link_older;
  Writer* prev = &write_stall_dummy_;
  while (w != nullptr && w->write_group == nullptr) {
    // An oldest WBM flush writer already owns leadership. Unlike the existing
    // no_slowdown case, it must finish and hand off to the stall barrier;
    // unlinking it would strand newer writers.
    const bool is_leader = w->link_older == nullptr;
    if (w->no_slowdown || (!is_leader && w->non_blocking_join)) {
      prev->link_older = w->link_older;
      w->status = Status::Incomplete("Write stall");
      SetState(w, STATE_COMPLETED);
      // Only update `link_newer` if it's already set.
      // `CreateMissingNewerLinks()` will update the nullptr `link_newer` later,
      // which assumes the the first non-nullptr `link_newer` is the last
      // nullptr link in the writer list.
      // If `link_newer` is set here, `CreateMissingNewerLinks()` may stop
      // updating the whole list when it sees the first non nullptr link.
      if (prev->link_older && prev->link_older->link_newer) {
        prev->link_older->link_newer = prev;
      }
      w = prev->link_older;
    } else {
      prev = w;
      w = w->link_older;
    }
  }
}

void WriteThread::EndWriteStall() {
  MutexLock lock(&stall_mu_);

  // Unlink write_stall_dummy_ from the write queue. This will unblock
  // pending write threads to enqueue themselves
  assert(newest_writer_.load(std::memory_order_relaxed) == &write_stall_dummy_);
  // write_stall_dummy_.link_older can be nullptr only if LockWAL() has been
  // called.
  if (write_stall_dummy_.link_older) {
    write_stall_dummy_.link_older->link_newer = write_stall_dummy_.link_newer;
  }
  newest_writer_.exchange(write_stall_dummy_.link_older);

  ++stall_ended_count_;

  // Wake up writers
  stall_cv_.SignalAll();
}

uint64_t WriteThread::GetBegunCountOfOutstandingStall() {
  if (stall_begun_count_ > stall_ended_count_) {
    // Oustanding stall in queue
    assert(newest_writer_.load(std::memory_order_relaxed) ==
           &write_stall_dummy_);
    return stall_begun_count_;
  } else {
    // No stall in queue
    assert(newest_writer_.load(std::memory_order_relaxed) !=
           &write_stall_dummy_);
    return 0;
  }
}

void WriteThread::WaitForStallEndedCount(uint64_t stall_count) {
  MutexLock lock(&stall_mu_);

  while (stall_ended_count_ < stall_count) {
    stall_cv_.Wait();
  }
}

static WriteThread::AdaptationContext jbg_ctx("JoinBatchGroup");
void WriteThread::JoinBatchGroup(Writer* w) {
  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Start", w);
  assert(w->batch != nullptr);

  bool linked_as_leader = LinkOne(w, &newest_writer_);

  w->CheckWriteEnqueuedCallback();

  if (linked_as_leader) {
    SetState(w, STATE_GROUP_LEADER);
  }

  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Wait", w);
  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Wait2", w);

  if (!linked_as_leader) {
    /**
     * Wait util:
     * 1) An existing leader pick us as the new leader when it finishes
     * 2) An existing leader pick us as its follewer and
     * 2.1) finishes the memtable writes on our behalf
     * 2.2) Or tell us to finish the memtable writes in pralallel
     * 3) (pipelined write) An existing leader pick us as its follower and
     *    finish book-keeping and WAL write for us, enqueue us as pending
     *    memtable writer, and
     * 3.1) we become memtable writer group leader, or
     * 3.2) an existing memtable writer group leader tell us to finish memtable
     *      writes in parallel.
     */
    TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:BeganWaiting", w);
    AwaitState(w,
               STATE_GROUP_LEADER | STATE_MEMTABLE_WRITER_LEADER |
                   STATE_PARALLEL_MEMTABLE_CALLER |
                   STATE_PARALLEL_MEMTABLE_WRITER | STATE_COMPLETED,
               &jbg_ctx);
    TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:DoneWaiting", w);
  }
}

size_t WriteThread::EnterAsBatchGroupLeader(Writer* leader,
                                            WriteGroup* write_group) {
  assert(leader->link_older == nullptr);
  assert(leader->batch != nullptr);
  assert(write_group != nullptr);

  size_t size = WriteBatchInternal::ByteSize(leader->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = max_write_batch_group_size_bytes;
  const uint64_t min_batch_size_bytes = max_write_batch_group_size_bytes / 8;
  if (size <= min_batch_size_bytes) {
    max_size = size + min_batch_size_bytes;
  }

  leader->write_group = write_group;
  write_group->leader = leader;
  write_group->last_writer = leader;
  write_group->size = 1;
  Writer* newest_writer = newest_writer_.load(std::memory_order_acquire);

  // This is safe regardless of any db mutex status of the caller. Previous
  // calls to ExitAsGroupLeader either didn't call CreateMissingNewerLinks
  // (they emptied the list and then we added ourself as leader) or had to
  // explicitly wake us up (the list was non-empty when we added ourself,
  // so we have already received our MarkJoined).
  CreateMissingNewerLinks(newest_writer);

  // This comment illustrates how the rest of the function works using an
  // example. Notation:
  //
  // - Items are `Writer`s
  // - Items prefixed by "@" have been included in `write_group`
  // - Items prefixed by "*" have compatible options with `leader`, but have not
  //   been included in `write_group` yet
  // - Items after several spaces are in `r_list`. These have incompatible
  //   options with `leader` and are temporarily separated from the main list.
  //
  // Each line below depicts the state of the linked lists at the beginning of
  // an iteration of the while-loop.
  //
  // @leader, n1, *n2, n3, *newest_writer
  // @leader, *n2, n3, *newest_writer,    n1
  // @leader, @n2, n3, *newest_writer,    n1
  //
  // After the while-loop, the `r_list` is grafted back onto the main list.
  //
  // case A: no new `Writer`s arrived
  // @leader, @n2, @newest_writer,        n1, n3
  // @leader, @n2, @newest_writer, n1, n3
  //
  // case B: a new `Writer` (n4) arrived
  // @leader, @n2, @newest_writer, n4     n1, n3
  // @leader, @n2, @newest_writer, n1, n3, n4

  // Tricky. Iteration start (leader) is exclusive and finish
  // (newest_writer) is inclusive. Iteration goes from old to new.
  Writer* w = leader;
  // write_group end
  Writer* we = leader;
  // declare r_list
  Writer* rb = nullptr;
  Writer* re = nullptr;

  while (w != newest_writer) {
    assert(w->link_newer);
    w = w->link_newer;

    if ((w->sync && !leader->sync) ||
        // Do not include a sync write into a batch handled by a non-sync write.
        (w->no_slowdown != leader->no_slowdown) ||
        // Do not mix writes that are ok with delays with the ones that request
        // fail on delays.
        (w->disable_wal != leader->disable_wal) ||
        // Do not mix writes that enable WAL with the ones whose WAL disabled.
        (w->protection_bytes_per_key != leader->protection_bytes_per_key) ||
        // Do not mix writes with different levels of integrity protection.
        (w->rate_limiter_priority != leader->rate_limiter_priority) ||
        // Do not mix writes with different rate limiter priorities.
        (w->batch == nullptr) ||
        // Do not include those writes with nullptr batch. Those are not writes
        // those are something else. They want to be alone
        (w->callback != nullptr && !w->callback->AllowWriteBatching()) ||
        // dont batch writes that don't want to be batched
        (size + WriteBatchInternal::ByteSize(w->batch) > max_size) ||
        // Do not make batch too big
        (leader->ingest_wbwi || w->ingest_wbwi)
        // ingesting WBWI needs to be its own group
    ) {
      // remove from list
      w->link_older->link_newer = w->link_newer;
      if (w->link_newer != nullptr) {
        w->link_newer->link_older = w->link_older;
      }
      // insert into r_list
      if (re == nullptr) {
        rb = re = w;
        w->link_older = nullptr;
      } else {
        w->link_older = re;
        re->link_newer = w;
        re = w;
      }
    } else {
      // grow up
      we = w;
      w->write_group = write_group;
      size += WriteBatchInternal::ByteSize(w->batch);
      write_group->last_writer = w;
      write_group->size++;
    }
  }
  // append r_list after write_group end
  if (rb != nullptr) {
    rb->link_older = we;
    re->link_newer = nullptr;
    we->link_newer = rb;
    if (!newest_writer_.compare_exchange_weak(w, re)) {
      while (w->link_older != newest_writer) {
        w = w->link_older;
      }
      w->link_older = re;
    }
  }

  TEST_SYNC_POINT_CALLBACK("WriteThread::EnterAsBatchGroupLeader:End", w);
  return size;
}

void WriteThread::EnterAsMemTableWriter(Writer* leader,
                                        WriteGroup* write_group) {
  assert(leader != nullptr);
  assert(leader->link_older == nullptr);
  assert(leader->batch != nullptr);
  assert(write_group != nullptr);

  size_t size = WriteBatchInternal::ByteSize(leader->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = max_write_batch_group_size_bytes;
  const uint64_t min_batch_size_bytes = max_write_batch_group_size_bytes / 8;
  if (size <= min_batch_size_bytes) {
    max_size = size + min_batch_size_bytes;
  }

  leader->write_group = write_group;
  write_group->leader = leader;
  write_group->size = 1;
  Writer* last_writer = leader;

  if (!allow_concurrent_memtable_write_ || !leader->batch->HasMerge()) {
    Writer* newest_writer = newest_memtable_writer_.load();
    CreateMissingNewerLinks(newest_writer);

    Writer* w = leader;
    while (w != newest_writer) {
      assert(w->link_newer);
      w = w->link_newer;

      if (w->batch == nullptr) {
        break;
      }

      if (w->batch->HasMerge()) {
        break;
      }

      if (!allow_concurrent_memtable_write_) {
        auto batch_size = WriteBatchInternal::ByteSize(w->batch);
        if (size + batch_size > max_size) {
          // Do not make batch too big
          break;
        }
        size += batch_size;
      }

      w->write_group = write_group;
      last_writer = w;
      write_group->size++;
    }
  }

  write_group->last_writer = last_writer;
  write_group->last_sequence =
      last_writer->sequence + WriteBatchInternal::Count(last_writer->batch) - 1;
}

void WriteThread::ExitAsMemTableWriter(Writer* /*self*/,
                                       WriteGroup& write_group) {
  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;

  Writer* newest_writer = last_writer;
  if (!newest_memtable_writer_.compare_exchange_strong(newest_writer,
                                                       nullptr)) {
    CreateMissingNewerLinks(newest_writer);
    Writer* next_leader = last_writer->link_newer;
    assert(next_leader != nullptr);
    next_leader->link_older = nullptr;
    SetState(next_leader, STATE_MEMTABLE_WRITER_LEADER);
  }
  Writer* w = leader;
  while (true) {
    if (!write_group.status.ok()) {
      w->status = write_group.status;
    }
    Writer* next = w->link_newer;
    if (w != leader) {
      SetState(w, STATE_COMPLETED);
    }
    if (w == last_writer) {
      break;
    }
    assert(next);
    w = next;
  }
  // Note that leader has to exit last, since it owns the write group.
  SetState(leader, STATE_COMPLETED);
}

void WriteThread::SetMemWritersEachStride(Writer* w) {
  WriteGroup* write_group = w->write_group;
  Writer* last_writer = write_group->last_writer;

  // The stride is the same for each writer in write_group, so w will
  // call the writers with the same number in write_group mod total size
  size_t stride = static_cast<size_t>(std::sqrt(write_group->size));
  size_t count = 0;
  while (w) {
    if (count++ % stride == 0) {
      SetState(w, STATE_PARALLEL_MEMTABLE_WRITER);
    }
    w = (w == last_writer) ? nullptr : w->link_newer;
  }
}

void WriteThread::LaunchParallelMemTableWriters(WriteGroup* write_group) {
  assert(write_group != nullptr);
  size_t group_size = write_group->size;
  write_group->running.store(group_size);

  // The minimum number to allow the group use parallel caller mode.
  // The number must no lower than 3;
  const size_t MinParallelSize = 20;

  // The group_size is too small, and there is no need to have
  // the parallel partial callers.
  if (group_size < MinParallelSize) {
    for (auto w : *write_group) {
      SetState(w, STATE_PARALLEL_MEMTABLE_WRITER);
    }
    return;
  }

  // The stride is equal to std::sqrt(group_size) which can minimize
  // the total number of leader SetSate.
  // Set the leader itself STATE_PARALLEL_MEMTABLE_WRITER, and set
  // (stride-1) writers to be STATE_PARALLEL_MEMTABLE_CALLER.
  size_t stride = static_cast<size_t>(std::sqrt(group_size));
  auto w = write_group->leader;
  SetState(w, STATE_PARALLEL_MEMTABLE_WRITER);

  for (size_t i = 1; i < stride; i++) {
    w = w->link_newer;
    SetState(w, STATE_PARALLEL_MEMTABLE_CALLER);
  }

  // After setting all STATE_PARALLEL_MEMTABLE_CALLER, the leader also
  // does the job as STATE_PARALLEL_MEMTABLE_CALLER.
  w = w->link_newer;
  SetMemWritersEachStride(w);
}

static WriteThread::AdaptationContext cpmtw_ctx(
    "CompleteParallelMemTableWriter");
// This method is called by both the leader and parallel followers
bool WriteThread::CompleteParallelMemTableWriter(Writer* w) {
  auto* write_group = w->write_group;
  if (!w->status.ok()) {
    std::lock_guard<std::mutex> guard(write_group->leader->StateMutex());
    write_group->status = w->status;
  }

  if (write_group->running-- > 1) {
    // we're not the last one
    AwaitState(w, STATE_COMPLETED, &cpmtw_ctx);
    return false;
  }
  // else we're the last parallel worker and should perform exit duties.
  w->status = write_group->status;
  // Callers of this function must ensure w->status is checked.
  write_group->status.PermitUncheckedError();
  return true;
}

void WriteThread::ExitAsBatchGroupFollower(Writer* w) {
  auto* write_group = w->write_group;

  assert(w->state == STATE_PARALLEL_MEMTABLE_WRITER);
  assert(write_group->status.ok());
  ExitAsBatchGroupLeader(*write_group, write_group->status);
  assert(w->status.ok());
  assert(w->state == STATE_COMPLETED);
  SetState(write_group->leader, STATE_COMPLETED);
}

static WriteThread::AdaptationContext eabgl_ctx("ExitAsBatchGroupLeader");
void WriteThread::ExitAsBatchGroupLeader(WriteGroup& write_group,
                                         Status& status) {
  TEST_SYNC_POINT_CALLBACK("WriteThread::ExitAsBatchGroupLeader:Start",
                           &write_group);

  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;
  assert(leader->link_older == nullptr);

  // If status is non-ok already, then write_group.status won't have the chance
  // of being propagated to caller.
  if (!status.ok()) {
    write_group.status.PermitUncheckedError();
  }

  // Propagate memtable write error to the whole group.
  if (status.ok() && !write_group.status.ok()) {
    status = write_group.status;
  }

  if (enable_pipelined_write_) {
    // We insert a dummy Writer right before our current write_group. This
    // allows us to unlink our write_group without the risk that a subsequent
    // writer becomes a new leader and might overtake us and add itself to the
    // memtable-writer-list before we can do so. This ensures that writers are
    // added to the memtable-writer-list in the exact same order in which they
    // were in the newest_writer list.
    // This must happen before completing the writers from our group to prevent
    // a race where the owning thread of one of these writers can start a new
    // write operation.
    Writer dummy;
    Writer* head = newest_writer_.load(std::memory_order_acquire);
    if (head != last_writer ||
        !newest_writer_.compare_exchange_strong(head, &dummy)) {
      // Either last_writer wasn't the head during the load(), or it was the
      // head during the load() but somebody else pushed onto the list before
      // we did the compare_exchange_strong (causing it to fail). In the latter
      // case compare_exchange_strong has the effect of re-reading its first
      // param (head). No need to retry a failing CAS, because only a departing
      // leader (which we are at the moment) can remove nodes from the list.
      assert(head != last_writer);

      // After walking link_older starting from head (if not already done) we
      // will be able to traverse w->link_newer below.
      CreateMissingNewerLinks(head);
      assert(last_writer->link_newer != nullptr);
      last_writer->link_newer->link_older = &dummy;
      dummy.link_newer = last_writer->link_newer;
    }

    // Complete writers that don't write to memtable
    for (Writer* w = last_writer; w != leader;) {
      Writer* next = w->link_older;
      w->status = status;
      if (!w->ShouldWriteToMemtable()) {
        CompleteFollower(w, write_group);
      }
      w = next;
    }
    if (!leader->ShouldWriteToMemtable()) {
      CompleteLeader(write_group);
    }

    TEST_SYNC_POINT_CALLBACK(
        "WriteThread::ExitAsBatchGroupLeader:AfterCompleteWriters",
        &write_group);

    // Link the remaining of the group to memtable writer list.
    // We have to link our group to memtable writer queue before wake up the
    // next leader or set newest_writer_ to null, otherwise the next leader
    // can run ahead of us and link to memtable writer queue before we do.
    if (write_group.size > 0) {
      if (LinkGroup(write_group, &newest_memtable_writer_)) {
        // The leader can now be different from current writer.
        SetState(write_group.leader, STATE_MEMTABLE_WRITER_LEADER);
      }
    }

    // Unlink the dummy writer from the list and identify the new leader
    head = newest_writer_.load(std::memory_order_acquire);
    if (head != &dummy ||
        !newest_writer_.compare_exchange_strong(head, nullptr)) {
      CreateMissingNewerLinks(head);
      Writer* new_leader = dummy.link_newer;
      assert(new_leader != nullptr);
      new_leader->link_older = nullptr;
      SetState(new_leader, STATE_GROUP_LEADER);
    }

    AwaitState(leader,
               STATE_MEMTABLE_WRITER_LEADER | STATE_PARALLEL_MEMTABLE_CALLER |
                   STATE_PARALLEL_MEMTABLE_WRITER | STATE_COMPLETED,
               &eabgl_ctx);
  } else {
    Writer* head = newest_writer_.load(std::memory_order_acquire);
    if (head != last_writer ||
        !newest_writer_.compare_exchange_strong(head, nullptr)) {
      // Either last_writer wasn't the head during the load(), or it was the
      // head during the load() but somebody else pushed onto the list before
      // we did the compare_exchange_strong (causing it to fail).  In the
      // latter case compare_exchange_strong has the effect of re-reading
      // its first param (head).  No need to retry a failing CAS, because
      // only a departing leader (which we are at the moment) can remove
      // nodes from the list.
      assert(head != last_writer);

      // After walking link_older starting from head (if not already done)
      // we will be able to traverse w->link_newer below. This function
      // can only be called from an active leader, only a leader can
      // clear newest_writer_, we didn't, and only a clear newest_writer_
      // could cause the next leader to start their work without a call
      // to MarkJoined, so we can definitely conclude that no other leader
      // work is going on here (with or without db mutex).
      CreateMissingNewerLinks(head);
      assert(last_writer->link_newer != nullptr);
      assert(last_writer->link_newer->link_older == last_writer);
      last_writer->link_newer->link_older = nullptr;

      // Next leader didn't self-identify, because newest_writer_ wasn't
      // nullptr when they enqueued (we were definitely enqueued before them
      // and are still in the list).  That means leader handoff occurs when
      // we call MarkJoined
      SetState(last_writer->link_newer, STATE_GROUP_LEADER);
    }
    // else nobody else was waiting, although there might already be a new
    // leader now

    while (last_writer != leader) {
      assert(last_writer);
      last_writer->status = status;
      // we need to read link_older before calling SetState, because as soon
      // as it is marked committed the other thread's Await may return and
      // deallocate the Writer.
      auto next = last_writer->link_older;
      SetState(last_writer, STATE_COMPLETED);

      last_writer = next;
    }
  }
}

static WriteThread::AdaptationContext eu_ctx("EnterUnbatched");
void WriteThread::EnterUnbatched(Writer* w, InstrumentedMutex* mu) {
  assert(w != nullptr && w->batch == nullptr);
  mu->Unlock();
  bool linked_as_leader = LinkOne(w, &newest_writer_);
  if (!linked_as_leader) {
    TEST_SYNC_POINT("WriteThread::EnterUnbatched:Wait");
    // Last leader will not pick us as a follower since our batch is nullptr
    AwaitState(w, STATE_GROUP_LEADER, &eu_ctx);
  }
  if (enable_pipelined_write_) {
    WaitForMemTableWriters();
  }
  mu->Lock();
}

bool WriteThread::EnterUnbatchedNonBlocking(Writer* w, InstrumentedMutex* mu) {
  assert(w != nullptr && w->batch == nullptr);
  w->non_blocking_join = true;
  mu->Unlock();
  const bool linked_as_leader = LinkOne(w, &newest_writer_);
  if (!linked_as_leader) {
    TEST_SYNC_POINT("WriteThread::EnterUnbatched:Wait");
    // A new stall may sweep this queued writer and mark it completed.
    AwaitState(w, STATE_GROUP_LEADER | STATE_COMPLETED, &eu_ctx);
  }
  TEST_SYNC_POINT("WriteThread::EnterUnbatchedNonBlocking:BeforeLock");
  mu->Lock();
  w->non_blocking_join = false;
  if (w->state.load(std::memory_order_acquire) == STATE_COMPLETED) {
    assert(!w->status.ok());
    return false;
  }
  if (enable_pipelined_write_) {
    mu->Unlock();
    WaitForMemTableWriters();
    mu->Lock();
  }
  return true;
}

void WriteThread::ExitUnbatched(Writer* w) {
  assert(w != nullptr);
  Writer* newest_writer = w;
  if (!newest_writer_.compare_exchange_strong(newest_writer, nullptr)) {
    CreateMissingNewerLinks(newest_writer);
    Writer* next_leader = w->link_newer;
    assert(next_leader != nullptr);
    next_leader->link_older = nullptr;
    SetState(next_leader, STATE_GROUP_LEADER);
  }
}

static WriteThread::AdaptationContext wfmw_ctx("WaitForMemTableWriters");
void WriteThread::WaitForMemTableWriters() {
  assert(enable_pipelined_write_);
  if (newest_memtable_writer_.load() == nullptr) {
    return;
  }
  Writer w;
  if (!LinkOne(&w, &newest_memtable_writer_)) {
    AwaitState(&w, STATE_MEMTABLE_WRITER_LEADER, &wfmw_ctx);
  }
  newest_memtable_writer_.store(nullptr);
}

}  // namespace ROCKSDB_NAMESPACE
