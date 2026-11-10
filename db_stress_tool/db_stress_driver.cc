//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include "db_stress_tool/db_stress_shared_state.h"
#ifdef GFLAGS
#include <mutex>

#include "db_stress_tool/db_stress_common.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
void ThreadBody(void* v) {
  ThreadStatusUtil::RegisterThread(raw_env, ThreadStatus::USER);
  ThreadState* thread = static_cast<ThreadState*>(v);
  SharedState* shared = thread->shared;

  if (!FLAGS_skip_verifydb && shared->ShouldVerifyAtBeginning()) {
    thread->shared->GetStressTest()->VerifyDb(thread);
  }
  {
    MutexLock l(shared->GetMutex());
    shared->IncInitialized();
    if (shared->AllInitialized()) {
      shared->GetCondVar()->SignalAll();
    }
  }
  if (!FLAGS_verification_only) {
    {
      MutexLock l(shared->GetMutex());
      while (!shared->Started()) {
        shared->GetCondVar()->Wait();
      }
    }
    thread->shared->GetStressTest()->OperateDb(thread);
    {
      MutexLock l(shared->GetMutex());
      shared->IncOperated();
      if (shared->AllOperated()) {
        shared->GetCondVar()->SignalAll();
      }
      while (!shared->VerifyStarted()) {
        shared->GetCondVar()->Wait();
      }
    }

    if (!FLAGS_skip_verifydb) {
      thread->shared->GetStressTest()->VerifyDb(thread);
    }

    {
      MutexLock l(shared->GetMutex());
      shared->IncDone();
      if (shared->AllDone()) {
        shared->GetCondVar()->SignalAll();
      }
    }
  }

  ThreadStatusUtil::UnregisterThread();
}
bool RunStressTestImpl(SharedState* shared) {
  SystemClock* clock = raw_env->GetSystemClock().get();
  StressTest* stress = shared->GetStressTest();
  const std::string db_label =
      (FLAGS_num_dbs > 1) ? "[" + stress->GetDbLabel() + "] " : "";

  if (shared->ShouldVerifyAtBeginning() && FLAGS_preserve_unverified_changes) {
    const auto& db_path = stress->GetDbPath();
    const auto& ev_dir = stress->GetExpectedValuesDir();
    Status s = InitUnverifiedSubdir(db_path);
    if (s.ok() && !ev_dir.empty()) {
      s = InitUnverifiedSubdir(ev_dir);
    }
    DB_STRESS_ASSERT_OK_MSG(s, "%sFailed to setup unverified state dir",
                            db_label.c_str());
  }

  stress->InitDb(shared);
  stress->FinishInitDb(shared);

  uint32_t n = FLAGS_threads;
  uint64_t now = clock->NowMicros();
  fprintf(stdout, "%s %sInitializing worker threads\n",
          clock->TimeToString(now / 1000000).c_str(), db_label.c_str());

  shared->SetThreads(n);

  const bool liveness_watchdog_enabled =
      FLAGS_liveness_check_interval_sec > 0 &&
      FLAGS_liveness_no_progress_timeout_sec > 0;
  if (liveness_watchdog_enabled) {
    shared->IncBgThreads();
  }

  if (FLAGS_continuous_verification_interval > 0) {
    shared->IncBgThreads();
  }

  if (FLAGS_tail_wal_updates) {
    shared->IncBgThreads();
  }

  uint32_t remote_compaction_worker_thread_count =
      FLAGS_remote_compaction_worker_threads;
  if (remote_compaction_worker_thread_count > 0) {
    for (uint32_t i = 0; i < remote_compaction_worker_thread_count; i++) {
      shared->IncBgThreads();
    }
  }

  std::vector<ThreadState*> threads(n);
  for (uint32_t i = 0; i < n; i++) {
    threads[i] = new ThreadState(i, shared);
    raw_env->StartThread(ThreadBody, threads[i]);
  }

  // Spawn at most one PoolSizeChangeThread globally. All DBs share the
  // same raw_env, so one thread resizing the pool is sufficient.
  // IncBgThreads inside call_once so only the launching DB's SharedState
  // tracks this thread (avoids deadlock for non-first DBs).
  ThreadState bg_thread(0, shared);
  static std::once_flag pool_size_thread_flag;
  if (FLAGS_compaction_thread_pool_adjust_interval > 0) {
    std::call_once(pool_size_thread_flag, [&]() {
      shared->IncBgThreads();
      raw_env->StartThread(PoolSizeChangeThread, &bg_thread);
    });
  }

  ThreadState continuous_verification_thread(0, shared);
  if (FLAGS_continuous_verification_interval > 0) {
    raw_env->StartThread(DbVerificationThread, &continuous_verification_thread);
  }

  // The WAL tailer is per-DB (it tails this DB's GetUpdatesSince stream), so it
  // is spawned once per RunStressTestImpl invocation like DbVerificationThread.
  ThreadState wal_tail_thread(0, shared);
  if (FLAGS_tail_wal_updates) {
    raw_env->StartThread(WalTailThread, &wal_tail_thread);
  }

  ThreadState liveness_watchdog_thread(0, shared);
  if (liveness_watchdog_enabled) {
    raw_env->StartThread(LivenessWatchdogThread, &liveness_watchdog_thread);
  }

  // Spawn at most one CompressedCacheSetCapacityThread globally. The cache
  // is shared across all DBs, and the thread's SetCapacity(0)/assert(==0)
  // sequence would race if multiple DBs each spawned their own copy.
  ThreadState compressed_cache_set_capacity_thread(0, shared);
  static std::once_flag compressed_cache_thread_flag;
  if (FLAGS_compressed_secondary_cache_size > 0 ||
      FLAGS_compressed_secondary_cache_ratio > 0.0) {
    std::call_once(compressed_cache_thread_flag, [&]() {
      shared->IncBgThreads();
      raw_env->StartThread(CompressedCacheSetCapacityThread,
                           &compressed_cache_set_capacity_thread);
    });
  }

  std::vector<ThreadState*> remote_compaction_worker_threads;
  if (remote_compaction_worker_thread_count > 0) {
    remote_compaction_worker_threads.reserve(
        remote_compaction_worker_thread_count);
    for (uint32_t i = 0; i < remote_compaction_worker_thread_count; i++) {
      ThreadState* ts = new ThreadState(i, shared);
      remote_compaction_worker_threads.push_back(ts);
      raw_env->StartThread(RemoteCompactionWorkerThread, ts);
    }
  }

  // Each thread goes through the following states:
  // initializing -> wait for others to init -> read/populate/depopulate
  // wait for others to operate -> verify -> done

  {
    MutexLock l(shared->GetMutex());
    while (!shared->AllInitialized()) {
      shared->GetCondVar()->Wait();
    }
    if (shared->ShouldVerifyAtBeginning()) {
      if (shared->HasVerificationFailedYet()) {
        fprintf(stderr, "%sCrash-recovery verification failed :(\n",
                db_label.c_str());
      } else {
        fprintf(stdout, "%sCrash-recovery verification passed :)\n",
                db_label.c_str());
        const auto& ev_dir = stress->GetExpectedValuesDir();
        Status s = DestroyUnverifiedSubdir(stress->GetDbPath());
        if (s.ok() && !ev_dir.empty()) {
          s = DestroyUnverifiedSubdir(ev_dir);
        }
        DB_STRESS_ASSERT_OK_MSG(s, "%sFailed to cleanup unverified state dir",
                                db_label.c_str());
      }
    }

    if (!FLAGS_verification_only) {
      // This is after the verification step to avoid making all those `Get()`s
      // and `MultiGet()`s contend on the DB-wide trace mutex.
      if (!stress->GetExpectedValuesDir().empty()) {
        stress->TrackExpectedState(shared);
      }

      if (FLAGS_sync_fault_injection || FLAGS_write_fault_one_in > 0) {
        auto fault_fs = stress->GetDbFaultInjectionFs();
        fault_fs->SetFilesystemDirectWritable(false);
        fault_fs->SetInjectUnsyncedDataLoss(FLAGS_sync_fault_injection);
        if (FLAGS_exclude_wal_from_write_fault_injection) {
          fault_fs->SetFileTypesExcludedFromWriteFaultInjection(
              {FileType::kWalFile});
        }
      }
      if (ShouldDisableAutoCompactionsBeforeVerifyDb()) {
        Status s = stress->EnableAutoCompaction();
        DB_STRESS_ASSERT_OK(s);
      }
      fprintf(stdout, "%s %sStarting database operations\n",
              clock->TimeToString(now / 1000000).c_str(), db_label.c_str());

      shared->SetStart();
      shared->GetCondVar()->SignalAll();
      while (!shared->AllOperated()) {
        shared->GetCondVar()->Wait();
      }

      now = clock->NowMicros();
      if (FLAGS_test_batches_snapshots) {
        fprintf(stdout, "%s %sLimited verification already done during gets\n",
                clock->TimeToString((uint64_t)now / 1000000).c_str(),
                db_label.c_str());
      } else if (FLAGS_skip_verifydb) {
        fprintf(stdout, "%s %sVerification skipped\n",
                clock->TimeToString((uint64_t)now / 1000000).c_str(),
                db_label.c_str());
      } else {
        fprintf(stdout, "%s %sStarting verification\n",
                clock->TimeToString((uint64_t)now / 1000000).c_str(),
                db_label.c_str());
      }

      shared->SetStartVerify();
      shared->GetCondVar()->SignalAll();
      while (!shared->AllDone()) {
        shared->GetCondVar()->Wait();
      }
    }
  }

  // If we are running verification_only
  // stats will be empty and trying to report them will
  // emit no ops or writes error. To avoid this, merging and reporting stats
  // are not executed when running with verification_only
  // TODO: We need to create verification stats (e.g. how many keys
  // are verified by which method) and report them here instead of operation
  // stats.
  if (!FLAGS_verification_only) {
    for (unsigned int i = 1; i < n; i++) {
      threads[0]->stats.Merge(threads[i]->stats);
    }
    threads[0]->stats.Report((db_label + "Stress Test").c_str());
  }

  for (unsigned int i = 0; i < n; i++) {
    delete threads[i];
    threads[i] = nullptr;
  }

  now = clock->NowMicros();
  if (!FLAGS_skip_verifydb && !FLAGS_test_batches_snapshots &&
      !shared->HasVerificationFailedYet()) {
    fprintf(stdout, "%s %sVerification successful\n",
            clock->TimeToString(now / 1000000).c_str(), db_label.c_str());
  }

  if (!FLAGS_verification_only) {
    stress->PrintStatistics();
  }

  if (FLAGS_compaction_thread_pool_adjust_interval > 0 ||
      FLAGS_continuous_verification_interval > 0 || liveness_watchdog_enabled ||
      FLAGS_compressed_secondary_cache_size > 0 ||
      FLAGS_compressed_secondary_cache_ratio > 0.0 || FLAGS_tail_wal_updates ||
      remote_compaction_worker_thread_count > 0) {
    MutexLock l(shared->GetMutex());
    shared->SetShouldStopBgThread();
    while (!shared->BgThreadsFinished()) {
      shared->GetCondVar()->Wait();
    }
  }
  now = clock->NowMicros();
  fprintf(stdout, "%s %sStress test bg threads finished\n",
          clock->TimeToString(now / 1000000).c_str(), db_label.c_str());

  assert(remote_compaction_worker_threads.size() ==
         remote_compaction_worker_thread_count);
  if (remote_compaction_worker_thread_count > 0) {
    for (ThreadState* thread_state : remote_compaction_worker_threads) {
      delete thread_state;
    }
    remote_compaction_worker_threads.clear();
  }

  if (shared->HasVerificationFailedYet()) {
    fprintf(stderr, "%sVerification failed :(\n", db_label.c_str());
    return false;
  }
  return true;
}
bool RunStressTest(SharedState* shared) {
  ThreadStatusUtil::RegisterThread(raw_env, ThreadStatus::USER);
  bool result = RunStressTestImpl(shared);
  ThreadStatusUtil::UnregisterThread();
  return result;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
