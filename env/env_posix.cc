//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#include "port/lang.h"
#if !defined(OS_WIN)

#include <dirent.h>
#ifndef ROCKSDB_NO_DYNAMIC_EXTENSION
#include <dlfcn.h>
#endif
#include <fcntl.h>

#include <cerrno>

#if defined(ROCKSDB_IOURING_PRESENT)
#include <liburing.h>
#endif
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#if defined(OS_LINUX) || defined(OS_ANDROID)
#include <sys/statfs.h>
#endif
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>
#if defined(ROCKSDB_IOURING_PRESENT)
#include <sys/uio.h>
#endif
#include <unistd.h>

#include <algorithm>
#include <ctime>
// Get nano time includes
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD)
#elif defined(__MACH__)
#include <Availability.h>
#include <mach/clock.h>
#include <mach/mach.h>
#else
#include <chrono>
#endif
#include <deque>
#include <set>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "env/io_posix.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "port/port.h"
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/compression_context_cache.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/thread_local.h"
#include "util/threadpool_imp.h"

#if !defined(TMPFS_MAGIC)
#define TMPFS_MAGIC 0x01021994
#endif
#if !defined(XFS_SUPER_MAGIC)
#define XFS_SUPER_MAGIC 0x58465342
#endif
#if !defined(EXT4_SUPER_MAGIC)
#define EXT4_SUPER_MAGIC 0xEF53
#endif

namespace ROCKSDB_NAMESPACE {
#if defined(OS_WIN)
static const std::string kSharedLibExt = ".dll";
[[maybe_unused]] static const char kPathSeparator = ';';
#else
[[maybe_unused]] static const char kPathSeparator = ':';
#if defined(OS_MACOSX)
static const std::string kSharedLibExt = ".dylib";
#else
static const std::string kSharedLibExt = ".so";
#endif
#endif

namespace {

ThreadStatusUpdater* CreateThreadStatusUpdater() {
  return new ThreadStatusUpdater();
}

#ifndef ROCKSDB_NO_DYNAMIC_EXTENSION
class PosixDynamicLibrary : public DynamicLibrary {
 public:
  PosixDynamicLibrary(const std::string& name, void* handle)
      : name_(name), handle_(handle) {}
  ~PosixDynamicLibrary() override { dlclose(handle_); }

  Status LoadSymbol(const std::string& sym_name, void** func) override {
    assert(nullptr != func);
    dlerror();  // Clear any old error
    *func = dlsym(handle_, sym_name.c_str());
    if (*func != nullptr) {
      return Status::OK();
    } else {
      char* err = dlerror();
      return Status::NotFound("Error finding symbol: " + sym_name, err);
    }
  }

  const char* Name() const override { return name_.c_str(); }

 private:
  std::string name_;
  void* handle_;
};
#endif  // !ROCKSDB_NO_DYNAMIC_EXTENSION

class PosixClock : public SystemClock {
 public:
  static const char* kClassName() { return "PosixClock"; }
  const char* Name() const override { return kDefaultName(); }
  const char* NickName() const override { return kClassName(); }

  uint64_t NowMicros() override {
    port::TimeVal tv;
    port::GetTimeOfDay(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  uint64_t NowNanos() override {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD) || \
    defined(OS_AIX)
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#elif defined(__MACH__)
    clock_serv_t cclock;
    mach_timespec_t ts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &ts);
    mach_port_deallocate(mach_task_self(), cclock);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#else
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
#endif
  }

  uint64_t CPUMicros() override {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD) || \
    defined(OS_AIX) || (defined(__MACH__) && defined(__MAC_10_12))
    struct timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec) / 1000;
#else
    return 0;
#endif
  }

  uint64_t CPUNanos() override {
#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD) || \
    defined(OS_AIX) || (defined(__MACH__) && defined(__MAC_10_12))
    struct timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#else
    return 0;
#endif
  }

  void SleepForMicroseconds(int micros) override { usleep(micros); }

  Status GetCurrentTime(int64_t* unix_time) override {
    time_t ret = time(nullptr);
    if (ret == (time_t)-1) {
      return IOError("GetCurrentTime", "", errno);
    }
    *unix_time = (int64_t)ret;
    return Status::OK();
  }

  std::string TimeToString(uint64_t secondsSince1970) override {
    const time_t seconds = (time_t)secondsSince1970;
    struct tm t;
    int maxsize = 64;
    std::string dummy;
    dummy.reserve(maxsize);
    dummy.resize(maxsize);
    char* p = dummy.data();
    port::LocalTimeR(&seconds, &t);
    snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900,
             t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
    return dummy;
  }
};

class PosixEnv : public CompositeEnv {
 public:
  static const char* kClassName() { return "PosixEnv"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kDefaultName(); }

  struct JoinThreadsOnExit {
    explicit JoinThreadsOnExit(PosixEnv& _deflt) : deflt(_deflt) {}
    // No copying allowed
    JoinThreadsOnExit(const JoinThreadsOnExit&) = delete;
    JoinThreadsOnExit& operator=(const JoinThreadsOnExit&) = delete;
    JoinThreadsOnExit(JoinThreadsOnExit&&) = delete;
    JoinThreadsOnExit& operator=(JoinThreadsOnExit&&) = delete;
    ~JoinThreadsOnExit() {
      for (const auto tid : deflt.threads_to_join_) {
        pthread_join(tid, nullptr);
      }
      for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
        deflt.thread_pools_[pool_id].JoinAllThreads();
      }
      // Do not delete the thread_status_updater_ in order to avoid the
      // free after use when Env::Default() is destructed while some other
      // child threads are still trying to update thread status. All
      // PosixEnv instances use the same thread_status_updater_, so never
      // explicitly delete it.
    }
    PosixEnv& deflt;
  };

#ifndef ROCKSDB_NO_DYNAMIC_EXTENSION
  // Loads the named library into the result.
  // If the input name is empty, the current executable is loaded
  // On *nix systems, a "lib" prefix is added to the name if one is not supplied
  // Comparably, the appropriate shared library extension is added to the name
  // if not supplied. If search_path is not specified, the shared library will
  // be loaded using the default path (LD_LIBRARY_PATH) If search_path is
  // specified, the shared library will be searched for in the directories
  // provided by the search path
  Status LoadLibrary(const std::string& name, const std::string& path,
                     std::shared_ptr<DynamicLibrary>* result) override {
    assert(result != nullptr);
    if (name.empty()) {
      void* hndl = dlopen(NULL, RTLD_NOW);
      if (hndl != nullptr) {
        result->reset(new PosixDynamicLibrary(name, hndl));
        return Status::OK();
      }
    } else {
      std::string library_name = name;
      if (library_name.find(kSharedLibExt) == std::string::npos) {
        library_name = library_name + kSharedLibExt;
      }
#if !defined(OS_WIN)
      if (library_name.find('/') == std::string::npos &&
          library_name.compare(0, 3, "lib") != 0) {
        library_name = "lib" + library_name;
      }
#endif
      if (path.empty()) {
        void* hndl = dlopen(library_name.c_str(), RTLD_NOW);
        if (hndl != nullptr) {
          result->reset(new PosixDynamicLibrary(library_name, hndl));
          return Status::OK();
        }
      } else {
        std::string local_path;
        std::stringstream ss(path);
        while (getline(ss, local_path, kPathSeparator)) {
          if (!path.empty()) {
            std::string full_name = local_path + "/" + library_name;
            void* hndl = dlopen(full_name.c_str(), RTLD_NOW);
            if (hndl != nullptr) {
              result->reset(new PosixDynamicLibrary(full_name, hndl));
              return Status::OK();
            }
          }
        }
      }
    }
    return Status::IOError(
        IOErrorMsg("Failed to open shared library: xs", name), dlerror());
  }
#endif  // !ROCKSDB_NO_DYNAMIC_EXTENSION

  void Schedule(void (*function)(void* arg1), void* arg, Priority pri = LOW,
                void* tag = nullptr,
                void (*unschedFunction)(void* arg) = nullptr) override;

  int UnSchedule(void* arg, Priority pri) override;

  void StartThread(void (*function)(void* arg), void* arg) override;

  void WaitForJoin() override;

  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override;

  int ReserveThreads(int threads_to_be_reserved, Priority pri) override;

  int ReleaseThreads(int threads_to_be_released, Priority pri) override;

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    assert(thread_status_updater_);
    return thread_status_updater_->GetThreadList(thread_list);
  }

  uint64_t GetThreadID() const override {
    uint64_t thread_id = 0;
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 30)
    thread_id = ::gettid();
#else   // __GLIBC_PREREQ(2, 30)
    pthread_t tid = pthread_self();
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
#endif  // __GLIBC_PREREQ(2, 30)
#else   // defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
    pthread_t tid = pthread_self();
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
#endif  // defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
    return thread_id;
  }

  Status GetHostName(char* name, uint64_t len) override {
    const size_t max_len = static_cast<size_t>(len);
    int ret = gethostname(name, max_len);
    if (ret < 0) {
      if (errno == EFAULT || errno == EINVAL) {
        return Status::InvalidArgument(errnoStr(errno).c_str());
      } else if (errno == ENAMETOOLONG) {
        return IOError("GetHostName", std::string(name, strnlen(name, max_len)),
                       errno);
      } else {
        return IOError("GetHostName", "", errno);
      }
    }
    return Status::OK();
  }

  ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    return Env::GetThreadStatusUpdater();
  }

  std::string GenerateUniqueId() override { return Env::GenerateUniqueId(); }

  // Allow increasing the number of worker threads.
  void SetBackgroundThreads(int num, Priority pri) override {
    assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
    thread_pools_[pri].SetBackgroundThreads(num);
  }

  int GetBackgroundThreads(Priority pri) override {
    assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
    return thread_pools_[pri].GetBackgroundThreads();
  }

  Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override {
    allow_non_owner_access_ = allow_non_owner_access;
    return Status::OK();
  }

  // Allow increasing the number of worker threads.
  void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
    thread_pools_[pri].IncBackgroundThreadsIfNeeded(num);
  }

  void LowerThreadPoolIOPriority(Priority pool) override {
    assert(pool >= Priority::BOTTOM && pool <= Priority::HIGH);
#ifdef OS_LINUX
    thread_pools_[pool].LowerIOPriority();
#else
    (void)pool;
#endif
  }

  void LowerThreadPoolCPUPriority(Priority pool) override {
    assert(pool >= Priority::BOTTOM && pool <= Priority::HIGH);
    thread_pools_[pool].LowerCPUPriority(CpuPriority::kLow);
  }

  Status LowerThreadPoolCPUPriority(Priority pool, CpuPriority pri) override {
    assert(pool >= Priority::BOTTOM && pool <= Priority::HIGH);
    thread_pools_[pool].LowerCPUPriority(pri);
    return Status::OK();
  }

 private:
  friend Env* Env::Default();
  // Constructs the default Env, a singleton
  PosixEnv();

  // The below 4 members are only used by the default PosixEnv instance.
  // Non-default instances simply maintain references to the backing
  // members in te default instance
  std::vector<ThreadPoolImpl> thread_pools_storage_;
  pthread_mutex_t mu_storage_;
  std::vector<pthread_t> threads_to_join_storage_;
  bool allow_non_owner_access_storage_;

  std::vector<ThreadPoolImpl>& thread_pools_;
  pthread_mutex_t& mu_;
  std::vector<pthread_t>& threads_to_join_;
  // If true, allow non owner read access for db files. Otherwise, non-owner
  //  has no access to db files.
  bool& allow_non_owner_access_;
};

PosixEnv::PosixEnv()
    : CompositeEnv(FileSystem::Default(), SystemClock::Default()),
      thread_pools_storage_(Priority::TOTAL),
      allow_non_owner_access_storage_(true),
      thread_pools_(thread_pools_storage_),
      mu_(mu_storage_),
      threads_to_join_(threads_to_join_storage_),
      allow_non_owner_access_(allow_non_owner_access_storage_) {
  ThreadPoolImpl::PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
  for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
    thread_pools_[pool_id].SetThreadPriority(
        static_cast<Env::Priority>(pool_id));
    // This allows later initializing the thread-local-env of each thread.
    thread_pools_[pool_id].SetHostEnv(this);
  }
  thread_status_updater_ = CreateThreadStatusUpdater();
}

void PosixEnv::Schedule(void (*function)(void* arg1), void* arg, Priority pri,
                        void* tag, void (*unschedFunction)(void* arg)) {
  assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
  thread_pools_[pri].Schedule(function, arg, tag, unschedFunction);
}

int PosixEnv::UnSchedule(void* arg, Priority pri) {
  return thread_pools_[pri].UnSchedule(arg);
}

unsigned int PosixEnv::GetThreadPoolQueueLen(Priority pri) const {
  assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
  return thread_pools_[pri].GetQueueLen();
}

int PosixEnv::ReserveThreads(int threads_to_reserved, Priority pri) {
  assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
  return thread_pools_[pri].ReserveThreads(threads_to_reserved);
}

int PosixEnv::ReleaseThreads(int threads_to_released, Priority pri) {
  assert(pri >= Priority::BOTTOM && pri <= Priority::HIGH);
  return thread_pools_[pri].ReleaseThreads(threads_to_released);
}

struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};

static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = static_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return nullptr;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  ThreadPoolImpl::PthreadCall(
      "start thread", pthread_create(&t, nullptr, &StartThreadWrapper, state));
  ThreadPoolImpl::PthreadCall("lock", pthread_mutex_lock(&mu_));
  threads_to_join_.push_back(t);
  ThreadPoolImpl::PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::WaitForJoin() {
  for (const auto tid : threads_to_join_) {
    pthread_join(tid, nullptr);
  }
  threads_to_join_.clear();
}

}  // namespace

//
// Default Posix Env
//
Env* Env::Default() {
  // The following function call initializes the singletons of ThreadLocalPtr
  // right before the static default_env.  This guarantees default_env will
  // always being destructed before the ThreadLocalPtr singletons get
  // destructed as C++ guarantees that the destructions of static variables
  // is in the reverse order of their constructions.
  //
  // Since static members are destructed in the reverse order
  // of their construction, having this call here guarantees that
  // the destructor of static PosixEnv will go first, then the
  // the singletons of ThreadLocalPtr.
  ThreadLocalPtr::InitSingletons();
  CompressionContextCache::InitSingleton();
  INIT_SYNC_POINT_SINGLETONS();
  // Avoid problems with accessing most members of Env::Default() during
  // static destruction.
  STATIC_AVOID_DESTRUCTION(PosixEnv, default_env);
  // This destructor must be called on exit
  static PosixEnv::JoinThreadsOnExit thread_joiner(default_env);
  return &default_env;
}

//
// Default Posix SystemClock
//
const std::shared_ptr<SystemClock>& SystemClock::Default() {
  STATIC_AVOID_DESTRUCTION(std::shared_ptr<SystemClock>, instance)
  (std::make_shared<PosixClock>());
  return instance;
}
}  // namespace ROCKSDB_NAMESPACE

#endif
