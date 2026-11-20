//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "port/stack_trace.h"

#if !(defined(ROCKSDB_BACKTRACE) || defined(OS_MACOSX)) || defined(CYGWIN) || \
    defined(OS_WIN)

// noop

namespace ROCKSDB_NAMESPACE {
namespace port {
void InstallStackTraceHandler() {}
void PrintStack(int /*first_frames_to_skip*/) {}
void PrintAndFreeStack(void* /*callstack*/, int /*num_frames*/) {}
void* SaveStack(int* /*num_frames*/, int /*first_frames_to_skip*/) {
  return nullptr;
}
void RegisterCrashCallback(CrashCallback /*callback*/) {}
}  // namespace port
}  // namespace ROCKSDB_NAMESPACE

#else

#include <cxxabi.h>
#include <execinfo.h>
#include <pthread.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifdef OS_OPENBSD
#include <sys/sysctl.h>
#include <sys/wait.h>
#endif  // OS_OPENBSD
#ifdef OS_FREEBSD
#include <sys/sysctl.h>
#include <sys/wait.h>
#endif  // OS_FREEBSD
#ifdef OS_LINUX
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>
#if __GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 30)
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif  // GLIBC version
#endif  // OS_LINUX

#include <algorithm>
#include <atomic>

#include "port/lang.h"

namespace ROCKSDB_NAMESPACE::port {

namespace {

#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || \
    defined(OS_GNU_KFREEBSD)
const char* GetExecutableName() {
  static char name[1024];

#if defined(OS_FREEBSD)
  int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PATHNAME, -1};
  size_t namesz = sizeof(name);

  auto ret = sysctl(mib, 4, name, &namesz, nullptr, 0);
  if (-1 == ret) {
    return nullptr;
  } else {
    return name;
  }
#elif defined(OS_OPENBSD)
  int mib[4] = {CTL_KERN, KERN_PROC_ARGS, getpid(), KERN_PROC_ARGV};
  size_t namesz = sizeof(name);
  char* bin[namesz];

  auto ret = sysctl(mib, 4, bin, &namesz, nullptr, 0);
  if (-1 == ret) {
    return nullptr;
  } else {
    return bin[0];
  }
#else
  char link[1024];
  snprintf(link, sizeof(link), "/proc/%d/exe", getpid());
  auto read = readlink(link, name, sizeof(name) - 1);
  if (-1 == read) {
    return nullptr;
  } else {
    name[read] = 0;
    return name;
  }
#endif
}

void PrintStackTraceLine(const char* symbol, void* frame) {
  static const char* executable = GetExecutableName();
  if (symbol) {
    fprintf(stderr, "%s ", symbol);
  }
  if (executable) {
    // out source to addr2line, for the address translation
    const int kLineMax = 256;
    char cmd[kLineMax];
    snprintf(cmd, kLineMax, "addr2line %p -e %s -f -C 2>&1", frame, executable);
    auto f = popen(cmd, "r");
    if (f) {
      char line[kLineMax];
      while (fgets(line, sizeof(line), f)) {
        line[strlen(line) - 1] = 0;  // remove newline
        fprintf(stderr, "%s\t", line);
      }
      pclose(f);
    }
  } else {
    fprintf(stderr, " %p", frame);
  }

  fprintf(stderr, "\n");
}
#elif defined(OS_MACOSX)

void PrintStackTraceLine(const char* symbol, void* frame) {
  static int pid = getpid();
  // out source to atos, for the address translation
  const int kLineMax = 256;
  char cmd[kLineMax];
  snprintf(cmd, kLineMax, "xcrun atos %p -p %d  2>&1", frame, pid);
  auto f = popen(cmd, "r");
  if (f) {
    char line[kLineMax];
    while (fgets(line, sizeof(line), f)) {
      line[strlen(line) - 1] = 0;  // remove newline
      fprintf(stderr, "%s\t", line);
    }
    pclose(f);
  } else if (symbol) {
    fprintf(stderr, "%s ", symbol);
  }

  fprintf(stderr, "\n");
}

#endif

const char* GetLldbScriptSelectThread(long long tid) {
  // NOTE: called from a signal handler, so no heap allocation
  static char script[80];
  snprintf(script, sizeof(script),
           "script -l python -- lldb.process.SetSelectedThreadByID(%lld)", tid);
  return script;
}

}  // namespace

void PrintStack(void* frames[], int num_frames) {
  auto symbols = backtrace_symbols(frames, num_frames);

  for (int i = 0; i < num_frames; ++i) {
    fprintf(stderr, "#%-2d  ", i);
    PrintStackTraceLine((symbols != nullptr) ? symbols[i] : nullptr, frames[i]);
  }
  free(symbols);
}

void PrintStack(int first_frames_to_skip) {
  // Default to getting stack traces with GDB, at least on Linux where we
  // know how to attach to a particular thread.
  //
  // * Address space layout randomization (ASLR) interferes with getting good
  //   stack information from backtrace+addr2line. This is more likely to show
  //   up with LIB_MODE=shared builds (when kernel.randomize_va_space >= 1)
  //   but can also show up with LIB_MODE=static builds ((when
  //   kernel.randomize_va_space == 2).
  // * It doesn't appear easy to detect when ASLR is in use.
  // * With DEBUG_LEVEL < 2, backtrace() can skip frames that are not skipped
  //   in GDB.
  //
  // LLDB also available as an option
  bool lldb_stack_trace = getenv("ROCKSDB_LLDB_STACK") != nullptr;
#if defined(OS_LINUX)
  // Default true, override with ROCKSDB_BACKTRACE_STACK=1
  bool gdb_stack_trace =
      !lldb_stack_trace && getenv("ROCKSDB_BACKTRACE_STACK") == nullptr;
#else
  // Default false, override with ROCKSDB_GDB_STACK=1
  bool gdb_stack_trace = getenv("ROCKSDB_GDB_STACK") != nullptr;
#endif
  // Also support invoking interactive debugger on stack trace, with this
  // envvar set to non-empty
  char* debug_env = getenv("ROCKSDB_DEBUG");
  bool debug = debug_env != nullptr && strlen(debug_env) > 0;

  if (!debug && getenv("ROCKSDB_NO_STACK") != nullptr) {
    // Skip stack trace
    return;
  }

  if (lldb_stack_trace || gdb_stack_trace || debug) {
    // Allow ouside debugger to attach, even with Yama security restrictions
#ifdef PR_SET_PTRACER_ANY
    (void)prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
#endif
    // Try to invoke GDB, either for stack trace or debugging.
    long long attach_pid = getpid();
    // NOTE: we're in a signal handler, so no heap allocation
    char attach_pid_str[20];
    snprintf(attach_pid_str, sizeof(attach_pid_str), "%lld", attach_pid);

    // `gdb -p PID` seems to always attach to main thread, but `gdb -p TID`
    // seems to be able to attach to a particular thread in a process, which
    // makes sense as the main thread TID == PID of the process.
    // But I haven't found that gdb capability documented anywhere, so leave
    // a back door to attach to main thread.
    long long gdb_attach_id = attach_pid;
    // Save current thread id before fork
    long long attach_tid = 0;
#ifdef OS_LINUX
    attach_tid = gettid();
    if (getenv("ROCKSDB_DEBUG_USE_PID") == nullptr) {
      gdb_attach_id = attach_tid;
    }
#endif

    char gdb_attach_id_str[20];
    snprintf(gdb_attach_id_str, sizeof(gdb_attach_id_str), "%lld",
             gdb_attach_id);

    pid_t child_pid = fork();
    if (child_pid == 0) {
      // child process
      if (debug) {
        if (strcmp(debug_env, "lldb") == 0) {
          fprintf(stderr, "Invoking LLDB for debugging (ROCKSDB_DEBUG=%s)...\n",
                  debug_env);
          execlp(/*cmd in PATH*/ "lldb", /*arg0*/ "lldb", "-p", attach_pid_str,
                 /*"-Q",*/ "-o", GetLldbScriptSelectThread(attach_tid),
                 (char*)nullptr);
          return;
        } else {
          fprintf(stderr, "Invoking GDB for debugging (ROCKSDB_DEBUG=%s)...\n",
                  debug_env);
          execlp(/*cmd in PATH*/ "gdb", /*arg0*/ "gdb", "-p", gdb_attach_id_str,
                 (char*)nullptr);
          return;
        }
      } else {
        // Redirect child stdout to original stderr
        dup2(2, 1);
        // No child stdin (don't use pager)
        close(0);
        if (lldb_stack_trace) {
          fprintf(stderr, "Invoking LLDB for stack trace...\n");

          // Skip top ~4 frames here in PrintStack
          auto bt_in_lldb =
              "script -l python -- for f in lldb.thread.frames[4:]: print(f)";
          execlp(/*cmd in PATH*/ "lldb", /*arg0*/ "lldb", "-p", attach_pid_str,
                 "-b", "-Q", "-o", GetLldbScriptSelectThread(attach_tid), "-o",
                 bt_in_lldb, (char*)nullptr);
        } else {
          // gdb_stack_trace
          fprintf(stderr, "Invoking GDB for stack trace...\n");

          // Skip top ~4 frames here in PrintStack
          // See https://stackoverflow.com/q/40991943/454544
          auto bt_in_gdb =
              "frame apply level 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 "
              "21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 "
              "42 43 44 -q frame";
          // -n : Loading config files can apparently cause failures with the
          // other options here.
          // -batch : non-interactive; suppress banners as much as possible
          execlp(/*cmd in PATH*/ "gdb", /*arg0*/ "gdb", "-n", "-batch", "-p",
                 gdb_attach_id_str, "-ex", bt_in_gdb, (char*)nullptr);
        }
        return;
      }
    } else {
      // parent process; wait for child
      int wstatus;
      waitpid(child_pid, &wstatus, 0);
      if (WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == EXIT_SUCCESS) {
        // Good
        return;
      }
    }
    fprintf(stderr, "GDB failed; falling back on backtrace+addr2line...\n");
  }

  const int kMaxFrames = 100;
  void* frames[kMaxFrames];

  int num_frames = (int)backtrace(frames, kMaxFrames);
  PrintStack(&frames[first_frames_to_skip], num_frames - first_frames_to_skip);
}

void PrintAndFreeStack(void* callstack, int num_frames) {
  PrintStack(static_cast<void**>(callstack), num_frames);
  free(callstack);
}

void* SaveStack(int* num_frames, int first_frames_to_skip) {
  const int kMaxFrames = 100;
  void* frames[kMaxFrames];

  int count = (int)backtrace(frames, kMaxFrames);
  *num_frames = count - first_frames_to_skip;
  void* callstack = malloc(sizeof(void*) * *num_frames);
  memcpy(callstack, &frames[first_frames_to_skip], sizeof(void*) * *num_frames);
  return callstack;
}

static std::atomic<uint64_t> g_thread_handling_stack_trace{0};
static int g_recursion_count = 0;
static std::atomic<bool> g_at_exit_called{false};
static std::atomic<CrashCallback> g_crash_callback{nullptr};

static void StackTraceHandler(int sig) {
  fprintf(stderr, "Received signal %d (%s)\n", sig, strsignal(sig));
  // Crude recursive mutex with no signal-unsafe system calls, to avoid
  // re-entrance from multiple threads and avoid core dumping while trying
  // to print the stack trace.
  uint64_t tid = 0;
  {
    const auto ptid = pthread_self();
    // pthread_t is an opaque type
    memcpy(&tid, &ptid, std::min(sizeof(tid), sizeof(ptid)));
    // Essentially ensure non-zero
    ++tid;
  }
  for (;;) {
    uint64_t expected = 0;
    if (g_thread_handling_stack_trace.compare_exchange_strong(expected, tid)) {
      // Acquired mutex
      g_recursion_count = 0;
      break;
    }
    if (expected == tid) {
      ++g_recursion_count;
      fprintf(stderr, "Recursive call to stack trace handler (%d)\n",
              g_recursion_count);
      break;
    }
    // Sleep before trying again
    usleep(1000);
  }

  if (g_recursion_count > 2) {
    // Give up after too many recursions
    fprintf(stderr, "Too many recursive calls to stack trace handler (%d)\n",
            g_recursion_count);
  } else {
    if (g_at_exit_called.load(std::memory_order_acquire)) {
      fprintf(stderr, "In a race with process already exiting...\n");
    }

    // Invoke registered crash callback before printing stack trace
    auto callback = g_crash_callback.load(std::memory_order_acquire);
    if (callback) {
      callback();
    }

    // skip the top three signal handler related frames
    PrintStack(3);

    // Efforts to fix or suppress TSAN warnings "signal-unsafe call inside of
    // a signal" have failed, so just warn the user about them.
#ifdef __SANITIZE_THREAD__
    fprintf(stderr,
            "==> NOTE: any above warnings about \"signal-unsafe call\" are\n"
            "==> ignorable, as they are expected when generating a stack\n"
            "==> trace because of a signal under TSAN. Consider why the\n"
            "==> signal was generated to begin with, and the stack trace\n"
            "==> in the TSAN warning can be useful for that. (The stack\n"
            "==> trace printed by the signal handler is likely obscured\n"
            "==> by TSAN output.)\n");
#endif
  }

  // reset to default handler
  signal(sig, SIG_DFL);
  // re-signal to default handler (so we still get core dump if needed...)
  raise(sig);

  // release the mutex, in case this is somehow recoverable
  if (g_recursion_count > 0) {
    --g_recursion_count;
  } else {
    g_thread_handling_stack_trace.store(0, std::memory_order_release);
  }
}

static void AtExit() {
  // wait for stack trace handler to finish, if needed
  while (g_thread_handling_stack_trace.load(std::memory_order_acquire)) {
    usleep(1000);
  }
  g_at_exit_called.store(true, std::memory_order_release);
}

// Lightweight handler for graceful termination signals (SIGTERM, SIGINT).
// Prints the crash callback (e.g., ring buffer) but skips the
// expensive GDB/LLDB stack trace, since these are intentional terminations.
static void TerminationHandler(int sig) {
  char buf[64];
  int len = snprintf(buf, sizeof(buf), "Received signal %d (%s)\n", sig,
                     strsignal(sig));
  if (len > 0) {
    auto unused __attribute__((unused)) = write(STDOUT_FILENO, buf, len);
  }
  auto callback = g_crash_callback.load(std::memory_order_acquire);
  if (callback) {
    callback();
  }
  signal(sig, SIG_DFL);
  raise(sig);
}

void RegisterCrashCallback(CrashCallback callback) {
  g_crash_callback.store(callback, std::memory_order_release);
}

void InstallStackTraceHandler() {
  // just use the plain old signal as it's simple and sufficient
  // for this use case
  // Ignore SIGPIPE so that broken-pipe writes (e.g. to a closed stdout)
  // return EPIPE instead of killing the process.
  signal(SIGPIPE, SIG_IGN);
  // Crash signals -- invoke full stack trace + ring buffer
  signal(SIGILL, StackTraceHandler);
  signal(SIGSEGV, StackTraceHandler);
  signal(SIGBUS, StackTraceHandler);
  signal(SIGABRT, StackTraceHandler);
  signal(SIGFPE, StackTraceHandler);
  signal(SIGQUIT, StackTraceHandler);
  // Termination signals -- print ring buffer only, no stack trace
  signal(SIGTERM, TerminationHandler);
  signal(SIGINT, TerminationHandler);
  atexit(AtExit);
  // Allow ouside debugger to attach, even with Yama security restrictions.
  // This is needed even outside of PrintStack() so that external mechanisms
  // can dump stacks if they suspect that a test has hung.
#ifdef PR_SET_PTRACER_ANY
  (void)prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
#endif
}

}  // namespace ROCKSDB_NAMESPACE::port

#endif
