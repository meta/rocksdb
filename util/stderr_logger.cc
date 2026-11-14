//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <stdlib.h>

#include "util/stderr_logger.h"

#include "port/malloc.h"
#include "port/sys_time.h"

namespace ROCKSDB_NAMESPACE {
StderrLogger::~StderrLogger() {
  if (log_prefix != nullptr) {
    free((void*)log_prefix);
  }
}

void StderrLogger::Logv(const char* format, va_list ap) {
  const uint64_t thread_id = Env::Default()->GetThreadID();

  port::TimeVal now_tv;
  port::GetTimeOfDay(&now_tv, nullptr);
  const time_t seconds = now_tv.tv_sec;
  struct tm t;
  port::LocalTimeR(&seconds, &t);

  // The string we eventually log has three parts: the context (time, thread),
  // optional user-supplied prefix, and the actual log message (the "suffix").
  //
  // We compute their lengths so that we can allocate a buffer big enough to
  // print it. The context string (with the date and thread id) is really only
  // 44 bytes, but we allocate 50 to be safe.
  //
  //    ctx_len = 44         = ( 4+ 1+ 2+1+2+ 1+2+ 1+2+ 1+ 2+1+6+ 1+16+1)
  const char* ctx_prefix_fmt = "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx %s";
  size_t ctx_len = 50;

  va_list ap_copy;
  va_copy(ap_copy, ap);
  const size_t log_suffix_len = vsnprintf(nullptr, 0, format, ap_copy) + 1;
  va_end(ap_copy);

  // Allocate space for the context, log_prefix, and log itself
  // Extra byte for null termination
  size_t buf_len = ctx_len + log_prefix_len + log_suffix_len;
  std::unique_ptr<char[]> buf(new char[buf_len]);

  // If the logger was created without a prefix, the prefix is a nullptr
  const char* prefix = log_prefix == nullptr ? "" : log_prefix;

  // Write out the context and prefix string
  int written =
      snprintf(buf.get(), ctx_len + log_prefix_len, ctx_prefix_fmt,
               t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min,
               t.tm_sec, static_cast<int>(now_tv.tv_usec),
               static_cast<long long unsigned int>(thread_id), prefix);
  vsnprintf(buf.get() + written, log_suffix_len, format, ap);

  fprintf(stderr, "%s%c", buf.get(), '\n');
}
}  // namespace ROCKSDB_NAMESPACE
