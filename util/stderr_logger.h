//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdarg.h>
#include <stdio.h>

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// Prints logs to stderr for faster debugging
class StderrLogger : public Logger {
 public:
  explicit StderrLogger(const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL)
      : Logger(log_level), log_prefix(nullptr), log_prefix_len(0) {}
  explicit StderrLogger(const InfoLogLevel log_level, const std::string& prefix)
      : Logger(log_level),
        log_prefix(strdup(prefix.c_str())),
        log_prefix_len(strlen(log_prefix)) {}

  ~StderrLogger() override;

  // Brings overloaded Logv()s into scope so they're not hidden when we override
  // a subset of them.
  using Logger::Logv;

  void Logv(const char* format, va_list ap) override;

 private:
  // This prefix will be appended after the time/thread info of every log
  const char* log_prefix;
  // The length of the log_prefix
  size_t log_prefix_len;
};

}  // namespace ROCKSDB_NAMESPACE
