//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Must not be included from any .h files to avoid polluting the namespace
// with macros.

#pragma once

// Helper macros that include information about file name and line number
#define ROCKS_LOG_STRINGIFY(x) #x
#define ROCKS_LOG_TOSTRING(x) ROCKS_LOG_STRINGIFY(x)
#define ROCKS_LOG_PREPEND_FILE_LINE(FMT) \
  ("[%s:" ROCKS_LOG_TOSTRING(__LINE__) "] " FMT)

inline const char* RocksLogShorterFileName(const char* file) {
  // 18 is the length of "logging/logging.h".
  // If the name of this file changed, please change this number, too.
  return file + (sizeof(__FILE__) > 18 ? sizeof(__FILE__) - 18 : 0);
}

// Don't include file/line info in HEADER level
#define ROCKS_LOG_HEADER(LGR, FMT, ...) \
  ROCKSDB_NAMESPACE::Log(InfoLogLevel::HEADER_LEVEL, LGR, FMT, ##__VA_ARGS__)

#define ROCKS_LOG_AT_LEVEL(LGR, LVL, FMT, ...)                           \
  ROCKSDB_NAMESPACE::Log((LVL), (LGR), ROCKS_LOG_PREPEND_FILE_LINE(FMT), \
                         RocksLogShorterFileName(__FILE__), ##__VA_ARGS__)

#define ROCKS_LOG_DEBUG(LGR, FMT, ...) \
  ROCKS_LOG_AT_LEVEL((LGR), InfoLogLevel::DEBUG_LEVEL, FMT, ##__VA_ARGS__)

#define ROCKS_LOG_INFO(LGR, FMT, ...) \
  ROCKS_LOG_AT_LEVEL((LGR), InfoLogLevel::INFO_LEVEL, FMT, ##__VA_ARGS__)

#define ROCKS_LOG_WARN(LGR, FMT, ...) \
  ROCKS_LOG_AT_LEVEL((LGR), InfoLogLevel::WARN_LEVEL, FMT, ##__VA_ARGS__)

#define ROCKS_LOG_ERROR(LGR, FMT, ...) \
  ROCKS_LOG_AT_LEVEL((LGR), InfoLogLevel::ERROR_LEVEL, FMT, ##__VA_ARGS__)

#define ROCKS_LOG_FATAL(LGR, FMT, ...) \
  ROCKS_LOG_AT_LEVEL((LGR), InfoLogLevel::FATAL_LEVEL, FMT, ##__VA_ARGS__)

#define ROCKS_LOG_BUFFER(LOG_BUF, FMT, ...)                                 \
  ROCKSDB_NAMESPACE::LogToBuffer(LOG_BUF, ROCKS_LOG_PREPEND_FILE_LINE(FMT), \
                                 RocksLogShorterFileName(__FILE__),         \
                                 ##__VA_ARGS__)

#define ROCKS_LOG_BUFFER_MAX_SZ(LOG_BUF, MAX_LOG_SIZE, FMT, ...) \
  ROCKSDB_NAMESPACE::LogToBuffer(                                \
      LOG_BUF, MAX_LOG_SIZE, ROCKS_LOG_PREPEND_FILE_LINE(FMT),   \
      RocksLogShorterFileName(__FILE__), ##__VA_ARGS__)

#define ROCKS_LOG_DETAILS(LGR, FMT, ...) \
  ;  // due to overhead by default skip such lines
// ROCKS_LOG_DEBUG(LGR, FMT, ##__VA_ARGS__)
