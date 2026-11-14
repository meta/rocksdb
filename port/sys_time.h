//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This file is a portable substitute for sys/time.h which does not exist on
// Windows

#pragma once

#include "rocksdb/rocksdb_namespace.h"

#if defined(OS_WIN) && (defined(_MSC_VER) || defined(__MINGW32__))

#include <time.h>

namespace ROCKSDB_NAMESPACE {

namespace port {

struct TimeVal {
  long tv_sec;
  long tv_usec;
};

void GetTimeOfDay(TimeVal* tv, struct timezone* tz);

inline struct tm* LocalTimeR(const time_t* timep, struct tm* result) {
  errno_t ret = localtime_s(result, timep);
  return (ret == 0) ? result : NULL;
}

}  // namespace port

}  // namespace ROCKSDB_NAMESPACE

#else
#include <sys/time.h>
#include <time.h>

namespace ROCKSDB_NAMESPACE {

namespace port {

using TimeVal = struct timeval;

inline void GetTimeOfDay(TimeVal* tv, struct timezone* tz) {
  gettimeofday(tv, tz);
}

struct tm* LocalTimeR(const time_t* timep, struct tm* result);

}  // namespace port

}  // namespace ROCKSDB_NAMESPACE

#endif
