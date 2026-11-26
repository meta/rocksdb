//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

// Keeps a function out of line, for a rarely run path, so that its caller
// stays small.
#if defined(__GNUC__) || defined(__clang__)
#define ROCKSDB_NOINLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define ROCKSDB_NOINLINE __declspec(noinline)
#else
#define ROCKSDB_NOINLINE
#endif
