//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::FlushWALOptions.

#include <jni.h>

#include "include/org_rocksdb_FlushWALOptions.h"
#include "rocksdb/db.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_FlushWALOptions
 * Method:    newFlushWALOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_FlushWALOptions_newFlushWALOptions(JNIEnv*, jclass) {
  auto* flush_wal_opts = new ROCKSDB_NAMESPACE::FlushWALOptions();
  return GET_CPLUSPLUS_POINTER(flush_wal_opts);
}

/*
 * Class:     org_rocksdb_FlushWALOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_FlushWALOptions_setSync(JNIEnv*, jclass, jlong jhandle,
                                              jboolean jsync) {
  auto* flush_wal_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushWALOptions*>(jhandle);
  flush_wal_opts->sync = jsync;
}

/*
 * Class:     org_rocksdb_FlushWALOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_FlushWALOptions_sync(JNIEnv*, jclass, jlong jhandle) {
  auto* flush_wal_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushWALOptions*>(jhandle);
  return static_cast<jboolean>(flush_wal_opts->sync);
}

/*
 * Class:     org_rocksdb_FlushWALOptions
 * Method:    setRateLimiterPriority
 * Signature: (JB)V
 */
void Java_org_rocksdb_FlushWALOptions_setRateLimiterPriority(
    JNIEnv*, jclass, jlong jhandle, jbyte jrate_limiter_priority) {
  auto* flush_wal_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushWALOptions*>(jhandle);
  flush_wal_opts->rate_limiter_priority =
      ROCKSDB_NAMESPACE::IOPriorityJni::toCppIOPriority(jrate_limiter_priority);
}

/*
 * Class:     org_rocksdb_FlushWALOptions
 * Method:    rateLimiterPriority
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_FlushWALOptions_rateLimiterPriority(JNIEnv*, jclass,
                                                           jlong jhandle) {
  auto* flush_wal_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushWALOptions*>(jhandle);
  return ROCKSDB_NAMESPACE::IOPriorityJni::toJavaIOPriority(
      flush_wal_opts->rate_limiter_priority);
}

/*
 * Class:     org_rocksdb_FlushWALOptions
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
void Java_org_rocksdb_FlushWALOptions_disposeInternalJni(JNIEnv*, jclass,
                                                         jlong jhandle) {
  auto* flush_wal_opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::FlushWALOptions*>(jhandle);
  assert(flush_wal_opts != nullptr);
  delete flush_wal_opts;
}
